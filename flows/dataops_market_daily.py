"""Main Data Ops v1 market daily orchestration flow."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.derived.market_stats import compute_ticker_market_stats
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.ops.incidents import classify_failure
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.publish.client import Publisher, RecordingPublisher, SupabaseRestPublisher
from finance_data_ops.publish.prices import publish_prices_surfaces
from finance_data_ops.publish.product_metrics import publish_product_metrics
from finance_data_ops.publish.status import fetch_symbol_data_coverage_rows, publish_status_surfaces
from finance_data_ops.refresh.market_daily import RefreshRunResult, refresh_market_daily
from finance_data_ops.refresh.quotes_latest import refresh_latest_quotes
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import DataOpsSettings, load_settings
from finance_data_ops.validation.coverage import (
    assess_symbol_coverage,
    build_symbol_coverage_rows,
    merge_symbol_coverage_rows_for_market,
)
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness

_PROMOTABLE_STATUSES = frozenset({"validated_market_only", "validated_full"})


def run_dataops_market_daily(
    *,
    symbols: list[str],
    start: str,
    end: str,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: MarketDataProvider | None = None,
    publisher: Publisher | None = None,
    existing_symbol_coverage_rows: list[dict[str, object]] | None = None,
    max_attempts: int = 3,
    raise_on_failed_hard: bool = True,
    symbol_batch_size: int | None = None,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_market_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)

    normalized_symbols = [str(v).strip().upper() for v in symbols if str(v).strip()]
    if not normalized_symbols:
        raise ValueError("symbols must contain at least one ticker.")

    provider_impl = provider or MarketDataProvider()
    batch_size = int(symbol_batch_size if symbol_batch_size is not None else settings.symbol_batch_size)

    prices_frame, prices_run = refresh_market_daily(
        symbols=normalized_symbols,
        start=start,
        end=end,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        max_attempts=max_attempts,
    )
    quotes_frame, quotes_run = refresh_latest_quotes(
        symbols=normalized_symbols,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        symbol_batch_size=batch_size,
    )

    cached_prices = read_parquet_table("market_price_daily", cache_root=settings.cache_root, required=False)
    cached_quotes = read_parquet_table("market_quotes", cache_root=settings.cache_root, required=False)
    cached_fundamentals = read_parquet_table(
        "market_fundamentals_v2",
        cache_root=settings.cache_root,
        required=False,
    )
    cached_earnings_events = read_parquet_table(
        "market_earnings_events",
        cache_root=settings.cache_root,
        required=False,
    )
    existing_coverage_rows = list(existing_symbol_coverage_rows or [])
    if publish_enabled and publisher is None and not existing_coverage_rows:
        existing_coverage_rows = _load_existing_symbol_coverage_rows(
            supabase_url=settings.supabase_url,
            service_role_key=settings.supabase_secret_key,
            symbols=normalized_symbols,
        )

    coverage_rows = build_symbol_coverage_rows(
        required_symbols=normalized_symbols,
        prices_frame=cached_prices,
        quotes_frame=cached_quotes,
        fundamentals_frame=(cached_fundamentals if not cached_fundamentals.empty else None),
        earnings_events_frame=(cached_earnings_events if not cached_earnings_events.empty else None),
    )
    coverage_rows = merge_symbol_coverage_rows_for_market(
        computed_rows=coverage_rows,
        existing_rows=existing_coverage_rows,
    )
    coverage_summary = assess_symbol_coverage(
        required_symbols=normalized_symbols,
        observed_symbols=list(cached_prices.get("symbol", pd.Series(dtype=str)).astype(str).tolist()),
    )

    market_stats = compute_ticker_market_stats(cached_prices, as_of_date=end)
    write_parquet_table(
        "ticker_market_stats_snapshot",
        market_stats,
        cache_root=settings.cache_root,
        mode="replace",
        dedupe_subset=["ticker"],
    )

    status_rows = _build_asset_status_rows(
        prices_frame=cached_prices,
        quotes_frame=cached_quotes,
        market_stats_frame=market_stats,
        prices_run=prices_run,
        quotes_run=quotes_run,
    )
    run_rows = [
        _refresh_run_to_row(prices_run),
        _refresh_run_to_row(quotes_run),
    ]

    publisher_impl: Publisher
    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_supabase()
            publisher_impl = SupabaseRestPublisher(
                supabase_url=settings.supabase_url,
                service_role_key=settings.supabase_secret_key,
            )
    else:
        publisher_impl = publisher or RecordingPublisher()

    publish_failures: list[dict[str, Any]] = []
    publish_results: dict[str, Any] = {}
    if publish_enabled or isinstance(publisher_impl, RecordingPublisher):
        prices_result = _execute_publish_step(
            "prices",
            lambda: publish_prices_surfaces(
                publisher=publisher_impl,
                market_price_daily=cached_prices,
                market_quotes=cached_quotes,
                refresh_materialized_view=bool(publish_enabled),
            ),
            failures=publish_failures,
        )
        publish_results["prices"] = prices_result

        product_result = _execute_publish_step(
            "product_metrics",
            lambda: publish_product_metrics(
                publisher=publisher_impl,
                market_stats_snapshot=market_stats,
            ),
            failures=publish_failures,
        )
        publish_results["product_metrics"] = product_result

    if publish_failures:
        for failure in publish_failures:
            run_rows.append(
                _flow_run_row(
                    run_id=f"{flow_run_id}_publish_{failure['step']}",
                    job_name=f"dataops_market_daily_publish_{failure['step']}",
                    source_type="publish",
                    scope=str(failure["step"]),
                    flow_started_at=flow_started_at,
                    status=failure["classification"]["code"],
                    context={
                        "step": failure["step"],
                        "error": failure["error"],
                        "retryable": failure["classification"]["retryable"],
                    },
                )
            )
    status_rows.append(
        _build_publish_pipeline_status_row(
            run_id=flow_run_id,
            as_of_date=end,
            has_publish_failures=bool(publish_failures),
        )
    )

    symbols_succeeded = sorted(
        {
            str(row.get("ticker", "")).strip().upper()
            for row in coverage_rows
            if bool(row.get("market_data_available"))
        }
    )
    symbols_failed = [symbol for symbol in normalized_symbols if symbol not in set(symbols_succeeded)]

    overall_state = _overall_status(status_rows)
    orchestration_status = _orchestration_run_status(
        overall_state=overall_state,
        has_publish_failures=bool(publish_failures),
    )
    run_rows.append(
        _flow_run_row(
            run_id=flow_run_id,
            job_name="dataops_market_daily",
            source_type="orchestration",
            scope=f"{start}:{end}",
            flow_started_at=flow_started_at,
            status=orchestration_status,
            context={
                "symbols": normalized_symbols,
                "symbols_succeeded": symbols_succeeded,
                "symbols_failed": symbols_failed,
                "start": start,
                "end": end,
            },
        )
    )

    status_result = _execute_publish_step(
        "status",
        lambda: publish_status_surfaces(
            publisher=publisher_impl,
            data_source_runs=run_rows,
            data_asset_status=status_rows,
            symbol_data_coverage=coverage_rows,
        ),
        failures=publish_failures,
    )
    publish_results["status"] = status_result

    hard_failure = bool(publish_failures) or overall_state in {
        FreshnessState.FAILED_HARD,
        FreshnessState.FAILED_RETRYING,
    }
    signal_jobs_result = _dispatch_ticker_signal_jobs_after_market_publish(
        publish_enabled=bool(publish_enabled),
        hard_failure=hard_failure,
        as_of_date=datetime.now(UTC).date().isoformat(),
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_secret_key,
    )
    publish_results["ticker_signal_v1_jobs"] = signal_jobs_result
    if str(signal_jobs_result.get("status") or "").strip().lower() == "failed":
        raise RuntimeError(str(signal_jobs_result.get("error") or "ticker_signal_v1 dispatch failed"))

    if raise_on_failed_hard and hard_failure:
        payload = build_alert_payload(
            severity="error",
            message="Data Ops market daily flow ended unhealthy.",
            run_id=flow_run_id,
            context={
                "overall_state": overall_state,
                "publish_failures": publish_failures,
                "coverage": coverage_summary,
                "status_rows": status_rows,
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        failure_json = json.dumps(
            {"overall_state": overall_state, "publish_failures": publish_failures},
            default=str,
        )
        raise RuntimeError(f"Data Ops market daily unhealthy status: {failure_json}")

    return {
        "run_id": flow_run_id,
        "run_date": date.today().isoformat(),
        "cache_root": str(settings.cache_root),
        "symbols": normalized_symbols,
        "refresh": {
            "market_daily": prices_run.as_dict(),
            "quotes_latest": quotes_run.as_dict(),
        },
        "coverage": coverage_summary,
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "market_price_daily": int(len(cached_prices.index)),
            "market_quotes": int(len(cached_quotes.index)),
            "ticker_market_stats_snapshot": int(len(market_stats.index)),
        },
    }


def _execute_publish_step(
    step_name: str,
    operation: Callable[[], dict[str, Any]],
    *,
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        return operation()
    except Exception as exc:
        classification = classify_failure(exc)
        failures.append(
            {
                "step": step_name,
                "error": repr(exc),
                "classification": {
                    "code": classification.code,
                    "retryable": classification.retryable,
                    "message": classification.message,
                },
            }
        )
        return {
            "status": classification.code,
            "error": repr(exc),
            "retryable": classification.retryable,
            "message": classification.message,
        }


def _count_items(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        return 0 if not value.strip() else 1
    try:
        return len(value)
    except TypeError:
        return 0


def _refresh_run_to_row(result: RefreshRunResult) -> dict[str, Any]:
    failure_classification = (
        str(result.status)
        if str(result.status).strip().lower() in {"failed_hard", "failed_retrying"}
        else None
    )
    error_messages = [str(value) for value in result.error_messages if str(value).strip()]
    error_message = error_messages[0] if error_messages else None
    return {
        "run_id": result.run_id,
        "job_name": result.asset_name,
        "source_type": "refresh",
        "scope": "symbol_universe",
        "status": result.status,
        "started_at": result.started_at,
        "finished_at": result.ended_at,
        "rows_written": int(result.rows_written),
        "error_class": failure_classification,
        "error_message": error_message,
        "failure_classification": failure_classification,
        "symbols_requested": _count_items(result.symbols_requested),
        "symbols_succeeded": _count_items(result.symbols_succeeded),
        "symbols_failed": _count_items(result.symbols_failed),
        "error_messages": error_messages,
        "created_at": datetime.now(UTC).isoformat(),
    }


def _flow_run_row(
    *,
    run_id: str,
    job_name: str,
    source_type: str,
    scope: str,
    flow_started_at: datetime,
    status: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    normalized_status = str(status).strip().lower()
    failure_classification = (
        normalized_status if normalized_status in {"failed_hard", "failed_retrying", "failed"} else None
    )
    details = json.dumps(context, default=str)
    return {
        "run_id": str(run_id),
        "job_name": job_name,
        "source_type": source_type,
        "scope": scope,
        "status": status,
        "started_at": flow_started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "rows_written": int(context.get("rows_written", 0) or 0),
        "error_class": failure_classification,
        "error_message": details if failure_classification else None,
        "failure_classification": failure_classification,
        "symbols_requested": _count_items(context.get("symbols")),
        "symbols_succeeded": _count_items(context.get("symbols_succeeded")),
        "symbols_failed": _count_items(context.get("symbols_failed")),
        "error_messages": [details],
        "created_at": datetime.now(UTC).isoformat(),
    }


def _build_asset_status_rows(
    *,
    prices_frame: pd.DataFrame,
    quotes_frame: pd.DataFrame,
    market_stats_frame: pd.DataFrame,
    prices_run: RefreshRunResult,
    quotes_run: RefreshRunResult,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    prices_last = _frame_datetime_max(prices_frame, "date")
    quote_time_column = "quote_ts" if "quote_ts" in quotes_frame.columns else "fetched_at"
    quotes_last = _frame_datetime_max(quotes_frame, quote_time_column, utc=True)
    stats_last = _frame_datetime_max(market_stats_frame, "updated_at", utc=True)

    price_state = classify_freshness(
        last_observed_at=prices_last,
        now=now,
        fresh_within=timedelta(days=2),
        tolerance=timedelta(days=2),
        partial=str(prices_run.status) == FreshnessState.PARTIAL,
        failure_state=prices_run.status,
    )
    quote_state = classify_freshness(
        last_observed_at=quotes_last,
        now=now,
        fresh_within=timedelta(hours=26),
        tolerance=timedelta(hours=24),
        partial=str(quotes_run.status) == FreshnessState.PARTIAL,
        failure_state=quotes_run.status,
    )
    stats_state = classify_freshness(
        last_observed_at=stats_last,
        now=now,
        fresh_within=timedelta(hours=6),
        tolerance=timedelta(hours=12),
        failure_state="failed_hard" if market_stats_frame.empty else None,
    )

    prices_provider = _provider_from_frame(prices_frame, fallback="unknown")
    quotes_provider = _provider_from_frame(quotes_frame, fallback="unknown")
    now_iso = now.isoformat()
    return [
        {
            "asset_key": "market_price_daily",
            "asset_type": "market_data",
            "provider": prices_provider,
            "last_success_at": _last_success_timestamp(prices_last, price_state),
            "last_available_date": _date_or_none(prices_last),
            "freshness_status": str(price_state),
            "coverage_status": str(prices_run.status),
            "reason": _asset_reason(
                rows_written=int(len(prices_frame.index)),
                run_id=prices_run.run_id,
                errors=prices_run.error_messages,
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "market_quotes",
            "asset_type": "market_data",
            "provider": quotes_provider,
            "last_success_at": _last_success_timestamp(quotes_last, quote_state),
            "last_available_date": _date_or_none(quotes_last),
            "freshness_status": str(quote_state),
            "coverage_status": str(quotes_run.status),
            "reason": _asset_reason(
                rows_written=int(len(quotes_frame.index)),
                run_id=quotes_run.run_id,
                errors=quotes_run.error_messages,
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "ticker_market_stats_snapshot",
            "asset_type": "derived",
            "provider": "data_ops",
            "last_success_at": _last_success_timestamp(stats_last, stats_state),
            "last_available_date": _date_or_none(stats_last),
            "freshness_status": str(stats_state),
            "coverage_status": "fresh" if not market_stats_frame.empty else "failed_hard",
            "reason": _asset_reason(
                rows_written=int(len(market_stats_frame.index)),
                run_id="ticker_market_stats_snapshot_build",
                errors=["no_rows_computed"] if market_stats_frame.empty else [],
            ),
            "updated_at": now_iso,
        },
    ]


def _provider_from_frame(frame: pd.DataFrame, *, fallback: str) -> str:
    if "provider" not in frame.columns:
        return fallback
    providers = [str(value).strip() for value in frame["provider"].dropna().tolist() if str(value).strip()]
    return providers[0] if providers else fallback


def _frame_datetime_max(frame: pd.DataFrame, column: str, *, utc: bool = False) -> pd.Timestamp | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], utc=utc, errors="coerce")
    value = values.max() if hasattr(values, "max") else values
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value)


def _date_or_none(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _last_success_timestamp(value: Any, freshness_state: str) -> str | None:
    if str(freshness_state).strip().lower() in {"failed_hard", "failed_retrying"}:
        return None
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _asset_reason(*, rows_written: int, run_id: str, errors: list[str]) -> str:
    if errors:
        return f"rows={rows_written}; run_id={run_id}; errors={'; '.join(errors)}"
    return f"rows={rows_written}; run_id={run_id}"


def _orchestration_run_status(*, overall_state: str, has_publish_failures: bool) -> str:
    if has_publish_failures:
        return "failed"
    if str(overall_state).strip().lower() in {FreshnessState.FAILED_HARD, FreshnessState.FAILED_RETRYING}:
        return "failed"
    return "success"


def _build_publish_pipeline_status_row(
    *,
    run_id: str,
    as_of_date: str,
    has_publish_failures: bool,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    if has_publish_failures:
        freshness_status = FreshnessState.FAILED_HARD
        coverage_status = FreshnessState.FAILED_HARD
        reason = f"publish_failure; run_id={run_id}"
        last_success_at = None
    else:
        freshness_status = FreshnessState.FRESH
        coverage_status = FreshnessState.FRESH
        reason = f"success; run_id={run_id}"
        last_success_at = now.isoformat()

    return {
        "asset_key": "data_ops_publish_pipeline",
        "asset_type": "pipeline",
        "provider": "data_ops",
        "last_success_at": last_success_at,
        "last_available_date": pd.Timestamp(as_of_date).date().isoformat(),
        "freshness_status": freshness_status,
        "coverage_status": coverage_status,
        "reason": reason,
        "updated_at": now.isoformat(),
    }


def _overall_status(status_rows: list[dict[str, Any]]) -> str:
    statuses = {str(row.get("freshness_status", "")).strip().lower() for row in status_rows}
    if FreshnessState.FAILED_HARD in statuses:
        return FreshnessState.FAILED_HARD
    if FreshnessState.FAILED_RETRYING in statuses:
        return FreshnessState.FAILED_RETRYING
    if FreshnessState.PARTIAL in statuses:
        return FreshnessState.PARTIAL
    if FreshnessState.STALE_WITHIN_TOLERANCE in statuses:
        return FreshnessState.STALE_WITHIN_TOLERANCE
    if FreshnessState.DELAYED_EXPECTED in statuses:
        return FreshnessState.DELAYED_EXPECTED
    if FreshnessState.UNKNOWN in statuses:
        return FreshnessState.UNKNOWN
    return FreshnessState.FRESH


def _parse_symbols(raw: str) -> list[str]:
    return [str(v).strip().upper() for v in str(raw).split(",") if str(v).strip()]


def _load_existing_symbol_coverage_rows(
    *,
    supabase_url: str,
    service_role_key: str,
    symbols: list[str],
) -> list[dict[str, object]]:
    if not str(supabase_url).strip() or not str(service_role_key).strip():
        return []
    try:
        rows = fetch_symbol_data_coverage_rows(
            supabase_url=supabase_url,
            service_role_key=service_role_key,
            tickers=symbols,
        )
        return [row for row in rows if isinstance(row, dict)]
    except Exception:
        return []


def _dispatch_ticker_signal_jobs_after_market_publish(
    *,
    publish_enabled: bool,
    hard_failure: bool,
    as_of_date: str,
    supabase_url: str,
    service_role_key: str,
) -> dict[str, Any]:
    if not publish_enabled:
        return {"status": "skipped", "reason": "publish_disabled"}
    if hard_failure:
        return {"status": "skipped", "reason": "market_publish_unhealthy"}

    backend_base_url = str(
        os.environ.get("BACKEND_BASE_URL")
        or os.environ.get("FINANCE_BACKEND_URL")
        or ""
    ).strip()
    backend_service_token = str(
        os.environ.get("BACKEND_SERVICE_TOKEN")
        or os.environ.get("FINANCE_BACKEND_SERVICE_TOKEN")
        or ""
    ).strip()
    if not backend_base_url or not backend_service_token:
        return {"status": "skipped", "reason": "backend_env_missing"}
    if not str(supabase_url).strip() or not str(service_role_key).strip():
        return {"status": "skipped", "reason": "supabase_env_missing"}

    registry_rows = _fetch_promotable_registry_rows(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
    )
    if not registry_rows:
        return {"status": "ok", "requested": 0, "submitted": 0, "skipped_existing": 0}

    completed_today = _fetch_completed_signal_jobs_for_date(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
        as_of_date=as_of_date,
    )
    submitted_jobs: list[dict[str, Any]] = []
    skipped_jobs: list[dict[str, Any]] = []
    for row in registry_rows:
        ticker = str(row.get("ticker") or "").strip().upper()
        region = str(row.get("region") or "").strip().lower() or None
        exchange = (str(row.get("exchange") or "").strip().upper() or None)
        dedupe_key = (ticker, region, exchange)
        if dedupe_key in completed_today:
            skipped_jobs.append(
                {
                    "ticker": ticker,
                    "region": region,
                    "exchange": exchange,
                    "reason": "completed_today",
                }
            )
            continue
        response = _submit_ticker_signal_job(
            backend_base_url=backend_base_url,
            backend_service_token=backend_service_token,
            ticker=ticker,
            region=region,
            exchange=exchange,
        )
        completed_today.add(dedupe_key)
        submitted_jobs.append(
            {
                "ticker": ticker,
                "region": region,
                "exchange": exchange,
                "job_id": response.get("job_id"),
                "status": response.get("status"),
            }
        )

    return {
        "status": "ok",
        "requested": len(registry_rows),
        "submitted": len(submitted_jobs),
        "skipped_existing": len(skipped_jobs),
        "submitted_jobs": submitted_jobs,
        "skipped_jobs": skipped_jobs,
    }


def _fetch_promotable_registry_rows(
    *,
    supabase_url: str,
    service_role_key: str,
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    params = {
        "select": "normalized_symbol,region,exchange,status,promotion_status",
        "status": "eq.active",
        "promotion_status": "in.(validated_market_only,validated_full)",
        "normalized_symbol": "not.is.null",
    }
    url = f"{str(supabase_url).strip().rstrip('/')}/rest/v1/ticker_registry?{urllib.parse.urlencode(params)}"
    payload = _supabase_get_json(
        url=url,
        service_role_key=service_role_key,
        timeout_seconds=timeout_seconds,
    )
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None, str | None]] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("normalized_symbol") or "").strip().upper()
        region = str(item.get("region") or "").strip().lower() or None
        exchange = str(item.get("exchange") or "").strip().upper() or None
        if not ticker or ticker in {"NAN", "NONE", "NULL"}:
            continue
        if str(item.get("status") or "").strip().lower() != "active":
            continue
        if str(item.get("promotion_status") or "").strip().lower() not in _PROMOTABLE_STATUSES:
            continue
        dedupe_key = (ticker, region, exchange)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append({"ticker": ticker, "region": region, "exchange": exchange})
    rows.sort(key=lambda row: (str(row.get("region") or ""), str(row.get("exchange") or ""), str(row.get("ticker") or "")))
    return rows


def _fetch_completed_signal_jobs_for_date(
    *,
    supabase_url: str,
    service_role_key: str,
    as_of_date: str,
    timeout_seconds: int = 30,
) -> set[tuple[str, str | None, str | None]]:
    start_date = pd.Timestamp(as_of_date).date()
    end_date = start_date + timedelta(days=1)
    params = [
        ("select", "ticker,region,exchange"),
        ("analysis_type", "eq.ticker_signal_v1"),
        ("status", "eq.completed"),
        ("finished_at", f"gte.{start_date.isoformat()}T00:00:00+00:00"),
        ("finished_at", f"lt.{end_date.isoformat()}T00:00:00+00:00"),
        ("limit", "200000"),
    ]
    url = f"{str(supabase_url).strip().rstrip('/')}/rest/v1/analysis_jobs?{urllib.parse.urlencode(params)}"
    payload = _supabase_get_json(
        url=url,
        service_role_key=service_role_key,
        timeout_seconds=timeout_seconds,
    )
    completed: set[tuple[str, str | None, str | None]] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        region = str(item.get("region") or "").strip().lower() or None
        exchange = str(item.get("exchange") or "").strip().upper() or None
        if not ticker:
            continue
        completed.add((ticker, region, exchange))
    return completed


def _supabase_get_json(
    *,
    url: str,
    service_role_key: str,
    timeout_seconds: int,
) -> list[Any]:
    key = str(service_role_key).strip()
    request = urllib.request.Request(
        url=url,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw) if raw else []
    if isinstance(parsed, list):
        return parsed
    return []


def _submit_ticker_signal_job(
    *,
    backend_base_url: str,
    backend_service_token: str,
    ticker: str,
    region: str | None,
    exchange: str | None,
    timeout_seconds: int = 60,
) -> dict[str, Any]:
    payload = {
        "ticker": str(ticker).strip().upper(),
        "region": region,
        "exchange": exchange,
        "analysis_type": "ticker_signal_v1",
    }
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=f"{str(backend_base_url).strip().rstrip('/')}/analyst/jobs",
        data=body,
        headers={
            "Authorization": f"Bearer {str(backend_service_token).strip()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            if isinstance(parsed, dict):
                return parsed
            return {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Failed to submit ticker_signal_v1 job for {ticker}/{region or 'default'}"
            f"{('/' + exchange) if exchange else ''}: HTTP {exc.code} {detail}"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Data Ops market daily flow.")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbol universe.")
    parser.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD.")
    parser.add_argument("--cache-root", type=str, default=None, help="Override canonical local cache root.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Retry attempts for retryable failures.")
    parser.add_argument(
        "--symbol-batch-size",
        type=int,
        default=None,
        help="Symbol batch size used by quote refresh calls.",
    )
    parser.add_argument("--no-publish", action="store_true", help="Skip Supabase publishing (local dry-run).")
    parser.add_argument(
        "--allow-unhealthy",
        action="store_true",
        help="Do not raise when any asset ends in failed_hard/failed_retrying.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)

    symbols = _parse_symbols(args.symbols) if args.symbols else list(settings.default_symbols)
    if not symbols:
        raise ValueError("No symbols configured. Set --symbols or DATA_OPS_SYMBOLS.")

    end_ts = pd.Timestamp(args.end).date() if args.end else datetime.now(UTC).date()
    if args.start:
        start_ts = pd.Timestamp(args.start).date()
    else:
        start_ts = end_ts - timedelta(days=int(settings.default_lookback_days))

    summary = run_dataops_market_daily(
        symbols=symbols,
        start=start_ts.isoformat(),
        end=end_ts.isoformat(),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        symbol_batch_size=args.symbol_batch_size,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
