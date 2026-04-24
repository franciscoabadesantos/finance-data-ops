"""Main Data Ops v2 earnings daily orchestration flow."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.derived.earnings_summary import compute_next_earnings
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.ops.incidents import classify_failure
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.publish.client import Publisher, RecordingPublisher, SupabaseRestPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.publish.status import fetch_symbol_data_coverage_rows, publish_status_surfaces
from finance_data_ops.refresh.earnings_daily import refresh_earnings_daily
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.coverage import (
    assess_symbol_coverage,
    build_symbol_coverage_rows,
    merge_symbol_coverage_rows_for_earnings,
)
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness


def run_dataops_earnings_daily(
    *,
    symbols: list[str],
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: EarningsDataProvider | None = None,
    publisher: Publisher | None = None,
    existing_symbol_coverage_rows: list[dict[str, object]] | None = None,
    max_attempts: int = 3,
    history_limit: int = 12,
    raise_on_failed_hard: bool = True,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_earnings_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)

    normalized_symbols = [str(v).strip().upper() for v in symbols if str(v).strip()]
    if not normalized_symbols:
        raise ValueError("symbols must contain at least one ticker.")

    provider_impl = provider or EarningsDataProvider()
    _, _, earnings_run = refresh_earnings_daily(
        symbols=normalized_symbols,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        max_attempts=max_attempts,
        history_limit=history_limit,
    )

    cached_earnings_events = read_parquet_table(
        "market_earnings_events",
        cache_root=settings.cache_root,
        required=False,
    )
    cached_earnings_history = read_parquet_table(
        "market_earnings_history",
        cache_root=settings.cache_root,
        required=False,
    )

    next_earnings = compute_next_earnings(cached_earnings_events)
    write_parquet_table(
        "mv_next_earnings",
        next_earnings,
        cache_root=settings.cache_root,
        mode="replace",
        dedupe_subset=["ticker"],
    )

    cached_prices = read_parquet_table("market_price_daily", cache_root=settings.cache_root, required=False)
    cached_quotes = read_parquet_table("market_quotes", cache_root=settings.cache_root, required=False)
    cached_fundamentals = read_parquet_table("market_fundamentals_v2", cache_root=settings.cache_root, required=False)
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
        fundamentals_frame=cached_fundamentals,
        earnings_events_frame=next_earnings,
    )
    coverage_rows = merge_symbol_coverage_rows_for_earnings(
        computed_rows=coverage_rows,
        existing_rows=existing_coverage_rows,
    )

    observed_symbols = set(_symbol_values(cached_earnings_events)).union(_symbol_values(cached_earnings_history))
    coverage_summary = assess_symbol_coverage(
        required_symbols=normalized_symbols,
        observed_symbols=list(observed_symbols),
    )

    status_rows = _build_asset_status_rows(
        earnings_events=cached_earnings_events,
        earnings_history=cached_earnings_history,
        next_earnings=next_earnings,
        refresh_run=earnings_run,
        flow_run_id=flow_run_id,
    )
    run_rows = [_refresh_run_to_row(earnings_run)]

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
        publish_results["earnings"] = _execute_publish_step(
            "earnings",
            lambda: publish_earnings_surfaces(
                publisher=publisher_impl,
                earnings_events=cached_earnings_events,
                earnings_history=cached_earnings_history,
                refresh_materialized_view=bool(publish_enabled),
            ),
            failures=publish_failures,
        )

    if publish_failures:
        for failure in publish_failures:
            run_rows.append(
                _flow_run_row(
                    run_id=f"{flow_run_id}_publish_{failure['step']}",
                    job_name=f"dataops_earnings_daily_publish_{failure['step']}",
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
            has_publish_failures=bool(publish_failures),
            asset_key="data_ops_publish_pipeline_earnings",
            reference_date=_latest_earnings_date(cached_earnings_events),
        )
    )

    symbols_succeeded = sorted({str(value).strip().upper() for value in earnings_run.symbols_succeeded if str(value).strip()})
    symbols_failed = sorted({str(value).strip().upper() for value in earnings_run.symbols_failed if str(value).strip()})

    overall_state = _overall_status(status_rows)
    orchestration_status = _orchestration_run_status(
        overall_state=overall_state,
        has_publish_failures=bool(publish_failures),
    )
    run_rows.append(
        _flow_run_row(
            run_id=flow_run_id,
            job_name="dataops_earnings_daily",
            source_type="orchestration",
            scope="symbol_universe",
            flow_started_at=flow_started_at,
            status=orchestration_status,
            context={
                "symbols": normalized_symbols,
                "symbols_succeeded": symbols_succeeded,
                "symbols_failed": symbols_failed,
            },
        )
    )

    publish_results["status"] = _execute_publish_step(
        "status",
        lambda: publish_status_surfaces(
            publisher=publisher_impl,
            data_source_runs=run_rows,
            data_asset_status=status_rows,
            symbol_data_coverage=coverage_rows,
        ),
        failures=publish_failures,
    )

    hard_failure = bool(publish_failures) or overall_state in {
        FreshnessState.FAILED_HARD,
        FreshnessState.FAILED_RETRYING,
    }
    if raise_on_failed_hard and hard_failure:
        payload = build_alert_payload(
            severity="error",
            message="Data Ops earnings daily flow ended unhealthy.",
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
        raise RuntimeError(f"Data Ops earnings daily unhealthy status: {failure_json}")

    return {
        "run_id": flow_run_id,
        "cache_root": str(settings.cache_root),
        "symbols": normalized_symbols,
        "refresh": {
            "earnings_daily": earnings_run.as_dict(),
        },
        "coverage": coverage_summary,
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "market_earnings_events": int(len(cached_earnings_events.index)),
            "market_earnings_history": int(len(cached_earnings_history.index)),
            "mv_next_earnings": int(len(next_earnings.index)),
        },
    }


def _build_asset_status_rows(
    *,
    earnings_events: pd.DataFrame,
    earnings_history: pd.DataFrame,
    next_earnings: pd.DataFrame,
    refresh_run: RefreshRunResult,
    flow_run_id: str,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    events_col = "fetched_at" if "fetched_at" in earnings_events.columns else "ingested_at"
    events_last = _frame_datetime_max(earnings_events, events_col, utc=True)
    history_last = _frame_datetime_max(earnings_history, "earnings_date")
    next_last = _frame_datetime_max(next_earnings, "updated_at", utc=True)

    events_state = classify_freshness(
        last_observed_at=events_last,
        now=now,
        fresh_within=timedelta(hours=30),
        tolerance=timedelta(hours=24),
        partial=str(refresh_run.status) == FreshnessState.PARTIAL,
        failure_state=refresh_run.status,
    )
    history_state = classify_freshness(
        last_observed_at=history_last,
        now=now,
        fresh_within=timedelta(days=180),
        tolerance=timedelta(days=120),
        failure_state="failed_hard" if earnings_history.empty else None,
    )
    next_state = classify_freshness(
        last_observed_at=next_last,
        now=now,
        fresh_within=timedelta(hours=30),
        tolerance=timedelta(hours=24),
        failure_state="failed_hard" if next_earnings.empty else None,
    )

    provider = _provider_from_frame(earnings_events, fallback="unknown")
    now_iso = now.isoformat()

    return [
        {
            "asset_key": "market_earnings_events",
            "asset_type": "earnings",
            "provider": provider,
            "last_success_at": _last_success_timestamp(events_last, events_state),
            "last_available_date": _date_or_none(_latest_earnings_date(earnings_events)),
            "freshness_status": str(events_state),
            "coverage_status": str(refresh_run.status),
            "reason": _asset_reason(
                rows_written=int(len(earnings_events.index)),
                run_id=refresh_run.run_id,
                errors=refresh_run.error_messages,
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "market_earnings_history",
            "asset_type": "earnings",
            "provider": _provider_from_frame(earnings_history, fallback=provider),
            "last_success_at": _last_success_timestamp(history_last, history_state),
            "last_available_date": _date_or_none(history_last),
            "freshness_status": str(history_state),
            "coverage_status": "fresh" if not earnings_history.empty else "failed_hard",
            "reason": _asset_reason(
                rows_written=int(len(earnings_history.index)),
                run_id="market_earnings_history_build",
                errors=["no_rows_found"] if earnings_history.empty else [],
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "mv_next_earnings",
            "asset_type": "derived",
            "provider": "data_ops",
            "last_success_at": _last_success_timestamp(next_last, next_state),
            "last_available_date": _date_or_none(_latest_earnings_date(next_earnings)),
            "freshness_status": str(next_state),
            "coverage_status": "fresh" if not next_earnings.empty else "failed_hard",
            "reason": _asset_reason(
                rows_written=int(len(next_earnings.index)),
                run_id=f"{flow_run_id}_mv_next_earnings",
                errors=["no_rows_to_materialize"] if next_earnings.empty else [],
            ),
            "updated_at": now_iso,
        },
    ]


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


def _build_publish_pipeline_status_row(
    *,
    run_id: str,
    has_publish_failures: bool,
    asset_key: str,
    reference_date: str | None,
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
        "asset_key": asset_key,
        "asset_type": "pipeline",
        "provider": "data_ops",
        "last_success_at": last_success_at,
        "last_available_date": reference_date,
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


def _orchestration_run_status(*, overall_state: str, has_publish_failures: bool) -> str:
    if has_publish_failures:
        return "failed"
    if str(overall_state).strip().lower() in {FreshnessState.FAILED_HARD, FreshnessState.FAILED_RETRYING}:
        return "failed"
    return "success"


def _provider_from_frame(frame: pd.DataFrame, *, fallback: str) -> str:
    column = "source" if "source" in frame.columns else ("provider" if "provider" in frame.columns else None)
    if column is None:
        return fallback
    providers = [str(value).strip() for value in frame[column].dropna().tolist() if str(value).strip()]
    return providers[0] if providers else fallback


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


def _latest_earnings_date(frame: pd.DataFrame) -> str | None:
    if frame.empty or "earnings_date" not in frame.columns:
        return None
    value = pd.to_datetime(frame["earnings_date"], errors="coerce").max()
    if pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _frame_datetime_max(frame: pd.DataFrame, column: str, *, utc: bool = False) -> pd.Timestamp | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_datetime(frame[column], utc=utc, errors="coerce")
    value = values.max() if hasattr(values, "max") else values
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value)


def _symbol_values(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    symbol_col = "ticker" if "ticker" in frame.columns else ("symbol" if "symbol" in frame.columns else None)
    if symbol_col is None:
        return []
    return [str(v).strip().upper() for v in frame[symbol_col].dropna().tolist() if str(v).strip()]


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Data Ops earnings daily flow.")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbol universe.")
    parser.add_argument("--cache-root", type=str, default=None, help="Override canonical local cache root.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Retry attempts for retryable failures.")
    parser.add_argument("--history-limit", type=int, default=12, help="Rows to request from provider earnings history.")
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

    summary = run_dataops_earnings_daily(
        symbols=symbols,
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts or settings.default_max_attempts),
        history_limit=int(args.history_limit),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
