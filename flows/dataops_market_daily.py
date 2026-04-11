"""Main Data Ops v1 market daily orchestration flow."""

from __future__ import annotations

import argparse
import json
import sys
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
from finance_data_ops.publish.status import publish_status_surfaces
from finance_data_ops.refresh.market_daily import RefreshRunResult, refresh_market_daily
from finance_data_ops.refresh.quotes_latest import refresh_latest_quotes
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import DataOpsSettings, load_settings
from finance_data_ops.validation.coverage import assess_symbol_coverage, build_symbol_coverage_rows
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness


def run_dataops_market_daily(
    *,
    symbols: list[str],
    start: str,
    end: str,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: MarketDataProvider | None = None,
    publisher: Publisher | None = None,
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

    coverage_rows = build_symbol_coverage_rows(
        required_symbols=normalized_symbols,
        prices_frame=cached_prices,
        quotes_frame=cached_quotes,
        as_of_date=end,
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
        dedupe_subset=["symbol", "as_of_date"],
    )

    status_rows = _build_asset_status_rows(
        as_of_date=end,
        prices_frame=cached_prices,
        quotes_frame=cached_quotes,
        market_stats_frame=market_stats,
        prices_run=prices_run,
        quotes_run=quotes_run,
    )
    run_rows = [
        _refresh_run_to_row(prices_run),
        _refresh_run_to_row(quotes_run),
        _flow_run_row(
            flow_run_id=flow_run_id,
            flow_started_at=flow_started_at,
            status=str(_overall_status(status_rows)),
            details={
                "symbols": normalized_symbols,
                "start": start,
                "end": end,
            },
        ),
    ]

    publisher_impl: Publisher
    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_supabase()
            publisher_impl = SupabaseRestPublisher(
                supabase_url=settings.supabase_url,
                service_role_key=settings.supabase_service_role_key,
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
                    flow_run_id=f"{flow_run_id}_publish_{failure['step']}",
                    flow_started_at=flow_started_at,
                    status=failure["classification"]["code"],
                    details={
                        "step": failure["step"],
                        "error": failure["error"],
                        "retryable": failure["classification"]["retryable"],
                    },
                )
            )
        status_rows.append(
            {
                "asset_name": "data_ops_publish_pipeline",
                "as_of_date": pd.Timestamp(end).date().isoformat(),
                "freshness_status": FreshnessState.FAILED_HARD,
                "last_observed_at": datetime.now(UTC).isoformat(),
                "details": {"failures": publish_failures},
                "updated_at": datetime.now(UTC).isoformat(),
            }
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

    overall_state = _overall_status(status_rows)
    hard_failure = bool(publish_failures) or overall_state in {
        FreshnessState.FAILED_HARD,
        FreshnessState.FAILED_RETRYING,
    }
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


def _refresh_run_to_row(result: RefreshRunResult) -> dict[str, Any]:
    failure_classification = (
        str(result.status)
        if str(result.status).strip().lower() in {"failed_hard", "failed_retrying"}
        else None
    )
    return {
        "run_id": result.run_id,
        "asset_name": result.asset_name,
        "status": result.status,
        "failure_classification": failure_classification,
        "started_at": result.started_at,
        "ended_at": result.ended_at,
        "symbols_requested": result.symbols_requested,
        "symbols_succeeded": result.symbols_succeeded,
        "symbols_failed": result.symbols_failed,
        "rows_written": int(result.rows_written),
        "error_messages": result.error_messages,
    }


def _flow_run_row(
    *,
    flow_run_id: str,
    flow_started_at: datetime,
    status: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    failure_classification = str(status) if str(status) in {"failed_hard", "failed_retrying"} else None
    return {
        "run_id": flow_run_id,
        "asset_name": "dataops_market_daily",
        "status": status,
        "failure_classification": failure_classification,
        "started_at": flow_started_at.isoformat(),
        "ended_at": datetime.now(UTC).isoformat(),
        "symbols_requested": details.get("symbols", []),
        "symbols_succeeded": [],
        "symbols_failed": [],
        "rows_written": 0,
        "error_messages": [json.dumps(details, default=str)],
    }


def _build_asset_status_rows(
    *,
    as_of_date: str,
    prices_frame: pd.DataFrame,
    quotes_frame: pd.DataFrame,
    market_stats_frame: pd.DataFrame,
    prices_run: RefreshRunResult,
    quotes_run: RefreshRunResult,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    prices_last = pd.to_datetime(prices_frame.get("date"), errors="coerce").max()
    quotes_last = pd.to_datetime(quotes_frame.get("quote_ts"), utc=True, errors="coerce").max()
    stats_last = pd.to_datetime(market_stats_frame.get("updated_at"), utc=True, errors="coerce").max()

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

    as_of = pd.Timestamp(as_of_date).date().isoformat()
    return [
        {
            "asset_name": "market_price_daily",
            "as_of_date": as_of,
            "freshness_status": str(price_state),
            "last_observed_at": None if pd.isna(prices_last) else pd.Timestamp(prices_last).isoformat(),
            "details": {"rows": int(len(prices_frame.index)), "run_id": prices_run.run_id},
            "updated_at": now.isoformat(),
        },
        {
            "asset_name": "market_quotes",
            "as_of_date": as_of,
            "freshness_status": str(quote_state),
            "last_observed_at": None if pd.isna(quotes_last) else pd.Timestamp(quotes_last).isoformat(),
            "details": {"rows": int(len(quotes_frame.index)), "run_id": quotes_run.run_id},
            "updated_at": now.isoformat(),
        },
        {
            "asset_name": "ticker_market_stats_snapshot",
            "as_of_date": as_of,
            "freshness_status": str(stats_state),
            "last_observed_at": None if pd.isna(stats_last) else pd.Timestamp(stats_last).isoformat(),
            "details": {"rows": int(len(market_stats_frame.index))},
            "updated_at": now.isoformat(),
        },
    ]


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
