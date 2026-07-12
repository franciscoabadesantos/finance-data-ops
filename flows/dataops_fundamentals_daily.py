"""Main Data Ops v2 fundamentals daily orchestration flow."""

from __future__ import annotations

import argparse
import json
import logging
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

from finance_data_ops.derived.fundamentals_summary import compute_fundamentals_summary
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.ops.incidents import classify_failure
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.publish.client import Publisher, RecordingPublisher, PostgresPublisher
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces
from finance_data_ops.publish.status import fetch_symbol_data_coverage_rows, publish_status_surfaces
from finance_data_ops.refresh.fundamentals_daily import refresh_fundamentals_daily
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.theme_etfs.holdings import fetch_theme_etf_holdings, write_theme_etf_outputs
from finance_data_ops.validation.coverage import (
    assess_symbol_coverage,
    build_symbol_coverage_rows,
    merge_symbol_coverage_rows_for_fundamentals,
)
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness

LOGGER = logging.getLogger("finance_data_ops.flows.dataops_fundamentals_daily")
ThemeETFRefreshFn = Callable[[], tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]]


def run_dataops_fundamentals_daily(
    *,
    symbols: list[str],
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: FundamentalsDataProvider | None = None,
    publisher: Publisher | None = None,
    existing_symbol_coverage_rows: list[dict[str, object]] | None = None,
    max_attempts: int = 3,
    raise_on_failed_hard: bool = True,
    refresh_theme_etfs: bool = False,
    theme_etf_refresh_fn: ThemeETFRefreshFn | None = None,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_fundamentals_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)

    normalized_symbols = [str(v).strip().upper() for v in symbols if str(v).strip()]
    if not normalized_symbols:
        raise ValueError("symbols must contain at least one ticker.")

    provider_impl = provider or FundamentalsDataProvider()
    _, fundamentals_run = refresh_fundamentals_daily(
        symbols=normalized_symbols,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        max_attempts=max_attempts,
    )
    theme_etf_refresh = _refresh_theme_etf_holdings(
        cache_root=str(settings.cache_root),
        enabled=bool(refresh_theme_etfs),
        refresh_fn=theme_etf_refresh_fn,
    )

    cached_fundamentals = read_parquet_table("source_cache.fundamentals", cache_root=settings.cache_root, required=False)
    cached_ticker_profile = read_parquet_table("ticker_profile", cache_root=settings.cache_root, required=False)
    cached_etf_holdings = read_parquet_table("etf_holdings", cache_root=settings.cache_root, required=False)
    cached_etf_holding_onboarding_identity = read_parquet_table(
        "etf_holding_onboarding_identity",
        cache_root=settings.cache_root,
        required=False,
    )
    cached_etf_themes = read_parquet_table("etf_themes", cache_root=settings.cache_root, required=False)
    cached_etf_sector_weights = read_parquet_table(
        "etf_sector_weights",
        cache_root=settings.cache_root,
        required=False,
    )
    fundamentals_summary = compute_fundamentals_summary(cached_fundamentals)

    cached_prices = read_parquet_table("source_cache.market_price_daily", cache_root=settings.cache_root, required=False)
    cached_quotes = read_parquet_table("latest_quotes", cache_root=settings.cache_root, required=False)
    cached_earnings_events = read_parquet_table("earnings_events", cache_root=settings.cache_root, required=False)

    existing_coverage_rows = list(existing_symbol_coverage_rows or [])
    if publish_enabled and publisher is None and not existing_coverage_rows:
        existing_coverage_rows = _load_existing_symbol_coverage_rows(
            database_dsn=settings.database_dsn,
            symbols=normalized_symbols,
        )

    coverage_rows = build_symbol_coverage_rows(
        required_symbols=normalized_symbols,
        prices_frame=cached_prices,
        quotes_frame=cached_quotes,
        fundamentals_frame=cached_fundamentals,
        earnings_events_frame=cached_earnings_events,
    )
    coverage_rows = merge_symbol_coverage_rows_for_fundamentals(
        computed_rows=coverage_rows,
        existing_rows=existing_coverage_rows,
    )
    coverage_summary = assess_symbol_coverage(
        required_symbols=normalized_symbols,
        observed_symbols=list(_symbol_values(cached_fundamentals)),
    )

    status_rows = _build_asset_status_rows(
        fundamentals_frame=cached_fundamentals,
        fundamentals_summary=fundamentals_summary,
        refresh_run=fundamentals_run,
        flow_run_id=flow_run_id,
    )
    run_rows = [_refresh_run_to_row(fundamentals_run)]

    publisher_impl: Publisher
    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_database()
            publisher_impl = PostgresPublisher(database_dsn=settings.database_dsn)
    else:
        publisher_impl = publisher or RecordingPublisher()

    publish_failures: list[dict[str, Any]] = []
    publish_results: dict[str, Any] = {}
    if publish_enabled or isinstance(publisher_impl, RecordingPublisher):
        publish_results["fundamentals"] = _execute_publish_step(
            "fundamentals",
            lambda: _publish_fundamentals_and_theme_surfaces(
                publisher=publisher_impl,
                fundamentals_history=cached_fundamentals,
                ticker_profile=cached_ticker_profile,
                etf_holdings=cached_etf_holdings,
                etf_holding_onboarding_identity=cached_etf_holding_onboarding_identity,
                etf_themes=cached_etf_themes,
                etf_sector_weights=cached_etf_sector_weights,
            ),
            failures=publish_failures,
        )

    if publish_failures:
        for failure in publish_failures:
            run_rows.append(
                _flow_run_row(
                    run_id=f"{flow_run_id}_publish_{failure['step']}",
                    job_name=f"dataops_fundamentals_daily_publish_{failure['step']}",
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
            asset_key="data_ops_publish_pipeline_fundamentals",
            reference_date=_latest_period_date(cached_fundamentals),
        )
    )

    symbols_succeeded = sorted(
        {
            str(row.get("ticker", "")).strip().upper()
            for row in coverage_rows
            if bool(row.get("fundamentals_available"))
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
            job_name="dataops_fundamentals_daily",
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
            message="Data Ops fundamentals daily flow ended unhealthy.",
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
        raise RuntimeError(f"Data Ops fundamentals daily unhealthy status: {failure_json}")

    return {
        "run_id": flow_run_id,
        "cache_root": str(settings.cache_root),
        "symbols": normalized_symbols,
        "refresh": {
            "fundamentals_daily": fundamentals_run.as_dict(),
            "theme_etf_holdings": theme_etf_refresh,
        },
        "coverage": coverage_summary,
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "source_cache.fundamentals": int(len(cached_fundamentals.index)),
            "ticker_profile": int(len(cached_ticker_profile.index)),
            "etf_holdings": int(len(cached_etf_holdings.index)),
            "etf_holding_onboarding_identity": int(len(cached_etf_holding_onboarding_identity.index)),
            "etf_themes": int(len(cached_etf_themes.index)),
            "etf_sector_weights": int(len(cached_etf_sector_weights.index)),
        },
    }


def _refresh_theme_etf_holdings(
    *,
    cache_root: str,
    enabled: bool,
    refresh_fn: ThemeETFRefreshFn | None,
) -> dict[str, Any]:
    if not enabled:
        return {"status": "skipped", "reason": "disabled"}
    fetcher = refresh_fn or fetch_theme_etf_holdings
    try:
        holdings, themes, failures = fetcher()
        outputs = write_theme_etf_outputs(
            holdings=holdings,
            themes=themes,
            cache_root=cache_root,
            replace_refreshed_holdings=True,
            deactivate_missing_themes=refresh_fn is None,
        )
        status = "fresh" if not failures else "partial"
        return {
            "status": status,
            "holdings_rows": int(len(holdings.index)),
            "theme_count": int(themes["theme"].nunique()) if not themes.empty and "theme" in themes.columns else 0,
            "failures": failures,
            "outputs": outputs,
        }
    except Exception as exc:  # noqa: BLE001 - theme refresh should not kill fundamentals ingestion
        LOGGER.warning("Theme ETF holdings refresh failed (non-fatal): %r", exc)
        return {"status": "failed_hard", "error": repr(exc)}


def _publish_fundamentals_and_theme_surfaces(
    *,
    publisher: Publisher,
    fundamentals_history: pd.DataFrame,
    ticker_profile: pd.DataFrame,
    etf_holdings: pd.DataFrame,
    etf_holding_onboarding_identity: pd.DataFrame,
    etf_themes: pd.DataFrame,
    etf_sector_weights: pd.DataFrame,
) -> dict[str, Any]:
    result = publish_fundamentals_surfaces(
        publisher=publisher,
        fundamentals_history=fundamentals_history,
        ticker_profile=ticker_profile,
        etf_holdings=etf_holdings,
        etf_holding_onboarding_identity=etf_holding_onboarding_identity,
        etf_sector_weights=etf_sector_weights,
    )
    if not etf_themes.empty:
        result["etf_themes"] = publisher.upsert(
            "etf_themes",
            etf_themes.to_dict(orient="records"),
            on_conflict="etf_ticker",
        )
    else:
        result["etf_themes"] = {"status": "skipped", "rows": 0}
    return result


def _build_asset_status_rows(
    *,
    fundamentals_frame: pd.DataFrame,
    fundamentals_summary: pd.DataFrame,
    refresh_run: RefreshRunResult,
    flow_run_id: str,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    fundamentals_last = _frame_datetime_max(fundamentals_frame, "period_end")

    fundamentals_state = classify_freshness(
        last_observed_at=fundamentals_last,
        now=now,
        fresh_within=timedelta(days=160),
        tolerance=timedelta(days=120),
        partial=str(refresh_run.status) == FreshnessState.PARTIAL,
        failure_state=refresh_run.status,
    )
    provider = _provider_from_frame(fundamentals_frame, fallback="unknown")
    now_iso = now.isoformat()
    return [
        {
            "asset_key": "source_cache.fundamentals",
            "asset_type": "fundamentals",
            "provider": provider,
            "last_success_at": _last_success_timestamp(fundamentals_last, fundamentals_state),
            "last_available_date": _date_or_none(fundamentals_last),
            "freshness_status": str(fundamentals_state),
            "coverage_status": str(refresh_run.status),
            "reason": _asset_reason(
                rows_written=int(len(fundamentals_frame.index)),
                run_id=refresh_run.run_id,
                errors=refresh_run.error_messages,
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


def _latest_period_date(frame: pd.DataFrame) -> str | None:
    if frame.empty or "period_end" not in frame.columns:
        return None
    value = pd.to_datetime(frame["period_end"], errors="coerce").max()
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
    database_dsn: str,
    symbols: list[str],
) -> list[dict[str, object]]:
    if not str(database_dsn).strip():
        return []
    try:
        rows = fetch_symbol_data_coverage_rows(
            database_dsn=database_dsn,
            tickers=symbols,
        )
        return [row for row in rows if isinstance(row, dict)]
    except Exception:
        return []


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Data Ops fundamentals daily flow.")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbol universe.")
    parser.add_argument("--cache-root", type=str, default=None, help="Override canonical local cache root.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Retry attempts for retryable failures.")
    parser.add_argument("--no-publish", action="store_true", help="Skip Postgres publishing (local dry-run).")
    parser.add_argument(
        "--skip-theme-etf-refresh",
        action="store_true",
        help="Do not refresh curated thematic ETF holdings during fundamentals daily.",
    )
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

    summary = run_dataops_fundamentals_daily(
        symbols=symbols,
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts or settings.default_max_attempts),
        refresh_theme_etfs=not bool(args.skip_theme_etf_refresh),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
