"""Prefect orchestration wrappers for Data Ops daily domain flows.

This module is orchestration-only: it delegates all domain logic to the
existing run_dataops_* functions.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from flows.dataops_earnings_daily import run_dataops_earnings_daily
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily
from flows.dataops_macro_daily import run_dataops_macro_daily
from flows.dataops_market_daily import run_dataops_market_daily
from flows.dataops_release_calendar_daily import run_dataops_release_calendar_daily
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.refresh.gap_repair import resolve_gap_aware_window, resolve_watermark_execution
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import DataOpsSettings, load_settings
from finance_data_ops.validation.ticker_registry import (
    build_pending_registry_row,
    fetch_registry_row_by_key,
    is_promotable_registry_row,
    upsert_ticker_registry_rows,
)
from finance_data_ops.validation.symbol_resolution import resolve_symbols
from finance_data_ops.validation.ticker_validation import run_single_ticker_validation
DEFAULT_TICKER_BACKFILL_YEARS = 5
DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT = 24
TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")


def _resolve_symbols(
    *,
    symbols: str | list[str] | None,
    region: str | None,
    settings: DataOpsSettings,
) -> list[str]:
    return resolve_symbols(
        symbols=symbols,
        region=region,
        settings=settings,
        env=dict(os.environ),
    )


def _resolve_market_window(
    *,
    start: str | None,
    end: str | None,
    lookback_days: int | None,
    settings: DataOpsSettings,
) -> tuple[str, str]:
    return _resolve_date_window(
        start=start,
        end=end,
        lookback_days=lookback_days,
        default_lookback_days=settings.default_lookback_days,
    )


def _resolve_date_window(
    *,
    start: str | None,
    end: str | None,
    lookback_days: int | None,
    default_lookback_days: int,
) -> tuple[str, str]:
    end_date = pd.Timestamp(end).date() if end else datetime.now(UTC).date()
    if start:
        start_date = pd.Timestamp(start).date()
    else:
        resolved_lookback = int(lookback_days if lookback_days is not None else default_lookback_days)
        start_date = end_date - timedelta(days=resolved_lookback)
    if start_date > end_date:
        raise ValueError(f"Invalid window: start ({start_date.isoformat()}) is after end ({end_date.isoformat()}).")
    return start_date.isoformat(), end_date.isoformat()


def _resolve_ticker_backfill_window(
    *,
    start: str | None,
    end: str | None,
) -> tuple[str, str]:
    end_date = pd.Timestamp(end).date() if end else datetime.now(UTC).date()
    if start:
        start_date = pd.Timestamp(start).date()
    else:
        start_date = (pd.Timestamp(end_date) - pd.DateOffset(years=DEFAULT_TICKER_BACKFILL_YEARS)).date()
    if start_date > end_date:
        raise ValueError(f"Invalid window: start ({start_date.isoformat()}) is after end ({end_date.isoformat()}).")
    return start_date.isoformat(), end_date.isoformat()


def _quarters_between(start_date: str, end_date: str) -> int:
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    start_period = start_ts.to_period("Q")
    end_period = end_ts.to_period("Q")
    return int(max((end_period - start_period).n, 0))


def _normalize_ticker(raw: str) -> str:
    ticker = str(raw).strip().upper()
    if not ticker:
        raise ValueError("ticker is required.")
    if not TICKER_PATTERN.fullmatch(ticker):
        raise ValueError(
            "Invalid ticker format. Expected plain uppercase symbol (for example: AAPL, BRK.B, RDS-A) without prefixes."
        )
    return ticker


def _normalize_optional_text(raw: object | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() in {"none", "null"}:
        return None
    return text


def _normalize_history_limit(raw: object | None, *, default: int) -> int:
    if raw is None:
        return int(default)
    text = _normalize_optional_text(raw)
    if text is None:
        return int(default)
    parsed = int(text)
    if parsed <= 0:
        raise ValueError("history_limit must be > 0.")
    return parsed


def _to_json_text(value: object) -> str:
    if value is None:
        return ""
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def _record_ticker_backfill_status(
    *,
    cache_root: str | Path,
    ticker: str,
    region: str | None,
    run_id: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    market_start: str,
    market_end: str,
    history_limit: int,
    failed_step: str | None,
    error_message: str | None,
) -> None:
    existing = read_parquet_table("ticker_backfill_status", cache_root=cache_root, required=False)
    previous_last_success = None
    if not existing.empty and "ticker" in existing.columns:
        matches = existing.loc[existing["ticker"].astype(str).str.upper() == ticker]
        if not matches.empty and "last_success_at" in matches.columns:
            candidate = matches["last_success_at"].iloc[-1]
            if pd.notna(candidate):
                previous_last_success = str(candidate)

    last_success_at = ended_at.isoformat() if status == "success" else previous_last_success
    row = pd.DataFrame(
        [
            {
                "ticker": ticker,
                "region": str(region or "").strip().lower() or None,
                "status": status,
                "run_id": run_id,
                "market_start": market_start,
                "market_end": market_end,
                "history_limit": int(history_limit),
                "failed_step": failed_step,
                "error_message": str(error_message or "") or None,
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "last_success_at": last_success_at,
                "updated_at": datetime.now(UTC).isoformat(),
            }
        ]
    )
    write_parquet_table(
        "ticker_backfill_status",
        row,
        cache_root=cache_root,
        mode="append",
        dedupe_subset=["ticker"],
    )


@flow(
    name="dataops_market_daily",
    retries=2,
    retry_delay_seconds=300,
    log_prints=True,
)
def dataops_market_daily_flow(
    *,
    symbols: str | list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    lookback_days: int | None = None,
    region: str | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    symbol_batch_size: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    resolved_symbols = _resolve_symbols(symbols=symbols, region=region, settings=settings)
    if not resolved_symbols:
        raise ValueError(
            "No symbols configured. Pass `symbols` or set DATA_OPS_SYMBOLS / DATA_OPS_SYMBOLS_<REGION>."
        )

    execution_plan = resolve_gap_aware_window(
        domain="market",
        table_name="market_price_daily",
        date_column="date",
        cadence="business",
        lookback_days=int(lookback_days if lookback_days is not None else settings.default_lookback_days),
        explicit_start=start,
        explicit_end=end,
        safety_overlap_days=2,
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    resolved_start, resolved_end = execution_plan.start_date, execution_plan.end_date

    logger = get_run_logger()
    logger.info(
        (
            "Running market flow (region=%s, symbols=%s, start=%s, end=%s, mode=%s, "
            "latest_complete=%s, gap_exists=%s)."
        ),
        str(region or "default"),
        len(resolved_symbols),
        resolved_start,
        resolved_end,
        execution_plan.mode,
        execution_plan.latest_complete_canonical_date,
        execution_plan.gap_exists,
    )

    summary = run_dataops_market_daily(
        symbols=resolved_symbols,
        start=resolved_start,
        end=resolved_end,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
        symbol_batch_size=symbol_batch_size,
    )
    summary["execution"] = execution_plan.as_dict()
    return summary


@flow(
    name="dataops_fundamentals_daily",
    retries=1,
    retry_delay_seconds=900,
    log_prints=True,
)
def dataops_fundamentals_daily_flow(
    *,
    symbols: str | list[str] | None = None,
    region: str | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    resolved_symbols = _resolve_symbols(symbols=symbols, region=region, settings=settings)
    if not resolved_symbols:
        raise ValueError(
            "No symbols configured. Pass `symbols` or set DATA_OPS_SYMBOLS / DATA_OPS_SYMBOLS_<REGION>."
        )

    logger = get_run_logger()
    execution_plan = resolve_watermark_execution(
        domain="fundamentals",
        table_name="market_fundamentals_v2",
        date_column="period_end",
        lookback_days=3650,
        grace_days=180,
        safety_overlap_days=14,
        explicit_end=None,
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    logger.info(
        "Running fundamentals flow (region=%s, symbols=%s, mode=%s, latest_complete=%s, gap_exists=%s).",
        str(region or "default"),
        len(resolved_symbols),
        execution_plan.mode,
        execution_plan.latest_complete_canonical_date,
        execution_plan.gap_exists,
    )

    summary = run_dataops_fundamentals_daily(
        symbols=resolved_symbols,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
    )
    summary["execution"] = execution_plan.as_dict()
    return summary


@flow(
    name="dataops_earnings_daily",
    retries=2,
    retry_delay_seconds=600,
    log_prints=True,
)
def dataops_earnings_daily_flow(
    *,
    symbols: str | list[str] | None = None,
    history_limit: int = 12,
    region: str | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    resolved_symbols = _resolve_symbols(symbols=symbols, region=region, settings=settings)
    if not resolved_symbols:
        raise ValueError(
            "No symbols configured. Pass `symbols` or set DATA_OPS_SYMBOLS / DATA_OPS_SYMBOLS_<REGION>."
        )

    execution_plan = resolve_watermark_execution(
        domain="earnings",
        table_name="market_earnings_history",
        date_column="earnings_date",
        lookback_days=3650,
        grace_days=180,
        safety_overlap_days=14,
        explicit_end=None,
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    resolved_history_limit = int(history_limit)
    if execution_plan.gap_exists and execution_plan.earliest_missing_date:
        quarters_back = _quarters_between(
            execution_plan.earliest_missing_date,
            execution_plan.expected_end_date,
        )
        resolved_history_limit = max(resolved_history_limit, quarters_back + 2)

    logger = get_run_logger()
    logger.info(
        (
            "Running earnings flow (region=%s, symbols=%s, history_limit=%s, mode=%s, "
            "latest_complete=%s, gap_exists=%s)."
        ),
        str(region or "default"),
        len(resolved_symbols),
        int(resolved_history_limit),
        execution_plan.mode,
        execution_plan.latest_complete_canonical_date,
        execution_plan.gap_exists,
    )

    summary = run_dataops_earnings_daily(
        symbols=resolved_symbols,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        history_limit=int(resolved_history_limit),
        raise_on_failed_hard=not bool(allow_unhealthy),
    )
    summary["execution"] = execution_plan.as_dict()
    summary["execution"]["resolved_history_limit"] = int(resolved_history_limit)
    return summary


@flow(
    name="dataops_macro_daily",
    retries=2,
    retry_delay_seconds=600,
    log_prints=True,
)
def dataops_macro_daily_flow(
    *,
    start: str | None = None,
    end: str | None = None,
    lookback_days: int | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
    force_recompute: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    execution_plan = resolve_gap_aware_window(
        domain="macro",
        table_name="macro_daily",
        date_column="as_of_date",
        cadence="business",
        lookback_days=int(lookback_days if lookback_days is not None else settings.default_lookback_days),
        explicit_start=start,
        explicit_end=end,
        safety_overlap_days=2,
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    resolved_start, resolved_end = execution_plan.start_date, execution_plan.end_date

    logger = get_run_logger()
    logger.info(
        (
            "Running macro flow (start=%s, end=%s, force_recompute=%s, mode=%s, "
            "latest_complete=%s, gap_exists=%s)."
        ),
        resolved_start,
        resolved_end,
        bool(force_recompute),
        execution_plan.mode,
        execution_plan.latest_complete_canonical_date,
        execution_plan.gap_exists,
    )

    summary = run_dataops_macro_daily(
        start=resolved_start,
        end=resolved_end,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
        force_recompute=bool(force_recompute),
    )
    summary["execution"] = execution_plan.as_dict()
    return summary


@flow(
    name="dataops_release_calendar_daily",
    retries=2,
    retry_delay_seconds=600,
    log_prints=True,
)
def dataops_release_calendar_daily_flow(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
    force_recompute: bool = False,
    sleep_seconds: float = 0.0,
    official_start_year: int | None = None,
    official_end_year: int | None = None,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    execution_plan = resolve_watermark_execution(
        domain="release-calendar",
        table_name="economic_release_calendar",
        date_column="release_date_local",
        lookback_days=int(lookback_days if lookback_days is not None else settings.default_lookback_days),
        grace_days=5,
        safety_overlap_days=3,
        explicit_end=end_date,
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    resolved_start = str(start_date).strip() if start_date else execution_plan.start_date
    resolved_end = execution_plan.end_date

    logger = get_run_logger()
    logger.info(
        (
            "Running release-calendar flow (start_date=%s, end_date=%s, force_recompute=%s, mode=%s, "
            "latest_complete=%s, gap_exists=%s)."
        ),
        resolved_start,
        resolved_end,
        bool(force_recompute),
        execution_plan.mode,
        execution_plan.latest_complete_canonical_date,
        execution_plan.gap_exists,
    )

    summary = run_dataops_release_calendar_daily(
        start_date=resolved_start,
        end_date=resolved_end,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
        force_recompute=bool(force_recompute),
        sleep_seconds=float(sleep_seconds),
        official_start_year=official_start_year,
        official_end_year=official_end_year,
    )
    summary["execution"] = execution_plan.as_dict()
    return summary


@flow(
    name="dataops_ticker_backfill",
    retries=0,
    log_prints=True,
)
def dataops_ticker_backfill_flow(
    *,
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    history_limit: int = DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT,
    region: str | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    normalized_ticker = _normalize_ticker(ticker)
    market_start, market_end = _resolve_ticker_backfill_window(
        start=_normalize_optional_text(start),
        end=_normalize_optional_text(end),
    )
    resolved_history_limit = _normalize_history_limit(history_limit, default=DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT)
    run_id = f"run_dataops_ticker_backfill_{uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    logger = get_run_logger()

    market_summary: dict[str, Any] | None = None
    earnings_summary: dict[str, Any] | None = None
    fundamentals_summary: dict[str, Any] | None = None
    failed_step: str | None = None
    error_message: str | None = None
    flow_status = "success"

    logger.info(
        "Starting ticker backfill (ticker=%s, start=%s, end=%s, history_limit=%s, region=%s).",
        normalized_ticker,
        market_start,
        market_end,
        resolved_history_limit,
        str(region or "default"),
    )
    if region:
        logger.info("Region metadata received (%s); stored for observability/routing context.", str(region).strip().lower())
    try:
        failed_step = "market"
        market_summary = run_dataops_market_daily(
            symbols=[normalized_ticker],
            start=market_start,
            end=market_end,
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            max_attempts=int(max_attempts or settings.default_max_attempts),
            raise_on_failed_hard=not bool(allow_unhealthy),
        )

        failed_step = "earnings"
        earnings_summary = run_dataops_earnings_daily(
            symbols=[normalized_ticker],
            history_limit=resolved_history_limit,
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            max_attempts=int(max_attempts or settings.default_max_attempts),
            raise_on_failed_hard=not bool(allow_unhealthy),
        )

        failed_step = "fundamentals"
        fundamentals_summary = run_dataops_fundamentals_daily(
            symbols=[normalized_ticker],
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            max_attempts=int(max_attempts or settings.default_max_attempts),
            raise_on_failed_hard=not bool(allow_unhealthy),
        )

        ended_at = datetime.now(UTC)
        _record_ticker_backfill_status(
            cache_root=settings.cache_root,
            ticker=normalized_ticker,
            region=region,
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            status=flow_status,
            market_start=market_start,
            market_end=market_end,
            history_limit=resolved_history_limit,
            failed_step=None,
            error_message=None,
        )

        logger.info("Ticker backfill completed (ticker=%s).", normalized_ticker)
        return {
            "run_id": run_id,
            "ticker": normalized_ticker,
            "region": str(region or "").strip().lower() or None,
            "status": flow_status,
            "market_window": {"start": market_start, "end": market_end},
            "history_limit": resolved_history_limit,
            "steps": {
                "market": market_summary or {},
                "earnings": earnings_summary or {},
                "fundamentals": fundamentals_summary or {},
            },
        }
    except Exception as exc:
        flow_status = "failed"
        error_message = str(exc)
        ended_at = datetime.now(UTC)
        _record_ticker_backfill_status(
            cache_root=settings.cache_root,
            ticker=normalized_ticker,
            region=region,
            run_id=run_id,
            started_at=started_at,
            ended_at=ended_at,
            status=flow_status,
            market_start=market_start,
            market_end=market_end,
            history_limit=resolved_history_limit,
            failed_step=failed_step,
            error_message=error_message,
        )

        payload = build_alert_payload(
            severity="error",
            message="Data Ops ticker backfill failed.",
            run_id=run_id,
            context={
                "ticker": normalized_ticker,
                "region": str(region or "").strip().lower() or None,
                "failed_step": failed_step,
                "error": error_message,
                "market_window": {"start": market_start, "end": market_end},
                "history_limit": resolved_history_limit,
                "partial_results": {
                    "market": _to_json_text(market_summary),
                    "earnings": _to_json_text(earnings_summary),
                    "fundamentals": _to_json_text(fundamentals_summary),
                },
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        raise


@flow(
    name="dataops_ticker_validation",
    retries=0,
    log_prints=True,
)
def dataops_ticker_validation_flow(
    *,
    input_symbol: str,
    region: str | None = None,
    exchange: str | None = None,
    instrument_type_hint: str | None = None,
    history_limit: int | None = 12,
    cache_root: str | None = None,
    publish_registry: bool = True,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    logger = get_run_logger()

    result = run_single_ticker_validation(
        input_symbol=input_symbol,
        region=str(region or "us").strip().lower(),
        exchange=_normalize_optional_text(exchange),
        instrument_type_hint=_normalize_optional_text(instrument_type_hint),
        history_limit=int(history_limit or 12),
    )

    upsert_ticker_registry_rows(
        cache_root=settings.cache_root,
        rows=[result["registry_row"]],
    )

    remote_publish_result: dict[str, Any] = {"status": "skipped"}
    if bool(publish_registry) and settings.supabase_url and settings.supabase_service_role_key:
        publisher = SupabaseRestPublisher(
            supabase_url=settings.supabase_url,
            service_role_key=settings.supabase_service_role_key,
        )
        remote_publish_result = publish_ticker_registry(
            publisher=publisher,
            rows=[result["registry_row"]],
        )
    elif bool(publish_registry):
        logger.info("Ticker registry remote publish skipped: SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY missing.")

    selected = result.get("selected", {})
    if str(selected.get("validation_status")) == "rejected":
        payload = build_alert_payload(
            severity="warning",
            message="Ticker validation rejected symbol candidate.",
            run_id=f"run_dataops_ticker_validation_{uuid4().hex[:12]}",
            context={
                "input_symbol": result.get("input_symbol"),
                "region": result.get("region"),
                "exchange": result.get("exchange"),
                "selected": selected,
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)

    result["registry_persistence"] = {
        "local_table": "ticker_registry",
        "cache_root": str(settings.cache_root),
        "remote": remote_publish_result,
    }
    return result


@flow(
    name="dataops_ticker_onboarding",
    retries=0,
    log_prints=True,
)
def dataops_ticker_onboarding_flow(
    *,
    input_symbol: str,
    region: str | None = None,
    exchange: str | None = None,
    instrument_type_hint: str | None = None,
    start: str | None = None,
    end: str | None = None,
    history_limit: int | None = None,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    validation_deployment_name: str = "dataops_ticker_validation/ticker-validation",
    backfill_deployment_name: str = "dataops_ticker_backfill/ticker-backfill",
    deployment_timeout_seconds: int = 7200,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    normalized_input = _normalize_ticker(input_symbol)
    normalized_region = str(region or "us").strip().lower()
    normalized_exchange = _normalize_optional_text(exchange)
    normalized_hint = _normalize_optional_text(instrument_type_hint)
    resolved_history_limit = int(history_limit or DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT)
    logger = get_run_logger()

    pending_row = build_pending_registry_row(
        input_symbol=normalized_input,
        region=normalized_region,
        exchange=normalized_exchange,
        instrument_type=str(normalized_hint or "unknown"),
    )
    upsert_ticker_registry_rows(
        cache_root=settings.cache_root,
        rows=[pending_row],
    )

    pending_publish_result: dict[str, Any] = {"status": "skipped"}
    if bool(publish_enabled) and settings.supabase_url and settings.supabase_service_role_key:
        publisher = SupabaseRestPublisher(
            supabase_url=settings.supabase_url,
            service_role_key=settings.supabase_service_role_key,
        )
        pending_publish_result = publish_ticker_registry(
            publisher=publisher,
            rows=[pending_row],
        )

    logger.info(
        "Starting onboarding validation deployment (ticker=%s, region=%s, exchange=%s).",
        normalized_input,
        normalized_region,
        str(normalized_exchange or "default"),
    )
    validation_run = run_deployment(
        validation_deployment_name,
        parameters={
            "input_symbol": normalized_input,
            "region": normalized_region,
            "exchange": normalized_exchange,
            "instrument_type_hint": normalized_hint,
            "history_limit": 12,
            "publish_registry": bool(publish_enabled),
        },
        timeout=float(deployment_timeout_seconds),
        poll_interval=10,
        flow_run_name=f"onboarding-validate-{normalized_input.lower()}",
    )
    validation_state = str(validation_run.state_name or "")

    registry_row = fetch_registry_row_by_key(
        registry_key=str(pending_row["registry_key"]),
        cache_root=settings.cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    if str(validation_state).lower() != "completed":
        fallback_row = build_pending_registry_row(
            input_symbol=normalized_input,
            region=normalized_region,
            exchange=normalized_exchange,
            instrument_type=str(normalized_hint or "unknown"),
        )
        fallback_row.update(
            {
                "status": "rejected",
                "validation_status": "rejected",
                "promotion_status": "rejected",
                "validation_reason": f"validation_flow_state_{str(validation_state).lower()}",
                "notes": f"validation_flow_failed state={validation_state}",
            }
        )
        upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=[fallback_row])
        if bool(publish_enabled) and settings.supabase_url and settings.supabase_service_role_key:
            publisher = SupabaseRestPublisher(
                supabase_url=settings.supabase_url,
                service_role_key=settings.supabase_service_role_key,
            )
            publish_ticker_registry(publisher=publisher, rows=[fallback_row])
        registry_row = fallback_row

    promotable = is_promotable_registry_row(registry_row)
    if not promotable:
        logger.info(
            "Ticker onboarding rejected (ticker=%s, reason=%s).",
            normalized_input,
            str((registry_row or {}).get("validation_reason") or "unknown"),
        )
        return {
            "input_symbol": normalized_input,
            "region": normalized_region,
            "exchange": normalized_exchange,
            "pending_registry_publish": pending_publish_result,
            "validation_flow_run_id": str(validation_run.id),
            "validation_state": validation_state,
            "decision": "rejected",
            "registry_row": registry_row,
            "backfill_flow_run_id": None,
        }

    promoted_symbol = str((registry_row or {}).get("normalized_symbol") or "").strip().upper()
    logger.info(
        "Ticker promotable; launching backfill deployment (input=%s, promoted=%s).",
        normalized_input,
        promoted_symbol,
    )
    backfill_run = run_deployment(
        backfill_deployment_name,
        parameters={
            "ticker": promoted_symbol,
            "region": normalized_region,
            "start": _normalize_optional_text(start),
            "end": _normalize_optional_text(end),
            "history_limit": resolved_history_limit,
            "publish_enabled": bool(publish_enabled),
        },
        timeout=float(deployment_timeout_seconds),
        poll_interval=10,
        flow_run_name=f"onboarding-backfill-{promoted_symbol.lower()}",
    )
    backfill_state = str(backfill_run.state_name or "")
    return {
        "input_symbol": normalized_input,
        "promoted_symbol": promoted_symbol,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "pending_registry_publish": pending_publish_result,
        "validation_flow_run_id": str(validation_run.id),
        "validation_state": validation_state,
        "decision": "promoted",
        "registry_row": registry_row,
        "backfill_flow_run_id": str(backfill_run.id),
        "backfill_state": backfill_state,
    }


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Prefect Data Ops flows locally.")
    parser.add_argument(
        "domain",
        choices=[
            "market",
            "fundamentals",
            "earnings",
            "macro",
            "release-calendar",
            "ticker-backfill",
            "ticker-validation",
            "ticker-onboarding",
        ],
        help="Domain flow to execute.",
    )
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbol universe override.")
    parser.add_argument("--ticker", type=str, default=None, help="Single ticker symbol for ticker-backfill/onboarding.")
    parser.add_argument(
        "--input-symbol",
        type=str,
        default=None,
        help="Single ticker symbol input for ticker-validation flow.",
    )
    parser.add_argument("--region", type=str, default=None, help="Region key (us, eu, apac).")
    parser.add_argument("--exchange", type=str, default=None, help="Optional exchange code (for example: ASX).")
    parser.add_argument(
        "--instrument-type-hint",
        type=str,
        default=None,
        help="Optional instrument type hint (equity, adr, etf, index_proxy, country_fund, unknown).",
    )
    parser.add_argument("--cache-root", type=str, default=None, help="Override local cache root.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Retry attempts override.")
    parser.add_argument("--no-publish", action="store_true", help="Skip Supabase publishing.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy status.")
    parser.add_argument("--start", type=str, default=None, help="Market only. Start date YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="Market only. End date YYYY-MM-DD.")
    parser.add_argument("--start-date", type=str, default=None, help="Release-calendar only. Start date YYYY-MM-DD.")
    parser.add_argument("--end-date", type=str, default=None, help="Release-calendar only. End date YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=None, help="Market only. Lookback days if start is unset.")
    parser.add_argument(
        "--history-limit",
        type=int,
        default=None,
        help="Earnings/ticker-backfill provider history row limit override.",
    )
    parser.add_argument("--force-recompute", action="store_true", help="Macro/release only. Recompute selected range.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Release-calendar only. Provider pacing.")
    parser.add_argument("--official-start-year", type=int, default=None, help="Release-calendar official start year.")
    parser.add_argument("--official-end-year", type=int, default=None, help="Release-calendar official end year.")
    return parser


def main() -> None:
    args = _build_cli_parser().parse_args()
    common_kwargs = {
        "symbols": args.symbols,
        "region": args.region,
        "cache_root": args.cache_root,
        "max_attempts": args.max_attempts,
        "publish_enabled": not bool(args.no_publish),
        "allow_unhealthy": bool(args.allow_unhealthy),
    }

    if args.domain == "market":
        result = dataops_market_daily_flow(
            **common_kwargs,
            start=args.start,
            end=args.end,
            lookback_days=args.lookback_days,
        )
    elif args.domain == "fundamentals":
        result = dataops_fundamentals_daily_flow(**common_kwargs)
    elif args.domain == "earnings":
        result = dataops_earnings_daily_flow(
            **common_kwargs,
            history_limit=int(args.history_limit or 12),
        )
    elif args.domain == "macro":
        result = dataops_macro_daily_flow(
            start=args.start,
            end=args.end,
            lookback_days=args.lookback_days,
            cache_root=args.cache_root,
            max_attempts=args.max_attempts,
            publish_enabled=not bool(args.no_publish),
            allow_unhealthy=bool(args.allow_unhealthy),
            force_recompute=bool(args.force_recompute),
        )
    elif args.domain == "release-calendar":
        result = dataops_release_calendar_daily_flow(
            start_date=args.start_date or args.start,
            end_date=args.end_date or args.end,
            lookback_days=args.lookback_days,
            cache_root=args.cache_root,
            max_attempts=args.max_attempts,
            publish_enabled=not bool(args.no_publish),
            allow_unhealthy=bool(args.allow_unhealthy),
            force_recompute=bool(args.force_recompute),
            sleep_seconds=float(args.sleep_seconds),
            official_start_year=args.official_start_year,
            official_end_year=args.official_end_year,
        )
    else:
        if args.domain == "ticker-backfill":
            result = dataops_ticker_backfill_flow(
                ticker=str(args.ticker or "").strip(),
                start=args.start,
                end=args.end,
                history_limit=int(args.history_limit or DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT),
                region=args.region,
                cache_root=args.cache_root,
                max_attempts=args.max_attempts,
                publish_enabled=not bool(args.no_publish),
                allow_unhealthy=bool(args.allow_unhealthy),
            )
        elif args.domain == "ticker-validation":
            result = dataops_ticker_validation_flow(
                input_symbol=str(args.input_symbol or args.ticker or "").strip(),
                region=str(args.region or "").strip(),
                exchange=args.exchange,
                instrument_type_hint=args.instrument_type_hint,
                history_limit=int(args.history_limit or 12),
                cache_root=args.cache_root,
                publish_registry=not bool(args.no_publish),
            )
        else:
            result = dataops_ticker_onboarding_flow(
                input_symbol=str(args.input_symbol or args.ticker or "").strip(),
                region=str(args.region or "").strip() or None,
                exchange=args.exchange,
                instrument_type_hint=args.instrument_type_hint,
                start=args.start,
                end=args.end,
                history_limit=int(args.history_limit or DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT),
                cache_root=args.cache_root,
                publish_enabled=not bool(args.no_publish),
            )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
