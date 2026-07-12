"""Prefect orchestration wrappers for Data Ops daily domain flows.

This module is orchestration-only: it delegates all domain logic to the
existing run_dataops_* functions.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
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
from flows.dataops_trading_calendar_daily import run_dataops_trading_calendar_daily
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.refresh.gap_repair import resolve_gap_aware_window, resolve_watermark_execution
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import (
    DEFAULT_FEATURE_BUILD_DAILY_DEPLOYMENT,
    DEFAULT_FEATURE_SCORECARD_BUILD_DEPLOYMENT,
    DataOpsSettings,
    load_settings,
)
from finance_data_ops.validation.ticker_registry import (
    build_registry_key,
    build_pending_registry_row,
    fetch_registry_row_by_key,
    is_promotable_registry_row,
    upsert_ticker_registry_rows,
)
from finance_data_ops.validation.symbol_resolution import SourceRefreshUniverse, resolve_source_refresh_universe
from finance_data_ops.validation.ticker_validation import run_single_ticker_validation
DEFAULT_TICKER_BACKFILL_START_DATE = "1900-01-01"
DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT = 100
MAX_EARNINGS_HISTORY_LIMIT = 100
TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")
DEFAULT_FEATURE_BUILD_DEPLOYMENT_NAME = DEFAULT_FEATURE_BUILD_DAILY_DEPLOYMENT
DEFAULT_SCORECARD_BUILD_DEPLOYMENT_NAME = DEFAULT_FEATURE_SCORECARD_BUILD_DEPLOYMENT
DEFAULT_TECHNICAL_FEATURE_BACKFILL_DEPLOYMENT_NAME = (
    "technical-feature-backfill/technical-feature-backfill"
)


@dataclass(frozen=True, slots=True)
class FeatureBuildWatermarkRequirement:
    domain: str
    table_name: str
    date_column: str
    blocking: bool
    lookback_days: int = 14
    grace_days: int = 0
    safety_overlap_days: int = 0


FEATURE_BUILD_WATERMARK_REQUIREMENTS: tuple[FeatureBuildWatermarkRequirement, ...] = (
    FeatureBuildWatermarkRequirement(
        domain="market",
        table_name="source_cache.market_price_daily",
        date_column="price_date",
        blocking=True,
        lookback_days=14,
        grace_days=0,
    ),
    FeatureBuildWatermarkRequirement(
        domain="macro",
        table_name="source_cache.macro_daily",
        date_column="as_of_date",
        blocking=False,
        lookback_days=14,
        grace_days=0,
    ),
    FeatureBuildWatermarkRequirement(
        domain="calendars",
        table_name="source_cache.calendars",
        date_column="session_date",
        blocking=False,
        lookback_days=14,
        grace_days=0,
    ),
    FeatureBuildWatermarkRequirement(
        domain="fundamentals",
        table_name="source_cache.fundamentals",
        date_column="report_date",
        blocking=False,
        lookback_days=30,
        grace_days=1,
    ),
    FeatureBuildWatermarkRequirement(
        domain="earnings",
        table_name="source_cache.earnings",
        date_column="report_date",
        blocking=False,
        lookback_days=30,
        grace_days=1,
    ),
)


def _resolve_source_universe(
    *,
    symbols: str | list[str] | None,
    region: str | None,
    settings: DataOpsSettings,
) -> SourceRefreshUniverse:
    return resolve_source_refresh_universe(
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
        start_date = pd.Timestamp(DEFAULT_TICKER_BACKFILL_START_DATE).date()
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
            "Invalid ticker format. Expected plain uppercase symbol (for example: AAPL, BRK-B, RDS-A) without prefixes."
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


def _resolve_earnings_history_limit(raw: object | None, *, default: int) -> int:
    """Yahoo rejects large earnings limits; keep provider calls within the hard cap."""
    return min(_normalize_history_limit(raw, default=default), MAX_EARNINGS_HISTORY_LIMIT)


def _to_json_text(value: object) -> str:
    if value is None:
        return ""
    try:
        return json.dumps(value, default=str)
    except Exception:
        return str(value)


def resolve_feature_build_watermark_gate(
    *,
    as_of_date: str,
    settings: DataOpsSettings,
    requirements: tuple[FeatureBuildWatermarkRequirement, ...] = FEATURE_BUILD_WATERMARK_REQUIREMENTS,
) -> dict[str, Any]:
    resolved_as_of_date = pd.Timestamp(as_of_date).date().isoformat()
    as_of_day = pd.Timestamp(resolved_as_of_date).date()
    requirement_results: list[dict[str, Any]] = []
    degraded: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    for requirement in requirements:
        plan = resolve_watermark_execution(
            domain=requirement.domain,
            table_name=requirement.table_name,
            date_column=requirement.date_column,
            lookback_days=int(requirement.lookback_days),
            grace_days=int(requirement.grace_days),
            safety_overlap_days=int(requirement.safety_overlap_days),
            explicit_end=resolved_as_of_date,
            cache_root=settings.cache_root,
            database_dsn=settings.database_dsn,
        )
        plan_dict = plan.as_dict()
        requirement_ready = not bool(plan.gap_exists)
        item = {
            "domain": requirement.domain,
            "table_name": requirement.table_name,
            "date_column": requirement.date_column,
            "blocking": bool(requirement.blocking),
            "ready": bool(requirement_ready),
            "watermark": plan.latest_complete_canonical_date,
            "lag_days": _feature_build_lag_days(as_of_day, plan.latest_complete_canonical_date),
            "plan": plan_dict,
        }
        requirement_results.append(item)
        if not requirement_ready:
            if requirement.blocking:
                blocked.append(item)
            else:
                degraded.append(item)
    ready = not bool(blocked)
    return {
        "status": "ready" if ready else "not_ready",
        "ready": bool(ready),
        "as_of_date": resolved_as_of_date,
        "requirements": requirement_results,
        "degraded": degraded,
        "blocked": blocked,
    }


def _feature_build_lag_days(as_of_day: object, watermark: object | None) -> int | None:
    if watermark is None:
        return None
    parsed = pd.to_datetime(watermark, errors="coerce")
    if pd.isna(parsed):
        return None
    return max((pd.Timestamp(as_of_day).date() - pd.Timestamp(parsed).date()).days, 0)


def _format_feature_build_stale_sources(items: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for item in items:
        lag_days = item.get("lag_days")
        lag_text = "unknown lag" if lag_days is None else f"lag {int(lag_days)}d"
        parts.append(f"{item.get('domain')} {lag_text}")
    return ", ".join(parts)


def _emit_feature_build_alert(
    *,
    severity: str,
    message: str,
    settings: DataOpsSettings,
    context: dict[str, Any],
    run_id: str | None = None,
) -> dict[str, Any]:
    payload = build_alert_payload(
        severity=severity,
        message=message,
        run_id=run_id,
        context=context,
    )
    emit_alert(payload)
    emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
    return payload


def resolve_feature_build_daily_deployment_name(
    *,
    settings: DataOpsSettings,
    deployment_name: str | None = None,
) -> str:
    resolved = _normalize_optional_text(deployment_name) or settings.feature_build_daily_deployment
    if not resolved:
        return DEFAULT_FEATURE_BUILD_DEPLOYMENT_NAME
    return str(resolved).strip()


def resolve_scorecard_build_deployment_name(
    *,
    settings: DataOpsSettings,
    deployment_name: str | None = None,
) -> str:
    return str(
        _normalize_optional_text(deployment_name)
        or settings.feature_scorecard_build_deployment
        or DEFAULT_SCORECARD_BUILD_DEPLOYMENT_NAME
        or ""
    ).strip()


def trigger_feature_build_daily_if_ready(
    *,
    as_of_date: str,
    settings: DataOpsSettings,
    deployment_name: str | None = None,
    timeout_seconds: float | None = None,
    extra_parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_deployment_name = resolve_feature_build_daily_deployment_name(
        settings=settings,
        deployment_name=deployment_name,
    )
    gate = resolve_feature_build_watermark_gate(as_of_date=as_of_date, settings=settings)
    if not bool(gate["ready"]):
        blocked = list(gate.get("blocked") or [])
        blocked_sources = _format_feature_build_stale_sources(blocked)
        _emit_feature_build_alert(
            severity="error",
            message=(
                f"Feature build blocked: market prices not ready for {gate['as_of_date']}."
                if any(str(item.get("domain")) == "market" for item in blocked)
                else f"Feature build blocked for {gate['as_of_date']}: {blocked_sources}."
            ),
            settings=settings,
            context={
                "as_of_date": gate["as_of_date"],
                "blocked": blocked,
                "degraded": list(gate.get("degraded") or []),
            },
        )
        return {
            "status": "skipped",
            "reason": "watermarks_not_ready",
            "as_of_date": gate["as_of_date"],
            "gate": gate,
        }

    parameters = {"as_of_date": gate["as_of_date"], **dict(extra_parameters or {})}
    try:
        flow_run = run_deployment(
            resolved_deployment_name,
            parameters=parameters,
            timeout=timeout_seconds,
            poll_interval=10,
            flow_run_name=f"feature-build-daily-{str(gate['as_of_date'])}",
            idempotency_key=f"feature-build-daily:{str(gate['as_of_date'])}",
        )
    except Exception as exc:  # noqa: BLE001 - surface Prefect deployment-contract failures clearly
        raise RuntimeError(
            "Failed to trigger feature-store daily build deployment "
            f"{resolved_deployment_name!r}. Set FEATURE_BUILD_DAILY_DEPLOYMENT to a valid "
            "Prefect deployment name in <flow-name>/<deployment-name> form."
        ) from exc
    degraded = list(gate.get("degraded") or [])
    alert_payload = None
    if degraded:
        stale_sources = _format_feature_build_stale_sources(degraded)
        alert_payload = _emit_feature_build_alert(
            severity="warning",
            message=f"Feature build ran for {gate['as_of_date']} with stale sources: {stale_sources}.",
            settings=settings,
            run_id=str(flow_run.id),
            context={
                "as_of_date": gate["as_of_date"],
                "deployment_name": resolved_deployment_name,
                "flow_run_id": str(flow_run.id),
                "degraded": degraded,
            },
        )
    return {
        "status": "triggered",
        "as_of_date": gate["as_of_date"],
        "deployment_name": resolved_deployment_name,
        "flow_run_id": str(flow_run.id),
        "flow_run_state": str(getattr(flow_run, "state_name", "") or ""),
        "parameters": parameters,
        "gate": gate,
        "alert": alert_payload,
    }


def trigger_scorecard_build_for_ticker(
    *,
    ticker: str,
    as_of_date: str,
    settings: DataOpsSettings,
    publish_enabled: bool = True,
    deployment_name: str | None = None,
    timeout_seconds: float | None = 0,
) -> dict[str, Any]:
    normalized_ticker = _normalize_ticker(ticker)
    resolved_as_of_date = pd.Timestamp(as_of_date).date().isoformat()
    resolved_deployment_name = resolve_scorecard_build_deployment_name(
        settings=settings,
        deployment_name=deployment_name,
    )
    parameters = {"symbols": [normalized_ticker], "as_of_date": resolved_as_of_date}
    if not bool(publish_enabled):
        return {
            "status": "skipped",
            "reason": "publish_disabled",
            "deployment_name": resolved_deployment_name,
            "parameters": parameters,
        }
    if not resolved_deployment_name:
        return {
            "status": "skipped",
            "reason": "deployment_not_configured",
            "deployment_name": "",
            "parameters": parameters,
        }
    try:
        flow_run = run_deployment(
            resolved_deployment_name,
            parameters=parameters,
            timeout=timeout_seconds,
            poll_interval=10,
            flow_run_name=f"scorecard-build-{normalized_ticker.lower()}-{resolved_as_of_date}",
            idempotency_key=f"scorecard-build:{normalized_ticker}:{resolved_as_of_date}",
        )
    except Exception as exc:  # noqa: BLE001 - onboarding must expose downstream status without failing registry work
        return {
            "status": "failed",
            "reason": "deployment_trigger_failed",
            "deployment_name": resolved_deployment_name,
            "parameters": parameters,
            "error": (
                "Failed to trigger feature-store scorecard deployment "
                f"{resolved_deployment_name!r}. Set FEATURE_SCORECARD_BUILD_DEPLOYMENT to a valid "
                "Prefect deployment name or leave it unset to disable targeted scorecard builds."
            ),
            "exception": str(exc),
        }
    return {
        "status": "triggered",
        "deployment_name": resolved_deployment_name,
        "flow_run_id": str(flow_run.id),
        "flow_run_state": str(getattr(flow_run, "state_name", "") or ""),
        "parameters": parameters,
    }


def trigger_technical_feature_backfill(
    *,
    ticker: str,
    start: str,
    end: str,
    publish_enabled: bool = True,
    deployment_name: str = DEFAULT_TECHNICAL_FEATURE_BACKFILL_DEPLOYMENT_NAME,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    normalized_ticker = _normalize_ticker(ticker)
    parameters = {
        "symbols": [normalized_ticker],
        "start": str(start),
        "end": str(end),
    }
    if not bool(publish_enabled):
        return {
            "status": "skipped",
            "reason": "publish_disabled",
            "deployment_name": deployment_name,
            "parameters": parameters,
        }

    flow_run = run_deployment(
        deployment_name,
        parameters=parameters,
        timeout=timeout_seconds,
        poll_interval=10,
        flow_run_name=f"technical-backfill-{normalized_ticker.lower()}",
        idempotency_key=f"technical-backfill:{normalized_ticker}:{start}:{end}",
    )
    return {
        "status": "triggered",
        "deployment_name": deployment_name,
        "flow_run_id": str(flow_run.id),
        "flow_run_state": str(getattr(flow_run, "state_name", "") or ""),
        "parameters": parameters,
    }


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
    source_data_status: str | None = None,
    technical_features_status: str | None = None,
    scorecard_build_status: str | None = None,
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
                "source_data_status": str(source_data_status or "") or None,
                "technical_features_status": str(technical_features_status or "") or None,
                "scorecard_build_status": str(scorecard_build_status or "") or None,
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
    retries=1,
    retry_delay_seconds=120,
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
    universe = _resolve_source_universe(symbols=symbols, region=region, settings=settings)
    resolved_symbols = universe.symbols

    execution_plan = resolve_gap_aware_window(
        domain="market",
        table_name="source_cache.market_price_daily",
        date_column="price_date",
        cadence="business",
        lookback_days=int(lookback_days if lookback_days is not None else settings.default_lookback_days),
        explicit_start=start,
        explicit_end=end,
        safety_overlap_days=2,
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
        symbols=resolved_symbols,
    )
    resolved_start, resolved_end = execution_plan.start_date, execution_plan.end_date

    logger = get_run_logger()
    logger.info(
        (
            "Running market flow (region=%s, universe_source=%s, scope=%s, symbols=%s, start=%s, end=%s, "
            "mode=%s, latest_complete=%s, gap_exists=%s)."
        ),
        universe.region,
        universe.source,
        universe.scope,
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
        universe_source=universe.source,
        universe_scope=universe.scope,
        universe_selection_reason=universe.selection_reason,
    )
    summary["execution"] = execution_plan.as_dict()
    summary["universe"] = universe.as_dict()
    return summary


@flow(
    name="dataops_fundamentals_daily",
    retries=1,
    retry_delay_seconds=120,
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
    refresh_theme_etfs: bool = True,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    universe = _resolve_source_universe(symbols=symbols, region=region, settings=settings)
    resolved_symbols = universe.symbols

    logger = get_run_logger()
    execution_plan = resolve_watermark_execution(
        domain="fundamentals",
        table_name="source_cache.fundamentals",
        date_column="period_end",
        lookback_days=3650,
        grace_days=180,
        safety_overlap_days=14,
        explicit_end=None,
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
    )
    logger.info(
        "Running fundamentals flow (region=%s, universe_source=%s, scope=%s, symbols=%s, mode=%s, latest_complete=%s, gap_exists=%s).",
        universe.region,
        universe.source,
        universe.scope,
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
        refresh_theme_etfs=bool(refresh_theme_etfs),
        raise_on_failed_hard=not bool(allow_unhealthy),
        universe_source=universe.source,
        universe_scope=universe.scope,
        universe_selection_reason=universe.selection_reason,
    )
    summary["execution"] = execution_plan.as_dict()
    summary["universe"] = universe.as_dict()

    # Exchange trading calendars ride along with fundamentals-daily (runs once per day)
    # instead of owning a separate Prefect deployment (free-tier 5-deployment limit).
    # Daily cadence captures any calendar change (exchange_calendars update, new exchange)
    # within a day. Idempotent upsert; never fail the fundamentals flow on it.
    try:
        summary["trading_calendar"] = run_dataops_trading_calendar_daily(
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            raise_on_failed_hard=False,
        )
    except Exception as exc:  # noqa: BLE001 - calendar refresh is best-effort
        logger.warning("Trading-calendar refresh failed (non-fatal): %s", exc)
        summary["trading_calendar"] = {"status": "error", "error": str(exc)}

    return summary


@flow(
    name="dataops_earnings_daily",
    retries=1,
    retry_delay_seconds=120,
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
    universe = _resolve_source_universe(symbols=symbols, region=region, settings=settings)
    resolved_symbols = universe.symbols

    execution_plan = resolve_watermark_execution(
        domain="earnings",
        table_name="source_cache.earnings",
        date_column="earnings_date",
        lookback_days=3650,
        grace_days=180,
        safety_overlap_days=14,
        explicit_end=None,
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
    )
    resolved_history_limit = _resolve_earnings_history_limit(
        history_limit,
        default=DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT,
    )
    if execution_plan.gap_exists and execution_plan.earliest_missing_date:
        quarters_back = _quarters_between(
            execution_plan.earliest_missing_date,
            execution_plan.expected_end_date,
        )
        resolved_history_limit = min(
            max(resolved_history_limit, quarters_back + 2),
            MAX_EARNINGS_HISTORY_LIMIT,
        )

    logger = get_run_logger()
    logger.info(
        (
            "Running earnings flow (region=%s, universe_source=%s, scope=%s, symbols=%s, history_limit=%s, "
            "mode=%s, latest_complete=%s, gap_exists=%s)."
        ),
        universe.region,
        universe.source,
        universe.scope,
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
        universe_source=universe.source,
        universe_scope=universe.scope,
        universe_selection_reason=universe.selection_reason,
    )
    summary["execution"] = execution_plan.as_dict()
    summary["execution"]["resolved_history_limit"] = int(resolved_history_limit)
    summary["universe"] = universe.as_dict()
    return summary


@flow(
    name="dataops_macro_daily",
    retries=1,
    retry_delay_seconds=120,
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
    trigger_feature_build: bool = False,
    feature_build_deployment_name: str | None = None,
    feature_build_timeout_seconds: float | None = None,
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
        database_dsn=settings.database_dsn,
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
    if bool(trigger_feature_build):
        if not bool(publish_enabled):
            summary["feature_build_trigger"] = {
                "status": "skipped",
                "reason": "publish_disabled",
                "as_of_date": resolved_end,
            }
        else:
            summary["feature_build_trigger"] = trigger_feature_build_daily_if_ready(
                as_of_date=resolved_end,
                settings=settings,
                deployment_name=feature_build_deployment_name,
                timeout_seconds=feature_build_timeout_seconds,
            )
    return summary


@flow(
    name="dataops_daily",
    retries=0,
    log_prints=True,
)
def dataops_daily_flow(
    *,
    symbols: str | list[str] | None = None,
    region: str | None = "all",
    start: str | None = None,
    end: str | None = None,
    lookback_days: int | None = None,
    cache_root: str | None = None,
    max_attempts: int | None = None,
    publish_enabled: bool = True,
    allow_unhealthy: bool = False,
    force_recompute_macro: bool = False,
    force_recompute_release_calendar: bool = False,
    refresh_theme_etfs: bool = True,
    trigger_feature_build: bool = True,
    feature_build_deployment_name: str | None = None,
    feature_build_timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run source-domain daily refreshes, then hand off to feature-store builds."""
    settings = load_settings(cache_root=cache_root)
    logger = get_run_logger()
    normalized_region = str(region or "all").strip().lower() or "all"
    logger.info("Starting aggregate Data Ops daily flow (region=%s).", normalized_region)

    summaries: dict[str, Any] = {}
    summaries["release_calendar"] = dataops_release_calendar_daily_flow.fn(
        end_date=end,
        lookback_days=lookback_days,
        cache_root=cache_root,
        max_attempts=max_attempts,
        publish_enabled=bool(publish_enabled),
        allow_unhealthy=bool(allow_unhealthy),
        force_recompute=bool(force_recompute_release_calendar),
    )
    summaries["macro"] = dataops_macro_daily_flow.fn(
        start=start,
        end=end,
        lookback_days=lookback_days,
        cache_root=cache_root,
        max_attempts=max_attempts,
        publish_enabled=bool(publish_enabled),
        allow_unhealthy=bool(allow_unhealthy),
        force_recompute=bool(force_recompute_macro),
        trigger_feature_build=False,
    )
    summaries["earnings"] = dataops_earnings_daily_flow.fn(
        symbols=symbols,
        region=normalized_region,
        cache_root=cache_root,
        max_attempts=max_attempts,
        publish_enabled=bool(publish_enabled),
        allow_unhealthy=bool(allow_unhealthy),
    )
    summaries["fundamentals"] = dataops_fundamentals_daily_flow.fn(
        symbols=symbols,
        region=normalized_region,
        cache_root=cache_root,
        max_attempts=max_attempts,
        publish_enabled=bool(publish_enabled),
        allow_unhealthy=bool(allow_unhealthy),
        refresh_theme_etfs=bool(refresh_theme_etfs),
    )
    summaries["market"] = dataops_market_daily_flow.fn(
        symbols=symbols,
        start=start,
        end=end,
        lookback_days=lookback_days,
        region=normalized_region,
        cache_root=cache_root,
        max_attempts=max_attempts,
        publish_enabled=bool(publish_enabled),
        allow_unhealthy=bool(allow_unhealthy),
    )

    market_execution = dict((summaries.get("market") or {}).get("execution") or {})
    as_of_date = str(end or market_execution.get("end_date") or datetime.now(UTC).date().isoformat())
    if bool(trigger_feature_build):
        if not bool(publish_enabled):
            feature_build_trigger = {
                "status": "skipped",
                "reason": "publish_disabled",
                "as_of_date": as_of_date,
            }
        else:
            feature_build_trigger = trigger_feature_build_daily_if_ready(
                as_of_date=as_of_date,
                settings=settings,
                deployment_name=feature_build_deployment_name,
                timeout_seconds=feature_build_timeout_seconds,
            )
    else:
        feature_build_trigger = {
            "status": "skipped",
            "reason": "trigger_disabled",
            "as_of_date": as_of_date,
        }

    return {
        "status": "completed",
        "region": normalized_region,
        "as_of_date": as_of_date,
        "domains": summaries,
        "feature_build_trigger": feature_build_trigger,
    }


@flow(
    name="dataops_release_calendar_daily",
    retries=1,
    retry_delay_seconds=120,
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
        database_dsn=settings.database_dsn,
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


def _cleanup_isolated_cache(path: str | None) -> None:
    if path:
        shutil.rmtree(path, ignore_errors=True)


def _backend_onboarding_run_name(registry_key: str) -> str:
    """Onboarding flow-run name the backend expects for status lookup (finance-backend
    OnboardingService._onboarding_run_name). Must stay in sync with it."""
    slug = str(registry_key or "").strip().lower().replace("|", "-").replace(" ", "-")
    return f"onboard-{slug}" if slug else "onboard-unknown"


def _onboarding_attempt_id() -> str:
    return uuid4().hex[:12]


def _parse_registry_notes(raw_notes: object) -> dict[str, Any]:
    if isinstance(raw_notes, dict):
        return dict(raw_notes)
    if raw_notes is None:
        return {}
    try:
        parsed = json.loads(str(raw_notes))
    except (TypeError, ValueError, json.JSONDecodeError):
        text = str(raw_notes or "").strip()
        return {"raw": text} if text else {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _registry_lifecycle_row(
    row: dict[str, Any],
    *,
    lifecycle_state: str,
    notes: dict[str, Any] | None = None,
    status: str | None = None,
    validation_status: str | None = None,
    promotion_status: str | None = None,
    validation_reason: str | None = None,
) -> dict[str, Any]:
    out = dict(row)
    merged_notes = _parse_registry_notes(out.get("notes"))
    merged_notes["lifecycle_state"] = str(lifecycle_state)
    merged_notes["lifecycle_updated_at"] = datetime.now(UTC).isoformat()
    if notes:
        merged_notes.update({key: value for key, value in notes.items() if value is not None})
    out["notes"] = merged_notes
    out["updated_at"] = datetime.now(UTC).isoformat()
    if status is not None:
        out["status"] = status
    if validation_status is not None:
        out["validation_status"] = validation_status
    if promotion_status is not None:
        out["promotion_status"] = promotion_status
    if validation_reason is not None:
        out["validation_reason"] = validation_reason
    return out


def _persist_registry_row(
    *,
    settings: DataOpsSettings,
    row: dict[str, Any],
    publish_enabled: bool,
) -> dict[str, Any]:
    upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=[row])
    remote_publish_result: dict[str, Any] = {"status": "skipped"}
    if bool(publish_enabled) and settings.database_dsn:
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        remote_publish_result = publish_ticker_registry(publisher=publisher, rows=[row])
    return remote_publish_result


def _is_unresolved_pending_validation_row(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    return (
        str(row.get("status") or "").strip().lower() == "pending_validation"
        and str(row.get("validation_status") or "").strip().lower() == "pending_validation"
        and str(row.get("promotion_status") or "").strip().lower() == "pending_validation"
        and str(row.get("validation_reason") or "").strip().lower() == "pending_validation"
    )


def _run_best_effort_backfill_step(
    step: str,
    fn: Callable[[], dict[str, Any]],
    *,
    logger: Any,
) -> dict[str, Any]:
    """Run an onboarding backfill sub-step that must not abort the backfill.

    Earnings and fundamentals are best-effort: a freshly listed ticker (a recent
    IPO) legitimately has none yet, so a hard failure there must not stop the
    prices + technical-features chain that determines relationship-map eligibility.
    Any failure is captured and returned instead of raised.
    """
    try:
        result = fn()
        return result if isinstance(result, dict) else {"status": "completed", "step": step}
    except Exception as exc:  # noqa: BLE001 - best-effort onboarding step
        logger.warning("Onboarding backfill step %s failed (best-effort): %s", step, exc)
        return {"status": "skipped", "step": step, "best_effort": True, "error": str(exc)}


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
    trigger_technical_features: bool = True,
    technical_features_deployment_name: str = DEFAULT_TECHNICAL_FEATURE_BACKFILL_DEPLOYMENT_NAME,
    technical_features_timeout_seconds: float | None = 7200,
    trigger_scorecard_build: bool = True,
    scorecard_build_deployment_name: str | None = None,
    scorecard_build_timeout_seconds: float | None = 0,
    isolated_cache: bool = True,
) -> dict[str, Any]:
    # Onboarding backfills run in parallel (bulk). Give each run its OWN cache_root so concurrent
    # runs never share parquet cache tables (concurrent read-modify-write corrupts them). The data
    # still publishes to Postgres, so isolating the scratch cache loses nothing.
    isolated_cache_dir = tempfile.mkdtemp(prefix="ticker-backfill-") if isolated_cache else None
    if isolated_cache_dir is not None:
        cache_root = isolated_cache_dir
    settings = load_settings(cache_root=cache_root)
    normalized_ticker = _normalize_ticker(ticker)
    market_start, market_end = _resolve_ticker_backfill_window(
        start=_normalize_optional_text(start),
        end=_normalize_optional_text(end),
    )
    requested_history_limit = _normalize_history_limit(history_limit, default=DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT)
    resolved_history_limit = _resolve_earnings_history_limit(
        history_limit,
        default=DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT,
    )
    run_id = f"run_dataops_ticker_backfill_{uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    logger = get_run_logger()

    market_summary: dict[str, Any] | None = None
    earnings_summary: dict[str, Any] | None = None
    fundamentals_summary: dict[str, Any] | None = None
    technical_features_summary: dict[str, Any] | None = None
    scorecard_build_summary: dict[str, Any] | None = None
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

        # Earnings and fundamentals are best-effort during onboarding: a freshly
        # listed ticker (recent IPO) legitimately has none yet, and that must not
        # abort the prices + technical-features chain that drives eligibility.
        earnings_summary = _run_best_effort_backfill_step(
            "earnings",
            lambda: run_dataops_earnings_daily(
                symbols=[normalized_ticker],
                history_limit=resolved_history_limit,
                cache_root=cache_root,
                publish_enabled=bool(publish_enabled),
                max_attempts=int(max_attempts or settings.default_max_attempts),
                raise_on_failed_hard=False,
            ),
            logger=logger,
        )

        fundamentals_summary = _run_best_effort_backfill_step(
            "fundamentals",
            lambda: run_dataops_fundamentals_daily(
                symbols=[normalized_ticker],
                cache_root=cache_root,
                publish_enabled=bool(publish_enabled),
                max_attempts=int(max_attempts or settings.default_max_attempts),
                raise_on_failed_hard=False,
            ),
            logger=logger,
        )

        if bool(trigger_technical_features):
            failed_step = "technical_features"
            technical_features_summary = trigger_technical_feature_backfill(
                ticker=normalized_ticker,
                start=market_start,
                end=market_end,
                publish_enabled=bool(publish_enabled),
                deployment_name=technical_features_deployment_name,
                timeout_seconds=technical_features_timeout_seconds,
            )
        else:
            technical_features_summary = {
                "status": "skipped",
                "reason": "trigger_disabled",
                "deployment_name": technical_features_deployment_name,
                "parameters": {
                    "symbols": [normalized_ticker],
                    "start": market_start,
                    "end": market_end,
                },
            }

        if bool(trigger_scorecard_build):
            scorecard_build_summary = trigger_scorecard_build_for_ticker(
                ticker=normalized_ticker,
                as_of_date=market_end,
                settings=settings,
                publish_enabled=bool(publish_enabled),
                deployment_name=scorecard_build_deployment_name,
                timeout_seconds=scorecard_build_timeout_seconds,
            )
        else:
            scorecard_build_summary = {
                "status": "skipped",
                "reason": "trigger_disabled",
                "deployment_name": resolve_scorecard_build_deployment_name(
                    settings=settings,
                    deployment_name=scorecard_build_deployment_name,
                ),
                "parameters": {
                    "symbols": [normalized_ticker],
                    "as_of_date": market_end,
                },
            }

        materialization_status = _ticker_backfill_materialization_status(
            market_summary=market_summary,
            earnings_summary=earnings_summary,
            fundamentals_summary=fundamentals_summary,
            technical_features_summary=technical_features_summary,
            scorecard_build_summary=scorecard_build_summary,
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
            source_data_status=materialization_status["source_data_status"],
            technical_features_status=materialization_status["technical_features_status"],
            scorecard_build_status=materialization_status["scorecard_build_status"],
        )

        logger.info("Ticker backfill completed (ticker=%s).", normalized_ticker)
        _cleanup_isolated_cache(isolated_cache_dir)
        return {
            "run_id": run_id,
            "ticker": normalized_ticker,
            "region": str(region or "").strip().lower() or None,
            "status": flow_status,
            "market_window": {"start": market_start, "end": market_end},
            "history_limit": resolved_history_limit,
            "requested_history_limit": requested_history_limit,
            "materialization_status": materialization_status,
            "steps": {
                "market": market_summary or {},
                "earnings": earnings_summary or {},
                "fundamentals": fundamentals_summary or {},
                "technical_features": technical_features_summary or {},
                "scorecard_build": scorecard_build_summary or {},
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
            source_data_status="failed" if failed_step == "market" else "partial",
            technical_features_status="failed" if failed_step == "technical_features" else None,
            scorecard_build_status=None,
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
                "requested_history_limit": requested_history_limit,
                "partial_results": {
                    "market": _to_json_text(market_summary),
                    "earnings": _to_json_text(earnings_summary),
                    "fundamentals": _to_json_text(fundamentals_summary),
                    "technical_features": _to_json_text(technical_features_summary),
                    "scorecard_build": _to_json_text(scorecard_build_summary),
                },
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        _cleanup_isolated_cache(isolated_cache_dir)
        raise


def _ticker_backfill_materialization_status(
    *,
    market_summary: dict[str, Any] | None,
    earnings_summary: dict[str, Any] | None,
    fundamentals_summary: dict[str, Any] | None,
    technical_features_summary: dict[str, Any] | None,
    scorecard_build_summary: dict[str, Any] | None,
) -> dict[str, str]:
    source_steps = [earnings_summary or {}, fundamentals_summary or {}]
    if not market_summary:
        source_data_status = "failed"
    elif any(str(step.get("status") or "").lower() == "skipped" for step in source_steps):
        source_data_status = "partial"
    else:
        source_data_status = "complete"
    return {
        "source_data_status": source_data_status,
        "technical_features_status": str((technical_features_summary or {}).get("status") or "unknown"),
        "scorecard_build_status": str((scorecard_build_summary or {}).get("status") or "unknown"),
    }


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
    selected = result.get("selected", {})
    registry_row = _registry_lifecycle_row(
        dict(result["registry_row"]),
        lifecycle_state="validated" if str(selected.get("validation_status")) != "rejected" else "rejected",
        notes={
            "validation_flow": "dataops_ticker_validation",
            "validation_selected_status": str(selected.get("validation_status") or ""),
        },
    )
    result["registry_row"] = registry_row

    upsert_ticker_registry_rows(
        cache_root=settings.cache_root,
        rows=[registry_row],
    )

    remote_publish_result: dict[str, Any] = {"status": "skipped"}
    if bool(publish_registry) and settings.database_dsn:
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        remote_publish_result = publish_ticker_registry(
            publisher=publisher,
            rows=[registry_row],
        )
    elif bool(publish_registry):
        logger.info("Ticker registry remote publish skipped: DATA_OPS_DATABASE_URL missing.")

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
    trigger_technical_features: bool = True,
    trigger_scorecard_build: bool = True,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    normalized_input = _normalize_ticker(input_symbol)
    normalized_region = str(region or "us").strip().lower()
    normalized_exchange = _normalize_optional_text(exchange)
    normalized_hint = _normalize_optional_text(instrument_type_hint)
    resolved_history_limit = _resolve_earnings_history_limit(
        history_limit,
        default=DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT,
    )
    logger = get_run_logger()

    pending_row = build_pending_registry_row(
        input_symbol=normalized_input,
        region=normalized_region,
        exchange=normalized_exchange,
        instrument_type=str(normalized_hint or "unknown"),
    )
    registry_key = str(pending_row["registry_key"])
    onboarding_run_name = _backend_onboarding_run_name(registry_key)
    onboarding_attempt_id = _onboarding_attempt_id()

    existing_row = fetch_registry_row_by_key(
        registry_key=registry_key,
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
    )
    existing_promotable_row = dict(existing_row) if existing_row and is_promotable_registry_row(existing_row) else None
    if existing_row and is_promotable_registry_row(existing_row):
        existing_notes = _parse_registry_notes(existing_row.get("notes"))
        if str(existing_notes.get("lifecycle_state") or "").strip().lower() == "ready":
            return {
                "input_symbol": normalized_input,
                "promoted_symbol": str(existing_row.get("normalized_symbol") or "").strip().upper(),
                "region": normalized_region,
                "exchange": normalized_exchange,
                "decision": "already_ready",
                "registry_row": existing_row,
                "registry_key": registry_key,
                "onboarding_run_name": onboarding_run_name,
                "validation_flow_run_id": existing_notes.get("validation_flow_run_id"),
                "backfill_flow_run_id": existing_notes.get("backfill_flow_run_id"),
            }

    initial_registry_row = ({**pending_row, **existing_row} if existing_row else pending_row)
    pending_row = _registry_lifecycle_row(
        initial_registry_row,
        lifecycle_state="pending_validation",
        notes={
            "onboarding_run_name": onboarding_run_name,
            "requested_input_symbol": normalized_input,
            "requested_region": normalized_region,
            "requested_exchange": normalized_exchange,
            "requested_history_limit": resolved_history_limit,
            "onboarding_attempt_id": onboarding_attempt_id,
        },
        status="pending_validation",
        validation_status="pending_validation",
        promotion_status="pending_validation",
        validation_reason="pending_validation",
    )

    pending_publish_result = _persist_registry_row(
        settings=settings,
        row=pending_row,
        publish_enabled=bool(publish_enabled),
    )

    logger.info(
        "Starting onboarding validation deployment (ticker=%s, region=%s, exchange=%s).",
        normalized_input,
        normalized_region,
        str(normalized_exchange or "default"),
    )
    validation_idempotency_key = f"ticker-validation:{registry_key}:{onboarding_attempt_id}"
    validating_row = _registry_lifecycle_row(
        pending_row,
        lifecycle_state="validating",
        notes={
            "validation_deployment_name": validation_deployment_name,
            "validation_flow_run_name": f"onboarding-validate-{normalized_input.lower()}",
            "validation_idempotency_key": validation_idempotency_key,
        },
    )
    _persist_registry_row(settings=settings, row=validating_row, publish_enabled=bool(publish_enabled))
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
        idempotency_key=validation_idempotency_key,
    )
    validation_state = str(validation_run.state_name or "")

    registry_row = fetch_registry_row_by_key(
        registry_key=str(pending_row["registry_key"]),
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
    )
    if registry_row:
        registry_row = {**pending_row, **registry_row}
    validation_promotable = False
    if str(validation_state).lower() != "completed":
        fallback_row = _registry_lifecycle_row(
            registry_row or validating_row,
            lifecycle_state="rejected",
            notes={
                "validation_flow_run_id": str(validation_run.id),
                "validation_flow_state": validation_state,
                "validation_error": f"validation_flow_state_{str(validation_state).lower()}",
            },
            status="rejected",
            validation_status="rejected",
            promotion_status="rejected",
            validation_reason=f"validation_flow_state_{str(validation_state).lower()}",
        )
        _persist_registry_row(settings=settings, row=fallback_row, publish_enabled=bool(publish_enabled))
        registry_row = fallback_row
    elif registry_row:
        validation_promotable = is_promotable_registry_row(registry_row)
        if not validation_promotable and existing_promotable_row and _is_unresolved_pending_validation_row(registry_row):
            logger.info(
                "Validation child completed but registry remained pending; using confirmed pre-run promotable row (registry_key=%s).",
                registry_key,
            )
            registry_row = {**pending_row, **existing_promotable_row}
            validation_promotable = True
        if not validation_promotable and _is_unresolved_pending_validation_row(registry_row):
            unresolved_row = _registry_lifecycle_row(
                registry_row,
                lifecycle_state="validation_incomplete",
                notes={
                    "onboarding_run_name": onboarding_run_name,
                    "validation_flow_run_id": str(validation_run.id),
                    "validation_flow_state": validation_state,
                    "validation_error": "validation_completed_without_registry_update",
                },
                status="pending_validation",
                validation_status="pending_validation",
                promotion_status="pending_validation",
                validation_reason="validation_completed_without_registry_update",
            )
            _persist_registry_row(settings=settings, row=unresolved_row, publish_enabled=bool(publish_enabled))
            raise RuntimeError(
                f"Onboarding validation completed for {normalized_input}, but ticker_registry remained pending_validation."
            )
        registry_row = _registry_lifecycle_row(
            registry_row,
            lifecycle_state="pending_backfill" if validation_promotable else "rejected",
            notes={
                "onboarding_run_name": onboarding_run_name,
                "validation_flow_run_id": str(validation_run.id),
                "validation_flow_state": validation_state,
            },
            status="active" if validation_promotable else "rejected",
            promotion_status=str(registry_row.get("promotion_status") or "validated_full")
            if validation_promotable
            else "rejected",
        )
        _persist_registry_row(settings=settings, row=registry_row, publish_enabled=bool(publish_enabled))
    promotable = bool(validation_promotable)
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
    backfill_run_name = f"onboarding-backfill-{promoted_symbol.lower()}"
    backfill_idempotency_key = f"ticker-backfill:{registry_key}:{promoted_symbol}:{onboarding_attempt_id}"
    backfilling_row = _registry_lifecycle_row(
        registry_row or pending_row,
        lifecycle_state="backfilling",
        notes={
            "backfill_deployment_name": backfill_deployment_name,
            "backfill_flow_run_name": backfill_run_name,
            "backfill_idempotency_key": backfill_idempotency_key,
        },
        status="active",
    )
    _persist_registry_row(settings=settings, row=backfilling_row, publish_enabled=bool(publish_enabled))
    backfill_run = run_deployment(
        backfill_deployment_name,
        parameters={
            "ticker": promoted_symbol,
            "region": normalized_region,
            "start": _normalize_optional_text(start),
            "end": _normalize_optional_text(end),
            "history_limit": resolved_history_limit,
            "publish_enabled": bool(publish_enabled),
            "trigger_technical_features": bool(trigger_technical_features),
            "trigger_scorecard_build": bool(trigger_scorecard_build),
        },
        timeout=float(deployment_timeout_seconds),
        poll_interval=10,
        flow_run_name=backfill_run_name,
        idempotency_key=backfill_idempotency_key,
    )
    backfill_state = str(backfill_run.state_name or "")
    if backfill_state.strip().lower() != "completed":
        failed_row = _registry_lifecycle_row(
            registry_row or pending_row,
            lifecycle_state="pending_backfill",
            notes={
                "backfill_flow_run_id": str(backfill_run.id),
                "backfill_flow_state": backfill_state,
                "backfill_error": f"backfill_flow_state_{backfill_state.strip().lower()}",
            },
            status="active",
            validation_reason=f"backfill_flow_state_{backfill_state.strip().lower()}",
        )
        _persist_registry_row(settings=settings, row=failed_row, publish_enabled=bool(publish_enabled))
        # Do not report onboarding success when the backfill child failed: the parent run must
        # fail so the registry/status never shows "ready" without data (validation already ran,
        # so the ticker stays validated_full/pending_backfill and is re-triggerable).
        raise RuntimeError(
            f"Onboarding backfill did not complete for {promoted_symbol} (state={backfill_state})."
        )
    ready_row = _registry_lifecycle_row(
        registry_row or pending_row,
        lifecycle_state="ready",
        notes={
            "backfill_flow_run_id": str(backfill_run.id),
            "backfill_flow_state": backfill_state,
            "ready_at": datetime.now(UTC).isoformat(),
        },
        status="active",
        promotion_status=str((registry_row or {}).get("promotion_status") or "validated_full"),
    )
    _persist_registry_row(settings=settings, row=ready_row, publish_enabled=bool(publish_enabled))
    return {
        "input_symbol": normalized_input,
        "promoted_symbol": promoted_symbol,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "pending_registry_publish": pending_publish_result,
        "validation_flow_run_id": str(validation_run.id),
        "validation_state": validation_state,
        "decision": "promoted",
        "registry_row": ready_row,
        "registry_key": registry_key,
        "onboarding_run_name": onboarding_run_name,
        "backfill_flow_run_id": str(backfill_run.id),
        "backfill_state": backfill_state,
    }


@flow(
    name="dataops_ticker_remove",
    retries=0,
    log_prints=True,
)
def dataops_ticker_remove_flow(
    *,
    input_symbol: str,
    region: str | None = None,
    exchange: str | None = None,
    reason: str = "removed_by_request",
    requested_by: str | None = None,
    cache_root: str | None = None,
    publish_enabled: bool = True,
) -> dict[str, Any]:
    settings = load_settings(cache_root=cache_root)
    logger = get_run_logger()
    normalized_input = _normalize_ticker(input_symbol)
    normalized_region = str(region or "us").strip().lower()
    normalized_exchange = _normalize_optional_text(exchange)
    registry_key = build_registry_key(
        input_symbol=normalized_input,
        region=normalized_region,
        exchange=normalized_exchange,
    )
    row = fetch_registry_row_by_key(
        registry_key=registry_key,
        cache_root=settings.cache_root,
        database_dsn=settings.database_dsn,
    )
    if row is None:
        logger.info("Ticker remove skipped: registry row not found (registry_key=%s).", registry_key)
        return {
            "status": "skipped",
            "reason": "registry_row_not_found",
            "registry_key": registry_key,
            "input_symbol": normalized_input,
            "region": normalized_region,
            "exchange": normalized_exchange,
        }

    notes = _parse_registry_notes(row.get("notes"))
    lifecycle_state = str(notes.get("lifecycle_state") or row.get("status") or "").strip().lower()
    allowed_states = {
        "pending_validation",
        "validating",
        "validated",
        "pending_backfill",
        "backfilling",
        "partial",
        "review",
        "rejected",
    }
    current_status = str(row.get("status") or "").strip().lower()
    if lifecycle_state not in allowed_states and current_status == "active":
        return {
            "status": "blocked",
            "reason": "unsafe_lifecycle_state",
            "registry_key": registry_key,
            "lifecycle_state": lifecycle_state,
            "current_status": current_status,
            "allowed_states": sorted(allowed_states),
        }

    removal_reason = str(reason or "").strip() or "removed_by_request"
    rejected_row = _registry_lifecycle_row(
        row,
        lifecycle_state="rejected",
        notes={
            "remove_reason": removal_reason,
            "remove_requested_by": str(requested_by or "").strip() or None,
            "remove_requested_at": datetime.now(UTC).isoformat(),
            "previous_lifecycle_state": lifecycle_state,
            "prefect_run_cancellation": "not_attempted",
        },
        status="rejected",
        validation_status="rejected",
        promotion_status="rejected",
        validation_reason=removal_reason,
    )
    publish_result = _persist_registry_row(
        settings=settings,
        row=rejected_row,
        publish_enabled=bool(publish_enabled),
    )
    return {
        "status": "removed",
        "registry_key": registry_key,
        "input_symbol": normalized_input,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "previous_lifecycle_state": lifecycle_state,
        "registry_row": rejected_row,
        "registry_publish": publish_result,
    }


@flow(
    name="dataops_ticker_onboarding_bulk",
    retries=0,
    log_prints=True,
)
def dataops_ticker_onboarding_bulk_flow(
    *,
    tickers: list[str],
    region: str | None = None,
    exchange: str | None = None,
    start: str | None = None,
    end: str | None = None,
    history_limit: int | None = None,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    onboarding_deployment_name: str = "dataops_ticker_onboarding/ticker-onboarding",
    trigger_technical_features: bool = True,
    trigger_scorecard_build: bool = True,
    skip_existing: bool = True,
) -> dict[str, Any]:
    """Urgency / bulk onboarding: schedule the onboarding deployment for many tickers.

    Fire-and-forget: each ticker gets an onboarding flow run SCHEDULED (timeout=0) and the
    self-host work pool runs them concurrently, bounded by the pool/queue concurrency limit
    (infra) — no bespoke parallel runner. Idempotent: duplicate symbols are collapsed and,
    when skip_existing is set, already-active tickers are skipped.
    """
    settings = load_settings(cache_root=cache_root)
    logger = get_run_logger()
    normalized_region = str(region or "us").strip().lower()
    normalized_exchange = _normalize_optional_text(exchange)

    seen: set[str] = set()
    scheduled: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for raw in list(tickers or []):
        ticker = _normalize_ticker(str(raw))
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)

        pending = build_pending_registry_row(
            input_symbol=ticker,
            region=normalized_region,
            exchange=normalized_exchange,
            instrument_type="unknown",
        )
        registry_key = str(pending["registry_key"])

        if skip_existing:
            existing = fetch_registry_row_by_key(
                registry_key=registry_key,
                cache_root=settings.cache_root,
                database_dsn=settings.database_dsn,
            )
            if (
                existing
                and is_promotable_registry_row(existing)
                and str(existing.get("status") or "").strip().lower() == "active"
            ):
                skipped.append({"ticker": ticker, "reason": "already_active"})
                continue

        run = run_deployment(
            onboarding_deployment_name,
            parameters={
                "input_symbol": ticker,
                "region": normalized_region,
                "exchange": normalized_exchange,
                "start": _normalize_optional_text(start),
                "end": _normalize_optional_text(end),
                "history_limit": history_limit,
                "publish_enabled": bool(publish_enabled),
                "trigger_technical_features": bool(trigger_technical_features),
                "trigger_scorecard_build": bool(trigger_scorecard_build),
            },
            timeout=0,  # schedule and return; the work pool runs it concurrently
            # Name the run with the backend's deterministic convention so /tickers/status can find
            # it (the backend looks up onboard-<registry_key>); otherwise bulk runs show as
            # pending_backfill even when they completed with data.
            flow_run_name=_backend_onboarding_run_name(registry_key),
        )
        scheduled.append({"ticker": ticker, "flow_run_id": str(run.id)})

    logger.info(
        "Bulk onboarding scheduled %s run(s); skipped %s already-active.",
        len(scheduled),
        len(skipped),
    )
    return {
        "requested": len(seen),
        "scheduled_count": len(scheduled),
        "scheduled": scheduled,
        "skipped": skipped,
    }


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Prefect Data Ops flows locally.")
    parser.add_argument(
        "domain",
        choices=[
            "market",
            "daily",
            "fundamentals",
            "earnings",
            "macro",
            "release-calendar",
            "ticker-backfill",
            "ticker-validation",
            "ticker-onboarding",
            "ticker-remove",
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
    parser.add_argument("--reason", type=str, default="removed_by_request", help="Ticker-remove reason.")
    parser.add_argument("--requested-by", type=str, default=None, help="Ticker-remove requester/audit identity.")
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
    parser.add_argument("--no-publish", action="store_true", help="Skip Postgres publishing.")
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
    parser.add_argument(
        "--no-feature-build-trigger",
        action="store_true",
        help="Daily/macro/ticker-backfill only. Skip downstream feature-store deployment triggers.",
    )
    parser.add_argument(
        "--feature-build-trigger",
        action="store_true",
        help="Macro only. Opt in to the downstream feature-build trigger from the macro flow.",
    )
    parser.add_argument(
        "--feature-build-deployment-name",
        type=str,
        default=None,
        help="Daily/macro only. Prefect deployment name for the downstream feature build.",
    )
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

    if args.domain == "daily":
        result = dataops_daily_flow(
            **common_kwargs,
            start=args.start,
            end=args.end,
            lookback_days=args.lookback_days,
            force_recompute_macro=bool(args.force_recompute),
            force_recompute_release_calendar=bool(args.force_recompute),
            trigger_feature_build=not bool(args.no_feature_build_trigger),
            feature_build_deployment_name=args.feature_build_deployment_name,
        )
    elif args.domain == "market":
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
            trigger_feature_build=bool(args.feature_build_trigger) and not bool(args.no_feature_build_trigger),
            feature_build_deployment_name=args.feature_build_deployment_name,
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
                trigger_technical_features=not bool(args.no_feature_build_trigger),
                trigger_scorecard_build=not bool(args.no_feature_build_trigger),
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
        elif args.domain == "ticker-onboarding":
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
                trigger_technical_features=not bool(args.no_feature_build_trigger),
                trigger_scorecard_build=not bool(args.no_feature_build_trigger),
            )
        else:
            result = dataops_ticker_remove_flow(
                input_symbol=str(args.input_symbol or args.ticker or "").strip(),
                region=str(args.region or "").strip() or None,
                exchange=args.exchange,
                reason=args.reason,
                requested_by=args.requested_by,
                cache_root=args.cache_root,
                publish_enabled=not bool(args.no_publish),
            )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
