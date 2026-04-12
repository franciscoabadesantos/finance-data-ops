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

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from flows.dataops_earnings_daily import run_dataops_earnings_daily
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily
from flows.dataops_market_daily import run_dataops_market_daily
from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import DataOpsSettings, load_settings

REGION_SYMBOL_ENV = {
    "us": "DATA_OPS_SYMBOLS_US",
    "eu": "DATA_OPS_SYMBOLS_EU",
    "apac": "DATA_OPS_SYMBOLS_APAC",
}
DEFAULT_TICKER_BACKFILL_YEARS = 5
DEFAULT_TICKER_EARNINGS_HISTORY_LIMIT = 24
TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")


def _parse_symbols(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(v).strip().upper() for v in raw if str(v).strip()]
    return [str(v).strip().upper() for v in str(raw).split(",") if str(v).strip()]


def _resolve_symbols(
    *,
    symbols: str | list[str] | None,
    region: str | None,
    settings: DataOpsSettings,
) -> list[str]:
    parsed_symbols = _parse_symbols(symbols)
    if parsed_symbols:
        return parsed_symbols

    normalized_region = str(region or "").strip().lower()
    if normalized_region:
        region_env_key = REGION_SYMBOL_ENV.get(normalized_region, f"DATA_OPS_SYMBOLS_{normalized_region.upper()}")
        region_symbols = _parse_symbols(os.environ.get(region_env_key))
        if region_symbols:
            return region_symbols

    return list(settings.default_symbols)


def _resolve_market_window(
    *,
    start: str | None,
    end: str | None,
    lookback_days: int | None,
    settings: DataOpsSettings,
) -> tuple[str, str]:
    end_date = pd.Timestamp(end).date() if end else datetime.now(UTC).date()
    if start:
        start_date = pd.Timestamp(start).date()
    else:
        resolved_lookback = int(lookback_days if lookback_days is not None else settings.default_lookback_days)
        start_date = end_date - timedelta(days=resolved_lookback)
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

    resolved_start, resolved_end = _resolve_market_window(
        start=start,
        end=end,
        lookback_days=lookback_days,
        settings=settings,
    )

    logger = get_run_logger()
    logger.info(
        "Running market flow (region=%s, symbols=%s, start=%s, end=%s).",
        str(region or "default"),
        len(resolved_symbols),
        resolved_start,
        resolved_end,
    )

    return run_dataops_market_daily(
        symbols=resolved_symbols,
        start=resolved_start,
        end=resolved_end,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
        symbol_batch_size=symbol_batch_size,
    )


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
    logger.info("Running fundamentals flow (region=%s, symbols=%s).", str(region or "default"), len(resolved_symbols))

    return run_dataops_fundamentals_daily(
        symbols=resolved_symbols,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        raise_on_failed_hard=not bool(allow_unhealthy),
    )


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

    logger = get_run_logger()
    logger.info(
        "Running earnings flow (region=%s, symbols=%s, history_limit=%s).",
        str(region or "default"),
        len(resolved_symbols),
        int(history_limit),
    )

    return run_dataops_earnings_daily(
        symbols=resolved_symbols,
        cache_root=cache_root,
        publish_enabled=bool(publish_enabled),
        max_attempts=int(max_attempts or settings.default_max_attempts),
        history_limit=int(history_limit),
        raise_on_failed_hard=not bool(allow_unhealthy),
    )


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


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Prefect Data Ops flows locally.")
    parser.add_argument(
        "domain",
        choices=["market", "fundamentals", "earnings", "ticker-backfill"],
        help="Domain flow to execute.",
    )
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbol universe override.")
    parser.add_argument("--ticker", type=str, default=None, help="Single ticker symbol for ticker-backfill flow.")
    parser.add_argument("--region", type=str, default=None, help="Region key (us, eu, apac).")
    parser.add_argument("--cache-root", type=str, default=None, help="Override local cache root.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Retry attempts override.")
    parser.add_argument("--no-publish", action="store_true", help="Skip Supabase publishing.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy status.")
    parser.add_argument("--start", type=str, default=None, help="Market only. Start date YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="Market only. End date YYYY-MM-DD.")
    parser.add_argument("--lookback-days", type=int, default=None, help="Market only. Lookback days if start is unset.")
    parser.add_argument(
        "--history-limit",
        type=int,
        default=None,
        help="Earnings/ticker-backfill provider history row limit override.",
    )
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
    else:
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
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
