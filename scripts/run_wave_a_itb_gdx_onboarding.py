#!/usr/bin/env python3
"""Onboard Wave A ITB/GDX-US symbols and optionally run full-history backfills."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
import sys
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from flows.dataops_trading_calendar_daily import run_dataops_trading_calendar_daily
from finance_data_ops.onboarding.wave_a import (
    DEEP_EARNINGS_HISTORY_LIMIT,
    FULL_HISTORY_START_DATE,
    build_wave_a_onboarding_payloads,
)
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces
from finance_data_ops.publish.prices import publish_prices_surfaces
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.refresh.earnings_daily import refresh_earnings_daily
from finance_data_ops.refresh.fundamentals_daily import refresh_fundamentals_daily
from finance_data_ops.refresh.market_daily import refresh_market_daily
from finance_data_ops.refresh.quotes_latest import refresh_latest_quotes
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.ticker_registry import upsert_ticker_registry_rows


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    holdings = read_parquet_table("etf_holdings", cache_root=settings.cache_root, required=True)
    registry = read_parquet_table("ticker_registry", cache_root=settings.cache_root, required=False)
    payloads = build_wave_a_onboarding_payloads(holdings=holdings, existing_registry=registry)

    publisher = None
    publish_result: dict[str, Any] = {"status": "skipped"}
    if args.publish:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        publish_result = publish_ticker_registry(publisher=publisher, rows=payloads.registry_rows)

    cache_result: dict[str, Any] = {"status": "skipped"}
    if args.write_cache:
        upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=payloads.registry_rows)
        if payloads.entity_rows:
            write_parquet_table(
                "entity_attributes_static",
                pd.DataFrame(payloads.entity_rows),
                cache_root=settings.cache_root,
                mode="append",
                dedupe_subset=["entity_id"],
            )
        cache_result = {
            "status": "ok",
            "ticker_registry_rows": len(payloads.registry_rows),
            "entity_attributes_static_rows": len(payloads.entity_rows),
        }

    backfill_result: dict[str, Any] = {"status": "skipped"}
    if args.run_backfill and payloads.symbols_to_backfill:
        backfill_result = _run_full_history_backfill(
            symbols=payloads.symbols_to_backfill,
            cache_root=str(settings.cache_root),
            publisher=publisher,
            publish_enabled=bool(args.publish),
            start_date=str(args.start_date),
            end_date=str(args.end_date or datetime.now(UTC).date().isoformat()),
            batch_size=int(args.batch_size),
            max_attempts=int(args.max_attempts),
            history_limit=int(args.earnings_history_limit),
            refresh_calendar=not bool(args.skip_calendar),
        )

    summary = {
        **payloads.summary,
        "cache": cache_result,
        "publish": publish_result,
        "backfill": backfill_result,
    }
    print(json.dumps(summary, indent=2, default=str))


def _run_full_history_backfill(
    *,
    symbols: list[str],
    cache_root: str,
    publisher: PostgresPublisher | None,
    publish_enabled: bool,
    start_date: str,
    end_date: str,
    batch_size: int,
    max_attempts: int,
    history_limit: int,
    refresh_calendar: bool,
) -> dict[str, Any]:
    market_provider = MarketDataProvider()
    fundamentals_provider = FundamentalsDataProvider()
    earnings_provider = EarningsDataProvider()

    batches = _chunks(symbols, size=batch_size)
    market_rows = 0
    quote_rows = 0
    fundamentals_rows = 0
    earnings_event_rows = 0
    earnings_history_rows = 0
    earliest_prices: dict[str, str] = {}
    failures: list[dict[str, Any]] = []

    for batch in batches:
        prices, market_run = refresh_market_daily(
            symbols=batch,
            start=start_date,
            end=end_date,
            provider=market_provider,
            cache_root=cache_root,
            max_attempts=max_attempts,
        )
        quotes, quotes_run = refresh_latest_quotes(
            symbols=batch,
            provider=market_provider,
            cache_root=cache_root,
            symbol_batch_size=max(batch_size, 1),
        )
        market_rows += int(len(prices.index))
        quote_rows += int(len(quotes.index))
        earliest_prices.update(_earliest_price_dates(prices))
        _record_failures(failures, "source_cache.market_price_daily", market_run.as_dict())
        _record_failures(failures, "latest_quotes_provider_check", quotes_run.as_dict())
        if publish_enabled and publisher is not None:
            publish_prices_surfaces(
                publisher=publisher,
                market_price_daily=prices,
            )

        fundamentals, fundamentals_run = refresh_fundamentals_daily(
            symbols=batch,
            provider=fundamentals_provider,
            cache_root=cache_root,
            max_attempts=max_attempts,
        )
        fundamentals_rows += int(len(fundamentals.index))
        _record_failures(failures, "source_cache.fundamentals", fundamentals_run.as_dict())
        if publish_enabled and publisher is not None:
            publish_fundamentals_surfaces(
                publisher=publisher,
                fundamentals_history=fundamentals,
            )

        earnings_events, earnings_history, earnings_run = refresh_earnings_daily(
            symbols=batch,
            provider=earnings_provider,
            cache_root=cache_root,
            max_attempts=max_attempts,
            history_limit=history_limit,
        )
        earnings_event_rows += int(len(earnings_events.index))
        earnings_history_rows += int(len(earnings_history.index))
        _record_failures(failures, "market_earnings", earnings_run.as_dict())
        if publish_enabled and publisher is not None:
            publish_earnings_surfaces(
                publisher=publisher,
                earnings_events=earnings_events,
                earnings_history=earnings_history,
            )

    calendar_result: dict[str, Any] = {"status": "skipped"}
    if refresh_calendar:
        calendar_result = run_dataops_trading_calendar_daily(
            start_date=start_date,
            end_date=(pd.Timestamp(end_date) + pd.Timedelta(days=366)).date().isoformat(),
            cache_root=cache_root,
            publish_enabled=publish_enabled,
            publisher=publisher,
            raise_on_failed_hard=False,
        )

    return {
        "status": "completed",
        "full_history_start_date": start_date,
        "end_date": end_date,
        "batch_count": len(batches),
        "rows": {
            "source_cache.market_price_daily": market_rows,
            "latest_quotes_provider_check": quote_rows,
            "source_cache.fundamentals": fundamentals_rows,
            "source_cache.earnings": earnings_event_rows + earnings_history_rows,
        },
        "earliest_price_dates": dict(sorted(earliest_prices.items())),
        "earliest_price_date_examples": dict(list(sorted(earliest_prices.items()))[:5]),
        "calendar": {
            "status": "completed" if calendar_result.get("rows") else calendar_result.get("status", "skipped"),
            "rows": calendar_result.get("rows"),
            "window": calendar_result.get("window"),
        },
        "failures": failures,
    }


def _earliest_price_dates(prices: pd.DataFrame) -> dict[str, str]:
    if prices.empty or "symbol" not in prices.columns or "date" not in prices.columns:
        return {}
    local = prices.copy()
    local["symbol"] = local["symbol"].astype(str).str.strip().str.upper()
    local["date"] = pd.to_datetime(local["date"], errors="coerce")
    grouped = local.dropna(subset=["symbol", "date"]).groupby("symbol")["date"].min()
    return {str(symbol): value.date().isoformat() for symbol, value in grouped.items()}


def _record_failures(target: list[dict[str, Any]], asset: str, run: dict[str, Any]) -> None:
    failed = [str(value) for value in run.get("symbols_failed", []) if str(value).strip()]
    if failed:
        target.append(
            {
                "asset": asset,
                "status": run.get("status"),
                "symbols_failed": failed,
                "error_messages": run.get("error_messages", []),
            }
        )


def _chunks(values: list[str], *, size: int) -> list[list[str]]:
    chunk_size = max(int(size), 1)
    return [values[idx : idx + chunk_size] for idx in range(0, len(values), chunk_size)]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Onboard Wave A ITB/GDX-US symbols.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--write-cache", action="store_true", help="Write ticker registry/entity rows to local cache.")
    parser.add_argument("--publish", action="store_true", help="Publish ticker registry/entity rows to Postgres.")
    parser.add_argument("--run-backfill", action="store_true", help="Fetch full-history market/fundamentals/earnings data.")
    parser.add_argument("--start-date", default=FULL_HISTORY_START_DATE)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--earnings-history-limit", type=int, default=DEEP_EARNINGS_HISTORY_LIMIT)
    parser.add_argument("--skip-calendar", action="store_true", help="Skip exchange trading-calendar refresh.")
    return parser


if __name__ == "__main__":
    main()
