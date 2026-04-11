#!/usr/bin/env python3
"""Validate market cache status and exit non-zero on unhealthy assets."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate Data Ops market asset freshness.")
    parser.add_argument("--cache-root", type=str, default=None)
    parser.add_argument("--as-of-date", type=str, default=datetime.now(UTC).date().isoformat())
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)

    prices = read_parquet_table("market_price_daily", cache_root=settings.cache_root, required=False)
    quotes = read_parquet_table("market_quotes", cache_root=settings.cache_root, required=False)
    stats = read_parquet_table("ticker_market_stats_snapshot", cache_root=settings.cache_root, required=False)

    now = datetime.now(UTC)
    statuses = {
        "market_price_daily": str(
            classify_freshness(
                last_observed_at=pd.to_datetime(prices.get("date"), errors="coerce").max(),
                now=now,
                fresh_within=timedelta(days=2),
                tolerance=timedelta(days=2),
                failure_state="failed_hard" if prices.empty else None,
            )
        ),
        "market_quotes": str(
            classify_freshness(
                last_observed_at=pd.to_datetime(quotes.get("quote_ts"), utc=True, errors="coerce").max(),
                now=now,
                fresh_within=timedelta(hours=26),
                tolerance=timedelta(hours=24),
                failure_state="failed_hard" if quotes.empty else None,
            )
        ),
        "ticker_market_stats_snapshot": str(
            classify_freshness(
                last_observed_at=pd.to_datetime(stats.get("updated_at"), utc=True, errors="coerce").max(),
                now=now,
                fresh_within=timedelta(hours=6),
                tolerance=timedelta(hours=12),
                failure_state="failed_hard" if stats.empty else None,
            )
        ),
    }
    summary = {
        "as_of_date": pd.Timestamp(args.as_of_date).date().isoformat(),
        "cache_root": str(settings.cache_root),
        "statuses": statuses,
    }
    print(json.dumps(summary, indent=2, default=str))

    unhealthy = {
        str(FreshnessState.FAILED_HARD),
        str(FreshnessState.FAILED_RETRYING),
        str(FreshnessState.UNKNOWN),
    }
    if any(str(v) in unhealthy for v in statuses.values()):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
