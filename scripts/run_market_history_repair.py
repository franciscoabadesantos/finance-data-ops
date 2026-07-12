#!/usr/bin/env python3
"""Repair short or stale market-price history for registry-selected symbols."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from flows.dataops_market_daily import run_dataops_market_daily
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.symbol_resolution import resolve_source_refresh_universe


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair market history for stale or short registry symbols.")
    parser.add_argument("--region", default="all", help="Registry schedule region: us, eu, apac, or all.")
    parser.add_argument("--symbols", default=None, help="Optional manual comma-separated subset.")
    parser.add_argument("--start", default=None, help="Repair start date. Defaults to end minus --lookback-days.")
    parser.add_argument("--end", default=None, help="Repair end date. Defaults to today.")
    parser.add_argument("--lookback-days", type=int, default=3650, help="History lookback when --start is omitted.")
    parser.add_argument("--min-rows", type=int, default=500, help="Repair symbols below this row count.")
    parser.add_argument("--stale-before", default=None, help="Repair symbols whose latest price_date is before this date.")
    parser.add_argument("--chunk-size", type=int, default=50, help="Symbols per market refresh call.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Provider retry attempts.")
    parser.add_argument("--cache-root", default=None, help="Override cache root.")
    parser.add_argument("--no-publish", action="store_true", help="Refresh local cache only.")
    parser.add_argument("--dry-run", action="store_true", help="Print candidate chunks without refreshing.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    universe = resolve_source_refresh_universe(
        symbols=args.symbols,
        region=args.region,
        settings=settings,
        env=dict(os.environ),
    )
    end_date = pd.Timestamp(args.end).date() if args.end else datetime.now(UTC).date()
    start_date = pd.Timestamp(args.start).date() if args.start else end_date - timedelta(days=max(int(args.lookback_days), 1))
    if start_date > end_date:
        raise ValueError(f"Invalid window: start ({start_date}) is after end ({end_date}).")

    stats = _load_price_stats(symbols=universe.symbols, settings=settings)
    stale_before = pd.Timestamp(args.stale_before).date() if args.stale_before else end_date - timedelta(days=3)
    candidates = _repair_candidates(
        symbols=universe.symbols,
        stats=stats,
        min_rows=int(args.min_rows),
        stale_before=stale_before,
    )
    chunks = list(_chunks(candidates, max(int(args.chunk_size), 1)))
    summary: dict[str, Any] = {
        "universe": universe.as_dict(),
        "window": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "filters": {"min_rows": int(args.min_rows), "stale_before": stale_before.isoformat()},
        "candidate_count": len(candidates),
        "chunk_count": len(chunks),
        "chunks": chunks,
        "dry_run": bool(args.dry_run),
        "results": [],
    }
    if args.dry_run:
        print(json.dumps(summary, indent=2, default=str))
        return

    for chunk in chunks:
        result = run_dataops_market_daily(
            symbols=chunk,
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            cache_root=args.cache_root,
            publish_enabled=not bool(args.no_publish),
            max_attempts=int(args.max_attempts or settings.default_max_attempts),
            raise_on_failed_hard=False,
            universe_source=f"{universe.source}_history_repair",
            universe_scope="run_subset",
            universe_selection_reason="market history repair candidates",
        )
        summary["results"].append(
            {
                "symbols": chunk,
                "run_id": result.get("run_id"),
                "coverage": result.get("coverage"),
                "publish_failures": result.get("publish_failures"),
            }
        )
    print(json.dumps(summary, indent=2, default=str))


def _load_price_stats(*, symbols: list[str], settings: Any) -> dict[str, dict[str, Any]]:
    remote = _load_price_stats_from_postgres(symbols=symbols, database_dsn=settings.database_dsn)
    if remote is not None:
        return remote
    frame = read_parquet_table("source_cache.market_price_daily", cache_root=settings.cache_root, required=False)
    out = {symbol: {"rows": 0, "latest": None} for symbol in symbols}
    if frame.empty or "symbol" not in frame.columns:
        return out
    date_column = "price_date" if "price_date" in frame.columns else "date"
    if date_column not in frame.columns:
        return out
    safe = frame[["symbol", date_column]].copy()
    safe["symbol"] = safe["symbol"].astype(str).str.strip().str.upper()
    safe[date_column] = pd.to_datetime(safe[date_column], errors="coerce")
    grouped = safe[safe["symbol"].isin(symbols)].groupby("symbol", dropna=True)
    for symbol, group in grouped:
        latest = group[date_column].max()
        out[str(symbol)] = {
            "rows": int(len(group.index)),
            "latest": None if pd.isna(latest) else pd.Timestamp(latest).date().isoformat(),
        }
    return out


def _load_price_stats_from_postgres(*, symbols: list[str], database_dsn: str) -> dict[str, dict[str, Any]] | None:
    dsn = str(database_dsn or "").strip()
    if not dsn:
        return None
    try:
        import psycopg
    except ImportError:
        return None
    try:
        out = {symbol: {"rows": 0, "latest": None} for symbol in symbols}
        with psycopg.connect(dsn, connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select symbol, count(*)::int as rows, max(price_date) as latest
                    from source_cache.market_price_daily
                    where symbol = any(%s)
                    group by symbol
                    """,
                    (symbols,),
                )
                for symbol, rows, latest in cur.fetchall():
                    out[str(symbol).strip().upper()] = {
                        "rows": int(rows or 0),
                        "latest": None if latest is None else pd.Timestamp(latest).date().isoformat(),
                    }
        return out
    except Exception:
        return None


def _repair_candidates(
    *,
    symbols: list[str],
    stats: dict[str, dict[str, Any]],
    min_rows: int,
    stale_before: Any,
) -> list[str]:
    stale_date = pd.Timestamp(stale_before).date()
    out: list[str] = []
    for symbol in symbols:
        row = stats.get(symbol) or {}
        latest = row.get("latest")
        latest_date = pd.Timestamp(latest).date() if latest else None
        if int(row.get("rows") or 0) < int(min_rows) or latest_date is None or latest_date < stale_date:
            out.append(symbol)
    return out


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


if __name__ == "__main__":
    main()
