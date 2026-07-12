#!/usr/bin/env python
"""Rebuild public.symbol_data_coverage from current materialized source rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.diagnostics.symbol_data_coverage import (
    build_complete_symbol_data_coverage_rows,
    read_complete_symbol_data_coverage_rows_from_postgres,
    summarize_symbol_data_coverage_rebuild,
)
from finance_data_ops.publish.status import replace_symbol_data_coverage_rows
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    source = str(args.source).strip().lower()
    if source == "auto":
        source = "postgres" if settings.database_dsn else "local"

    if source == "postgres":
        rows = read_complete_symbol_data_coverage_rows_from_postgres(database_dsn=settings.database_dsn)
        existing_rows = _read_existing_postgres_coverage(database_dsn=settings.database_dsn)
    else:
        rows = _build_local_rows(cache_root=settings.cache_root)
        existing_rows = read_parquet_table("symbol_data_coverage", cache_root=settings.cache_root, required=False).to_dict(
            orient="records"
        )

    result: dict[str, Any] = {
        "source": source,
        "dry_run": not bool(args.apply),
        "tables": {
            "prices": "source_cache.market_price_daily",
            "fundamentals": "source_cache.fundamentals",
            "earnings": "source_cache.earnings" if source == "postgres" else "earnings_events",
            "coverage": "public.symbol_data_coverage" if source == "postgres" else "symbol_data_coverage",
        },
        "summary": summarize_symbol_data_coverage_rebuild(rows=rows, existing_rows=existing_rows),
        "sql_preview": [
            "delete from public.symbol_data_coverage;",
            f"-- insert {len(rows)} rebuilt diagnostic coverage rows",
        ],
    }

    if args.apply:
        if source == "postgres":
            result["apply_result"] = replace_symbol_data_coverage_rows(database_dsn=settings.database_dsn, rows=rows)
        else:
            path = write_parquet_table(
                "symbol_data_coverage",
                pd.DataFrame(rows),
                cache_root=settings.cache_root,
                mode="replace",
                dedupe_subset=["ticker"],
            )
            result["apply_result"] = {"table": "symbol_data_coverage", "status": "replaced", "path": str(path), "rows": len(rows)}
        result["dry_run"] = False

    if args.summary:
        print(json.dumps(_compact_summary(result), indent=2, default=str))
    else:
        print(json.dumps(result, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild diagnostic symbol_data_coverage from materialized source rows. Dry-run is the default."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True, help="Preview rebuild only (default).")
    mode.add_argument("--apply", action="store_true", help="Replace symbol_data_coverage with rebuilt rows.")
    parser.add_argument("--source", choices=["auto", "postgres", "local"], default="auto")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--summary", action="store_true", help="Print compact summary only.")
    return parser


def _build_local_rows(*, cache_root: str | Path) -> list[dict[str, object]]:
    prices = read_parquet_table("source_cache.market_price_daily", cache_root=cache_root, required=False)
    fundamentals = read_parquet_table("source_cache.fundamentals", cache_root=cache_root, required=False)
    earnings = _read_first_nonempty(
        ["source_cache.earnings", "earnings_events", "earnings_history"],
        cache_root=cache_root,
    )
    return build_complete_symbol_data_coverage_rows(
        prices_frame=prices,
        quotes_frame=pd.DataFrame(),
        fundamentals_frame=fundamentals,
        earnings_events_frame=earnings,
    )


def _read_first_nonempty(table_names: list[str], *, cache_root: str | Path) -> pd.DataFrame:
    for table_name in table_names:
        frame = read_parquet_table(table_name, cache_root=cache_root, required=False)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _read_existing_postgres_coverage(*, database_dsn: str) -> list[dict[str, Any]]:
    dsn = str(database_dsn or "").strip()
    if not dsn:
        return []
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres coverage rebuilds.") from exc
    with psycopg.connect(dsn, connect_timeout=30, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("select ticker, market_data_available from public.symbol_data_coverage")
            return [dict(row) for row in cur.fetchall()]


def _compact_summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = dict(result.get("summary") or {})
    return {
        "source": result.get("source"),
        "dry_run": result.get("dry_run"),
        "tables": result.get("tables"),
        "row_count": summary.get("row_count"),
        "market_data_available_count": summary.get("market_data_available_count"),
        "fundamentals_available_count": summary.get("fundamentals_available_count"),
        "earnings_available_count": summary.get("earnings_available_count"),
        "stale_existing_row_count": len(summary.get("stale_existing_rows_removed") or []),
        "market_true_without_prices_count": len(summary.get("market_true_without_prices") or []),
        "apply_result": result.get("apply_result"),
    }


if __name__ == "__main__":
    main()
