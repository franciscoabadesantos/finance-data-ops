#!/usr/bin/env python3
"""Audit or reconcile ticker_registry coverage for source refresh scheduling."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.source_universe_reconciliation import build_source_universe_reconciliation_plan
from finance_data_ops.validation.ticker_registry import upsert_ticker_registry_rows


TABLES = {
    "registry": "public.ticker_registry",
    "readiness": "feature_store.ticker_readiness",
    "prices": "source_cache.market_price_daily",
    "technicals": "feature_store.technical_features_daily",
    "summary": "feature_store.ticker_page_summary",
    "entity_attributes": "feature_store.entity_attributes_static",
    "fundamentals": "source_cache.fundamentals",
    "earnings": "source_cache.earnings",
}

LOCAL_TABLE_ALIASES = {
    "public.ticker_registry": ("ticker_registry",),
    "feature_store.ticker_readiness": ("ticker_readiness",),
    "feature_store.entity_attributes_static": ("entity_attributes_static",),
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile ticker_registry with canonical tracked/materialized symbols.")
    parser.add_argument("--cache-root", default=None, help="Override local cache root.")
    parser.add_argument("--source", choices=["postgres", "parquet"], default="postgres", help="Read source.")
    parser.add_argument("--apply", action="store_true", help="Apply generated ticker_registry upserts.")
    parser.add_argument("--fail-on-issues", action="store_true", help="Exit 1 when unresolved audit issues remain.")
    parser.add_argument("--reviewed-exclusions", default="", help="Comma-separated symbols allowed to remain excluded.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    frames = _load_frames(source=args.source, settings=settings)
    reviewed_exclusions = [value.strip().upper() for value in str(args.reviewed_exclusions or "").split(",") if value.strip()]
    plan = build_source_universe_reconciliation_plan(
        registry_frame=frames["registry"],
        readiness_frame=frames["readiness"],
        prices_frame=frames["prices"],
        technicals_frame=frames["technicals"],
        ticker_page_summary_frame=frames["summary"],
        entity_attributes_frame=frames["entity_attributes"],
        fundamentals_frame=frames["fundamentals"],
        earnings_frame=frames["earnings"],
        reviewed_exclusions=reviewed_exclusions,
    )
    result: dict[str, Any] = plan.as_dict()
    result["apply"] = bool(args.apply)
    result["source"] = args.source
    result["write_result"] = {"status": "dry_run", "rows": len(plan.upsert_rows)}

    if args.apply and plan.upsert_rows:
        upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=plan.upsert_rows)
        if args.source == "postgres":
            settings.require_database()
            publisher = PostgresPublisher(database_dsn=settings.database_dsn)
            result["write_result"] = publish_ticker_registry(publisher=publisher, rows=plan.upsert_rows)
        else:
            result["write_result"] = {"status": "ok", "target": "parquet", "rows": len(plan.upsert_rows)}

    print(json.dumps(result, indent=2, default=str))
    if args.fail_on_issues and plan.issues:
        raise SystemExit(1)


def _load_frames(*, source: str, settings: Any) -> dict[str, pd.DataFrame]:
    if source == "postgres":
        settings.require_database()
        return {name: _read_postgres_table(settings.database_dsn, table) for name, table in TABLES.items()}
    return {name: _read_local_table(settings.cache_root, table) for name, table in TABLES.items()}


def _read_postgres_table(database_dsn: str, table: str) -> pd.DataFrame:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to audit Postgres source universe.") from exc
    schema_name, table_name = table.split(".", 1)
    query = f'select * from "{schema_name}"."{table_name}"'
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _read_local_table(cache_root: str | Path, table: str) -> pd.DataFrame:
    frame = read_parquet_table(table, cache_root=cache_root, required=False)
    if not frame.empty:
        return frame
    for alias in LOCAL_TABLE_ALIASES.get(table, ()):
        frame = read_parquet_table(alias, cache_root=cache_root, required=False)
        if not frame.empty:
            return frame
    return pd.DataFrame()


if __name__ == "__main__":
    main()
