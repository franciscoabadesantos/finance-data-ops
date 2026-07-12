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
from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_payload, build_ticker_registry_payload
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.source_universe_reconciliation import build_source_universe_reconciliation_plan
from finance_data_ops.validation.ticker_registry import upsert_ticker_registry_rows


REGISTRY_COLUMNS = [
    "registry_key",
    "input_symbol",
    "normalized_symbol",
    "region",
    "exchange",
    "exchange_mic",
    "currency",
    "instrument_type",
    "status",
    "market_supported",
    "fundamentals_supported",
    "earnings_supported",
    "validation_status",
    "validation_reason",
    "promotion_status",
    "last_validated_at",
    "notes",
    "updated_at",
]

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
    registry_mutation_rows = plan.upsert_rows + plan.supersede_rows + plan.reject_rows
    result["write_result"] = {
        "status": "dry_run",
        "upsert_rows": len(plan.upsert_rows),
        "supersede_rows": len(plan.supersede_rows),
        "reject_rows": len(plan.reject_rows),
        "registry_rows": len(registry_mutation_rows),
    }

    if args.apply and registry_mutation_rows:
        upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=registry_mutation_rows)
        if args.source == "postgres":
            settings.require_database()
            publisher = PostgresPublisher(database_dsn=settings.database_dsn)
            registry_result = publisher.upsert(
                "ticker_registry",
                build_ticker_registry_payload(registry_mutation_rows),
                on_conflict="registry_key",
            )
            entity_result = publisher.upsert(
                "feature_store.entity_attributes_static",
                build_entity_attributes_static_payload(plan.upsert_rows),
                on_conflict="entity_id",
            )
            result["write_result"] = {
                "ticker_registry": registry_result,
                "feature_store.entity_attributes_static": entity_result,
            }
        else:
            result["write_result"] = {"status": "ok", "target": "parquet", "rows": len(registry_mutation_rows)}

    print(json.dumps(result, indent=2, default=str))
    if args.fail_on_issues and plan.issues:
        raise SystemExit(1)


def _load_frames(*, source: str, settings: Any) -> dict[str, pd.DataFrame]:
    if source == "postgres":
        settings.require_database()
        return _read_postgres_symbol_frames(settings.database_dsn)
    return {name: _read_local_table(settings.cache_root, table) for name, table in TABLES.items()}


def _read_postgres_symbol_frames(database_dsn: str) -> dict[str, pd.DataFrame]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to audit Postgres source universe.") from exc
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        readiness_columns = _fetch_columns(conn, schema_name="feature_store", table_name="ticker_readiness")
        frames = {
            "readiness": _fetch_frame(conn, _readiness_symbols_query(readiness_columns)),
            "prices": _fetch_frame(conn, PRICE_SYMBOL_STATS_QUERY),
            "technicals": _fetch_frame(conn, TECHNICAL_SYMBOLS_QUERY),
            "summary": _fetch_frame(conn, SUMMARY_SYMBOLS_QUERY),
            "fundamentals": _fetch_frame(conn, FUNDAMENTAL_SYMBOLS_QUERY),
            "earnings": _fetch_frame(conn, EARNINGS_SYMBOLS_QUERY),
        }
        candidate_symbols = _candidate_symbols_from_frames(frames)
        selected_registry = _fetch_frame(conn, SELECTED_REGISTRY_QUERY)
        related_registry = _fetch_registry_rows_for_symbols(conn, candidate_symbols)
        frames["registry"] = _merge_frames_by_key(
            frames=[selected_registry, related_registry],
            key="registry_key",
            columns=REGISTRY_COLUMNS,
        )
        frames["entity_attributes"] = _fetch_entity_attributes_for_symbols(conn, candidate_symbols)
        return frames


def _fetch_frame(conn: Any, query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _fetch_columns(conn: Any, *, schema_name: str, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
            (schema_name, table_name),
        )
        return {str(row["column_name"]) for row in cur.fetchall()}


def _candidate_symbols_from_frames(frames: dict[str, pd.DataFrame]) -> list[str]:
    out: set[str] = set()
    for frame in frames.values():
        if frame.empty or "symbol" not in frame.columns:
            continue
        out.update(str(value).strip().upper() for value in frame["symbol"].dropna().tolist() if str(value).strip())
    return sorted(out)


def _fetch_registry_rows_for_symbols(conn: Any, symbols: list[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(columns=REGISTRY_COLUMNS)
    return _fetch_frame(conn, RELATED_REGISTRY_QUERY, (symbols, symbols))


def _fetch_entity_attributes_for_symbols(conn: Any, symbols: list[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame(
            columns=["entity_id", "country", "home_country", "region", "exchange", "exchange_mic", "currency"]
        )
    return _fetch_frame(conn, ENTITY_ATTRIBUTES_QUERY, (symbols,))


def _merge_frames_by_key(*, frames: list[pd.DataFrame], key: str, columns: list[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    merged = pd.concat(non_empty, ignore_index=True)
    if key in merged.columns:
        merged = merged.drop_duplicates(subset=[key], keep="last")
    for column in columns:
        if column not in merged.columns:
            merged[column] = None
    return merged[columns].copy()


def _readiness_symbols_query(columns: set[str]) -> str:
    if "is_tracked" in columns:
        return READINESS_TRACKED_SYMBOLS_QUERY
    if "tracked_search_ready" in columns:
        return READINESS_TRACKED_SEARCH_READY_SYMBOLS_QUERY
    return READINESS_STATUS_SYMBOLS_QUERY


READINESS_TRACKED_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol, true as is_tracked
from feature_store.ticker_readiness
where is_tracked is true and symbol is not null
"""

READINESS_TRACKED_SEARCH_READY_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol, true as tracked_search_ready
from feature_store.ticker_readiness
where tracked_search_ready is true and symbol is not null
"""

READINESS_STATUS_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol, readiness_status
from feature_store.ticker_readiness
where symbol is not null
  and lower(readiness_status) in ('ready', 'tracked', 'fresh')
"""

PRICE_SYMBOL_STATS_QUERY = """
select upper(symbol) as symbol, count(*)::int as source_price_rows, max(price_date) as source_price_latest_date
from source_cache.market_price_daily
where symbol is not null
group by upper(symbol)
"""

TECHNICAL_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol
from feature_store.technical_features_daily
where symbol is not null
"""

SUMMARY_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol
from feature_store.ticker_page_summary
where symbol is not null
"""

FUNDAMENTAL_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol
from source_cache.fundamentals
where symbol is not null
"""

EARNINGS_SYMBOLS_QUERY = """
select distinct upper(symbol) as symbol
from source_cache.earnings
where symbol is not null
"""

SELECTED_REGISTRY_QUERY = """
select registry_key, input_symbol, normalized_symbol, region, exchange, exchange_mic, currency,
       instrument_type, status, market_supported, fundamentals_supported, earnings_supported,
       validation_status, validation_reason, promotion_status, last_validated_at, notes, updated_at
from public.ticker_registry
where status = 'active'
  and promotion_status = any(array['validated_market_only', 'validated_full'])
  and market_supported is true
  and normalized_symbol is not null
"""

RELATED_REGISTRY_QUERY = """
select registry_key, input_symbol, normalized_symbol, region, exchange, exchange_mic, currency,
       instrument_type, status, market_supported, fundamentals_supported, earnings_supported,
       validation_status, validation_reason, promotion_status, last_validated_at, notes, updated_at
from public.ticker_registry
where upper(input_symbol) = any(%s) or upper(coalesce(normalized_symbol, '')) = any(%s)
"""

ENTITY_ATTRIBUTES_QUERY = """
select entity_id, country, home_country, region, exchange, exchange_mic, currency
from feature_store.entity_attributes_static
where upper(entity_id) = any(%s)
"""


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
