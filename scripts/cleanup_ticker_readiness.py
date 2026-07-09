#!/usr/bin/env python
"""Plan and optionally apply controlled ticker readiness cleanup actions."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.readiness_cleanup import (
    build_ticker_readiness_cleanup_plan,
    sql_steps_for_plan,
)

DEFAULT_READINESS_TABLE = "feature_store.ticker_readiness"
DEFAULT_TECHNICAL_TABLE = "feature_store.technical_features_daily"
DEFAULT_SCORECARD_TABLE = "feature_store.scorecard_daily"
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    source = str(args.source).strip().lower()
    if source == "auto":
        source = "postgres" if settings.database_dsn else "local"

    if source == "postgres":
        registry, readiness, prices, technicals, scorecard, coverage, warnings = _read_postgres_inputs(
            database_dsn=settings.database_dsn,
            readiness_table=args.readiness_table,
            technical_table=args.technical_table,
            scorecard_table=args.scorecard_table,
        )
    else:
        registry, readiness, prices, technicals, scorecard, coverage, warnings = _read_local_inputs(
            cache_root=settings.cache_root,
            readiness_table=args.local_readiness_table,
            technical_table=args.local_technical_table,
            scorecard_table=args.local_scorecard_table,
        )

    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=registry,
        readiness_frame=readiness,
        prices_frame=prices,
        technicals_frame=technicals,
        scorecard_frame=scorecard,
        coverage_frame=coverage,
        superseded_aliases=_parse_aliases(args.alias),
        retry_allowlist=_parse_symbol_list(args.retry_allowlist),
        repair_allowlist=_parse_symbol_list(args.repair_allowlist),
        partial_price_row_threshold=args.partial_price_row_threshold,
    )
    plan["source"] = source
    plan["warnings"] = warnings
    plan["tables"] = {
        "registry": "public.ticker_registry" if source == "postgres" else "ticker_registry",
        "readiness": args.readiness_table if source == "postgres" else args.local_readiness_table,
        "prices": "source_cache.market_price_daily" if source == "postgres" else "market_price_daily",
        "technicals": args.technical_table if source == "postgres" else args.local_technical_table,
        "scorecard": args.scorecard_table if source == "postgres" else args.local_scorecard_table,
        "coverage": "public.symbol_data_coverage" if source == "postgres" else "symbol_data_coverage",
    }

    if args.apply:
        if source != "postgres":
            raise ValueError("--apply is only supported with --source postgres.")
        applied = _apply_postgres_steps(database_dsn=settings.database_dsn, steps=sql_steps_for_plan(plan))
        plan["dry_run"] = False
        plan["applied_sql_count"] = applied
    else:
        plan["dry_run"] = True
        plan["applied_sql_count"] = 0

    if args.summary:
        print(json.dumps(_summary(plan), indent=2, default=str))
    else:
        print(json.dumps(plan, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan ticker readiness cleanup actions. Dry-run is the default; writes require --apply."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", default=True, help="Preview actions only (default).")
    mode.add_argument("--apply", action="store_true", help="Execute generated cleanup SQL against Postgres.")
    parser.add_argument("--source", choices=["auto", "postgres", "local"], default="auto")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--summary", action="store_true", help="Print grouped counts and proposed actions.")
    parser.add_argument("--readiness-table", default=DEFAULT_READINESS_TABLE)
    parser.add_argument("--technical-table", default=DEFAULT_TECHNICAL_TABLE)
    parser.add_argument("--scorecard-table", default=DEFAULT_SCORECARD_TABLE)
    parser.add_argument("--local-readiness-table", default="ticker_readiness")
    parser.add_argument("--local-technical-table", default="technical_features_daily")
    parser.add_argument("--local-scorecard-table", default="scorecard_daily")
    parser.add_argument(
        "--alias",
        action="append",
        default=[],
        metavar="OLD=NEW",
        help="Additional superseded alias mapping. Built-in: 700.HK=0700.HK.",
    )
    parser.add_argument(
        "--retry-allowlist",
        default="",
        help="Comma-separated active/validated zero-data symbols to leave untouched for retry.",
    )
    parser.add_argument(
        "--repair-allowlist",
        default="",
        help="Comma-separated thin source-only symbols to classify as repairable despite the row threshold.",
    )
    parser.add_argument(
        "--partial-price-row-threshold",
        type=int,
        default=30,
        help="Price-only symbols at or above this row count are repairable; smaller source-only symbols stay review-only.",
    )
    return parser


def _summary(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": plan.get("generated_at"),
        "source": plan.get("source"),
        "dry_run": plan.get("dry_run"),
        "applied_sql_count": plan.get("applied_sql_count"),
        "tables": plan.get("tables"),
        "warnings": plan.get("warnings"),
        "issue_counts": plan.get("issue_counts"),
        "symbols": plan.get("symbols"),
        "groups": {
            issue_class: [
                {
                    "ticker": action.get("ticker"),
                    "proposed_action": action.get("proposed_action"),
                    "registry_keys": action.get("registry_keys"),
                    "superseded_by": action.get("superseded_by"),
                    "sql_preview": action.get("sql_preview"),
                    "evidence": action.get("evidence"),
                }
                for action in actions
            ]
            for issue_class, actions in (plan.get("groups") or {}).items()
        },
    }


def _read_local_inputs(
    *,
    cache_root: str | Path,
    readiness_table: str,
    technical_table: str,
    scorecard_table: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    warnings: list[str] = []
    return (
        read_parquet_table("ticker_registry", cache_root=cache_root, required=False),
        _read_local_optional_table(readiness_table, cache_root=cache_root, warnings=warnings),
        read_parquet_table("market_price_daily", cache_root=cache_root, required=False),
        _read_local_optional_table(technical_table, cache_root=cache_root, warnings=warnings),
        _read_local_optional_table(scorecard_table, cache_root=cache_root, warnings=warnings),
        read_parquet_table("symbol_data_coverage", cache_root=cache_root, required=False),
        warnings,
    )


def _read_local_optional_table(
    table_name: str,
    *,
    cache_root: str | Path,
    warnings: list[str],
) -> pd.DataFrame:
    frame = read_parquet_table(table_name, cache_root=cache_root, required=False)
    if frame.empty:
        warnings.append(f"Local optional table {table_name!r} missing or empty.")
    return frame


def _read_postgres_inputs(
    *,
    database_dsn: str,
    readiness_table: str,
    technical_table: str,
    scorecard_table: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for --source postgres.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres readiness cleanup.") from exc

    warnings: list[str] = []
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        registry = _query_known_table(
            conn,
            "public.ticker_registry",
            [
                "registry_key",
                "input_symbol",
                "normalized_symbol",
                "region",
                "exchange",
                "status",
                "validation_status",
                "validation_reason",
                "promotion_status",
                "notes",
                "updated_at",
            ],
            warnings=warnings,
        )
        readiness = _query_known_table(
            conn,
            readiness_table,
            [
                "ticker",
                "symbol",
                "entity_id",
                "readiness_state",
                "tracked_search_ready",
                "tracked",
                "is_tracked",
                "search_ready",
                "has_prices",
                "has_technicals",
                "has_fundamentals",
                "has_earnings",
                "has_scorecard",
                "is_scorecard_ready",
                "source_price_available",
                "technical_features_available",
                "scorecard_available",
                "fundamentals_available",
                "earnings_available",
                "missing_inputs",
                "updated_at",
            ],
            warnings=warnings,
        )
        prices = _query_materialized_table(
            conn,
            "source_cache.market_price_daily",
            symbol_candidates=("symbol", "ticker"),
            date_candidates=("price_date", "date", "as_of_date"),
            warnings=warnings,
        )
        technicals = _query_materialized_table(
            conn,
            technical_table,
            symbol_candidates=("ticker", "symbol"),
            date_candidates=("as_of_date", "date", "feature_date", "price_date"),
            warnings=warnings,
        )
        scorecard = _query_materialized_table(
            conn,
            scorecard_table,
            symbol_candidates=("ticker", "symbol"),
            date_candidates=("as_of_date", "date", "scorecard_date", "price_date"),
            warnings=warnings,
        )
        coverage = _query_known_table(
            conn,
            "public.symbol_data_coverage",
            [
                "ticker",
                "market_data_available",
                "fundamentals_available",
                "earnings_available",
                "signal_available",
                "market_data_last_date",
                "fundamentals_last_date",
                "coverage_status",
                "reason",
                "updated_at",
            ],
            warnings=warnings,
        )
    return registry, readiness, prices, technicals, scorecard, coverage, warnings


def _query_known_table(
    conn: Any,
    table_name: str,
    columns: list[str],
    *,
    warnings: list[str],
) -> pd.DataFrame:
    if not _valid_table_name(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    available = _table_columns(conn, table_name)
    if not available:
        warnings.append(f"Table {table_name!r} missing or inaccessible.")
        return pd.DataFrame()
    selected = [col for col in columns if col in available]
    if not selected:
        warnings.append(f"Table {table_name!r} has none of expected columns: {columns}.")
        return pd.DataFrame()
    sql = f"select {', '.join(selected)} from {_qualified_table_sql(table_name)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _query_materialized_table(
    conn: Any,
    table_name: str,
    *,
    symbol_candidates: tuple[str, ...],
    date_candidates: tuple[str, ...],
    warnings: list[str],
) -> pd.DataFrame:
    if not _valid_table_name(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    available = _table_columns(conn, table_name)
    if not available:
        warnings.append(f"Table {table_name!r} missing or inaccessible.")
        return pd.DataFrame()
    symbol_col = next((col for col in symbol_candidates if col in available), None)
    if symbol_col is None:
        warnings.append(f"Table {table_name!r} has no ticker/symbol column.")
        return pd.DataFrame()
    date_col = next((col for col in date_candidates if col in available), None)
    select_parts = [f"{symbol_col} as ticker"]
    if date_col:
        select_parts.append(f"{date_col} as as_of_date")
    sql = f"select {', '.join(select_parts)} from {_qualified_table_sql(table_name)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _apply_postgres_steps(*, database_dsn: str, steps: list[dict[str, Any]]) -> int:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for --apply.")
    if not steps:
        return 0
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres readiness cleanup.") from exc

    with psycopg.connect(database_dsn, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            for step in steps:
                cur.execute(str(step["statement"]), list(step.get("params") or []))
        conn.commit()
    return len(steps)


def _table_columns(conn: Any, table_name: str) -> set[str]:
    schema, table = _split_table_name(table_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
            (schema, table),
        )
        return {str(row["column_name"]) for row in cur.fetchall()}


def _split_table_name(table_name: str) -> tuple[str, str]:
    parts = str(table_name).split(".", maxsplit=1)
    if len(parts) == 1:
        return "public", parts[0]
    return parts[0], parts[1]


def _qualified_table_sql(table_name: str) -> str:
    schema, table = _split_table_name(table_name)
    return f'"{schema}"."{table}"'


def _valid_table_name(table_name: str) -> bool:
    return bool(TABLE_NAME_PATTERN.fullmatch(str(table_name or "").strip()))


def _parse_aliases(raw_aliases: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for raw in raw_aliases:
        if "=" not in str(raw):
            raise ValueError(f"Alias must use OLD=NEW format: {raw!r}")
        old, new = str(raw).split("=", maxsplit=1)
        old_symbol = old.strip().upper()
        new_symbol = new.strip().upper()
        if not old_symbol or not new_symbol:
            raise ValueError(f"Alias must include non-empty OLD and NEW symbols: {raw!r}")
        aliases[old_symbol] = new_symbol
    return aliases


def _parse_symbol_list(raw: str) -> set[str]:
    return {value.strip().upper() for value in str(raw or "").split(",") if value.strip()}


if __name__ == "__main__":
    main()
