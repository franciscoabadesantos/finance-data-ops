#!/usr/bin/env python3
"""Reset canonical data tables by domain in local Postgres.

Ops-only utility for wipe-and-rebuild workflows. It does not create schema and
must not be used by schedules.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableResetTarget:
    table: str


DOMAIN_TABLES: dict[str, list[TableResetTarget]] = {
    "market": [
        TableResetTarget("source_cache.market_price_daily"),
        TableResetTarget("public.ticker_market_stats_snapshot"),
        TableResetTarget("public.market_quotes_history"),
        TableResetTarget("public.market_quotes"),
        TableResetTarget("public.market_price_daily"),
    ],
    "fundamentals": [
        TableResetTarget("source_cache.fundamentals"),
        TableResetTarget("public.ticker_fundamental_summary"),
        TableResetTarget("public.ticker_fundamental_point_in_time"),
        TableResetTarget("public.market_fundamentals_v2"),
    ],
    "earnings": [
        TableResetTarget("source_cache.earnings"),
        TableResetTarget("public.market_earnings_events"),
        TableResetTarget("public.market_earnings_history"),
    ],
    "macro": [
        TableResetTarget("source_cache.macro_daily"),
        TableResetTarget("public.macro_daily"),
        TableResetTarget("public.macro_observations"),
    ],
    "release-calendar": [
        TableResetTarget("public.economic_release_calendar"),
    ],
    "calendars": [
        TableResetTarget("source_cache.calendars"),
        TableResetTarget("public.exchange_trading_calendar"),
    ],
}

STATUS_TABLES = [
    TableResetTarget("public.data_source_runs"),
    TableResetTarget("public.data_asset_status"),
    TableResetTarget("public.symbol_data_coverage"),
]
ALL_DOMAINS = tuple(DOMAIN_TABLES)


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    database_dsn = args.database_dsn or os.environ.get("DATA_OPS_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not database_dsn:
        raise RuntimeError("DATA_OPS_DATABASE_URL or DATABASE_URL must be set.")
    domains = _normalize_domains(args.domains)
    targets = [target for domain in domains for target in DOMAIN_TABLES[domain]]
    if bool(args.include_status):
        targets.extend(STATUS_TABLES)

    import psycopg

    summary: dict[str, Any] = {
        "domains": domains,
        "include_status": bool(args.include_status),
        "dry_run": bool(args.dry_run),
        "tables": [],
    }
    with psycopg.connect(database_dsn, autocommit=True) as conn:
        for target in targets:
            schema_name, table_name = _parse_table(target.table)
            rows_before = _count_rows(conn, schema_name=schema_name, table_name=table_name)
            item = {"table": target.table, "rows_before": rows_before}
            summary["tables"].append(item)
            if not args.dry_run:
                if not args.yes:
                    raise RuntimeError("Destructive reset requires --yes (or use --dry-run).")
                conn.execute(f'truncate table "{schema_name}"."{table_name}"')
                item["rows_deleted"] = rows_before
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset canonical domain data tables in local Postgres.")
    parser.add_argument("--database-dsn", default=None)
    parser.add_argument("--domains", default="all")
    parser.add_argument("--include-status", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true")
    return parser


def _normalize_domains(raw: str) -> list[str]:
    token = str(raw or "").strip().lower()
    if token in {"", "all"}:
        return list(ALL_DOMAINS)
    requested = [value.strip().lower() for value in token.split(",") if value.strip()]
    invalid = [value for value in requested if value not in DOMAIN_TABLES]
    if invalid:
        raise ValueError(f"Unsupported domain(s): {', '.join(invalid)}")
    return requested


def _count_rows(conn, *, schema_name: str, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'select count(*) from "{schema_name}"."{table_name}"')
        return int(cur.fetchone()[0])


def _parse_table(table_name: str) -> tuple[str, str]:
    parts = str(table_name).strip().split(".")
    if len(parts) == 1:
        parts = ["public", parts[0]]
    if len(parts) != 2:
        raise ValueError(f"Invalid table name: {table_name!r}")
    for part in parts:
        if not part.replace("_", "a").isalnum() or part[0].isdigit():
            raise ValueError(f"Invalid identifier in table name: {table_name!r}")
    return parts[0], parts[1]


if __name__ == "__main__":
    raise SystemExit(main())
