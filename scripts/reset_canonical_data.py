#!/usr/bin/env python3
"""Reset canonical data tables by domain using Supabase REST deletes.

This is an ops-only reset utility for wipe-and-rebuild workflows.
It does not create/alter schema.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableResetTarget:
    table: str
    key_column: str


DOMAIN_TABLES: dict[str, list[TableResetTarget]] = {
    "market": [
        TableResetTarget("ticker_market_stats_snapshot", "ticker"),
        TableResetTarget("market_quotes_history", "ticker"),
        TableResetTarget("market_quotes", "ticker"),
        TableResetTarget("market_price_daily", "ticker"),
    ],
    "fundamentals": [
        TableResetTarget("ticker_fundamental_summary", "ticker"),
        TableResetTarget("market_fundamentals_v2", "ticker"),
    ],
    "earnings": [
        TableResetTarget("market_earnings_events", "ticker"),
        TableResetTarget("market_earnings_history", "ticker"),
    ],
    "macro": [
        TableResetTarget("macro_daily", "series_key"),
        TableResetTarget("macro_observations", "series_key"),
    ],
    "release-calendar": [
        TableResetTarget("economic_release_calendar", "series_key"),
    ],
}

STATUS_TABLES: list[TableResetTarget] = [
    TableResetTarget("data_source_runs", "run_id"),
    TableResetTarget("data_asset_status", "asset_key"),
    TableResetTarget("symbol_data_coverage", "ticker"),
]

ALL_DOMAINS = ("market", "fundamentals", "earnings", "macro", "release-calendar")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reset canonical domain data tables (ops-only wipe).")
    parser.add_argument(
        "--domains",
        default="all",
        help="Comma-separated domains: market,fundamentals,earnings,macro,release-calendar or all.",
    )
    parser.add_argument(
        "--include-status",
        action="store_true",
        help="Also clear data_source_runs, data_asset_status, symbol_data_coverage.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print tables/counts only. No deletes executed.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for destructive execution when not using --dry-run.",
    )
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


def _request(
    *,
    supabase_url: str,
    service_role_key: str,
    path: str,
    method: str,
    timeout_seconds: int = 60,
) -> tuple[int, dict[str, str], str]:
    url = f"{supabase_url.rstrip('/')}{path}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Prefer": "count=exact,return=representation",
    }
    request = urllib.request.Request(url=url, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            response_headers = {k.lower(): v for k, v in response.headers.items()}
            return int(response.status), response_headers, body
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {exc.code} {detail}") from exc


def _count_rows(*, supabase_url: str, service_role_key: str, table: str, key_column: str) -> int:
    query = urllib.parse.urlencode(
        {
            "select": key_column,
            key_column: "not.is.null",
            "limit": "1",
        }
    )
    _, headers, _ = _request(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
        path=f"/rest/v1/{table}?{query}",
        method="GET",
    )
    content_range = str(headers.get("content-range") or "").strip()
    # Expected format: "0-0/1234" or "*/0"
    if "/" not in content_range:
        return 0
    total = content_range.split("/")[-1].strip()
    try:
        return int(total)
    except ValueError:
        return 0


def _delete_all_rows(*, supabase_url: str, service_role_key: str, table: str, key_column: str) -> int:
    query = urllib.parse.urlencode({key_column: "not.is.null"})
    _, headers, _ = _request(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
        path=f"/rest/v1/{table}?{query}",
        method="DELETE",
    )
    content_range = str(headers.get("content-range") or "").strip()
    if "/" not in content_range:
        return 0
    total = content_range.split("/")[-1].strip()
    try:
        return int(total)
    except ValueError:
        return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    domains = _normalize_domains(args.domains)

    supabase_url = str(os.environ.get("SUPABASE_URL") or "").strip()
    service_role_key = str(os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not supabase_url or not service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")

    targets: list[TableResetTarget] = []
    for domain in domains:
        targets.extend(DOMAIN_TABLES[domain])
    if bool(args.include_status):
        targets.extend(STATUS_TABLES)

    summary: dict[str, Any] = {
        "domains": domains,
        "include_status": bool(args.include_status),
        "dry_run": bool(args.dry_run),
        "tables": [],
    }

    for target in targets:
        count = _count_rows(
            supabase_url=supabase_url,
            service_role_key=service_role_key,
            table=target.table,
            key_column=target.key_column,
        )
        summary["tables"].append(
            {
                "table": target.table,
                "key_column": target.key_column,
                "rows_before": count,
            }
        )

    if bool(args.dry_run):
        print(json.dumps(summary, indent=2))
        return 0

    if not bool(args.yes):
        raise RuntimeError("Destructive reset requires --yes (or use --dry-run).")

    for row in summary["tables"]:
        deleted = _delete_all_rows(
            supabase_url=supabase_url,
            service_role_key=service_role_key,
            table=str(row["table"]),
            key_column=str(row["key_column"]),
        )
        row["rows_deleted"] = deleted

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
