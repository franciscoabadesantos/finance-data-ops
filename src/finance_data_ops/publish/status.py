"""Publish operational status/freshness/coverage surfaces."""

from __future__ import annotations

import json
from typing import Any

from finance_data_ops.publish.client import Publisher


def publish_status_surfaces(
    *,
    publisher: Publisher,
    data_source_runs: list[dict[str, Any]],
    data_asset_status: list[dict[str, Any]],
    symbol_data_coverage: list[dict[str, Any]],
) -> dict[str, Any]:
    runs_result = publisher.upsert(
        "data_source_runs",
        data_source_runs,
        on_conflict="run_id",
    )
    asset_result = publisher.upsert(
        "data_asset_status",
        data_asset_status,
        on_conflict="asset_key",
    )
    coverage_result = publisher.upsert(
        "symbol_data_coverage",
        symbol_data_coverage,
        on_conflict="ticker",
    )
    return {
        "data_source_runs": runs_result,
        "data_asset_status": asset_result,
        "symbol_data_coverage": coverage_result,
    }


def fetch_symbol_data_coverage_rows(
    *,
    database_dsn: str,
    tickers: list[str],
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    normalized = sorted({str(value).strip().upper() for value in tickers if str(value).strip()})
    if not normalized:
        return []

    dsn = str(database_dsn or "").strip()
    if not dsn:
        return []
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to read symbol coverage from Postgres.") from exc
    with psycopg.connect(dsn, connect_timeout=int(timeout_seconds), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select ticker, market_data_available, market_data_last_date,
                       fundamentals_available, fundamentals_last_date,
                       earnings_available, next_earnings_date, signal_available
                from public.symbol_data_coverage
                where ticker = any(%s)
                """,
                (normalized,),
            )
            rows = cur.fetchall()
    return [dict(row) for row in rows]
