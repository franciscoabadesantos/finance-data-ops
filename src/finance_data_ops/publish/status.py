"""Publish operational status/freshness/coverage surfaces.

`symbol_data_coverage` is a diagnostic publication surface. It is useful for
operator visibility and per-domain refresh outcomes, but it is not the
canonical tracked-universe or product-readiness contract.
"""

from __future__ import annotations

from typing import Any

from finance_data_ops.publish.client import (
    Publisher,
    _adapt_postgres_value,
    _ordered_columns,
    _quote_ident,
    to_json_safe,
)

SYMBOL_DATA_COVERAGE_SEMANTICS = "diagnostic"


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


def replace_symbol_data_coverage_rows(
    *,
    database_dsn: str,
    rows: list[dict[str, Any]],
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Transactionally replace diagnostic symbol coverage with a full rebuild."""

    dsn = str(database_dsn or "").strip()
    if not dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required to replace symbol_data_coverage.")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres coverage publishing.") from exc

    normalized_rows = to_json_safe(rows)
    with psycopg.connect(
        dsn,
        autocommit=False,
        connect_timeout=int(timeout_seconds),
        application_name="finance-data-ops",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("truncate table public.symbol_data_coverage")
            if normalized_rows:
                columns = _ordered_columns(normalized_rows)
                column_sql = ", ".join(_quote_ident(column) for column in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                query = f"insert into public.symbol_data_coverage ({column_sql}) values ({placeholders})"
                values = [[_adapt_postgres_value(row.get(column)) for column in columns] for row in normalized_rows]
                cur.executemany(query, values)
        conn.commit()
    return {"table": "symbol_data_coverage", "status": "replaced", "rows": len(normalized_rows), "status_code": 200}


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
