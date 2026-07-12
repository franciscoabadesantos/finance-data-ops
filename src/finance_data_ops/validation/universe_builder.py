"""Registry-driven symbol universe builders."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings

DEFAULT_REGIONS = ("us", "eu", "apac")
ALLOWED_PROMOTION_STATUSES = {"validated_market_only", "validated_full"}


def load_validated_symbols(
    region: str,
    *,
    require_market: bool = True,
    cache_root: str | Path | None = None,
    database_dsn: str | None = None,
) -> list[str]:
    normalized_region = str(region or "").strip().lower()
    if not normalized_region:
        return []

    settings = load_settings(cache_root=cache_root)
    resolved_database_dsn = str(database_dsn or settings.database_dsn).strip()

    rows: list[dict[str, Any]] = []
    if resolved_database_dsn:
        try:
            rows = _fetch_registry_rows_from_postgres(
                database_dsn=resolved_database_dsn,
                region=normalized_region,
                require_market=bool(require_market),
            )
        except Exception:
            rows = []

    if not rows:
        frame = read_parquet_table("ticker_registry", cache_root=settings.cache_root, required=False)
        if not frame.empty:
            rows = _rows_from_frame(frame)

    return _select_symbols_from_rows(
        rows=rows,
        region=normalized_region,
        require_market=bool(require_market),
    )


def load_all_region_universes(
    *,
    require_market: bool = True,
    cache_root: str | Path | None = None,
    database_dsn: str | None = None,
) -> dict[str, list[str]]:
    settings = load_settings(cache_root=cache_root)
    resolved_database_dsn = str(database_dsn or settings.database_dsn).strip()

    rows: list[dict[str, Any]] = []
    if resolved_database_dsn:
        try:
            rows = _fetch_registry_rows_from_postgres(
                database_dsn=resolved_database_dsn,
                region=None,
                require_market=bool(require_market),
            )
        except Exception:
            rows = []

    if not rows:
        frame = read_parquet_table("ticker_registry", cache_root=settings.cache_root, required=False)
        if not frame.empty:
            rows = _rows_from_frame(frame)

    universes: dict[str, list[str]] = {}
    regions = {str(value).strip().lower() for value in DEFAULT_REGIONS}
    for row in rows:
        region = str(row.get("region") or "").strip().lower()
        if region:
            regions.add(region)

    for region in sorted(regions):
        universes[region] = _select_symbols_from_rows(rows=rows, region=region, require_market=bool(require_market))
    return universes


def _fetch_registry_rows_from_postgres(
    *,
    database_dsn: str,
    region: str | None,
    require_market: bool,
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    clauses = [
        "status = 'active'",
        "promotion_status = any(%s)",
        "normalized_symbol is not null",
    ]
    params: list[Any] = [sorted(ALLOWED_PROMOTION_STATUSES)]
    if region:
        clauses.append("region = %s")
        params.append(region)
    if require_market:
        clauses.append("market_supported is true")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to read ticker registry from Postgres.") from exc
    with psycopg.connect(database_dsn, connect_timeout=int(timeout_seconds), row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select input_symbol, normalized_symbol, region, status, promotion_status, market_supported
                from public.ticker_registry
                where """ + " and ".join(clauses),
                tuple(params),
            )
            return [dict(row) for row in cur.fetchall()]


def _rows_from_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    safe = frame.copy()
    return safe.to_dict(orient="records")


def _select_symbols_from_rows(
    *,
    rows: list[dict[str, Any]],
    region: str,
    require_market: bool,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        row_region = str(row.get("region") or "").strip().lower()
        if row_region != region:
            continue
        status = str(row.get("status") or "").strip().lower()
        if status != "active":
            continue
        promotion = str(row.get("promotion_status") or "").strip().lower()
        if promotion not in ALLOWED_PROMOTION_STATUSES:
            continue
        symbol = str(row.get("normalized_symbol") or "").strip().upper()
        if not symbol or symbol in {"NAN", "NONE", "NULL"}:
            continue
        if require_market and not _coerce_bool(row.get("market_supported")):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    token = str(value).strip().lower()
    return token in {"true", "1", "yes", "y"}
