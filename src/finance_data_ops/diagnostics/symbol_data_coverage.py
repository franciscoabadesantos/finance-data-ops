"""Complete diagnostic rebuild for public.symbol_data_coverage.

The canonical product readiness surface is feature_store.ticker_readiness.
symbol_data_coverage is operational diagnostics only, so rebuilds should derive
it from current materialized source rows instead of preserving run-scoped state.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

COVERAGE_COLUMNS = [
    "ticker",
    "market_data_available",
    "fundamentals_available",
    "earnings_available",
    "signal_available",
    "market_data_last_date",
    "fundamentals_last_date",
    "next_earnings_date",
    "coverage_status",
    "reason",
    "updated_at",
]

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def build_complete_symbol_data_coverage_rows(
    *,
    prices_frame: pd.DataFrame,
    quotes_frame: pd.DataFrame | None = None,
    fundamentals_frame: pd.DataFrame | None = None,
    earnings_events_frame: pd.DataFrame | None = None,
    required_symbols: list[str] | None = None,
    as_of_date: str | date | None = None,
) -> list[dict[str, object]]:
    """Build a full diagnostic coverage snapshot from materialized source rows.

    Market coverage is intentionally price-source authoritative. Quotes can keep
    quote-only diagnostics visible, but they do not make
    `market_data_available=true` without materialized price rows.
    """

    price_stats = _stats_by_symbol(prices_frame, date_candidates=("price_date", "date", "as_of_date", "latest_date"))
    quote_stats = _stats_by_symbol(quotes_frame, date_candidates=("quote_ts", "updated_at", "fetched_at", "created_at", "latest_date"))
    fundamentals_stats = _stats_by_symbol(fundamentals_frame, date_candidates=("period_end", "report_date", "latest_date"))
    earnings_stats = _stats_by_symbol(earnings_events_frame, date_candidates=("earnings_date", "report_date", "latest_date"))
    next_earnings = _next_earnings_by_symbol(earnings_events_frame, as_of_date=as_of_date)

    if required_symbols is None:
        symbols = sorted(set(price_stats) | set(quote_stats) | set(fundamentals_stats) | set(earnings_stats))
    else:
        symbols = sorted({_normalize_symbol(value) for value in required_symbols if _normalize_symbol(value)})

    updated_at = datetime.now(UTC).isoformat()
    include_multi_domain = fundamentals_frame is not None or earnings_events_frame is not None
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        has_price = symbol in price_stats
        has_quote = symbol in quote_stats
        has_fundamentals = symbol in fundamentals_stats
        has_earnings = symbol in earnings_stats
        if include_multi_domain:
            status, reason = _multi_domain_status_reason(
                has_market_data=has_price,
                has_fundamentals=has_fundamentals,
                has_earnings=has_earnings,
            )
        else:
            status, reason = _market_status_reason(has_price=has_price, has_quote=has_quote)

        rows.append(
            {
                "ticker": symbol,
                "market_data_available": bool(has_price),
                "fundamentals_available": bool(has_fundamentals),
                "earnings_available": bool(has_earnings),
                "signal_available": False,
                "market_data_last_date": price_stats.get(symbol, {}).get("latest_date"),
                "fundamentals_last_date": fundamentals_stats.get(symbol, {}).get("latest_date"),
                "next_earnings_date": next_earnings.get(symbol),
                "coverage_status": status,
                "reason": reason,
                "updated_at": updated_at,
            }
        )
    return rows


def read_complete_symbol_data_coverage_rows_from_postgres(
    *,
    database_dsn: str,
    timeout_seconds: int = 30,
) -> list[dict[str, object]]:
    """Read a complete coverage snapshot from current Postgres materialized tables."""

    dsn = str(database_dsn or "").strip()
    if not dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for Postgres coverage rebuild.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres coverage rebuilds.") from exc

    with psycopg.connect(dsn, connect_timeout=int(timeout_seconds), row_factory=dict_row) as conn:
        prices = _read_aggregate_frame(
            conn,
            table_name="source_cache.market_price_daily",
            symbol_candidates=("symbol", "ticker"),
            date_candidates=("price_date", "date", "as_of_date"),
        )
        quotes = _read_aggregate_frame(
            conn,
            table_name="public.market_quotes",
            symbol_candidates=("ticker", "symbol"),
            date_candidates=("quote_ts", "updated_at", "fetched_at", "created_at"),
        )
        fundamentals = _read_aggregate_frame(
            conn,
            table_name="source_cache.fundamentals",
            symbol_candidates=("symbol", "ticker"),
            date_candidates=("period_end", "report_date"),
        )
        earnings = _read_earnings_aggregate_frame(
            conn,
            table_name="source_cache.earnings",
            symbol_candidates=("symbol", "ticker"),
            date_candidates=("earnings_date", "report_date"),
        )
    return build_complete_symbol_data_coverage_rows(
        prices_frame=prices,
        quotes_frame=quotes,
        fundamentals_frame=fundamentals,
        earnings_events_frame=earnings,
    )


def summarize_symbol_data_coverage_rebuild(
    *,
    rows: list[dict[str, object]],
    existing_rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    rebuilt_by_symbol = {_normalize_symbol(row.get("ticker")): row for row in rows if _normalize_symbol(row.get("ticker"))}
    existing_symbols = {
        _normalize_symbol(row.get("ticker"))
        for row in (existing_rows or [])
        if _normalize_symbol(row.get("ticker"))
    }
    price_symbols = {
        symbol
        for symbol, row in rebuilt_by_symbol.items()
        if bool(row.get("market_data_available"))
    }
    return {
        "row_count": len(rebuilt_by_symbol),
        "market_data_available_count": len(price_symbols),
        "fundamentals_available_count": sum(1 for row in rebuilt_by_symbol.values() if bool(row.get("fundamentals_available"))),
        "earnings_available_count": sum(1 for row in rebuilt_by_symbol.values() if bool(row.get("earnings_available"))),
        "stale_existing_rows_removed": sorted(existing_symbols - set(rebuilt_by_symbol)),
        "market_true_without_prices": [],
    }


def _read_aggregate_frame(
    conn: Any,
    *,
    table_name: str,
    symbol_candidates: tuple[str, ...],
    date_candidates: tuple[str, ...],
) -> pd.DataFrame:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    columns = _table_columns(conn, table_name)
    if not columns:
        return pd.DataFrame(columns=["ticker", "latest_date"])
    symbol_col = next((column for column in symbol_candidates if column in columns), None)
    if symbol_col is None:
        return pd.DataFrame(columns=["ticker", "latest_date"])
    date_col = next((column for column in date_candidates if column in columns), None)
    latest_sql = f"max({_quote_ident(date_col)})::date as latest_date" if date_col else "null::date as latest_date"
    sql = (
        f"select upper({_quote_ident(symbol_col)}::text) as ticker, {latest_sql} "
        f"from {_qualified_table_sql(table_name)} "
        f"where {_quote_ident(symbol_col)} is not null and btrim({_quote_ident(symbol_col)}::text) <> '' "
        f"group by 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _read_earnings_aggregate_frame(
    conn: Any,
    *,
    table_name: str,
    symbol_candidates: tuple[str, ...],
    date_candidates: tuple[str, ...],
) -> pd.DataFrame:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    columns = _table_columns(conn, table_name)
    if not columns:
        return pd.DataFrame(columns=["ticker", "latest_date", "next_earnings_date"])
    symbol_col = next((column for column in symbol_candidates if column in columns), None)
    if symbol_col is None:
        return pd.DataFrame(columns=["ticker", "latest_date", "next_earnings_date"])
    date_col = next((column for column in date_candidates if column in columns), None)
    if date_col is None:
        latest_sql = "null::date as latest_date"
        next_sql = "null::date as next_earnings_date"
    else:
        quoted_date = _quote_ident(date_col)
        latest_sql = f"max({quoted_date})::date as latest_date"
        next_sql = f"min({quoted_date}) filter (where {quoted_date} >= current_date)::date as next_earnings_date"
    sql = (
        f"select upper({_quote_ident(symbol_col)}::text) as ticker, {latest_sql}, {next_sql} "
        f"from {_qualified_table_sql(table_name)} "
        f"where {_quote_ident(symbol_col)} is not null and btrim({_quote_ident(symbol_col)}::text) <> '' "
        f"group by 1"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _table_columns(conn: Any, table_name: str) -> set[str]:
    schema, table = table_name.split(".", 1)
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


def _stats_by_symbol(frame: pd.DataFrame | None, *, date_candidates: tuple[str, ...]) -> dict[str, dict[str, object]]:
    if frame is None or frame.empty:
        return {}
    symbol_col = _symbol_column(frame)
    if symbol_col is None:
        return {}
    date_col = next((column for column in date_candidates if column in frame.columns), None)
    local = frame.copy()
    local["_coverage_symbol"] = local[symbol_col].map(_normalize_symbol)
    local = local.loc[local["_coverage_symbol"].astype(bool)]
    if local.empty:
        return {}
    grouped = local.groupby("_coverage_symbol", dropna=True)
    out: dict[str, dict[str, object]] = {}
    for symbol, group in grouped:
        latest_date = _latest_date(group[date_col]) if date_col else None
        out[str(symbol)] = {"latest_date": latest_date, "row_count": int(len(group.index))}
    return out


def _next_earnings_by_symbol(
    frame: pd.DataFrame | None,
    *,
    as_of_date: str | date | None,
) -> dict[str, str | None]:
    if frame is None or frame.empty:
        return {}
    symbol_col = _symbol_column(frame)
    if symbol_col is not None and "next_earnings_date" in frame.columns:
        local = frame[[symbol_col, "next_earnings_date"]].copy()
        local["_coverage_symbol"] = local[symbol_col].map(_normalize_symbol)
        local["next_earnings_date"] = pd.to_datetime(local["next_earnings_date"], errors="coerce").dt.date
        local = local.dropna(subset=["_coverage_symbol", "next_earnings_date"])
        return {
            str(row["_coverage_symbol"]): pd.Timestamp(row["next_earnings_date"]).date().isoformat()
            for row in local.to_dict(orient="records")
        }
    if symbol_col is None or "earnings_date" not in frame.columns:
        return {}
    as_of = pd.Timestamp(as_of_date).date() if as_of_date is not None else datetime.now(UTC).date()
    local = frame[[symbol_col, "earnings_date"]].copy()
    local["_coverage_symbol"] = local[symbol_col].map(_normalize_symbol)
    local["earnings_date"] = pd.to_datetime(local["earnings_date"], errors="coerce").dt.date
    local = local.dropna(subset=["_coverage_symbol", "earnings_date"])
    local = local.loc[local["earnings_date"] >= as_of]
    if local.empty:
        return {}
    next_dates = local.groupby("_coverage_symbol")["earnings_date"].min()
    return {str(symbol): pd.Timestamp(value).date().isoformat() for symbol, value in next_dates.items()}


def _latest_date(values: pd.Series) -> str | None:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).date().isoformat()


def _symbol_column(frame: pd.DataFrame) -> str | None:
    for column in ("ticker", "symbol", "normalized_symbol", "entity_id"):
        if column in frame.columns:
            return column
    return None


def _market_status_reason(*, has_price: bool, has_quote: bool) -> tuple[str, str]:
    if has_price and has_quote:
        return "fresh", "market_price_and_quote_available"
    if has_price:
        return "partial", "missing_market_quote"
    if has_quote:
        return "failed_hard", "missing_market_price_daily"
    return "failed_hard", "missing_market_price_and_quote"


def _multi_domain_status_reason(
    *,
    has_market_data: bool,
    has_fundamentals: bool,
    has_earnings: bool,
) -> tuple[str, str]:
    if has_market_data and has_fundamentals and has_earnings:
        return "fresh", "market_fundamentals_earnings_available"
    missing_components: list[str] = []
    if not has_market_data:
        missing_components.append("market_data")
    if not has_fundamentals:
        missing_components.append("fundamentals")
    if not has_earnings:
        missing_components.append("earnings")
    if len(missing_components) == 3:
        return "failed_hard", "missing_market_data_fundamentals_earnings"
    return "partial", f"missing_{'_'.join(missing_components)}"


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return ""
    return text


def _qualified_table_sql(table_name: str) -> str:
    schema, table = table_name.split(".", 1)
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def _quote_ident(value: str) -> str:
    if not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'
