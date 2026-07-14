"""Candidate universe readers for Entity Layer V0."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.refresh.storage import read_parquet_table

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def build_candidate_universe_from_frames(
    *,
    ticker_registry: pd.DataFrame | None = None,
    ticker_readiness: pd.DataFrame | None = None,
    entity_attributes_static: pd.DataFrame | None = None,
    etf_holding_identity: pd.DataFrame | None = None,
    market_price_daily: pd.DataFrame | None = None,
    technical_features_daily: pd.DataFrame | None = None,
    symbols: list[str] | None = None,
) -> list[ListingCandidate]:
    price_symbols = _symbol_set(market_price_daily, ("symbol", "ticker"))
    technical_symbols = _symbol_set(technical_features_daily, ("symbol", "ticker"))
    by_symbol: dict[str, dict[str, Any]] = {}

    for row in _active_registry_rows(ticker_registry):
        symbol = _first_symbol(row.get("normalized_symbol"), row.get("input_symbol"), row.get("symbol"))
        if symbol:
            _merge_candidate(
                by_symbol,
                symbol,
                provider_symbol=symbol,
                exchange=_text(row.get("exchange"), upper=True),
                exchange_mic=_text(row.get("exchange_mic"), upper=True),
                country=_country_from_region(row.get("region")),
                currency=_text(row.get("currency"), upper=True),
                source="ticker_registry",
            )

    for row in _tracked_readiness_rows(ticker_readiness):
        symbol = _first_symbol(row.get("symbol"), row.get("ticker"), row.get("entity_id"))
        if symbol:
            _merge_candidate(by_symbol, symbol, provider_symbol=symbol, source="ticker_readiness")

    for row in _frame_records(entity_attributes_static):
        symbol = _first_symbol(row.get("symbol"), row.get("ticker"), row.get("entity_id"))
        if symbol:
            _merge_candidate(
                by_symbol,
                symbol,
                provider_symbol=symbol,
                name=_first_text(
                    row.get("display_name"),
                    row.get("name"),
                    row.get("legal_name"),
                    row.get("company_name"),
                    row.get("security_name"),
                ),
                source="entity_attributes_static",
            )

    for row in _frame_records(etf_holding_identity):
        symbol = _first_symbol(row.get("onboard_symbol"), row.get("provider_symbol"), row.get("source_symbol"))
        if symbol:
            _merge_candidate(
                by_symbol,
                symbol,
                provider_symbol=_first_symbol(row.get("provider_symbol"), symbol),
                exchange=_text(row.get("source_exchange") or row.get("onboard_exchange"), upper=True),
                exchange_mic=_text(row.get("source_exchange_mic"), upper=True),
                country=_text(row.get("source_country"), upper=True),
                name=_text(row.get("source_name")),
                source="etf_holding_onboarding_identity",
                metadata={
                    "source_symbol": _symbol(row.get("source_symbol")),
                    "source_isin": _text(row.get("source_isin"), upper=True),
                    "source_figi": _text(row.get("source_figi"), upper=True),
                    "source_cusip": _text(row.get("source_cusip"), upper=True),
                },
            )

    for symbol in sorted(price_symbols | technical_symbols):
        _merge_candidate(by_symbol, symbol, provider_symbol=symbol, source="materialized_rows")

    selected = {_symbol(symbol) for symbol in (symbols or []) if _symbol(symbol)}
    out: list[ListingCandidate] = []
    for symbol, row in sorted(by_symbol.items()):
        if selected and symbol not in selected:
            continue
        out.append(
            ListingCandidate(
                symbol=symbol,
                provider_symbol=_text(row.get("provider_symbol"), upper=True) or symbol,
                exchange=_text(row.get("exchange"), upper=True),
                exchange_mic=_text(row.get("exchange_mic"), upper=True),
                country=_text(row.get("country"), upper=True),
                currency=_text(row.get("currency"), upper=True),
                name=_text(row.get("name")),
                source=",".join(sorted(row.get("sources") or [])),
                has_prices=symbol in price_symbols,
                has_technicals=symbol in technical_symbols,
                metadata=dict(row.get("metadata") or {}),
            )
        )
    return out


def read_local_candidate_universe(
    *,
    cache_root: str | Path,
    symbols: list[str] | None = None,
) -> list[ListingCandidate]:
    return build_candidate_universe_from_frames(
        ticker_registry=read_parquet_table("ticker_registry", cache_root=cache_root, required=False),
        ticker_readiness=_read_first_local(
            ("feature_store.ticker_readiness", "ticker_readiness"),
            cache_root=cache_root,
        ),
        entity_attributes_static=_read_first_local(
            ("feature_store.entity_attributes_static", "entity_attributes_static"),
            cache_root=cache_root,
        ),
        etf_holding_identity=_read_first_local(
            ("public.etf_holding_onboarding_identity", "etf_holding_onboarding_identity"),
            cache_root=cache_root,
        ),
        market_price_daily=read_parquet_table("source_cache.market_price_daily", cache_root=cache_root, required=False),
        technical_features_daily=_read_first_local(
            ("feature_store.technical_features_daily", "technical_features_daily"),
            cache_root=cache_root,
        ),
        symbols=symbols,
    )


def read_postgres_candidate_universe(
    *,
    database_dsn: str,
    symbols: list[str] | None = None,
) -> list[ListingCandidate]:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for --source postgres.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres identity universe reads.") from exc

    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        return build_candidate_universe_from_frames(
            ticker_registry=_query_optional_table(
                conn,
                "public.ticker_registry",
                [
                    "input_symbol",
                    "normalized_symbol",
                    "region",
                    "exchange",
                    "exchange_mic",
                    "currency",
                    "status",
                    "promotion_status",
                    "validation_status",
                    "market_supported",
                ],
            ),
            ticker_readiness=_query_optional_table(
                conn,
                "feature_store.ticker_readiness",
                [
                    "symbol",
                    "ticker",
                    "entity_id",
                    "readiness_status",
                    "tracked_search_ready",
                    "tracked",
                    "is_tracked",
                    "market_data_available",
                    "technical_features_available",
                ],
            ),
            entity_attributes_static=_query_optional_table(
                conn,
                "feature_store.entity_attributes_static",
                [
                    "symbol",
                    "ticker",
                    "entity_id",
                    "display_name",
                    "name",
                    "legal_name",
                    "company_name",
                    "security_name",
                ],
            ),
            etf_holding_identity=_query_optional_table(
                conn,
                "public.etf_holding_onboarding_identity",
                [
                    "source_symbol",
                    "source_name",
                    "source_country",
                    "source_exchange",
                    "source_exchange_mic",
                    "source_isin",
                    "source_figi",
                    "source_cusip",
                    "provider_symbol",
                    "onboard_symbol",
                    "is_onboardable",
                ],
            ),
            market_price_daily=_query_symbol_table(conn, "source_cache.market_price_daily", ("symbol", "ticker")),
            technical_features_daily=_query_symbol_table(
                conn,
                "feature_store.technical_features_daily",
                ("symbol", "ticker"),
            ),
            symbols=symbols,
        )


def fixture_candidate_universe(symbols: list[str] | None = None) -> list[ListingCandidate]:
    rows = [
        ("SAP", "US", "USD", "NYQ", ""),
        ("SAP.DE", "DE", "EUR", "", ""),
        ("ASML", "US", "USD", "NMS", ""),
        ("ASML.AS", "NL", "EUR", "", ""),
        ("NVO", "US", "USD", "NYQ", ""),
        ("NOVO-B.CO", "DK", "DKK", "", ""),
        ("TLS", "US", "USD", "NYQ", ""),
        ("TLS.AX", "AU", "AUD", "", ""),
        ("GOOG", "US", "USD", "NMS", ""),
        ("GOOGL", "US", "USD", "NMS", ""),
        ("LEN", "US", "USD", "NYQ", ""),
        ("LENB", "US", "USD", "NYQ", ""),
    ]
    selected = {_symbol(symbol) for symbol in (symbols or []) if _symbol(symbol)}
    return [
        ListingCandidate(
            symbol=symbol,
            provider_symbol=symbol,
            country=country,
            currency=currency,
            exchange=exchange,
            exchange_mic=mic,
            source="fixtures",
            has_prices=True,
            has_technicals=True,
        )
        for symbol, country, currency, exchange, mic in rows
        if not selected or symbol in selected
    ]


def _active_registry_rows(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    rows = []
    for row in _frame_records(frame):
        status = _text(row.get("status"), upper=False).lower()
        promotion = _text(row.get("promotion_status"), upper=False).lower()
        validation = _text(row.get("validation_status"), upper=False).lower()
        market_supported = _nullable_bool(row.get("market_supported"))
        if status and status != "active":
            continue
        if promotion and promotion not in {"validated_market_only", "validated_full"}:
            continue
        if validation and validation == "rejected":
            continue
        if market_supported is False:
            continue
        rows.append(row)
    return rows


def _tracked_readiness_rows(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    rows = []
    for row in _frame_records(frame):
        if _nullable_bool(row.get("tracked_search_ready")) is True or _nullable_bool(row.get("tracked")) is True:
            rows.append(row)
            continue
        if _nullable_bool(row.get("is_tracked")) is True:
            rows.append(row)
            continue
        if _nullable_bool(row.get("market_data_available")) is True or _nullable_bool(row.get("technical_features_available")) is True:
            rows.append(row)
            continue
        status = _text(row.get("readiness_status"), upper=False).lower()
        if status and status not in {"untracked", "not_ready", "not_materialized"}:
            rows.append(row)
    return rows


def _query_optional_table(conn: Any, table_name: str, columns: list[str]) -> pd.DataFrame:
    available = _table_columns(conn, table_name)
    if not available:
        return pd.DataFrame()
    selected = [column for column in columns if column in available]
    if not selected:
        return pd.DataFrame()
    with conn.cursor() as cur:
        cur.execute(f"select {', '.join(_quote(col) for col in selected)} from {_qualified_table_sql(table_name)}")
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _read_first_local(table_names: tuple[str, ...], *, cache_root: str | Path) -> pd.DataFrame:
    for table_name in table_names:
        frame = read_parquet_table(table_name, cache_root=cache_root, required=False)
        if not frame.empty:
            return frame
    return pd.DataFrame()


def _query_symbol_table(conn: Any, table_name: str, candidates: tuple[str, ...]) -> pd.DataFrame:
    available = _table_columns(conn, table_name)
    symbol_col = next((col for col in candidates if col in available), None)
    if not symbol_col:
        return pd.DataFrame()
    with conn.cursor() as cur:
        cur.execute(f"select distinct {_quote(symbol_col)} as symbol from {_qualified_table_sql(table_name)}")
        return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _table_columns(conn: Any, table_name: str) -> set[str]:
    if not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
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


def _merge_candidate(by_symbol: dict[str, dict[str, Any]], symbol: str, **values: Any) -> None:
    row = by_symbol.setdefault(symbol, {"sources": set(), "metadata": {}})
    source = _text(values.pop("source", ""), upper=False)
    if source:
        row["sources"].add(source)
    metadata = values.pop("metadata", {}) or {}
    row["metadata"].update({key: value for key, value in metadata.items() if _text(value)})
    for key, value in values.items():
        cleaned = _text(value, upper=isinstance(value, str) and value.isupper())
        if cleaned and not row.get(key):
            row[key] = cleaned


def _symbol_set(frame: pd.DataFrame | None, candidates: tuple[str, ...]) -> set[str]:
    out: set[str] = set()
    if frame is None or frame.empty:
        return out
    for column in candidates:
        if column in frame.columns:
            out.update(_symbol(value) for value in frame[column].dropna().tolist() if _symbol(value))
            break
    return out


def _frame_records(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    return frame.to_dict(orient="records")


def _country_from_region(value: Any) -> str:
    region = _text(value, upper=False).lower()
    if region in {"us", "usa"}:
        return "US"
    return region.upper() if len(region) == 2 else ""


def _nullable_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes"}:
        return True
    if text in {"false", "f", "0", "no"}:
        return False
    return None


def _split_table_name(table_name: str) -> tuple[str, str]:
    parts = table_name.split(".", maxsplit=1)
    if len(parts) == 1:
        return "public", parts[0]
    return parts[0], parts[1]


def _qualified_table_sql(table_name: str) -> str:
    schema, table = _split_table_name(table_name)
    return f"{_quote(schema)}.{_quote(table)}"


def _quote(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _symbol(value: Any) -> str:
    return _text(value, upper=True)


def _first_symbol(*values: Any) -> str:
    for value in values:
        symbol = _symbol(value)
        if symbol:
            return symbol
    return ""


def _first_text(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text
    return ""


def _text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a"}:
        return ""
    return text.upper() if upper else text
