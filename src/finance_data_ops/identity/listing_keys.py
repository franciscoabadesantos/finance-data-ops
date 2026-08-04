"""Stable listing identifiers shared by holdings and identity publication.

Provider symbols are routing identifiers, not globally unique security ids. A
listing key therefore prefers durable security identifiers and only falls back
to market-qualified symbols when stronger evidence is unavailable.
"""

from __future__ import annotations

from typing import Any, Mapping


def build_listing_key(
    *,
    symbol: Any,
    provider_symbol: Any = None,
    country: Any = None,
    exchange_mic: Any = None,
    isin: Any = None,
    figi: Any = None,
) -> str:
    normalized_symbol = _token(symbol)
    normalized_provider = _token(provider_symbol)
    normalized_country = _token(country)
    normalized_mic = _token(exchange_mic)
    normalized_isin = _token(isin)
    normalized_figi = _token(figi)

    if normalized_figi:
        return f"figi:{normalized_figi}"
    if normalized_isin and normalized_mic:
        return f"isin:{normalized_isin}:mic:{normalized_mic}"
    if normalized_isin:
        return f"isin:{normalized_isin}"
    if normalized_provider:
        return f"provider:{normalized_provider}"
    if normalized_mic and normalized_symbol:
        return f"mic:{normalized_mic}:symbol:{normalized_symbol}"
    if normalized_country and normalized_symbol:
        return f"country:{normalized_country}:symbol:{normalized_symbol}"
    if normalized_symbol:
        return f"symbol:{normalized_symbol}"
    raise ValueError("A listing key requires at least one usable symbol or security identifier.")


def build_holding_listing_key(row: Mapping[str, Any]) -> str:
    """Build a listing key from either canonical or source holding columns."""

    existing_key = _existing_key(row, "holding_listing_key", "canonical_listing_key")
    if existing_key:
        return existing_key
    return build_listing_key(
        symbol=_first(row, "holding_symbol", "source_symbol", "symbol"),
        provider_symbol=_first(row, "provider_symbol", "onboard_symbol"),
        country=_first(row, "holding_country", "source_country", "country"),
        exchange_mic=_first(row, "holding_exchange_mic", "source_exchange_mic", "exchange_mic"),
        isin=_first(row, "holding_isin", "source_isin", "isin"),
        figi=_first(row, "holding_figi", "source_figi", "figi"),
    )


def _existing_key(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        token = str(value).strip()
        if token and token.upper() not in {"NAN", "NONE", "NULL", "<NA>"}:
            return token
    return ""


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if _token(value):
            return value
    return None


def _token(value: Any) -> str:
    if value is None:
        return ""
    token = str(value).strip()
    if not token or token.upper() in {"NAN", "NONE", "NULL", "<NA>"}:
        return ""
    return token.upper()
