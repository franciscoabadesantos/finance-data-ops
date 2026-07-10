"""Provider-symbol resolution for ETF/source identities.

This layer preserves source identifiers separately from provider onboarding
identities. Backend/frontier consumers should read this output and pass through
`onboard_symbol` only when `is_onboardable` is true.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.geography import normalize_country, region_for_country
from finance_data_ops.symbology import ADR_HOME_COUNTRY_BY_SYMBOL, normalize_listing_symbol, normalize_symbol_with_country


ONBOARDING_IDENTITY_COLUMNS = [
    "etf_ticker",
    "theme",
    "source_symbol",
    "source_name",
    "source_country",
    "source_exchange",
    "source_exchange_mic",
    "source_isin",
    "source_figi",
    "source_cusip",
    "canonical_entity_id",
    "normalized_entity_symbol",
    "provider",
    "provider_symbol",
    "onboard_symbol",
    "onboard_region",
    "onboard_exchange",
    "is_onboardable",
    "not_onboardable_reason",
    "resolution_source",
    "resolution_confidence",
]

_SOURCE_EXCHANGE_COLUMNS = ["source_exchange", "exchange", "holding_exchange", "listing_exchange"]
_SOURCE_MIC_COLUMNS = ["source_exchange_mic", "exchange_mic", "holding_exchange_mic", "listing_mic", "mic"]
_SOURCE_ISIN_COLUMNS = ["source_isin", "isin", "ISIN"]
_SOURCE_FIGI_COLUMNS = ["source_figi", "figi", "FIGI"]
_SOURCE_CUSIP_COLUMNS = ["source_cusip", "cusip", "CUSIP"]

_KNOWN_PROVIDER_SYMBOLS_BY_SOURCE = {
    ("VWS", "DK"): "VWS.CO",
}

_YAHOO_SUFFIX_BY_COUNTRY = {
    "AU": ".AX",
    "CA": ".TO",
    "CH": ".SW",
    "CN": ".SS",
    "DE": ".DE",
    "DK": ".CO",
    "ES": ".MC",
    "FR": ".PA",
    "GB": ".L",
    "HK": ".HK",
    "IN": ".NS",
    "IT": ".MI",
    "JP": ".T",
    "KR": ".KS",
    "NL": ".AS",
    "NO": ".OL",
    "PT": ".LS",
    "SE": ".ST",
    "SG": ".SI",
    "TR": ".E",
    "TW": ".TW",
}

_YAHOO_SUFFIX_BY_EXCHANGE = {
    "ASX": ".AX",
    "BSE": ".BO",
    "CSE": ".CO",
    "CPH": ".CO",
    "ETR": ".DE",
    "HKEX": ".HK",
    "JPX": ".T",
    "LSE": ".L",
    "NASDAQ": "",
    "NMS": "",
    "NYSE": "",
    "NYQ": "",
    "NSE": ".NS",
    "OMXCOP": ".CO",
    "SHG": ".SS",
    "SHE": ".SZ",
    "TSE": ".T",
    "TSX": ".TO",
}

_US_EXCHANGES = {"", "AMEX", "ASE", "NASDAQ", "NMS", "NYQ", "NYSE", "PCX", "US"}


def build_holding_onboarding_identities(
    *,
    holdings: pd.DataFrame,
    etf_themes: pd.DataFrame | None = None,
    entity_attributes: pd.DataFrame | None = None,
    ticker_registry: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if holdings.empty:
        return pd.DataFrame(columns=ONBOARDING_IDENTITY_COLUMNS)

    theme_by_etf = _theme_by_etf(etf_themes)
    entity_by_symbol = _entity_attributes_by_symbol(entity_attributes)
    registry_by_symbol = _registry_by_symbol(ticker_registry)
    rows: list[dict[str, Any]] = []
    for _, holding in holdings.iterrows():
        etf_ticker = _text(holding.get("etf_ticker"), upper=True)
        identity = resolve_holding_onboarding_identity(
            source_symbol=holding.get("source_symbol") or holding.get("holding_symbol"),
            source_name=holding.get("source_name") or holding.get("holding_name"),
            source_country=holding.get("source_country") or holding.get("holding_country"),
            source_exchange=_first_value(holding, _SOURCE_EXCHANGE_COLUMNS),
            source_exchange_mic=_first_value(holding, _SOURCE_MIC_COLUMNS),
            source_isin=_first_value(holding, _SOURCE_ISIN_COLUMNS),
            source_figi=_first_value(holding, _SOURCE_FIGI_COLUMNS),
            source_cusip=_first_value(holding, _SOURCE_CUSIP_COLUMNS),
            entity_attributes=entity_by_symbol,
            ticker_registry=registry_by_symbol,
        )
        identity["etf_ticker"] = etf_ticker
        identity["theme"] = theme_by_etf.get(etf_ticker, "")
        rows.append(identity)

    frame = pd.DataFrame(rows)
    for col in ONBOARDING_IDENTITY_COLUMNS:
        if col not in frame.columns:
            frame[col] = "" if col != "is_onboardable" else False
    frame = frame[ONBOARDING_IDENTITY_COLUMNS].drop_duplicates(
        subset=["etf_ticker", "source_symbol", "source_country"],
        keep="last",
    )
    return frame.reset_index(drop=True)


def resolve_holding_onboarding_identity(
    *,
    source_symbol: Any,
    source_name: Any = None,
    source_country: Any = None,
    source_exchange: Any = None,
    source_exchange_mic: Any = None,
    source_isin: Any = None,
    source_figi: Any = None,
    source_cusip: Any = None,
    entity_attributes: dict[str, dict[str, Any]] | None = None,
    ticker_registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    raw_symbol = _text(source_symbol, upper=True)
    country = normalize_country(source_country)
    exchange = _text(source_exchange, upper=True)
    exchange_mic = _text(source_exchange_mic, upper=True)
    source_name_text = _text(source_name)

    base = {
        "source_symbol": raw_symbol,
        "source_name": source_name_text,
        "source_country": country,
        "source_exchange": exchange,
        "source_exchange_mic": exchange_mic,
        "source_isin": _text(source_isin, upper=True),
        "source_figi": _text(source_figi, upper=True),
        "source_cusip": _text(source_cusip, upper=True),
        "canonical_entity_id": "",
        "normalized_entity_symbol": "",
        "provider": "yahoo",
        "provider_symbol": "",
        "onboard_symbol": "",
        "onboard_region": "",
        "onboard_exchange": "",
        "is_onboardable": False,
        "not_onboardable_reason": "",
        "resolution_source": "",
        "resolution_confidence": 0.0,
    }
    if not raw_symbol:
        return _not_onboardable(base, "missing_source_symbol", source="source_identity", confidence=0.0)

    entity_match = _match_existing_identity(raw_symbol, country, entity_attributes)
    if entity_match:
        symbol = _text(entity_match.get("provider_symbol") or entity_match.get("normalized_symbol") or entity_match.get("entity_id"), upper=True)
        if symbol:
            return _onboardable(base, symbol, source="entity_attributes_static", confidence=0.95, entity=entity_match)

    registry_match = _match_existing_identity(raw_symbol, country, ticker_registry)
    if registry_match:
        symbol = _text(registry_match.get("provider_symbol") or registry_match.get("normalized_symbol") or registry_match.get("input_symbol"), upper=True)
        if symbol:
            return _onboardable(base, symbol, source="ticker_registry", confidence=0.92, entity=registry_match)

    known = _KNOWN_PROVIDER_SYMBOLS_BY_SOURCE.get((raw_symbol, country))
    if known:
        return _onboardable(base, known, source="known_mapping", confidence=0.99)

    by_exchange = _resolve_by_exchange(raw_symbol, exchange or exchange_mic)
    if by_exchange:
        return _onboardable(base, by_exchange, source="source_exchange", confidence=0.86)

    normalized = normalize_listing_symbol(raw_symbol)
    if "." in normalized:
        return _onboardable(base, normalized, source="provider_symbol_already_qualified", confidence=0.90)

    by_country = normalize_symbol_with_country(raw_symbol, country)
    if by_country and by_country != normalized:
        return _onboardable(base, by_country, source="source_country_numeric_mapping", confidence=0.84)

    if raw_symbol in ADR_HOME_COUNTRY_BY_SYMBOL:
        return _onboardable(base, normalized, source="known_adr_listing", confidence=0.88)

    suffix = _YAHOO_SUFFIX_BY_COUNTRY.get(country)
    if suffix and _safe_non_us_alpha_suffix_candidate(raw_symbol, country):
        return _onboardable(base, normalize_listing_symbol(f"{raw_symbol}{suffix}"), source="source_country_suffix_mapping", confidence=0.78)

    if _is_us_bare_symbol(raw_symbol, country=country, exchange=exchange):
        return _onboardable(base, normalized, source="us_bare_symbol", confidence=0.75)

    if country and country != "US":
        return _not_onboardable(base, "missing_provider_symbol", source="source_country", confidence=0.35)
    return _not_onboardable(base, "ambiguous_listing", source="source_identity", confidence=0.25)


def _onboardable(
    base: dict[str, Any],
    provider_symbol: str,
    *,
    source: str,
    confidence: float,
    entity: dict[str, Any] | None = None,
) -> dict[str, Any]:
    symbol = normalize_listing_symbol(provider_symbol)
    country = normalize_country((entity or {}).get("country")) or _country_for_provider_symbol(symbol) or base["source_country"] or "US"
    base.update(
        {
            "canonical_entity_id": _text((entity or {}).get("entity_id"), upper=True),
            "normalized_entity_symbol": symbol,
            "provider_symbol": symbol,
            "onboard_symbol": symbol,
            "onboard_region": region_for_country(country).lower(),
            "onboard_exchange": _exchange_for_provider_symbol(symbol) or base["source_exchange"],
            "is_onboardable": True,
            "not_onboardable_reason": "",
            "resolution_source": source,
            "resolution_confidence": float(confidence),
        }
    )
    if not base["canonical_entity_id"]:
        base["canonical_entity_id"] = symbol
    return base


def _not_onboardable(base: dict[str, Any], reason: str, *, source: str, confidence: float) -> dict[str, Any]:
    base.update(
        {
            "not_onboardable_reason": reason,
            "resolution_source": source,
            "resolution_confidence": float(confidence),
        }
    )
    return base


def _resolve_by_exchange(raw_symbol: str, exchange: str) -> str:
    if not raw_symbol or not exchange:
        return ""
    suffix = _YAHOO_SUFFIX_BY_EXCHANGE.get(exchange)
    if suffix is None:
        return ""
    return normalize_listing_symbol(f"{raw_symbol}{suffix}")


def _safe_non_us_alpha_suffix_candidate(raw_symbol: str, country: str) -> bool:
    if country in {"", "US"}:
        return False
    if "." in raw_symbol or not raw_symbol.isalpha():
        return False
    # Avoid unsafe broad guesses for markets where share classes commonly need
    # additional class suffixes. Add explicit known mappings for those.
    return country in {"DK", "FI", "NL", "NO", "PT", "SE"}


def _is_us_bare_symbol(raw_symbol: str, *, country: str, exchange: str) -> bool:
    if "." in raw_symbol or not raw_symbol or not raw_symbol.replace("-", "").isalnum():
        return False
    return country in {"", "US"} and exchange in _US_EXCHANGES


def _country_for_provider_symbol(symbol: str) -> str:
    if symbol in ADR_HOME_COUNTRY_BY_SYMBOL:
        return "US"
    if "." not in symbol:
        return "US"
    suffix = symbol.rsplit(".", 1)[1].upper()
    return {
        "AX": "AU",
        "CO": "DK",
        "DE": "DE",
        "E": "TR",
        "HK": "HK",
        "L": "GB",
        "MI": "IT",
        "NS": "IN",
        "OL": "NO",
        "PA": "FR",
        "SS": "CN",
        "SZ": "CN",
        "T": "JP",
        "TO": "CA",
        "TW": "TW",
    }.get(suffix, "")


def _exchange_for_provider_symbol(symbol: str) -> str:
    if "." not in symbol:
        return ""
    suffix = symbol.rsplit(".", 1)[1].upper()
    return {
        "AX": "ASX",
        "CO": "CPH",
        "DE": "ETR",
        "HK": "HKEX",
        "L": "LSE",
        "MI": "BIT",
        "NS": "NSE",
        "OL": "OSL",
        "SS": "SHG",
        "SZ": "SHE",
        "T": "TSE",
        "TO": "TSX",
        "TW": "TWSE",
    }.get(suffix, "")


def _match_existing_identity(
    source_symbol: str,
    source_country: str,
    rows_by_symbol: dict[str, dict[str, Any]] | None,
) -> dict[str, Any] | None:
    if not rows_by_symbol:
        return None
    candidates = [
        source_symbol,
        normalize_symbol_with_country(source_symbol, source_country),
        normalize_listing_symbol(source_symbol),
    ]
    for candidate in candidates:
        row = rows_by_symbol.get(_text(candidate, upper=True))
        if row:
            return row
    return None


def _entity_attributes_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        payload = row.to_dict()
        for key in ("entity_id", "normalized_symbol", "provider_symbol", "ticker"):
            symbol = _text(payload.get(key), upper=True)
            if symbol:
                out[symbol] = payload
    return out


def _registry_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        payload = row.to_dict()
        status = _text(payload.get("status")).lower()
        promotion = _text(payload.get("promotion_status")).lower()
        if status and status != "active":
            continue
        if promotion and promotion not in {"validated_market_only", "validated_full"}:
            continue
        for key in ("normalized_symbol", "input_symbol", "provider_symbol"):
            symbol = _text(payload.get(key), upper=True)
            if symbol:
                out[symbol] = payload
    return out


def _theme_by_etf(frame: pd.DataFrame | None) -> dict[str, str]:
    if frame is None or frame.empty or "etf_ticker" not in frame.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in frame.iterrows():
        ticker = _text(row.get("etf_ticker"), upper=True)
        theme = _text(row.get("theme"))
        if ticker and theme and ticker not in out:
            out[ticker] = theme
    return out


def _first_value(row: pd.Series, columns: list[str]) -> Any:
    for col in columns:
        if col in row.index:
            value = row.get(col)
            if _text(value):
                return value
    return None


def _text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return ""
    return text.upper() if upper else text
