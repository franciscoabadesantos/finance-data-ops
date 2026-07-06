"""Canonical ticker suffix and listing-symbol normalization."""

from __future__ import annotations

import re
from typing import Any


YAHOO_SUFFIX_TO_COUNTRY = {
    ".AT": "AT",
    ".DE": "DE",
    ".E": "TR",
    ".AS": "NL",
    ".PA": "FR",
    ".LS": "PT",
    ".L": "GB",
    ".R": "RU",
    ".CO": "DK",
    ".AX": "AU",
    ".T": "JP",
    ".HK": "HK",
    ".NS": "IN",
    ".BO": "IN",
    ".SS": "CN",
    ".SZ": "CN",
    ".KS": "KR",
    ".KQ": "KR",
    ".TW": "TW",
    ".SI": "SG",
    ".KL": "MY",
    ".JK": "ID",
    ".BK": "TH",
    ".SW": "CH",
    ".ST": "SE",
    ".TO": "CA",
    ".MI": "IT",
    ".MC": "ES",
    ".OL": "NO",
}

YAHOO_SUFFIX_BY_EXCHANGE = {
    "ASX": ".AX",
    "HKEX": ".HK",
    "TSE": ".T",
    "NSE": ".NS",
    "BSE": ".BO",
    "LSE": ".L",
    "TSX": ".TO",
    "ETR": ".DE",
    "AMS": ".AS",
    "EPA": ".PA",
    "LIS": ".LS",
    "CSE": ".CO",
    "BIT": ".MI",
    "SHG": ".SS",
    "SHE": ".SZ",
}

BLOOMBERG_EXCHANGE_TO_YAHOO_SUFFIX = {
    "US": "",
    "UW": "",
    "UN": "",
    "UR": "",
    "UQ": "",
    "LN": ".L",
    "L": ".L",
    "GR": ".DE",
    "GY": ".DE",
    "GA": ".AT",
    "DE": ".DE",
    "FP": ".PA",
    "NA": ".AS",
    "SW": ".SW",
    "SE": ".ST",
    "SS": ".SS",
    "CH": ".SW",
    "HK": ".HK",
    "JP": ".T",
    "JT": ".T",
    "AU": ".AX",
    "AT": ".AX",
    "AB": ".ST",
    "C1": ".SS",
    "C2": ".SZ",
    "CN": ".TO",
    "CT": ".TO",
    "KS": ".KS",
    "KQ": ".KQ",
    "TT": ".TW",
    "TW": ".TW",
    "IT": ".MI",
    "IM": ".MI",
    "SM": ".MC",
    "DC": ".CO",
    "NO": ".OL",
}

_YAHOO_SUFFIX_COUNTRY_PAIRS = sorted(YAHOO_SUFFIX_TO_COUNTRY.items(), key=lambda item: len(item[0]), reverse=True)

_KNOWN_BARE_NUMERIC_SUFFIX = {
    "700": ".HK",
    "0700": ".HK",
    "9988": ".HK",
    "3690": ".HK",
    "6758": ".T",
    "6501": ".T",
    "6503": ".T",
    "6861": ".T",
    "7203": ".T",
    "7267": ".T",
    "7974": ".T",
    "8035": ".T",
    "8058": ".T",
    "8306": ".T",
    "9432": ".T",
}

ADR_HOME_COUNTRY_BY_SYMBOL = {
    "AEM": "CA",
    "AU": "ZA",
    "EH": "CN",
    "JKS": "CN",
    "RIO": "GB",
    "SQM": "CL",
}


def normalize_listing_symbol(raw_symbol: Any) -> str:
    """Normalize a provider/listing symbol to the canonical Yahoo-compatible form."""

    token = str(raw_symbol or "").strip().upper().replace("/", "-")
    token = re.sub(r"\s+", " ", token)
    if not token:
        return ""
    if " " in token:
        base, exchange = token.rsplit(" ", 1)
        suffix = BLOOMBERG_EXCHANGE_TO_YAHOO_SUFFIX.get(exchange)
        if suffix is not None:
            token = f"{base}{suffix}"
    if "." not in token and " " not in token:
        token = _normalize_bare_numeric_symbol(token)
    return _normalize_yahoo_suffix_symbol(token)


def normalize_symbol_with_exchange(raw_symbol: Any, exchange_code: Any) -> str:
    base = str(raw_symbol or "").strip().upper().replace("/", "-")
    exchange = str(exchange_code or "").strip().upper()
    suffix = YAHOO_SUFFIX_BY_EXCHANGE.get(exchange, "")
    return normalize_listing_symbol(f"{base}{suffix}")


def normalize_symbol_with_country(raw_symbol: Any, country: Any) -> str:
    """Normalize symbols using source country when the provider omits an exchange suffix."""

    token = normalize_listing_symbol(raw_symbol)
    if not token or "." in token or not token.isdigit():
        return token
    bare_suffix = _bare_numeric_suffix(token)
    if bare_suffix is not None:
        return normalize_listing_symbol(f"{token}{bare_suffix}")
    country_code = str(country or "").strip().upper()
    if country_code in {"HK", "HONG KONG", "HONG KONG SAR"} and 1 <= len(token) <= 4:
        return normalize_listing_symbol(f"{token}.HK")
    if country_code in {"JP", "JAPAN"} and len(token) == 4:
        return normalize_listing_symbol(f"{token}.T")
    if country_code in {"TW", "TAIWAN"} and len(token) == 4:
        return normalize_listing_symbol(f"{token}.TW")
    return token


def infer_country_from_listing_symbol(raw_symbol: Any) -> str | None:
    normalized = normalize_listing_symbol(raw_symbol)
    for suffix, country in _YAHOO_SUFFIX_COUNTRY_PAIRS:
        if normalized.endswith(suffix):
            return country
    return None


def is_placeholder_identifier(raw_symbol: Any) -> bool:
    token = str(raw_symbol or "").strip().upper()
    return bool(re.fullmatch(r"\d{4,}[A-Z]", token))


def _normalize_yahoo_suffix_symbol(token: str) -> str:
    if "." not in token:
        return token
    base, suffix = token.rsplit(".", 1)
    suffix = suffix.upper()
    base = base.strip().upper()
    if suffix == "HK" and base.isdigit():
        base = base.zfill(4)
    return f"{base}.{suffix}"


def _normalize_bare_numeric_symbol(token: str) -> str:
    if not token.isdigit() or len(token) != 6:
        suffix = _bare_numeric_suffix(token)
        return f"{token}{suffix}" if suffix is not None else token
    if token.startswith("600"):
        return f"{token}.SS"
    if token.startswith(("000", "002", "300")):
        return f"{token}.SZ"
    return token


def _bare_numeric_suffix(token: str) -> str | None:
    if not token.isdigit():
        return None
    return _KNOWN_BARE_NUMERIC_SUFFIX.get(token.lstrip("0") or token) or _KNOWN_BARE_NUMERIC_SUFFIX.get(token)
