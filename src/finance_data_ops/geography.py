"""Canonical country normalization and region taxonomy."""

from __future__ import annotations

import math
import re
from typing import Any

from finance_data_ops.symbology import YAHOO_SUFFIX_TO_COUNTRY, infer_country_from_listing_symbol


REGION_US = "US"
REGION_AMER = "AMER"
REGION_EU = "EU"
REGION_APAC = "APAC"
REGION_OTHER = "OTHER"

_MISSING_TOKENS = {"", "NAN", "NONE", "NULL"}
_REGION_OR_AGGREGATE_TOKENS = {
    "ALL",
    "AMER",
    "AMERICAS",
    "APAC",
    "ASIA PACIFIC",
    "EMEA",
    "EU",
    "EUROPE",
    "GLOBAL",
    "INTL",
    "INTERNATIONAL",
    "LATAM",
    "OTHER",
    "WORLD",
}

_COUNTRY_NAME_TO_ISO2 = {
    "ARGENTINA": "AR",
    "AUSTRALIA": "AU",
    "AUSTRIA": "AT",
    "BAHAMAS": "BS",
    "BELGIUM": "BE",
    "BERMUDA": "BM",
    "BRAZIL": "BR",
    "CANADA": "CA",
    "CAYMAN ISLANDS": "KY",
    "CAYMAN ISLANDS THE": "KY",
    "CHILE": "CL",
    "CHINA": "CN",
    "COLOMBIA": "CO",
    "CZECH REPUBLIC": "CZ",
    "CZECHIA": "CZ",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "GREAT BRITAIN": "GB",
    "GREECE": "GR",
    "HONG KONG": "HK",
    "HONG KONG SAR": "HK",
    "INDIA": "IN",
    "INDONESIA": "ID",
    "IRELAND": "IE",
    "ISRAEL": "IL",
    "ITALY": "IT",
    "JAPAN": "JP",
    "KOREA": "KR",
    "KOREA REPUBLIC OF": "KR",
    "KOREA REPUBLIC": "KR",
    "KOREA SOUTH": "KR",
    "LUXEMBOURG": "LU",
    "MALAYSIA": "MY",
    "MEXICO": "MX",
    "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ",
    "NORWAY": "NO",
    "PERU": "PE",
    "PHILIPPINES": "PH",
    "POLAND": "PL",
    "PORTUGAL": "PT",
    "PUERTO RICO": "PR",
    "RUSSIA": "RU",
    "RUSSIAN FEDERATION": "RU",
    "SINGAPORE": "SG",
    "SOUTH AFRICA": "ZA",
    "SOUTH KOREA": "KR",
    "SPAIN": "ES",
    "SWEDEN": "SE",
    "SWITZERLAND": "CH",
    "TAIWAN": "TW",
    "THAILAND": "TH",
    "TURKEY": "TR",
    "TURKIYE": "TR",
    "UNITED KINGDOM": "GB",
    "UNITED ARAB EMIRATES": "AE",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "URUGUAY": "UY",
    "UAE": "AE",
    "USA": "US",
}

_CODE_ALIASES = {
    "UK": "GB",
}

_US_COUNTRIES = {"US"}

_AMER_COUNTRIES = {
    "AR",
    "BM",
    "BR",
    "BS",
    "CA",
    "CL",
    "CO",
    "KY",
    "MX",
    "PE",
    "PR",
    "UY",
}

_EU_COUNTRIES = {
    "AT",
    "BE",
    "CH",
    "CZ",
    "DE",
    "DK",
    "ES",
    "FI",
    "FR",
    "GB",
    "GR",
    "IE",
    "IT",
    "LU",
    "NL",
    "NO",
    "PL",
    "PT",
    "SE",
    "TR",
}

_APAC_COUNTRIES = {
    "AU",
    "CN",
    "HK",
    "ID",
    "IN",
    "JP",
    "KR",
    "MY",
    "NZ",
    "PH",
    "SG",
    "TH",
    "TW",
}

SUFFIX_TO_COUNTRY = YAHOO_SUFFIX_TO_COUNTRY
_ISO2_COUNTRIES = (
    set(_COUNTRY_NAME_TO_ISO2.values())
    | _US_COUNTRIES
    | _AMER_COUNTRIES
    | _EU_COUNTRIES
    | _APAC_COUNTRIES
    | set(YAHOO_SUFFIX_TO_COUNTRY.values())
)


def normalize_country(raw: Any) -> str:
    """Normalize a country code or country name to ISO-2 when known."""

    token = _normalize_token(raw)
    if token in _MISSING_TOKENS:
        return ""
    if token in _REGION_OR_AGGREGATE_TOKENS:
        return ""
    if token in _CODE_ALIASES:
        return _CODE_ALIASES[token]
    if token in _COUNTRY_NAME_TO_ISO2:
        return _COUNTRY_NAME_TO_ISO2[token]
    if len(token) == 2 and token.isalpha() and token in _ISO2_COUNTRIES:
        return token
    return token


def region_for_country(country: Any) -> str:
    """Map a country code or country name to the canonical product region."""

    code = normalize_country(country)
    if code in _US_COUNTRIES:
        return REGION_US
    if code in _AMER_COUNTRIES:
        return REGION_AMER
    if code in _EU_COUNTRIES:
        return REGION_EU
    if code in _APAC_COUNTRIES:
        return REGION_APAC
    return REGION_OTHER


def infer_country_from_symbol(symbol: Any, *, default: str = "US") -> str:
    country = infer_country_from_listing_symbol(symbol)
    if country:
        return country
    return normalize_country(default)


def country_from_source_or_symbol(source_country: Any, symbol: Any, *, default: str = "US") -> str:
    """Resolve country from holdings source first, with listing suffix/default fallback."""

    country = normalize_country(source_country)
    inferred = infer_country_from_listing_symbol(symbol)
    if inferred and country == "US" and inferred != "US":
        return inferred
    if country:
        return country
    return inferred or infer_country_from_symbol(symbol, default=default)


def _normalize_token(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, float) and math.isnan(raw):
        return ""
    token = str(raw).strip().upper()
    token = re.sub(r"[^A-Z0-9]+", " ", token)
    return " ".join(token.split())
