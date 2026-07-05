"""Canonical country normalization and region taxonomy."""

from __future__ import annotations

import math
import re
from typing import Any


REGION_US = "US"
REGION_AMER = "AMER"
REGION_EU = "EU"
REGION_APAC = "APAC"
REGION_OTHER = "OTHER"

_MISSING_TOKENS = {"", "NAN", "NONE", "NULL"}

_COUNTRY_NAME_TO_ISO2 = {
    "ARGENTINA": "AR",
    "AUSTRALIA": "AU",
    "AUSTRIA": "AT",
    "BELGIUM": "BE",
    "BRAZIL": "BR",
    "CANADA": "CA",
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
    "MALAYSIA": "MY",
    "MEXICO": "MX",
    "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ",
    "NORWAY": "NO",
    "PERU": "PE",
    "PHILIPPINES": "PH",
    "POLAND": "PL",
    "PORTUGAL": "PT",
    "SINGAPORE": "SG",
    "SOUTH AFRICA": "ZA",
    "SOUTH KOREA": "KR",
    "SPAIN": "ES",
    "SWEDEN": "SE",
    "SWITZERLAND": "CH",
    "TAIWAN": "TW",
    "THAILAND": "TH",
    "UNITED KINGDOM": "GB",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
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

SUFFIX_TO_COUNTRY = {
    ".DE": "DE",
    ".AS": "NL",
    ".PA": "FR",
    ".LS": "PT",
    ".L": "GB",
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

_SUFFIX_COUNTRY_PAIRS = sorted(SUFFIX_TO_COUNTRY.items(), key=lambda item: len(item[0]), reverse=True)


def normalize_country(raw: Any) -> str:
    """Normalize a country code or country name to ISO-2 when known."""

    token = _normalize_token(raw)
    if token in _MISSING_TOKENS:
        return ""
    if token in _CODE_ALIASES:
        return _CODE_ALIASES[token]
    if token in _COUNTRY_NAME_TO_ISO2:
        return _COUNTRY_NAME_TO_ISO2[token]
    if len(token) == 2 and token.isalpha():
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
    normalized = str(symbol or "").strip().upper()
    for suffix, country in _SUFFIX_COUNTRY_PAIRS:
        if normalized.endswith(suffix):
            return country
    return normalize_country(default)


def _normalize_token(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, float) and math.isnan(raw):
        return ""
    token = str(raw).strip().upper()
    token = re.sub(r"[^A-Z0-9]+", " ", token)
    return " ".join(token.split())
