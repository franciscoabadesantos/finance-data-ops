"""Publish ticker validation registry rows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


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
}
COUNTRY_TO_REGION = {
    "US": "US",
    "DE": "EU",
    "NL": "EU",
    "FR": "EU",
    "PT": "EU",
    "GB": "EU",
    "DK": "EU",
    "AU": "APAC",
    "JP": "APAC",
    "HK": "APAC",
    "IN": "APAC",
    "CN": "APAC",
    "KR": "APAC",
    "TW": "APAC",
    "SG": "APAC",
    "MY": "APAC",
    "ID": "APAC",
    "TH": "APAC",
}
_SUFFIX_COUNTRY_PAIRS = sorted(SUFFIX_TO_COUNTRY.items(), key=lambda item: len(item[0]), reverse=True)


REGISTRY_COLUMNS = [
    "registry_key",
    "input_symbol",
    "normalized_symbol",
    "region",
    "exchange",
    "exchange_mic",
    "currency",
    "instrument_type",
    "status",
    "market_supported",
    "fundamentals_supported",
    "earnings_supported",
    "validation_status",
    "validation_reason",
    "promotion_status",
    "last_validated_at",
    "notes",
    "updated_at",
]


def build_ticker_registry_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    now_utc = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
    for col in REGISTRY_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
    frame["input_symbol"] = frame["input_symbol"].astype(str).str.upper()
    frame["normalized_symbol"] = frame["normalized_symbol"].astype(str).str.upper()
    frame["region"] = frame["region"].astype(str).str.lower()
    frame["exchange"] = frame["exchange"].astype(str).str.upper()
    frame["exchange_mic"] = frame["exchange_mic"].astype(str).str.upper()
    frame["currency"] = frame["currency"].astype(str).str.upper()
    frame["last_validated_at"] = pd.to_datetime(frame["last_validated_at"], utc=True, errors="coerce").fillna(now_utc)
    frame["updated_at"] = pd.to_datetime(frame["updated_at"], utc=True, errors="coerce").fillna(now_utc)
    frame["market_supported"] = frame["market_supported"].fillna(False).astype(bool)
    frame["fundamentals_supported"] = frame["fundamentals_supported"].fillna(False).astype(bool)
    frame["earnings_supported"] = frame["earnings_supported"].fillna(False).astype(bool)
    frame["registry_key"] = frame["registry_key"].astype(str)

    payload = frame[REGISTRY_COLUMNS].to_dict(orient="records")
    for row in payload:
        for nullable in ("exchange", "exchange_mic", "currency", "notes", "normalized_symbol"):
            if row.get(nullable) in {"", "NONE", "NULL"}:
                row[nullable] = None
    return payload


def build_entity_attributes_static_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    registry_rows = build_ticker_registry_payload(rows)
    now_utc = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC").isoformat()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in registry_rows:
        entity_id = str(row.get("normalized_symbol") or row.get("input_symbol") or "").strip().upper()
        if not entity_id or entity_id in {"NONE", "NULL", "NAN"} or entity_id in seen:
            continue
        seen.add(entity_id)
        country = _infer_country(entity_id)
        out.append(
            {
                "entity_id": entity_id,
                "country": country,
                "region": str(row.get("region") or COUNTRY_TO_REGION.get(country, "US")).strip().upper(),
                "exchange": row.get("exchange"),
                "exchange_mic": row.get("exchange_mic"),
                "currency": row.get("currency"),
                "sector": None,
                "updated_at": now_utc,
            }
        )
    return out


def publish_ticker_registry(
    *,
    publisher: Publisher,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = build_ticker_registry_payload(rows)
    registry_result = publisher.upsert("ticker_registry", payload, on_conflict="registry_key")
    entity_result = publisher.upsert(
        "feature_store.entity_attributes_static",
        build_entity_attributes_static_payload(rows),
        on_conflict="entity_id",
    )
    return {"ticker_registry": registry_result, "feature_store.entity_attributes_static": entity_result}


def _infer_country(symbol: str) -> str:
    normalized = str(symbol).strip().upper()
    for suffix, country in _SUFFIX_COUNTRY_PAIRS:
        if normalized.endswith(suffix):
            return country
    return "US"
