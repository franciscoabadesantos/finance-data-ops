"""Publish ticker validation registry rows."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from typing import Any

import pandas as pd

from finance_data_ops.geography import country_from_source_or_symbol, infer_country_from_symbol, normalize_country, region_for_country
from finance_data_ops.publish.client import Publisher
from finance_data_ops.symbology import ADR_HOME_COUNTRY_BY_SYMBOL, is_placeholder_identifier, normalize_symbol_with_country


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
        row["notes"] = _coerce_notes(row.get("notes"))
        for nullable in ("exchange", "exchange_mic", "currency", "normalized_symbol"):
            if row.get(nullable) in {"", "NONE", "NULL"}:
                row[nullable] = None
    return payload


def build_entity_attributes_static_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    registry_rows = build_ticker_registry_payload(rows)
    extras_by_entity: dict[str, dict[str, Any]] = {}
    for raw in rows:
        entity_key = str(raw.get("normalized_symbol") or raw.get("input_symbol") or "").strip().upper()
        if entity_key:
            extras_by_entity[entity_key] = dict(raw)
    now_utc = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC").isoformat()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in registry_rows:
        raw_entity_id = str(row.get("normalized_symbol") or row.get("input_symbol") or "").strip().upper()
        extras = extras_by_entity.get(raw_entity_id, {})
        entity_id = _normalize_entity_id(raw_entity_id, extras)
        if not entity_id or entity_id in {"NONE", "NULL", "NAN"} or entity_id in seen or is_placeholder_identifier(entity_id):
            continue
        seen.add(entity_id)
        country = _resolve_listing_country(entity_id, extras)
        home_country = _resolve_home_country(entity_id, extras, listing_country=country)
        out.append(
            {
                "entity_id": entity_id,
                "name": _nullable_text(extras.get("name") or extras.get("holding_name")),
                "country": country,
                "home_country": home_country,
                "region": region_for_country(country),
                "exchange": row.get("exchange"),
                "exchange_mic": row.get("exchange_mic"),
                "currency": row.get("currency"),
                "sector": extras.get("sector"),
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


def build_entity_attributes_static_backfill_payload(
    rows: list[dict[str, Any]],
    *,
    name_by_entity: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    now_utc = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC").isoformat()
    out_by_entity: dict[str, dict[str, Any]] = {}
    normalized_names = {
        str(key).strip().upper(): value
        for key, value in dict(name_by_entity or {}).items()
        if str(key).strip() and _nullable_text(value)
    }
    for raw in rows:
        raw_entity_id = str(raw.get("entity_id") or raw.get("normalized_symbol") or raw.get("input_symbol") or "")
        entity_id = _normalize_entity_id(raw_entity_id, raw)
        if not entity_id or entity_id in {"NONE", "NULL", "NAN"} or is_placeholder_identifier(entity_id):
            continue
        country = _resolve_listing_country(entity_id, raw)
        home_country = _resolve_home_country(entity_id, raw, listing_country=country)
        name = _nullable_text(raw.get("name")) or _nullable_text(normalized_names.get(entity_id))
        out_by_entity[entity_id] = {
            "entity_id": entity_id,
            "name": name,
            "country": country,
            "home_country": home_country,
            "region": region_for_country(country),
            "exchange": _nullable_text(raw.get("exchange"), upper=True),
            "exchange_mic": _nullable_text(raw.get("exchange_mic"), upper=True),
            "currency": _nullable_text(raw.get("currency"), upper=True),
            "sector": _nullable_text(raw.get("sector")),
            "updated_at": now_utc,
        }
    return list(out_by_entity.values())


def _coerce_notes(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, float) and pd.isna(value):
        return {}
    if isinstance(value, dict):
        return {str(key): inner for key, inner in value.items()}
    if not isinstance(value, str):
        return {"value": value}

    raw = value.strip()
    if not raw or raw.upper() in {"NONE", "NULL", "NAN"}:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        parsed = None
    if isinstance(parsed, dict):
        return {str(key): inner for key, inner in parsed.items()}
    if "=" not in raw:
        return {"raw": raw}

    parsed: dict[str, Any] = {}
    for part in raw.split(";"):
        token = part.strip()
        if not token or "=" not in token:
            continue
        key, raw_value = token.split("=", 1)
        normalized_key = key.strip()
        if not normalized_key:
            continue
        parsed[normalized_key] = _coerce_note_value(normalized_key, raw_value.strip())
    return parsed or {"raw": raw}


def _coerce_note_value(key: str, value: str) -> Any:
    if key in {"wave", "theme_count"}:
        try:
            return int(value)
        except ValueError:
            return value
    if key == "aggregate_weight":
        try:
            return float(value)
        except ValueError:
            return value
    if key in {"themes", "source_etfs", "candidates"}:
        return [item for item in value.split(",") if item]
    return value


def _normalize_entity_id(raw_entity_id: Any, row: dict[str, Any]) -> str:
    country = normalize_country(row.get("holding_country") or row.get("country") or row.get("home_country"))
    return normalize_symbol_with_country(raw_entity_id, country)


def _resolve_listing_country(entity_id: str, row: dict[str, Any]) -> str:
    if _is_adr_row(entity_id, row):
        return infer_country_from_symbol(entity_id)
    return country_from_source_or_symbol(row.get("holding_country") or row.get("country"), entity_id)


def _resolve_home_country(entity_id: str, row: dict[str, Any], *, listing_country: str) -> str:
    if _is_adr_row(entity_id, row):
        mapped = _mapped_adr_home_country(entity_id)
        if mapped:
            return mapped
        candidate_values = [
            row.get("holding_country"),
            row.get("issuer_country"),
            row.get("origin_country"),
            row.get("country"),
            row.get("home_country"),
            listing_country,
        ]
        for value in candidate_values:
            country = normalize_country(value)
            if country and country != "US":
                return country
        return normalize_country(
            row.get("holding_country")
            or row.get("issuer_country")
            or row.get("origin_country")
            or row.get("country")
            or row.get("home_country")
            or listing_country
        )
    return normalize_country(listing_country)


def _is_adr_row(entity_id: str, row: dict[str, Any]) -> bool:
    if _mapped_adr_home_country(entity_id):
        return True
    instrument_type = str(row.get("instrument_type") or row.get("quoteType") or row.get("quote_type") or "").upper()
    if instrument_type == "ADR":
        return True
    name = str(row.get("name") or row.get("holding_name") or "").upper()
    if any(marker in name for marker in (" ADR", " ADS", "AMERICAN DEPOSIT")):
        return True
    return False


def _mapped_adr_home_country(entity_id: str) -> str:
    return ADR_HOME_COUNTRY_BY_SYMBOL.get(str(entity_id or "").strip().upper(), "")


def _nullable_text(value: Any, *, upper: bool = False) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NONE", "NULL", "NAN"}:
        return None
    return text.upper() if upper else text
