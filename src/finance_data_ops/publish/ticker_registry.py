"""Publish ticker validation registry rows."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


REGISTRY_COLUMNS = [
    "registry_key",
    "input_symbol",
    "normalized_symbol",
    "region",
    "exchange",
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
    frame["last_validated_at"] = pd.to_datetime(frame["last_validated_at"], utc=True, errors="coerce").fillna(now_utc)
    frame["updated_at"] = pd.to_datetime(frame["updated_at"], utc=True, errors="coerce").fillna(now_utc)
    frame["market_supported"] = frame["market_supported"].fillna(False).astype(bool)
    frame["fundamentals_supported"] = frame["fundamentals_supported"].fillna(False).astype(bool)
    frame["earnings_supported"] = frame["earnings_supported"].fillna(False).astype(bool)
    frame["registry_key"] = frame["registry_key"].astype(str)

    payload = frame[REGISTRY_COLUMNS].to_dict(orient="records")
    for row in payload:
        for nullable in ("exchange", "notes", "normalized_symbol"):
            if row.get(nullable) in {"", "NONE", "NULL"}:
                row[nullable] = None
    return payload


def publish_ticker_registry(
    *,
    publisher: Publisher,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = build_ticker_registry_payload(rows)
    return publisher.upsert("ticker_registry", payload, on_conflict="registry_key")
