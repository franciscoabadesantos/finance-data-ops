"""Ticker validation registry persistence."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd

from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table


TICKER_REGISTRY_TABLE = "ticker_registry"
TICKER_REGISTRY_COLUMNS = [
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

PROMOTABLE_STATUSES = {"validated_market_only", "validated_full"}


def build_registry_key(*, input_symbol: str, region: str | None, exchange: str | None) -> str:
    symbol = str(input_symbol).strip().upper()
    region_token = str(region or "").strip().lower() or "global"
    exchange_token = str(exchange or "").strip().upper() or "default"
    return f"{symbol}|{region_token}|{exchange_token}"


def read_ticker_registry(*, cache_root: str | Path) -> pd.DataFrame:
    frame = read_parquet_table(TICKER_REGISTRY_TABLE, cache_root=cache_root, required=False)
    if frame.empty:
        return pd.DataFrame(columns=TICKER_REGISTRY_COLUMNS)
    for col in TICKER_REGISTRY_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
    frame["notes"] = frame["notes"].map(_deserialize_notes_from_storage)
    return frame[TICKER_REGISTRY_COLUMNS].copy()


def upsert_ticker_registry_rows(*, cache_root: str | Path, rows: list[dict[str, Any]]) -> Path:
    if not rows:
        frame = pd.DataFrame(columns=TICKER_REGISTRY_COLUMNS)
        return write_parquet_table(
            TICKER_REGISTRY_TABLE,
            frame,
            cache_root=cache_root,
            mode="replace",
            dedupe_subset=["registry_key"],
        )

    frame = pd.DataFrame(rows)
    now_iso = datetime.now(UTC).isoformat()
    for col in TICKER_REGISTRY_COLUMNS:
        if col not in frame.columns:
            frame[col] = None
    frame["input_symbol"] = frame["input_symbol"].astype(str).str.upper()
    frame["normalized_symbol"] = frame["normalized_symbol"].astype(str).str.upper()
    frame["region"] = frame["region"].astype(str).str.lower().replace({"none": "", "null": ""})
    frame["exchange"] = frame["exchange"].astype(str).str.upper().replace({"NONE": "", "NULL": ""})
    frame["exchange_mic"] = frame["exchange_mic"].astype(str).str.upper().replace({"NONE": "", "NULL": ""})
    frame["currency"] = frame["currency"].astype(str).str.upper().replace({"NONE": "", "NULL": ""})
    frame["last_validated_at"] = frame["last_validated_at"].fillna(now_iso)
    frame["updated_at"] = frame["updated_at"].fillna(now_iso)
    frame["notes"] = frame["notes"].map(_serialize_notes_for_storage)

    frame = frame[TICKER_REGISTRY_COLUMNS]
    return write_parquet_table(
        TICKER_REGISTRY_TABLE,
        frame,
        cache_root=cache_root,
        mode="append",
        dedupe_subset=["registry_key"],
    )


def build_pending_registry_row(
    *,
    input_symbol: str,
    region: str | None,
    exchange: str | None,
    instrument_type: str = "unknown",
) -> dict[str, Any]:
    now_iso = datetime.now(UTC).isoformat()
    normalized_input = str(input_symbol).strip().upper()
    normalized_region = str(region or "us").strip().lower()
    normalized_exchange = str(exchange).strip().upper() if exchange else None
    return {
        "registry_key": build_registry_key(
            input_symbol=normalized_input,
            region=normalized_region,
            exchange=normalized_exchange,
        ),
        "input_symbol": normalized_input,
        "normalized_symbol": None,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "exchange_mic": None,
        "currency": None,
        "instrument_type": str(instrument_type or "unknown").strip().lower() or "unknown",
        "status": "pending_validation",
        "market_supported": False,
        "fundamentals_supported": False,
        "earnings_supported": False,
        "validation_status": "pending_validation",
        "validation_reason": "pending_validation",
        "promotion_status": "pending_validation",
        "last_validated_at": now_iso,
        "notes": "created_by=ticker_onboarding",
        "updated_at": now_iso,
    }


def fetch_registry_row_by_key(
    *,
    registry_key: str,
    cache_root: str | Path,
    database_dsn: str | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any] | None:
    normalized_key = str(registry_key).strip()
    if not normalized_key:
        return None

    dsn = str(database_dsn or "").strip()
    if dsn:
        try:
            import psycopg
            from psycopg.rows import dict_row

            with psycopg.connect(dsn, connect_timeout=int(timeout_seconds), row_factory=dict_row) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"select {', '.join(TICKER_REGISTRY_COLUMNS)} from public.ticker_registry where registry_key = %s limit 1",
                        (normalized_key,),
                    )
                    row = cur.fetchone()
            if row is not None:
                return dict(row)
        except Exception:
            pass

    local = read_ticker_registry(cache_root=cache_root)
    if local.empty:
        return None
    matches = local.loc[local["registry_key"].astype(str) == normalized_key]
    if matches.empty:
        return None
    return matches.iloc[-1].to_dict()


def is_promotable_registry_row(row: dict[str, Any] | None) -> bool:
    if not row:
        return False
    status = str(row.get("status") or "").strip().lower()
    promotion = str(row.get("promotion_status") or "").strip().lower()
    symbol = str(row.get("normalized_symbol") or "").strip().upper()
    return status == "active" and promotion in PROMOTABLE_STATUSES and symbol not in {"", "NONE", "NULL", "NAN"}


def _serialize_notes_for_storage(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.upper() in {"NONE", "NULL", "NAN"}:
            return ""
        parsed = _parse_json_object(raw)
        if parsed is not None:
            return _canonical_json(parsed)
        return raw
    if isinstance(value, dict):
        return _canonical_json({str(key): inner for key, inner in value.items()})
    return _canonical_json({"value": value})


def _deserialize_notes_from_storage(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, str):
        raw = value.strip()
        if not raw or raw.upper() in {"NONE", "NULL", "NAN"}:
            return ""
        parsed = _parse_json_object(raw)
        return parsed if parsed is not None else raw
    return value


def _parse_json_object(value: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return dict(parsed) if isinstance(parsed, dict) else None


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
