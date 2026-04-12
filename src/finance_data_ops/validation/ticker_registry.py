"""Ticker validation registry persistence."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import urllib.parse
import urllib.request
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
    frame["last_validated_at"] = frame["last_validated_at"].fillna(now_iso)
    frame["updated_at"] = frame["updated_at"].fillna(now_iso)

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
    supabase_url: str | None = None,
    service_role_key: str | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any] | None:
    normalized_key = str(registry_key).strip()
    if not normalized_key:
        return None

    url = str(supabase_url or "").strip().rstrip("/")
    key = str(service_role_key or "").strip()
    if url and key:
        try:
            params = urllib.parse.urlencode(
                {
                    "select": ",".join(TICKER_REGISTRY_COLUMNS),
                    "registry_key": f"eq.{normalized_key}",
                    "limit": "1",
                }
            )
            request = urllib.request.Request(
                url=f"{url}/rest/v1/ticker_registry?{params}",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                },
                method="GET",
            )
            with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
                raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else []
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return parsed[0]
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
