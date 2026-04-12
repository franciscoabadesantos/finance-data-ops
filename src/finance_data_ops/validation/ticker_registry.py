"""Ticker validation registry persistence."""

from __future__ import annotations

from datetime import UTC, datetime
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
