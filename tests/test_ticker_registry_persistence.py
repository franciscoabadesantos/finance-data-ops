from __future__ import annotations

import json

from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.validation.ticker_registry import read_ticker_registry, upsert_ticker_registry_rows


def _registry_row(symbol: str, *, notes: object) -> dict[str, object]:
    return {
        "registry_key": f"{symbol}|us|default",
        "input_symbol": symbol,
        "normalized_symbol": symbol,
        "region": "us",
        "exchange": None,
        "instrument_type": "equity",
        "status": "pending_validation",
        "market_supported": False,
        "fundamentals_supported": False,
        "earnings_supported": False,
        "validation_status": "pending_validation",
        "validation_reason": "pending_validation",
        "promotion_status": "pending_validation",
        "notes": notes,
    }


def test_ticker_registry_upsert_serializes_dict_notes_for_parquet(tmp_path) -> None:
    upsert_ticker_registry_rows(
        cache_root=tmp_path,
        rows=[
            _registry_row(
                "GSAT",
                notes={
                    "lifecycle_state": "pending_validation",
                    "onboarding_run_name": "onboard-gsat-us-default",
                },
            )
        ],
    )

    raw = read_parquet_table("ticker_registry", cache_root=tmp_path, required=True)
    stored_notes = raw.iloc[0]["notes"]
    assert isinstance(stored_notes, str)
    assert json.loads(stored_notes) == {
        "lifecycle_state": "pending_validation",
        "onboarding_run_name": "onboard-gsat-us-default",
    }

    registry = read_ticker_registry(cache_root=tmp_path)
    assert registry.iloc[0]["notes"] == {
        "lifecycle_state": "pending_validation",
        "onboarding_run_name": "onboard-gsat-us-default",
    }


def test_ticker_registry_upsert_preserves_plain_string_notes(tmp_path) -> None:
    upsert_ticker_registry_rows(
        cache_root=tmp_path,
        rows=[_registry_row("U", notes="created_by=ticker_onboarding")],
    )

    raw = read_parquet_table("ticker_registry", cache_root=tmp_path, required=True)
    assert raw.iloc[0]["notes"] == "created_by=ticker_onboarding"

    registry = read_ticker_registry(cache_root=tmp_path)
    assert registry.iloc[0]["notes"] == "created_by=ticker_onboarding"
