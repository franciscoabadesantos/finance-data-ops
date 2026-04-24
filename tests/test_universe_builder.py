from __future__ import annotations

import pandas as pd

from finance_data_ops.refresh.storage import write_parquet_table
from finance_data_ops.validation.universe_builder import load_all_region_universes, load_validated_symbols


def test_load_validated_symbols_filters_by_registry_rules(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SECRET_KEY", raising=False)

    frame = pd.DataFrame(
        [
            {
                "registry_key": "AAPL|us|default",
                "input_symbol": "AAPL",
                "normalized_symbol": "AAPL",
                "region": "us",
                "status": "active",
                "promotion_status": "validated_full",
                "market_supported": True,
            },
            {
                "registry_key": "MSFT|us|default",
                "input_symbol": "MSFT",
                "normalized_symbol": "MSFT",
                "region": "us",
                "status": "active",
                "promotion_status": "validated_market_only",
                "market_supported": True,
            },
            {
                "registry_key": "QQQ|us|default",
                "input_symbol": "QQQ",
                "normalized_symbol": "QQQ",
                "region": "us",
                "status": "active",
                "promotion_status": "validated_market_only",
                "market_supported": False,
            },
            {
                "registry_key": "BAD1|us|default",
                "input_symbol": "BAD1",
                "normalized_symbol": "BAD1",
                "region": "us",
                "status": "rejected",
                "promotion_status": "rejected",
                "market_supported": False,
            },
            {
                "registry_key": "BAD2|us|default",
                "input_symbol": "BAD2",
                "normalized_symbol": None,
                "region": "us",
                "status": "active",
                "promotion_status": "validated_full",
                "market_supported": True,
            },
            {
                "registry_key": "ASML|eu|default",
                "input_symbol": "ASML",
                "normalized_symbol": "ASML",
                "region": "eu",
                "status": "active",
                "promotion_status": "validated_full",
                "market_supported": True,
            },
        ]
    )
    write_parquet_table(
        "ticker_registry",
        frame,
        cache_root=tmp_path,
        mode="replace",
        dedupe_subset=["registry_key"],
    )

    assert load_validated_symbols("us", require_market=True) == ["AAPL", "MSFT"]
    assert load_validated_symbols("us", require_market=False) == ["AAPL", "MSFT", "QQQ"]
    assert load_validated_symbols("eu", require_market=True) == ["ASML"]


def test_load_all_region_universes_returns_defaults_when_empty(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SECRET_KEY", raising=False)

    universes = load_all_region_universes()
    assert set(universes.keys()) == {"us", "eu", "apac"}
    assert universes["us"] == []
    assert universes["eu"] == []
    assert universes["apac"] == []
