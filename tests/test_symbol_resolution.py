from __future__ import annotations

import pytest

from finance_data_ops.settings import load_settings
from finance_data_ops.validation.symbol_resolution import normalize_refresh_region, resolve_source_refresh_universe


REGISTRY_ROWS = [
    {
        "normalized_symbol": "AAPL",
        "region": "us",
        "status": "active",
        "promotion_status": "validated_full",
        "market_supported": True,
    },
    {
        "normalized_symbol": "NOVO-B.CO",
        "region": "dk",
        "status": "active",
        "promotion_status": "validated_market_only",
        "market_supported": True,
    },
    {
        "normalized_symbol": "7203.T",
        "region": "jp",
        "status": "active",
        "promotion_status": "validated_full",
        "market_supported": True,
    },
    {
        "normalized_symbol": "BHP.AX",
        "region": "au",
        "status": "active",
        "promotion_status": "validated_full",
        "market_supported": True,
    },
    {
        "normalized_symbol": "PENDING",
        "region": "us",
        "status": "pending_validation",
        "promotion_status": "pending_validation",
        "market_supported": True,
    },
    {
        "normalized_symbol": "NOPE",
        "region": "us",
        "status": "active",
        "promotion_status": "validated_full",
        "market_supported": False,
    },
]


def test_explicit_symbols_override_everything(tmp_path) -> None:
    settings = load_settings(cache_root=tmp_path)

    universe = resolve_source_refresh_universe(
        symbols=["spy", "qqq"],
        region="us",
        settings=settings,
        env={"DATA_OPS_SYMBOLS_OVERRIDE_US": "ENV1,ENV2"},
        registry_rows=REGISTRY_ROWS,
    )

    assert universe.symbols == ["SPY", "QQQ"]
    assert universe.source == "explicit_symbols"
    assert universe.scope == "run_subset"


def test_env_override_wins_before_registry(tmp_path) -> None:
    settings = load_settings(cache_root=tmp_path)

    universe = resolve_source_refresh_universe(
        symbols=None,
        region="eu",
        settings=settings,
        env={"DATA_OPS_SYMBOLS_OVERRIDE_EU": "SAP.DE,ASML.AS"},
        registry_rows=REGISTRY_ROWS,
    )

    assert universe.symbols == ["SAP.DE", "ASML.AS"]
    assert universe.source == "env_override"
    assert universe.scope == "run_subset"


def test_registry_universe_by_schedule_region(tmp_path) -> None:
    settings = load_settings(cache_root=tmp_path)

    us = resolve_source_refresh_universe(symbols=None, region="us", settings=settings, registry_rows=REGISTRY_ROWS)
    eu = resolve_source_refresh_universe(symbols=None, region="eu", settings=settings, registry_rows=REGISTRY_ROWS)
    apac = resolve_source_refresh_universe(symbols=None, region="apac", settings=settings, registry_rows=REGISTRY_ROWS)

    assert us.symbols == ["AAPL"]
    assert us.source == "ticker_registry"
    assert us.scope == "region:us"
    assert eu.symbols == ["NOVO-B.CO"]
    assert eu.scope == "region:eu"
    assert apac.symbols == ["7203.T", "BHP.AX"]
    assert apac.scope == "region:apac"


def test_non_canonical_registry_regions_map_to_schedule_regions() -> None:
    assert normalize_refresh_region("dk") == "eu"
    assert normalize_refresh_region("jp") == "apac"
    assert normalize_refresh_region("au") == "apac"
    assert normalize_refresh_region("AMER") == "us"
    assert normalize_refresh_region("BR") == "us"
    assert normalize_refresh_region("CA") == "us"
    assert normalize_refresh_region("IL") == "apac"


def test_registry_read_failure_without_override_is_clear(tmp_path) -> None:
    settings = load_settings(cache_root=tmp_path)

    with pytest.raises(RuntimeError, match="DATA_OPS_DATABASE_URL"):
        resolve_source_refresh_universe(symbols=None, region="us", settings=settings, env={})
