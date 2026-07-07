from __future__ import annotations

from finance_data_ops.settings import load_settings
from finance_data_ops.validation.symbol_resolution import resolve_symbols


def test_resolve_symbols_prefers_region_env_over_registry_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    monkeypatch.setenv("DATA_OPS_SYMBOLS_US", "ENV1,ENV2")
    settings = load_settings(env={"DATA_OPS_SYMBOLS": "DEF1,DEF2"}, cache_root=tmp_path)

    monkeypatch.setattr(
        "finance_data_ops.validation.symbol_resolution.load_validated_symbols",
        lambda *args, **kwargs: ["REG1", "REG2"],
    )

    resolved = resolve_symbols(
        symbols=None,
        region="us",
        settings=settings,
        env={"DATA_OPS_SYMBOLS_US": "ENV1,ENV2"},
    )
    assert resolved == ["ENV1", "ENV2"]


def test_resolve_symbols_can_opt_into_registry_migration_fallback(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    settings = load_settings(env={"DATA_OPS_ALLOW_TICKER_REGISTRY_UNIVERSE": "true"}, cache_root=tmp_path)

    monkeypatch.setattr(
        "finance_data_ops.validation.symbol_resolution.load_validated_symbols",
        lambda *args, **kwargs: ["REG1", "REG2"],
    )

    resolved = resolve_symbols(
        symbols=None,
        region="us",
        settings=settings,
        env={},
    )
    assert resolved == ["REG1", "REG2"]


def test_resolve_symbols_fallbacks_when_region_env_empty(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    settings = load_settings(env={"DATA_OPS_SYMBOLS": "DEF1,DEF2"}, cache_root=tmp_path)

    monkeypatch.setattr(
        "finance_data_ops.validation.symbol_resolution.load_validated_symbols",
        lambda *args, **kwargs: [],
    )

    by_region = resolve_symbols(
        symbols=None,
        region="us",
        settings=settings,
        env={"DATA_OPS_SYMBOLS_US": "ENV1,ENV2"},
    )
    assert by_region == ["ENV1", "ENV2"]

    by_default = resolve_symbols(
        symbols=None,
        region="apac",
        settings=settings,
        env={},
    )
    assert by_default == ["DEF1", "DEF2"]


def test_resolve_symbols_all_combines_regions_without_duplicates(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DATA_OPS_CACHE_ROOT", str(tmp_path))
    settings = load_settings(env={"DATA_OPS_SYMBOLS": "DEF1,DEF2"}, cache_root=tmp_path)

    monkeypatch.setattr(
        "finance_data_ops.validation.symbol_resolution.load_validated_symbols",
        lambda *args, **kwargs: [],
    )

    resolved = resolve_symbols(
        symbols=None,
        region="all",
        settings=settings,
        env={
            "DATA_OPS_SYMBOLS_US": "AAPL,VGK",
            "DATA_OPS_SYMBOLS_EU": "SAP.DE,ASML.AS,VGK",
            "DATA_OPS_SYMBOLS_APAC": "TSM,ASML.AS",
        },
    )

    assert resolved == ["AAPL", "VGK", "SAP.DE", "ASML.AS", "TSM"]
