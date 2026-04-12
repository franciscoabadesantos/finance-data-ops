from __future__ import annotations

from finance_data_ops.settings import load_settings
from finance_data_ops.validation.symbol_resolution import resolve_symbols


def test_resolve_symbols_prefers_registry_over_env(monkeypatch, tmp_path) -> None:
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
    assert resolved == ["REG1", "REG2"]


def test_resolve_symbols_fallbacks_when_registry_empty(monkeypatch, tmp_path) -> None:
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
