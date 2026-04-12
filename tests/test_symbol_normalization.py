from __future__ import annotations

from finance_data_ops.providers.symbols import normalize_symbol_for_provider


def test_explicit_overrides_prioritize_asx_symbols() -> None:
    assert normalize_symbol_for_provider("ANZ", region="apac")[0] == "ANZ.AX"
    assert normalize_symbol_for_provider("WBC", region="apac")[0] == "WBC.AX"
    assert normalize_symbol_for_provider("WOW", region="apac")[0] == "WOW.AX"


def test_region_defaults_include_raw_symbol() -> None:
    candidates = normalize_symbol_for_provider("EGPT", region="apac")
    assert "EGPT" in candidates


def test_already_suffixed_symbol_stays_stable() -> None:
    assert normalize_symbol_for_provider("ANZ.AX", region="apac") == ["ANZ.AX"]
