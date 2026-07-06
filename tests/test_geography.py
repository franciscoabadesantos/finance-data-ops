from __future__ import annotations

from finance_data_ops.geography import country_from_source_or_symbol, infer_country_from_symbol, normalize_country, region_for_country


def test_normalize_country_accepts_full_names_and_codes() -> None:
    assert normalize_country("INDIA") == "IN"
    assert normalize_country("Finland") == "FI"
    assert normalize_country("United States of America") == "US"
    assert normalize_country("UK") == "GB"
    assert normalize_country("br") == "BR"
    assert normalize_country("Bermuda") == "BM"
    assert normalize_country("Cayman Islands") == "KY"
    assert normalize_country("Luxembourg") == "LU"
    assert normalize_country("Uruguay") == "UY"
    assert normalize_country("Korea South") == "KR"
    assert normalize_country("Turkey") == "TR"


def test_region_for_country_uses_canonical_taxonomy() -> None:
    assert region_for_country("US") == "US"
    assert region_for_country("BR") == "AMER"
    assert region_for_country("Canada") == "AMER"
    assert region_for_country("INDIA") == "APAC"
    assert region_for_country("FINLAND") == "EU"
    assert region_for_country("Atlantis") == "OTHER"
    assert region_for_country("") == "OTHER"


def test_infer_country_from_symbol_uses_canonical_suffix_map() -> None:
    assert infer_country_from_symbol("SHOP.TO") == "CA"
    assert infer_country_from_symbol("INFY.NS") == "IN"
    assert infer_country_from_symbol("NESN.SW") == "CH"
    assert infer_country_from_symbol("EBS.AT") == "AT"
    assert infer_country_from_symbol("CWENE.E") == "TR"
    assert infer_country_from_symbol("XYZ.R") == "RU"
    assert infer_country_from_symbol("AAPL") == "US"


def test_country_from_source_or_symbol_lets_foreign_numeric_suffix_beat_stale_us() -> None:
    assert country_from_source_or_symbol("US", "600900.SS") == "CN"
    assert country_from_source_or_symbol("US", "0700.HK") == "HK"
    assert country_from_source_or_symbol("US", "6758.T") == "JP"
    assert country_from_source_or_symbol("Bermuda", "AAPL") == "BM"
