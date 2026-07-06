from __future__ import annotations

from finance_data_ops.symbology import (
    infer_country_from_listing_symbol,
    is_placeholder_identifier,
    normalize_listing_symbol,
    normalize_symbol_with_exchange,
    normalize_symbol_with_country,
)


def test_normalize_listing_symbol_zero_pads_hong_kong_symbols() -> None:
    assert normalize_listing_symbol("700 HK") == "0700.HK"
    assert normalize_listing_symbol("700.HK") == "0700.HK"
    assert normalize_listing_symbol("1093 HK") == "1093.HK"


def test_normalize_listing_symbol_keeps_other_foreign_suffixes_stable() -> None:
    assert normalize_listing_symbol("6763 TT") == "6763.TW"
    assert normalize_listing_symbol("NYAX IM") == "NYAX.MI"
    assert normalize_listing_symbol("6758 JP") == "6758.T"
    assert normalize_listing_symbol("000001 C2") == "000001.SZ"
    assert normalize_listing_symbol("600519 C1") == "600519.SS"
    assert normalize_listing_symbol("VOD LN") == "VOD.L"


def test_normalize_listing_symbol_resolves_bare_a_shares() -> None:
    assert normalize_listing_symbol("600900") == "600900.SS"
    assert normalize_listing_symbol("000001") == "000001.SZ"
    assert normalize_listing_symbol("002415") == "002415.SZ"
    assert normalize_listing_symbol("300750") == "300750.SZ"


def test_normalize_symbol_with_country_resolves_identifiable_bare_numeric_listings() -> None:
    assert normalize_symbol_with_country("700", "HK") == "0700.HK"
    assert normalize_symbol_with_country("6758", "JP") == "6758.T"


def test_normalize_symbol_with_exchange_uses_canonical_suffixes() -> None:
    assert normalize_symbol_with_exchange("700", "HKEX") == "0700.HK"
    assert normalize_symbol_with_exchange("NYAX", "BIT") == "NYAX.MI"


def test_placeholder_identifier_detection() -> None:
    assert is_placeholder_identifier("2670549D")
    assert not is_placeholder_identifier("CURLD")
    assert not is_placeholder_identifier("600900")


def test_infer_country_from_listing_symbol_uses_canonical_suffixes() -> None:
    assert infer_country_from_listing_symbol("700.HK") == "HK"
    assert infer_country_from_listing_symbol("NYAX.MI") == "IT"
