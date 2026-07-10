from __future__ import annotations

import pandas as pd

from finance_data_ops.identity.provider_symbols import (
    build_holding_onboarding_identities,
    resolve_holding_onboarding_identity,
)


def test_vestas_source_symbol_resolves_to_copenhagen_provider_symbol() -> None:
    identity = resolve_holding_onboarding_identity(
        source_symbol="VWS",
        source_name="VESTAS WIND SYSTEMS",
        source_country="DK",
    )

    assert identity["source_symbol"] == "VWS"
    assert identity["source_country"] == "DK"
    assert identity["provider"] == "yahoo"
    assert identity["provider_symbol"] == "VWS.CO"
    assert identity["onboard_symbol"] == "VWS.CO"
    assert identity["onboard_region"] == "eu"
    assert identity["is_onboardable"] is True
    assert identity["resolution_source"] == "known_mapping"


def test_existing_provider_symbols_and_us_bare_symbols_remain_onboardable() -> None:
    jp = resolve_holding_onboarding_identity(source_symbol="9766.T", source_country="JP")
    us = resolve_holding_onboarding_identity(source_symbol="ALK", source_country="US")

    assert jp["onboard_symbol"] == "9766.T"
    assert jp["is_onboardable"] is True
    assert us["onboard_symbol"] == "ALK"
    assert us["is_onboardable"] is True


def test_bare_non_us_unresolved_symbol_is_not_onboardable() -> None:
    identity = resolve_holding_onboarding_identity(source_symbol="ABC", source_country="ES")

    assert identity["is_onboardable"] is False
    assert identity["onboard_symbol"] == ""
    assert identity["not_onboardable_reason"] == "missing_provider_symbol"


def test_existing_hk_jp_tw_cn_numeric_normalization_remains_onboardable() -> None:
    cases = {
        ("700", "HK"): "0700.HK",
        ("6758", "JP"): "6758.T",
        ("2330", "TW"): "2330.TW",
        ("600900", "CN"): "600900.SS",
        ("000001", "CN"): "000001.SZ",
    }

    for (symbol, country), expected in cases.items():
        identity = resolve_holding_onboarding_identity(source_symbol=symbol, source_country=country)
        assert identity["onboard_symbol"] == expected
        assert identity["is_onboardable"] is True


def test_adr_preserves_listing_provider_symbol_and_home_source_country() -> None:
    identity = resolve_holding_onboarding_identity(source_symbol="RIO", source_country="GB")

    assert identity["source_country"] == "GB"
    assert identity["onboard_symbol"] == "RIO"
    assert identity["onboard_region"] == "us"
    assert identity["is_onboardable"] is True


def test_build_holding_onboarding_identities_outputs_backend_read_model() -> None:
    holdings = pd.DataFrame(
        [
            {
                "etf_ticker": "ICLN",
                "holding_symbol": "VWS",
                "holding_name": "VESTAS WIND SYSTEMS",
                "holding_country": "DK",
            }
        ]
    )
    themes = pd.DataFrame([{"etf_ticker": "ICLN", "theme": "clean_energy"}])

    frame = build_holding_onboarding_identities(holdings=holdings, etf_themes=themes)

    row = frame.iloc[0].to_dict()
    assert row["etf_ticker"] == "ICLN"
    assert row["theme"] == "clean_energy"
    assert row["source_symbol"] == "VWS"
    assert row["source_name"] == "VESTAS WIND SYSTEMS"
    assert row["source_country"] == "DK"
    assert row["onboard_symbol"] == "VWS.CO"
    assert row["is_onboardable"] is True
