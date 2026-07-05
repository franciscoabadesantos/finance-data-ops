from __future__ import annotations

import pandas as pd

from finance_data_ops.onboarding.wave_a import (
    FULL_HISTORY_START_DATE,
    build_wave_a_onboarding_payloads,
    build_wave_a_targets,
)


def test_wave_a_targets_include_itb_and_us_listed_gdx_only() -> None:
    holdings = pd.DataFrame(
        [
            {"etf_ticker": "ITB", "holding_symbol": "DHI", "holding_name": "D.R. Horton Inc", "weight": 0.15},
            {"etf_ticker": "ITB", "holding_symbol": "LEN", "holding_name": "Lennar Corp", "weight": 0.08},
            {"etf_ticker": "GDX", "holding_symbol": "NEM", "holding_name": "Newmont Corp", "weight": 0.10},
            {"etf_ticker": "GDX", "holding_symbol": "AU", "holding_name": "Anglogold Ashanti Plc", "weight": 0.05},
            {"etf_ticker": "GDX", "holding_symbol": "NST.AX", "holding_name": "Northern Star", "weight": 0.03},
            {"etf_ticker": "GDX", "holding_symbol": "EDV.L", "holding_name": "Endeavour Mining", "weight": 0.02},
            {"etf_ticker": "GDX", "holding_symbol": "2259.HK", "holding_name": "Zijin Gold", "weight": 0.01},
            {"etf_ticker": "GDX", "holding_symbol": "PE&OLES* MF", "holding_name": "Industrias Penoles", "weight": 0.01},
            {"etf_ticker": "GDX", "holding_symbol": "-USD CASH-", "holding_name": "Cash", "weight": 0.01},
            {"etf_ticker": "SMH", "holding_symbol": "NVDA", "holding_name": "Nvidia Corp", "weight": 0.18},
        ]
    )

    targets = build_wave_a_targets(holdings)

    assert targets["symbol"].tolist() == ["NEM", "AU", "DHI", "LEN"]
    by_symbol = targets.set_index("symbol").to_dict(orient="index")
    assert by_symbol["DHI"]["name"] == "D.R. Horton Inc"
    assert by_symbol["NEM"]["source_etfs"] == "GDX"
    assert "NST.AX" not in set(targets["symbol"])
    assert "2259.HK" not in set(targets["symbol"])


def test_wave_a_payloads_dedupe_existing_registry_and_populate_names() -> None:
    holdings = pd.DataFrame(
        [
            {"etf_ticker": "ITB", "holding_symbol": "DHI", "holding_name": "D.R. Horton Inc", "weight": 0.15},
            {"etf_ticker": "ITB", "holding_symbol": "LEN", "holding_name": "Lennar Corp", "weight": 0.08},
            {"etf_ticker": "GDX", "holding_symbol": "NEM", "holding_name": "Newmont Corp", "weight": 0.10},
        ]
    )
    existing = pd.DataFrame(
        [
            {
                "registry_key": "DHI|us|default",
                "input_symbol": "DHI",
                "normalized_symbol": "DHI",
                "region": "us",
                "status": "active",
                "market_supported": True,
                "promotion_status": "validated_full",
            }
        ]
    )

    payloads = build_wave_a_onboarding_payloads(holdings=holdings, existing_registry=existing)

    assert payloads.summary["target_symbols"] == 3
    assert payloads.summary["existing_symbols_skipped"] == 1
    assert payloads.summary["new_symbols_to_onboard"] == 2
    assert payloads.summary["full_history_start_date"] == FULL_HISTORY_START_DATE
    assert payloads.symbols_to_backfill == ["LEN", "NEM"]
    assert [row["normalized_symbol"] for row in payloads.registry_rows] == ["NEM", "LEN"]
    assert all(row["market_supported"] is True for row in payloads.registry_rows)
    assert all(row["validation_status"] == "validated_full" for row in payloads.registry_rows)

    names = {row["entity_id"]: row["name"] for row in payloads.entity_rows}
    assert names == {"LEN": "Lennar Corp", "NEM": "Newmont Corp"}
