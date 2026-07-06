from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def _load_backfill_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_foreign_symbol_backfill.py"
    spec = importlib.util.spec_from_file_location("run_foreign_symbol_backfill", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replaced_bare_entity_pairs_only_marks_confirmed_replacements() -> None:
    module = _load_backfill_module()

    assert module._replaced_bare_entity_pairs(
        [
            "1911",
            "1911.T",
            "3576",
            "3576.TW",
            "700",
            "0700.HK",
            "600900",
            "600900.SS",
            "002176",
            "002176.SZ",
            "1234",
        ]
    ) == [
        ("002176", "002176.SZ"),
        ("1911", "1911.T"),
        ("3576", "3576.TW"),
        ("600900", "600900.SS"),
        ("700", "0700.HK"),
    ]


def test_entity_backfill_uses_original_holding_country_for_stale_bare_entities() -> None:
    module = _load_backfill_module()
    original = pd.DataFrame(
        [
            {"holding_symbol": "1911", "holding_country": "Japan"},
            {"holding_symbol": "2200963D", "holding_country": "United States"},
        ]
    )
    normalized = pd.DataFrame(
        [
            {"holding_symbol": "1911.T", "holding_country": "JP"},
        ]
    )
    country_by_entity = module._holding_country_lookup(original_holdings=original, normalized_holdings=normalized)

    rows = module._entity_backfill_source_rows(
        entities=pd.DataFrame(
            [
                {"entity_id": "1911", "country": "US", "home_country": "US", "name": "Sumitomo Forestry"},
                {"entity_id": "2200963D", "country": "US", "home_country": "US", "name": "Placeholder"},
            ]
        ),
        name_by_entity={},
        country_by_entity=country_by_entity,
    )

    assert [row["entity_id"] for row in rows] == ["1911.T"]
