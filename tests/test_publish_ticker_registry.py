from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.ticker_registry import (
    build_entity_attributes_static_backfill_payload,
    build_entity_attributes_static_payload,
    build_ticker_registry_payload,
    publish_ticker_registry,
)
from finance_data_ops.theme_etfs.universe import build_wave_universe_additions


class JsonbNotesPublisher:
    def __init__(self) -> None:
        self.upserts: list[dict[str, Any]] = []

    def upsert(
        self,
        table: str,
        rows: list[dict[str, Any]],
        *,
        on_conflict: str | None = None,
    ) -> dict[str, Any]:
        if table == "ticker_registry":
            for row in rows:
                notes = row.get("notes")
                if not isinstance(notes, dict):
                    raise TypeError(f"ticker_registry.notes must be a jsonb object, got {type(notes).__name__}")
        self.upserts.append({"table": table, "rows": rows, "on_conflict": on_conflict})
        return {"table": table, "status": "ok", "rows": len(rows)}

    def rpc(self, name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"status": "ok", "name": name, "args": args or {}}


def test_ticker_registry_payload_coerces_notes_to_jsonb_object() -> None:
    payload = build_ticker_registry_payload(
        [
            {
                "registry_key": "NVDA|us|NMS",
                "input_symbol": "NVDA",
                "normalized_symbol": "NVDA",
                "region": "us",
                "exchange": "NMS",
                "notes": (
                    "created_by=theme_etf_universe;wave=1;themes=ai,ai_semis;"
                    "source_etfs=AIQ,SMH;aggregate_weight=0.12340000;theme_count=2"
                ),
            },
            {
                "registry_key": "AAPL|us|NMS",
                "input_symbol": "AAPL",
                "normalized_symbol": "AAPL",
                "region": "us",
                "exchange": "NMS",
                "notes": "manual review",
            },
            {
                "registry_key": "MSFT|us|NMS",
                "input_symbol": "MSFT",
                "normalized_symbol": "MSFT",
                "region": "us",
                "exchange": "NMS",
                "notes": "",
            },
        ]
    )

    assert payload[0]["notes"] == {
        "created_by": "theme_etf_universe",
        "wave": 1,
        "themes": ["ai", "ai_semis"],
        "source_etfs": ["AIQ", "SMH"],
        "aggregate_weight": 0.1234,
        "theme_count": 2,
    }
    assert payload[1]["notes"] == {"raw": "manual review"}
    assert payload[2]["notes"] == {}


def test_entity_attributes_payload_normalizes_country_and_recomputes_region() -> None:
    payload = build_entity_attributes_static_payload(
        [
            {"input_symbol": "PAGS", "normalized_symbol": "PAGS", "region": "apac", "country": "Brazil"},
            {
                "input_symbol": "INFY.NS",
                "normalized_symbol": "INFY.NS",
                "region": "us",
                "country": "INDIA",
                "holding_name": "Infosys Ltd",
            },
            {"input_symbol": "NOKIA", "normalized_symbol": "NOKIA", "region": "apac", "country": "FINLAND"},
            {"input_symbol": "MYST", "normalized_symbol": "MYST", "region": "us", "country": "Atlantis"},
        ]
    )

    by_entity = {row["entity_id"]: row for row in payload}
    assert by_entity["PAGS"]["country"] == "BR"
    assert by_entity["PAGS"]["home_country"] == "BR"
    assert by_entity["PAGS"]["region"] == "AMER"
    assert by_entity["INFY.NS"]["country"] == "IN"
    assert by_entity["INFY.NS"]["home_country"] == "IN"
    assert by_entity["INFY.NS"]["region"] == "APAC"
    assert by_entity["INFY.NS"]["name"] == "Infosys Ltd"
    assert by_entity["NOKIA"]["country"] == "FI"
    assert by_entity["NOKIA"]["home_country"] == "FI"
    assert by_entity["NOKIA"]["region"] == "EU"
    assert by_entity["MYST"]["country"] == "ATLANTIS"
    assert by_entity["MYST"]["region"] == "OTHER"


def test_entity_attributes_backfill_payload_repairs_existing_regions() -> None:
    payload = build_entity_attributes_static_backfill_payload(
        [
            {"entity_id": "PAGS", "country": "Brazil", "region": "APAC", "exchange": "NMS", "currency": "usd"},
            {
                "entity_id": "SHOP.TO",
                "name": "Existing Shopify Inc.",
                "country": "CA",
                "region": "US",
                "exchange": "tor",
                "currency": "cad",
            },
            {"entity_id": "UNKNOWN", "country": "Atlantis", "region": "US"},
        ],
        name_by_entity={"PAGS": "PagSeguro Digital Ltd", "SHOP.TO": "Shopify Inc.", "UNKNOWN": "Mystery Co"},
    )

    by_entity = {row["entity_id"]: row for row in payload}
    assert by_entity["PAGS"]["region"] == "AMER"
    assert by_entity["PAGS"]["country"] == "BR"
    assert by_entity["PAGS"]["name"] == "PagSeguro Digital Ltd"
    assert by_entity["SHOP.TO"]["region"] == "AMER"
    assert by_entity["SHOP.TO"]["name"] == "Existing Shopify Inc."
    assert by_entity["SHOP.TO"]["exchange"] == "TOR"
    assert by_entity["SHOP.TO"]["currency"] == "CAD"
    assert by_entity["UNKNOWN"]["region"] == "OTHER"
    assert by_entity["UNKNOWN"]["name"] == "Mystery Co"


def test_entity_attributes_payload_adds_adr_home_country_separate_from_listing_country() -> None:
    payload = build_entity_attributes_static_payload(
        [
            {
                "input_symbol": "RIO",
                "normalized_symbol": "RIO",
                "region": "us",
                "exchange": "NYQ",
                "currency": "USD",
                "instrument_type": "adr",
                "country": "United Kingdom",
                "holding_name": "Rio Tinto PLC ADR",
            }
        ]
    )

    assert payload[0]["country"] == "US"
    assert payload[0]["home_country"] == "GB"
    assert payload[0]["region"] == "US"


def test_entity_attributes_backfill_normalizes_bare_symbols_and_home_country() -> None:
    payload = build_entity_attributes_static_backfill_payload(
        [
            {"entity_id": "600900", "country": "US", "holding_country": "China", "name": "China Yangtze Power"},
            {
                "entity_id": "RIO",
                "country": "United Kingdom",
                "instrument_type": "adr",
                "name": "Rio Tinto PLC ADR",
            },
        ]
    )

    by_entity = {row["entity_id"]: row for row in payload}
    assert by_entity["600900.SS"]["country"] == "CN"
    assert by_entity["600900.SS"]["home_country"] == "CN"
    assert by_entity["RIO"]["country"] == "US"
    assert by_entity["RIO"]["home_country"] == "GB"


def test_theme_universe_expansion_publish_accepts_jsonb_notes_column() -> None:
    holdings = pd.DataFrame(
        [
            {"etf_ticker": "AIQ", "holding_symbol": "AAA", "holding_name": "A", "weight": 0.05},
            {"etf_ticker": "AIQ", "holding_symbol": "BBB", "holding_name": "B", "weight": 0.04},
        ]
    )
    themes = pd.DataFrame([{"etf_ticker": "AIQ", "theme": "ai", "wave": 1}])
    registry_rows, _, summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        existing_registry=pd.DataFrame(),
        wave=1,
        max_new_tickers=2,
        batch_size=2,
        metadata_lookup=lambda _symbol: {"country": "US", "sector": "Technology"},
    )
    publisher = JsonbNotesPublisher()

    result = publish_ticker_registry(
        publisher=publisher,
        rows=registry_rows.drop(columns=["theme_ramp_batch"], errors="ignore").to_dict(orient="records"),
    )

    assert summary["new_tickers_selected"] == 2
    assert result["ticker_registry"]["status"] == "ok"
    registry_upsert = next(call for call in publisher.upserts if call["table"] == "ticker_registry")
    assert registry_upsert["on_conflict"] == "registry_key"
    assert all(isinstance(row["notes"], dict) for row in registry_upsert["rows"])
    assert registry_upsert["rows"][0]["notes"]["created_by"] == "theme_etf_universe"
