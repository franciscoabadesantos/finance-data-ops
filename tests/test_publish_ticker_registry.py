from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.ticker_registry import build_ticker_registry_payload, publish_ticker_registry
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
