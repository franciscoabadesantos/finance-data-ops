from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_market_daily import run_dataops_market_daily


class FakeMarketProvider:
    def fetch_daily_prices(self, symbols: list[str], *, start: str, end: str) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "date": start,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "adj_close": 100.5,
                    "volume": 1_000_000,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
                }
            )
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "date": end,
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "close": 101.5,
                    "adj_close": 101.5,
                    "volume": 1_100_000,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 11, 8, 0, tzinfo=UTC),
                }
            )
        return pd.DataFrame(rows)

    def fetch_latest_quotes(self, symbols: list[str]) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "quote_ts": datetime(2026, 4, 11, 8, 30, tzinfo=UTC),
                    "price": 101.5,
                    "previous_close": 100.5,
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "volume": 1_100_000,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 11, 8, 30, tzinfo=UTC),
                }
            )
        return pd.DataFrame(rows)


class FailOncePublisher(RecordingPublisher):
    def upsert(self, table: str, rows: list[dict[str, Any]], *, on_conflict: str | None = None) -> dict[str, Any]:
        if table == "market_quotes":
            raise RuntimeError("simulated market_quotes publish failure")
        return super().upsert(table, rows, on_conflict=on_conflict)


def test_smoke_refresh_publish_status_generation(tmp_path) -> None:
    publisher = RecordingPublisher()
    summary = run_dataops_market_daily(
        symbols=["SPY", "QQQ"],
        start="2026-04-10",
        end="2026-04-11",
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMarketProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert table_path("market_price_daily", cache_root=tmp_path).exists()
    assert table_path("market_quotes", cache_root=tmp_path).exists()
    assert table_path("ticker_market_stats_snapshot", cache_root=tmp_path).exists()
    assert summary["refresh"]["market_daily"]["status"] == "fresh"
    assert summary["refresh"]["quotes_latest"]["status"] == "fresh"
    assert summary["coverage"]["status"] == "fresh"
    assert len(summary["asset_status"]) == 3
    assert summary["publish_failures"] == []

    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    assert status_upsert["rows"]
    coverage_row = status_upsert["rows"][0]
    assert coverage_row["market_data_last_date"] is not None
    assert coverage_row["reason"] == "market_price_and_quote_available"

    quotes_upsert = next(call for call in publisher.upserts if call["table"] == "market_quotes")
    assert quotes_upsert["on_conflict"] == "ticker"
    quote_row = quotes_upsert["rows"][0]
    assert set(quote_row.keys()) == {
        "ticker",
        "name",
        "price",
        "change",
        "change_percent",
        "market_cap_text",
        "source",
        "fetched_at",
        "created_at",
        "updated_at",
    }
    assert "high" not in quote_row

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    assert runs_upsert["rows"]
    for row in runs_upsert["rows"]:
        assert row["run_id"]


def test_smoke_publish_failure_still_attempts_status(tmp_path) -> None:
    publisher = FailOncePublisher()
    summary = run_dataops_market_daily(
        symbols=["SPY", "QQQ"],
        start="2026-04-10",
        end="2026-04-11",
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMarketProvider(),
        publisher=publisher,
        raise_on_failed_hard=False,
    )

    assert summary["publish_failures"]
    assert summary["publish_failures"][0]["step"] == "prices"

    written_tables = [call["table"] for call in publisher.upserts]
    assert "data_source_runs" in written_tables
    assert "data_asset_status" in written_tables
    assert "symbol_data_coverage" in written_tables


def test_publish_enabled_requires_supabase_env_without_injected_publisher(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    try:
        run_dataops_market_daily(
            symbols=["SPY"],
            start="2026-04-10",
            end="2026-04-11",
            cache_root=str(tmp_path),
            publish_enabled=True,
            provider=FakeMarketProvider(),
            publisher=None,
            raise_on_failed_hard=False,
        )
    except ValueError as exc:
        message = str(exc)
        assert "SUPABASE_URL" in message or "SUPABASE_SERVICE_ROLE_KEY" in message
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected missing Supabase env validation error.")
