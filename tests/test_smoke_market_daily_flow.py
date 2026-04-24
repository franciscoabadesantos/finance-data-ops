from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_market_daily import run_dataops_market_daily

TEST_RUN_NOW = datetime.now(UTC).replace(microsecond=0)
TEST_END_DATE = TEST_RUN_NOW.date().isoformat()
TEST_START_DATE = (TEST_RUN_NOW.date() - timedelta(days=1)).isoformat()


class FakeMarketProvider:
    def fetch_daily_prices(self, symbols: list[str], *, start: str, end: str) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        price_ingested_at = TEST_RUN_NOW - timedelta(hours=1)
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
                    "ingested_at": price_ingested_at,
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
                    "ingested_at": price_ingested_at,
                }
            )
        return pd.DataFrame(rows)

    def fetch_latest_quotes(self, symbols: list[str]) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        quote_timestamp = TEST_RUN_NOW
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol.upper(),
                    "quote_ts": quote_timestamp,
                    "price": 101.5,
                    "previous_close": 100.5,
                    "open": 101.0,
                    "high": 102.0,
                    "low": 100.0,
                    "volume": 1_100_000,
                    "provider": "fake",
                    "ingested_at": quote_timestamp,
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
        start=TEST_START_DATE,
        end=TEST_END_DATE,
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
    assert len(summary["asset_status"]) == 4
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

    quotes_history_upsert = next(call for call in publisher.upserts if call["table"] == "market_quotes_history")
    assert quotes_history_upsert["on_conflict"] == "ticker,fetched_at"
    history_row = quotes_history_upsert["rows"][0]
    assert set(history_row.keys()) == {
        "ticker",
        "fetched_at",
        "price",
        "change",
        "change_percent",
        "market_cap",
        "source",
    }
    assert "high" not in history_row

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    pipeline_row = next(row for row in asset_status_upsert["rows"] if row["asset_key"] == "data_ops_publish_pipeline")
    assert pipeline_row["freshness_status"] == "fresh"
    assert pipeline_row["coverage_status"] == "fresh"
    assert pipeline_row["last_success_at"] is not None
    assert "success" in str(pipeline_row["reason"])

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    assert runs_upsert["rows"]
    for row in runs_upsert["rows"]:
        assert row["run_id"]
    orchestration_row = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_market_daily")
    assert orchestration_row["status"] == "success"
    assert orchestration_row["symbols_requested"] == 2
    assert orchestration_row["symbols_succeeded"] == 2
    assert orchestration_row["symbols_failed"] == 0


def test_smoke_publish_failure_still_attempts_status(tmp_path) -> None:
    publisher = FailOncePublisher()
    summary = run_dataops_market_daily(
        symbols=["SPY", "QQQ"],
        start=TEST_START_DATE,
        end=TEST_END_DATE,
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

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    pipeline_row = next(row for row in asset_status_upsert["rows"] if row["asset_key"] == "data_ops_publish_pipeline")
    assert pipeline_row["freshness_status"] == "failed_hard"
    assert pipeline_row["coverage_status"] == "failed_hard"


def test_publish_enabled_requires_supabase_env_without_injected_publisher(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SECRET_KEY", raising=False)

    try:
        run_dataops_market_daily(
            symbols=["SPY"],
            start=TEST_START_DATE,
            end=TEST_END_DATE,
            cache_root=str(tmp_path),
            publish_enabled=True,
            provider=FakeMarketProvider(),
            publisher=None,
            raise_on_failed_hard=False,
        )
    except ValueError as exc:
        message = str(exc)
        assert "SUPABASE_URL" in message or "SUPABASE_SECRET_KEY" in message
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected missing Supabase env validation error.")


def test_market_coverage_merge_preserves_existing_fundamentals_and_earnings(tmp_path) -> None:
    publisher = RecordingPublisher()
    existing_rows = [
        {
            "ticker": "SPY",
            "fundamentals_available": True,
            "fundamentals_last_date": TEST_START_DATE,
            "earnings_available": True,
            "next_earnings_date": TEST_END_DATE,
            "signal_available": False,
        },
        {
            "ticker": "QQQ",
            "fundamentals_available": True,
            "fundamentals_last_date": TEST_START_DATE,
            "earnings_available": True,
            "next_earnings_date": TEST_END_DATE,
            "signal_available": False,
        },
    ]

    summary = run_dataops_market_daily(
        symbols=["SPY", "QQQ"],
        start=TEST_START_DATE,
        end=TEST_END_DATE,
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMarketProvider(),
        publisher=publisher,
        existing_symbol_coverage_rows=existing_rows,
        raise_on_failed_hard=True,
    )

    assert summary["publish_failures"] == []
    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    rows_by_ticker = {row["ticker"]: row for row in status_upsert["rows"]}
    assert rows_by_ticker["SPY"]["market_data_available"] is True
    assert rows_by_ticker["SPY"]["fundamentals_available"] is True
    assert rows_by_ticker["SPY"]["earnings_available"] is True
    assert rows_by_ticker["SPY"]["coverage_status"] == "fresh"
    assert rows_by_ticker["SPY"]["reason"] == "market_fundamentals_earnings_available"
    assert rows_by_ticker["SPY"]["next_earnings_date"] == TEST_END_DATE
