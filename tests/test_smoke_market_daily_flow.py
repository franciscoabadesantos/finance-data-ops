from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows import dataops_market_daily as market_flow
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
                    "sector": "Technology",
                    "industry": "Software - Infrastructure",
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
        if table == "source_cache.market_price_daily":
            raise RuntimeError("simulated source_cache.market_price_daily publish failure")
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

    assert table_path("source_cache.market_price_daily", cache_root=tmp_path).exists()
    assert table_path("latest_quotes", cache_root=tmp_path).exists()
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

    prices_upsert = next(call for call in publisher.upserts if call["table"] == "source_cache.market_price_daily")
    assert prices_upsert["on_conflict"] == "symbol,price_date"
    price_row = prices_upsert["rows"][0]
    assert set(price_row.keys()) == {
        "symbol",
        "price_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source_updated_at",
        "ingested_at",
    }
    assert price_row["high"] == 101.0
    assert price_row["adj_close"] == 100.5

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    asset_keys = {row["asset_key"] for row in asset_status_upsert["rows"]}
    assert "source_cache.market_price_daily" not in asset_keys
    assert "source_cache.market_price_daily:run_subset" in asset_keys
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


def test_publish_enabled_requires_database_env_without_injected_publisher(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("DATA_OPS_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("PG_DSN", raising=False)

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
        assert "DATA_OPS_DATABASE_URL" in message
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected missing database env validation error.")


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


def test_market_freshness_uses_trading_calendar_over_calendar_days() -> None:
    calendar = pd.DataFrame(
        [
            {"exchange_mic": "XNYS", "session_date": "2026-06-26", "is_trading_day": True},
            {"exchange_mic": "XNYS", "session_date": "2026-06-29", "is_trading_day": True},
        ]
    )

    state = market_flow._classify_exchange_calendar_freshness(
        last_observed_at=datetime(2026, 6, 26, 21, 0, tzinfo=UTC),
        now=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        fresh_within=timedelta(hours=26),
        tolerance=timedelta(hours=24),
        partial=False,
        failure_state="failed_hard",
        trading_calendar_frame=calendar,
        symbols=["SPY"],
    )

    assert state == market_flow.FreshnessState.STALE_WITHIN_TOLERANCE


def test_market_freshness_treats_holiday_gap_as_current_session() -> None:
    calendar = pd.DataFrame(
        [
            {"exchange_mic": "XNYS", "session_date": "2026-07-02", "is_trading_day": True},
        ]
    )

    state = market_flow._classify_exchange_calendar_freshness(
        last_observed_at=datetime(2026, 7, 2, 21, 0, tzinfo=UTC),
        now=datetime(2026, 7, 3, 18, 0, tzinfo=UTC),
        fresh_within=timedelta(hours=26),
        tolerance=timedelta(hours=24),
        partial=False,
        failure_state="failed_hard",
        trading_calendar_frame=calendar,
        symbols=["SPY"],
    )

    assert state == market_flow.FreshnessState.FRESH


def test_ticker_signal_dispatch_is_best_effort_after_market_publish(monkeypatch) -> None:
    result = market_flow._dispatch_ticker_signal_jobs_after_market_publish(
        publish_enabled=True,
        hard_failure=False,
        as_of_date=TEST_END_DATE,
    )

    assert result == {"status": "skipped", "reason": "replaced_by_daily_production_inference"}
