from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import table_path
from flows.dataops_earnings_daily import _build_asset_status_rows, run_dataops_earnings_daily


class FakeEarningsProvider:
    def fetch_symbol_earnings(self, symbol: str, *, history_limit: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = str(symbol).strip().upper()
        events = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "earnings_date": "2026-05-01",
                    "earnings_time": "amc",
                    "fiscal_period": "2026Q1",
                    "estimate_eps": 5.1,
                    "estimate_revenue": 100.0,
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 11, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 11, 0, tzinfo=UTC),
                }
            ]
        )
        history = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "earnings_date": "2026-02-01",
                    "fiscal_period": "2025Q4",
                    "actual_eps": 5.0,
                    "estimate_eps": 4.8,
                    "surprise_eps": 0.2,
                    "actual_revenue": 96.5,
                    "estimate_revenue": 95.0,
                    "surprise_revenue": 1.5,
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 11, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 11, 0, tzinfo=UTC),
                }
            ]
        )
        return events, history


def test_smoke_earnings_refresh_publish_status(tmp_path) -> None:
    publisher = RecordingPublisher()

    summary = run_dataops_earnings_daily(
        symbols=["SPY", "QQQ"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeEarningsProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert table_path("market_earnings_events", cache_root=tmp_path).exists()
    assert table_path("market_earnings_history", cache_root=tmp_path).exists()
    assert table_path("mv_next_earnings", cache_root=tmp_path).exists()

    assert summary["refresh"]["earnings_daily"]["status"] == "fresh"
    assert summary["coverage"]["status"] == "fresh"
    assert summary["publish_failures"] == []
    earnings_history_upsert = next(call for call in publisher.upserts if call["table"] == "market_earnings_history")
    assert earnings_history_upsert["on_conflict"] == "ticker,earnings_date"

    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    coverage_row = status_upsert["rows"][0]
    assert coverage_row["earnings_available"] is True
    assert coverage_row["next_earnings_date"] == "2026-05-01"

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    asset_keys = {row["asset_key"] for row in asset_status_upsert["rows"]}
    assert {"market_earnings_events", "market_earnings_history", "mv_next_earnings"}.issubset(asset_keys)

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration_row = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_earnings_daily")
    assert orchestration_row["status"] == "success"
    assert sorted(orchestration_row["symbols_succeeded"]) == ["QQQ", "SPY"]


def test_build_asset_status_rows_handles_empty_frames() -> None:
    refresh_run = RefreshRunResult(
        run_id="run_earnings_daily_test",
        asset_name="market_earnings_events",
        status="failed_hard",
        started_at="2026-04-11T00:00:00+00:00",
        ended_at="2026-04-11T00:01:00+00:00",
        symbols_requested=["AAPL"],
        symbols_succeeded=[],
        symbols_failed=["AAPL"],
        retry_exhausted_symbols=[],
        rows_written=0,
        error_messages=["provider returned zero earnings rows"],
    )

    rows = _build_asset_status_rows(
        earnings_events=pd.DataFrame(),
        earnings_history=pd.DataFrame(),
        next_earnings=pd.DataFrame(),
        refresh_run=refresh_run,
        flow_run_id="run_dataops_earnings_daily_test",
    )

    assert len(rows) == 3
    assert rows[0]["asset_key"] == "market_earnings_events"
    assert rows[0]["freshness_status"] == "failed_hard"


def test_earnings_coverage_merge_preserves_existing_market_and_fundamentals(tmp_path) -> None:
    publisher = RecordingPublisher()
    existing_rows = [
        {
            "ticker": "SPY",
            "market_data_available": True,
            "market_data_last_date": "2026-04-11",
            "fundamentals_available": True,
            "fundamentals_last_date": "2026-04-10",
            "signal_available": False,
        },
        {
            "ticker": "QQQ",
            "market_data_available": True,
            "market_data_last_date": "2026-04-11",
            "fundamentals_available": True,
            "fundamentals_last_date": "2026-04-10",
            "signal_available": False,
        },
    ]

    summary = run_dataops_earnings_daily(
        symbols=["SPY", "QQQ"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeEarningsProvider(),
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
    assert rows_by_ticker["SPY"]["next_earnings_date"] == "2026-05-01"
