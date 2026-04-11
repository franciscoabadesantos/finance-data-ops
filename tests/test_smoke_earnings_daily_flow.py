from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_earnings_daily import run_dataops_earnings_daily


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
