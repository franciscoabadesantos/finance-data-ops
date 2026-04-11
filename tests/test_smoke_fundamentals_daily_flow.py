from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily


class FakeFundamentalsProvider:
    def fetch_symbol_fundamentals(self, symbol: str) -> pd.DataFrame:
        ticker = str(symbol).strip().upper()
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "metric": "revenue",
                    "value": 550.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "net_income",
                    "value": 110.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "eps",
                    "value": 10.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "market_cap",
                    "value": 2200.0,
                    "period_end": "2026-04-11",
                    "period_type": "point_in_time",
                    "fiscal_year": 2026,
                    "fiscal_quarter": "Q2",
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
            ]
        )


def test_smoke_fundamentals_refresh_publish_status(tmp_path) -> None:
    publisher = RecordingPublisher()

    summary = run_dataops_fundamentals_daily(
        symbols=["SPY", "QQQ"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeFundamentalsProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert table_path("market_fundamentals_v2", cache_root=tmp_path).exists()
    assert table_path("ticker_fundamental_summary", cache_root=tmp_path).exists()

    assert summary["refresh"]["fundamentals_daily"]["status"] == "fresh"
    assert summary["coverage"]["status"] == "fresh"
    assert summary["publish_failures"] == []
    fundamentals_upsert = next(call for call in publisher.upserts if call["table"] == "market_fundamentals_v2")
    assert fundamentals_upsert["on_conflict"] == "ticker,period,period_end,metric"
    fundamentals_row = fundamentals_upsert["rows"][0]
    assert set(fundamentals_row.keys()) == {
        "ticker",
        "period",
        "period_end",
        "metric",
        "value",
        "value_text",
        "source",
        "fetched_at",
    }

    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    coverage_row = status_upsert["rows"][0]
    assert coverage_row["fundamentals_available"] is True
    assert coverage_row["fundamentals_last_date"] is not None

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    asset_keys = {row["asset_key"] for row in asset_status_upsert["rows"]}
    assert {"market_fundamentals_v2", "ticker_fundamental_summary", "mv_latest_fundamentals"}.issubset(
        asset_keys
    )

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration_row = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_fundamentals_daily")
    assert orchestration_row["status"] == "success"
    assert sorted(orchestration_row["symbols_succeeded"]) == ["QQQ", "SPY"]
