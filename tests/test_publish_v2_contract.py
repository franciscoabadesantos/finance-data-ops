from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces


def test_publish_v2_contract_writes_expected_tables() -> None:
    publisher = RecordingPublisher()

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "period": "2025",
                "metric": "revenue",
                "value": 550.0,
                "value_text": "550.0",
                "period_end": "2025-12-31",
                "fiscal_year": 2025,
                "fiscal_quarter": None,
                "currency": "USD",
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )
    summary = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "latest_revenue": 550.0,
                "latest_eps": 10.0,
                "trailing_pe": 20.0,
                "market_cap": 2200.0,
                "revenue_growth_yoy": 0.1,
                "earnings_growth_yoy": 0.1,
                "latest_period_end": "2025-12-31",
                "source": "fake",
                "updated_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )

    earnings_events = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "earnings_date": "2026-05-01",
                "earnings_time": "amc",
                "fiscal_period": "2026Q1",
                "estimate_eps": 5.1,
                "estimate_revenue": 100.0,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )
    earnings_history = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "earnings_date": "2026-02-01",
                "fiscal_period": "2025Q4",
                "actual_eps": 5.0,
                "estimate_eps": 4.8,
                "surprise_eps": 0.2,
                "actual_revenue": 96.5,
                "estimate_revenue": 95.0,
                "surprise_revenue": 1.5,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )

    publish_fundamentals_surfaces(
        publisher=publisher,
        fundamentals_history=fundamentals,
        fundamentals_summary=summary,
        refresh_materialized_view=True,
    )
    publish_earnings_surfaces(
        publisher=publisher,
        earnings_events=earnings_events,
        earnings_history=earnings_history,
        refresh_materialized_view=True,
    )

    tables = [call["table"] for call in publisher.upserts]
    assert tables == [
        "market_fundamentals_v2",
        "ticker_fundamental_summary",
        "market_earnings_events",
        "market_earnings_history",
    ]

    conflicts = {call["table"]: call["on_conflict"] for call in publisher.upserts}
    assert conflicts["market_fundamentals_v2"] == "ticker,period,period_end,metric"
    assert conflicts["ticker_fundamental_summary"] == "ticker"
    assert conflicts["market_earnings_events"] == "ticker,earnings_date"
    assert conflicts["market_earnings_history"] == "ticker,earnings_date"

    fundamentals_row = next(call for call in publisher.upserts if call["table"] == "market_fundamentals_v2")["rows"][0]
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

    summary_row = next(call for call in publisher.upserts if call["table"] == "ticker_fundamental_summary")["rows"][0]
    assert summary_row["ticker"] == "SPY"
    assert summary_row["trailing_pe"] == 20.0

    event_row = next(call for call in publisher.upserts if call["table"] == "market_earnings_events")["rows"][0]
    assert set(event_row.keys()) == {
        "ticker",
        "earnings_date",
        "earnings_time",
        "fiscal_period",
        "estimate_eps",
        "estimate_revenue",
        "source",
        "fetched_at",
        "created_at",
        "updated_at",
    }

    history_row = next(call for call in publisher.upserts if call["table"] == "market_earnings_history")["rows"][0]
    assert set(history_row.keys()) == {
        "ticker",
        "earnings_date",
        "fiscal_period",
        "actual_eps",
        "estimate_eps",
        "surprise_eps",
        "actual_revenue",
        "estimate_revenue",
        "surprise_revenue",
        "source",
        "fetched_at",
        "created_at",
        "updated_at",
    }

    rpc_names = [call["name"] for call in publisher.rpcs]
    assert rpc_names == ["refresh_mv_latest_fundamentals", "refresh_mv_next_earnings"]
