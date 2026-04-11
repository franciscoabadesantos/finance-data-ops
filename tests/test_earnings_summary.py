from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.derived.earnings_summary import compute_next_earnings


def test_compute_next_earnings_selects_next_event_per_ticker() -> None:
    events = pd.DataFrame(
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
            },
            {
                "ticker": "SPY",
                "earnings_date": "2026-08-01",
                "earnings_time": "amc",
                "fiscal_period": "2026Q2",
                "estimate_eps": 5.3,
                "estimate_revenue": 102.0,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            },
            {
                "ticker": "QQQ",
                "earnings_date": "2026-04-20",
                "earnings_time": "bmo",
                "fiscal_period": "2026Q1",
                "estimate_eps": 3.2,
                "estimate_revenue": 90.0,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            },
        ]
    )

    out = compute_next_earnings(events, as_of_date="2026-04-11")

    assert sorted(out["ticker"].tolist()) == ["QQQ", "SPY"]
    spy = out.loc[out["ticker"] == "SPY"].iloc[0]
    assert str(spy["earnings_date"]) == "2026-05-01"
    qqq = out.loc[out["ticker"] == "QQQ"].iloc[0]
    assert str(qqq["earnings_date"]) == "2026-04-20"
