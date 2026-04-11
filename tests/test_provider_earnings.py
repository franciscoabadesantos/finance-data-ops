from __future__ import annotations

import math

import numpy as np
import pandas as pd

from finance_data_ops.providers.earnings import EarningsDataProvider


def test_fetch_symbol_earnings_normalizes_events_and_history() -> None:
    index = pd.to_datetime(["2026-05-01", "2026-02-01"])  # upcoming first, historical second
    earnings_dates = pd.DataFrame(
        {
            "EPS Estimate": [5.1, 4.8],
            "Reported EPS": [np.nan, 5.0],
            "Revenue Estimate": [100.0, 95.0],
            "Reported Revenue": [np.nan, 96.5],
        },
        index=index,
    )

    provider = EarningsDataProvider(
        earnings_dates_fn=lambda _symbol, _limit: earnings_dates,
        calendar_fn=lambda _symbol: {"Earnings Date": ["2026-05-01"], "Earnings Time": "amc"},
        provider_name="fake",
    )

    events, history = provider.fetch_symbol_earnings("spy", history_limit=8)

    assert len(events.index) == 1
    event_row = events.iloc[0]
    assert event_row["ticker"] == "SPY"
    assert str(event_row["earnings_date"]) == "2026-05-01"
    assert event_row["estimate_eps"] == 5.1

    assert len(history.index) == 1
    history_row = history.iloc[0]
    assert history_row["ticker"] == "SPY"
    assert str(history_row["earnings_date"]) == "2026-02-01"
    assert history_row["actual_eps"] == 5.0
    assert history_row["estimate_eps"] == 4.8
    assert math.isclose(float(history_row["surprise_eps"]), 0.2, rel_tol=0.0, abs_tol=1e-9)
    assert history_row["actual_revenue"] == 96.5
    assert history_row["estimate_revenue"] == 95.0
    assert math.isclose(float(history_row["surprise_revenue"]), 1.5, rel_tol=0.0, abs_tol=1e-9)
