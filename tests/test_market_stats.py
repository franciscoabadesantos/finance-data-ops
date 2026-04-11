from __future__ import annotations

import numpy as np
import pandas as pd

from finance_data_ops.derived.market_stats import compute_ticker_market_stats


def test_compute_ticker_market_stats_contains_expected_metrics() -> None:
    dates = pd.bdate_range("2025-01-01", periods=280)
    close = np.linspace(100.0, 140.0, num=len(dates))
    prices = pd.DataFrame(
        {
            "symbol": "SPY",
            "date": dates,
            "close": close,
        }
    )

    out = compute_ticker_market_stats(prices)

    assert len(out.index) == 1
    row = out.iloc[0]
    assert row["ticker"] == "SPY"
    assert row["last_price"] > 0
    assert row["return_1d_pct"] is not None
    assert row["return_1m_pct"] is not None
    assert row["return_3m_pct"] is not None
    assert row["return_1y_pct"] is not None
    assert row["vol_30d_pct"] is not None
    assert row["drawdown_1y_pct"] <= 0
    assert row["dist_from_52w_high_pct"] <= 0
    assert row["dist_from_52w_low_pct"] >= 0
