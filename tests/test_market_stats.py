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
    assert row["symbol"] == "SPY"
    assert row["latest_price"] > 0
    assert row["return_1d"] is not None
    assert row["return_1m"] is not None
    assert row["return_3m"] is not None
    assert row["return_1y"] is not None
    assert row["volatility_30d"] is not None
    assert row["drawdown_1y"] <= 0
    assert row["distance_from_52w_high"] <= 0
    assert row["distance_from_52w_low"] >= 0
