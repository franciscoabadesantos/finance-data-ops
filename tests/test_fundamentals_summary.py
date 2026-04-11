from __future__ import annotations

import pandas as pd

from finance_data_ops.derived.fundamentals_summary import compute_ticker_fundamental_summary


def test_compute_ticker_fundamental_summary_contains_expected_fields() -> None:
    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "metric": "revenue",
                "value": 500.0,
                "period_end": "2024-12-31",
                "period_type": "annual",
                "source": "fake",
            },
            {
                "ticker": "SPY",
                "metric": "revenue",
                "value": 550.0,
                "period_end": "2025-12-31",
                "period_type": "annual",
                "source": "fake",
            },
            {
                "ticker": "SPY",
                "metric": "net_income",
                "value": 100.0,
                "period_end": "2024-12-31",
                "period_type": "annual",
                "source": "fake",
            },
            {
                "ticker": "SPY",
                "metric": "net_income",
                "value": 110.0,
                "period_end": "2025-12-31",
                "period_type": "annual",
                "source": "fake",
            },
            {
                "ticker": "SPY",
                "metric": "eps",
                "value": 10.0,
                "period_end": "2025-12-31",
                "period_type": "annual",
                "source": "fake",
            },
            {
                "ticker": "SPY",
                "metric": "market_cap",
                "value": 2200.0,
                "period_end": "2025-12-31",
                "period_type": "point_in_time",
                "source": "fake",
            },
        ]
    )

    out = compute_ticker_fundamental_summary(fundamentals)

    assert len(out.index) == 1
    row = out.iloc[0]
    assert row["ticker"] == "SPY"
    assert row["latest_revenue"] == 550.0
    assert row["latest_eps"] == 10.0
    assert row["market_cap"] == 2200.0
    assert row["trailing_pe"] == 20.0
    assert row["revenue_growth_yoy"] == 0.1
    assert row["earnings_growth_yoy"] == 0.1
    assert row["latest_period_end"] == "2025-12-31"
