from __future__ import annotations

import pandas as pd

from finance_data_ops.providers.fundamentals import FundamentalsDataProvider


def test_fetch_symbol_fundamentals_normalizes_metrics() -> None:
    annual_income = pd.DataFrame(
        {
            "2025-12-31": {
                "Total Revenue": 550.0,
                "Net Income": 110.0,
                "Gross Profit": 300.0,
                "Operating Income": 160.0,
                "EBITDA": 170.0,
            }
        }
    )
    quarterly_income = pd.DataFrame(
        {
            "2026-03-31": {
                "Total Revenue": 140.0,
                "Diluted EPS": 2.4,
            }
        }
    )
    annual_cashflow = pd.DataFrame(
        {"2025-12-31": {"Free Cash Flow": 80.0}}
    )
    quarterly_cashflow = pd.DataFrame()
    annual_balance = pd.DataFrame(
        {
            "2025-12-31": {
                "Total Assets": 1200.0,
                "Total Liabilities": 500.0,
            }
        }
    )
    quarterly_balance = pd.DataFrame()

    provider = FundamentalsDataProvider(
        income_statements_fn=lambda _symbol: (quarterly_income, annual_income),
        cashflow_statements_fn=lambda _symbol: (quarterly_cashflow, annual_cashflow),
        balance_sheet_statements_fn=lambda _symbol: (quarterly_balance, annual_balance),
        info_fn=lambda _symbol: {
            "financialCurrency": "USD",
            "marketCap": 2200.0,
            "sharesOutstanding": 100.0,
            "trailingPE": 20.0,
        },
        fast_info_fn=lambda _symbol: {},
        provider_name="fake",
    )

    out = provider.fetch_symbol_fundamentals("spy")

    assert out["ticker"].unique().tolist() == ["SPY"]
    metrics = set(out["metric"].tolist())
    assert {"revenue", "eps", "net_income", "free_cash_flow", "total_assets", "total_liabilities"}.issubset(
        metrics
    )
    assert "market_cap" in metrics
    assert "shares_outstanding" in metrics
    assert set(out["period_type"].tolist()).issuperset({"annual", "quarterly", "point_in_time"})
