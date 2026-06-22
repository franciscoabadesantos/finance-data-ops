from __future__ import annotations

import logging

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


def test_optional_yahoo_surfaces_do_not_block_statement_fundamentals(caplog) -> None:
    annual_income = pd.DataFrame({"2025-12-31": {"Total Revenue": 550.0}})

    def fail_optional_surface(_symbol: str) -> object:
        raise RuntimeError("optional surface failed")

    provider = FundamentalsDataProvider(
        income_statements_fn=lambda _symbol: (pd.DataFrame(), annual_income),
        cashflow_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        balance_sheet_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        info_fn=fail_optional_surface,
        fast_info_fn=fail_optional_surface,
        dividends_fn=fail_optional_surface,
        funds_data_fn=fail_optional_surface,
        provider_name="fake",
    )

    caplog.set_level(logging.WARNING, logger="finance_data_ops.providers.fundamentals")
    fundamentals = provider.fetch_symbol_fundamentals("ko")
    profile = provider.fetch_symbol_profile("ko")
    holdings, sectors = provider.fetch_symbol_etf_funds_data("ko")

    assert fundamentals[["ticker", "metric", "value"]].to_dict(orient="records") == [
        {"ticker": "KO", "metric": "revenue", "value": 550.0}
    ]
    assert profile.empty
    assert holdings.empty
    assert sectors.empty
    assert "Optional fundamentals provider surface failed (symbol=KO surface=info)" in caplog.text
    assert "Optional fundamentals provider surface failed (symbol=KO surface=fast_info)" in caplog.text
    assert "Optional fundamentals provider surface failed (symbol=KO surface=dividends)" in caplog.text
    assert "Optional fundamentals provider surface failed (symbol=KO surface=funds_data)" in caplog.text


def test_fetch_symbol_fundamentals_extracts_yahoo_info_additions() -> None:
    provider = FundamentalsDataProvider(
        income_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        cashflow_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        balance_sheet_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        info_fn=lambda _symbol: {
            "financialCurrency": "USD",
            "marketCap": 2200.0,
            "dividendYield": 0.012,
            "dividendRate": 6.1,
            "trailingAnnualDividendYield": 0.011,
            "trailingAnnualDividendRate": 5.9,
            "payoutRatio": 0.43,
            "exDividendDate": 1_765_843_200,
            "payoutFrequency": 4,
            "beta": 1.05,
            "beta3Year": 0.98,
            "ytdReturn": 0.07,
            "threeYearAverageReturn": 0.11,
            "fiveYearAverageReturn": 0.12,
        },
        fast_info_fn=lambda _symbol: {},
        dividends_fn=lambda _symbol: None,  # no payment history -> falls back to .info fields
        provider_name="fake",
    )

    out = provider.fetch_symbol_fundamentals("spy")
    values = {row["metric"]: row for row in out.to_dict(orient="records")}

    assert values["dividend_yield"]["value"] == 0.012
    assert values["dividend_rate"]["value"] == 6.1
    assert values["trailing_annual_dividend_yield"]["value"] == 0.011
    assert values["trailing_annual_dividend_rate"]["value"] == 5.9
    assert values["payout_ratio"]["value"] == 0.43
    assert values["ex_dividend_date"]["value_text"] == "2025-12-16"
    assert values["payout_frequency"]["value_text"] == "4"
    assert values["beta"]["value"] == 1.05
    assert values["beta_3y"]["value"] == 0.98
    assert values["ytd_return"]["value"] == 0.07
    assert values["three_year_avg_return"]["value"] == 0.11
    assert values["five_year_avg_return"]["value"] == 0.12


def test_dividend_metrics_computed_from_payment_history() -> None:
    now = pd.Timestamp.now(tz="UTC")
    dividends = pd.Series(
        [0.50, 0.485, 0.485, 0.485, 0.485],
        index=[
            now - pd.Timedelta(days=400),  # older than 12 months -> excluded
            now - pd.Timedelta(days=300),
            now - pd.Timedelta(days=200),
            now - pd.Timedelta(days=100),
            now - pd.Timedelta(days=10),
        ],
    )
    provider = FundamentalsDataProvider(
        income_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        cashflow_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        balance_sheet_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        # .info has a bad/unscaled dividendYield (99.0) — the history must override it.
        info_fn=lambda _s: {"dividendYield": 99.0, "dividendRate": 1.0, "trailingEps": 2.5},
        fast_info_fn=lambda _s: {"last_price": 63.0},
        dividends_fn=lambda _s: dividends,
        provider_name="fake",
    )

    out = provider.fetch_symbol_fundamentals("ko")
    values = {row["metric"]: row["value"] for row in out.to_dict(orient="records")}

    # trailing 12m = 0.485 * 4 = 1.94 (the 0.50 at -400d is excluded)
    assert abs(values["dividend_rate"] - 1.94) < 1e-9
    # yield is a FRACTION (rate / price), overriding the bad .info 99.0
    assert abs(values["dividend_yield"] - (1.94 / 63.0)) < 1e-9
    # payout = rate / eps
    assert abs(values["payout_ratio"] - (1.94 / 2.5)) < 1e-9


def test_market_cap_falls_back_to_aum_for_funds() -> None:
    common = dict(
        income_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        cashflow_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        balance_sheet_statements_fn=lambda _s: (pd.DataFrame(), pd.DataFrame()),
        fast_info_fn=lambda _s: {},
        dividends_fn=lambda _s: None,
        funds_data_fn=lambda _s: None,
        provider_name="fake",
    )

    # ETF: no marketCap but has AUM (totalAssets) -> market_cap uses the AUM.
    etf = FundamentalsDataProvider(info_fn=lambda _s: {"quoteType": "ETF", "totalAssets": 1.4e12}, **common)
    etf_values = {r["metric"]: r["value"] for r in etf.fetch_symbol_fundamentals("voo").to_dict(orient="records")}
    assert etf_values["market_cap"] == 1.4e12

    # Equity: .info totalAssets is balance-sheet assets, NOT market cap -> must not leak into market_cap.
    eq = FundamentalsDataProvider(info_fn=lambda _s: {"quoteType": "EQUITY", "totalAssets": 3.7e11}, **common)
    eq_values = {r["metric"]: r["value"] for r in eq.fetch_symbol_fundamentals("aapl").to_dict(orient="records")}
    assert "market_cap" not in eq_values


def test_fetch_symbol_profile_and_etf_funds_data() -> None:
    funds_data = {
        "top_holdings": pd.DataFrame(
            [
                {"Symbol": "AAPL", "Name": "Apple Inc.", "Holding Percent": 0.071},
                {"Symbol": "MSFT", "Name": "Microsoft Corp.", "Holding Percent": 0.065},
            ]
        ),
        "sector_weightings": {"technology": 0.32, "healthcare": 0.12},
        "as_of_date": "2026-06-22",
    }
    provider = FundamentalsDataProvider(
        income_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        cashflow_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        balance_sheet_statements_fn=lambda _symbol: (pd.DataFrame(), pd.DataFrame()),
        info_fn=lambda _symbol: {
            "longBusinessSummary": "Tracks large US companies.",
            "category": "Large Blend",
            "fundFamily": "Example Funds",
            "feesExpensesInvestment": 0.0009,
            "fundInceptionDate": 728524800,
            "legalType": "ETF",
            "beta": 1.0,
            "beta3Year": 0.95,
        },
        fast_info_fn=lambda _symbol: {},
        funds_data_fn=lambda _symbol: funds_data,
        provider_name="fake",
    )

    profile = provider.fetch_symbol_profile("spy")
    holdings, sectors = provider.fetch_symbol_etf_funds_data("spy")

    profile_row = profile.iloc[0]
    assert profile_row["ticker"] == "SPY"
    assert profile_row["description"] == "Tracks large US companies."
    assert profile_row["etf_category"] == "Large Blend"
    assert profile_row["fund_family"] == "Example Funds"
    assert profile_row["expense_ratio"] == 0.0009
    assert str(profile_row["inception_date"]) == "1993-02-01"
    assert profile_row["legal_type"] == "ETF"
    assert profile_row["beta_3y"] == 0.95

    assert holdings[["etf_ticker", "holding_symbol", "holding_name", "weight"]].to_dict(orient="records") == [
        {"etf_ticker": "SPY", "holding_symbol": "AAPL", "holding_name": "Apple Inc.", "weight": 0.071},
        {"etf_ticker": "SPY", "holding_symbol": "MSFT", "holding_name": "Microsoft Corp.", "weight": 0.065},
    ]
    assert set(sectors["sector"].tolist()) == {"technology", "healthcare"}
    assert set(sectors["as_of"].astype(str).tolist()) == {"2026-06-22"}
