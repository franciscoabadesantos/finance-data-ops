from __future__ import annotations

from finance_data_ops.analysis.snapshots import build_ticker_snapshot_report


def test_build_ticker_snapshot_report_contract_shape() -> None:
    payload = build_ticker_snapshot_report(
        ticker="AAPL",
        region="us",
        exchange=None,
        analysis_type="ticker_snapshot",
        market_snapshot={
            "last_price": 189.11,
            "return_1d_pct": 0.45,
            "return_1m_pct": 2.1,
            "return_1y_pct": 18.2,
            "as_of_date": "2026-04-17",
        },
        coverage={
            "market_data_available": True,
            "fundamentals_available": True,
            "earnings_available": True,
            "coverage_status": "fresh",
        },
        asset_status_by_key={
            "market_price_daily": {"freshness_status": "fresh"},
            "market_quotes": {"freshness_status": "fresh"},
            "fundamentals_daily": {"freshness_status": "fresh"},
            "earnings_daily": {"freshness_status": "fresh"},
        },
        registry_row={"registry_key": "AAPL|us|default"},
        market_price_rows=[
            {"ticker": "AAPL", "date": "2026-04-16", "fetched_at": "2026-04-16T23:59:00+00:00"},
            {"ticker": "AAPL", "date": "2026-04-17", "fetched_at": "2026-04-17T23:59:00+00:00"},
        ],
        fundamentals_rows=[
            {"ticker": "AAPL", "period_end": "2025-12-31", "fetched_at": "2026-01-15T00:00:00+00:00"},
        ],
        earnings_rows=[
            {"ticker": "AAPL", "earnings_date": "2026-01-28", "fetched_at": "2026-01-28T12:00:00+00:00"},
        ],
        generated_at="2026-04-18T00:00:00+00:00",
    )

    assert payload["summary"]
    assert isinstance(payload["sections"], list)
    assert isinstance(payload["warnings"], list)
    assert any(section["title"] == "Data Window / Completeness" for section in payload["sections"])
    assert payload["metadata"] == {
        "ticker": "AAPL",
        "region": "us",
        "exchange": None,
        "analysis_type": "ticker_snapshot",
        "generated_at": "2026-04-18T00:00:00+00:00",
    }


def test_build_ticker_snapshot_report_emits_warnings_on_missing_inputs() -> None:
    payload = build_ticker_snapshot_report(
        ticker="AAPL",
        region="us",
        exchange=None,
        analysis_type="ticker_snapshot",
        market_snapshot=None,
        coverage=None,
        asset_status_by_key={},
        registry_row=None,
    )
    warnings = payload["warnings"]
    assert len(warnings) >= 3
    assert "No symbol_data_coverage row found for ticker." in warnings
    assert "No ticker_market_stats_snapshot row found for ticker." in warnings
    assert (
        "No ticker_registry onboarding row found for requested scope; canonical data availability is assessed independently."
        in warnings
    )
