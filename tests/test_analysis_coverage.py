from __future__ import annotations

from finance_data_ops.analysis.coverage import build_coverage_report


def test_build_coverage_report_contract_shape() -> None:
    payload = build_coverage_report(
        ticker="AAPL",
        region="us",
        exchange=None,
        analysis_type="coverage_report",
        coverage={
            "market_data_available": True,
            "fundamentals_available": True,
            "earnings_available": False,
            "coverage_status": "partial",
            "coverage_ratio": 0.67,
        },
        asset_status_by_key={
            "market_price_daily": {"freshness_status": "fresh"},
            "market_quotes": {"freshness_status": "fresh"},
            "fundamentals_daily": {"freshness_status": "fresh"},
            "earnings_daily": {"freshness_status": "stale"},
            "macro_observations": {"freshness_status": "fresh"},
            "economic_release_calendar": {"freshness_status": "fresh"},
        },
        registry_row={
            "registry_key": "AAPL|us|default",
            "status": "active",
            "validation_status": "validated",
            "promotion_status": "validated_full",
            "validation_reason": None,
        },
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
    assert payload["metadata"] == {
        "ticker": "AAPL",
        "region": "us",
        "exchange": None,
        "analysis_type": "coverage_report",
        "generated_at": "2026-04-18T00:00:00+00:00",
    }
    assert any(section.get("title") == "Domain Coverage" for section in payload["sections"])
    assert any(section.get("title") == "Data Window / Completeness" for section in payload["sections"])
    assert any("Freshness alerts" in warning for warning in payload["warnings"])


def test_build_coverage_report_missing_inputs_emit_warnings() -> None:
    payload = build_coverage_report(
        ticker="AAPL",
        region="us",
        exchange=None,
        analysis_type="coverage_report",
        coverage=None,
        asset_status_by_key={},
        registry_row=None,
    )

    warnings = payload["warnings"]
    assert "No symbol_data_coverage row found for ticker." in warnings
    assert (
        "No ticker_registry onboarding row found for requested scope; canonical data availability is assessed independently."
        in warnings
    )
