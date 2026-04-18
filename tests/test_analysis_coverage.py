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
    assert "No ticker_registry row found for requested scope." in warnings
