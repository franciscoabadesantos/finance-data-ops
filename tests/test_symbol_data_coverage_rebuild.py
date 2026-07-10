from __future__ import annotations

import pandas as pd

from finance_data_ops.diagnostics.symbol_data_coverage import (
    build_complete_symbol_data_coverage_rows,
    summarize_symbol_data_coverage_rebuild,
)
from finance_data_ops.validation.readiness import build_readiness_audit


def test_complete_coverage_rebuild_covers_all_materialized_source_symbols_and_drops_stale() -> None:
    rows = build_complete_symbol_data_coverage_rows(
        prices_frame=pd.DataFrame(
            [
                {"symbol": "AEM", "price_date": "2026-07-09"},
                {"symbol": "LOW", "price_date": "2026-07-09"},
            ]
        ),
        quotes_frame=pd.DataFrame([{"ticker": "QUOTEONLY", "updated_at": "2026-07-09T14:00:00+00:00"}]),
        fundamentals_frame=pd.DataFrame(
            [
                {"symbol": "AEM", "period_end": "2026-03-31"},
                {"symbol": "DHI", "period_end": "2026-03-31"},
            ]
        ),
        earnings_events_frame=pd.DataFrame(
            [
                {"symbol": "AEM", "earnings_date": "2026-08-01"},
                {"symbol": "DHI", "earnings_date": "2026-08-02"},
            ]
        ),
        as_of_date="2026-07-10",
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert set(by_ticker) == {"AEM", "DHI", "LOW", "QUOTEONLY"}
    assert by_ticker["AEM"]["market_data_available"] is True
    assert by_ticker["AEM"]["market_data_last_date"] == "2026-07-09"
    assert by_ticker["AEM"]["fundamentals_available"] is True
    assert by_ticker["AEM"]["earnings_available"] is True
    assert by_ticker["AEM"]["next_earnings_date"] == "2026-08-01"
    assert by_ticker["DHI"]["market_data_available"] is False
    assert by_ticker["QUOTEONLY"]["market_data_available"] is False

    summary = summarize_symbol_data_coverage_rebuild(
        rows=rows,
        existing_rows=[{"ticker": "OLD", "market_data_available": True}],
    )
    assert summary["row_count"] == 4
    assert summary["market_data_available_count"] == 2
    assert summary["stale_existing_rows_removed"] == ["OLD"]
    assert summary["market_true_without_prices"] == []


def test_rebuilt_coverage_matches_readiness_price_materialization() -> None:
    coverage_rows = build_complete_symbol_data_coverage_rows(
        prices_frame=pd.DataFrame([{"symbol": "AEM", "price_date": "2026-07-09"}]),
        quotes_frame=pd.DataFrame([{"ticker": "QUOTEONLY", "updated_at": "2026-07-09T14:00:00+00:00"}]),
        fundamentals_frame=pd.DataFrame(),
        earnings_events_frame=pd.DataFrame(),
    )

    audit = build_readiness_audit(
        registry_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame([{"ticker": "AEM", "date": "2026-07-09"}]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(coverage_rows),
    )

    coverage_by_ticker = {row["ticker"]: row for row in coverage_rows}
    assert coverage_by_ticker["AEM"]["market_data_available"] is True
    assert coverage_by_ticker["QUOTEONLY"]["market_data_available"] is False
    assert not any(issue["issue_type"] == "coverage_disagrees_with_materialized_rows" for issue in audit.issues)
