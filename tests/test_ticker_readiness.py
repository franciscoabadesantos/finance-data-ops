from __future__ import annotations

import pandas as pd

from finance_data_ops.publish.status import SYMBOL_DATA_COVERAGE_SEMANTICS
from finance_data_ops.validation.readiness import (
    build_readiness_audit,
    build_ticker_readiness_rows,
    rebuild_diagnostic_symbol_data_coverage_rows,
)


def test_active_registry_row_without_prices_is_not_materialized() -> None:
    audit = build_readiness_audit(
        registry_frame=pd.DataFrame(
            [
                {
                    "normalized_symbol": "NOPRICE",
                    "status": "active",
                    "promotion_status": "validated_full",
                    "validation_status": "validated_full",
                }
            ]
        ),
        prices_frame=pd.DataFrame(columns=["ticker", "date"]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
    )

    row = audit.rows[0]
    assert row["ticker"] == "NOPRICE"
    assert row["registry_active_validated"] is True
    assert row["source_price_available"] is False
    assert row["tracked_search_ready"] is False
    assert row["readiness_state"] == "not_materialized"
    assert any(issue["issue_type"] == "registry_active_validated_without_prices" for issue in audit.issues)


def test_price_only_ticker_is_not_tracked() -> None:
    rows = build_ticker_readiness_rows(
        registry_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame([{"ticker": "SPCX", "date": "2026-07-06"}]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
    )

    assert rows == [
        {
            "ticker": "SPCX",
            "is_placeholder_symbol": False,
            "registry_status": None,
            "registry_validation_status": None,
            "registry_promotion_status": None,
            "registry_active_validated": False,
            "source_price_available": True,
            "source_price_rows": 1,
            "source_price_latest_date": "2026-07-06",
            "technical_features_available": False,
            "technical_feature_rows": 0,
            "technical_feature_latest_date": None,
            "scorecard_available": False,
            "scorecard_rows": 0,
            "scorecard_latest_date": None,
            "tracked_search_ready": False,
            "readiness_state": "source_only",
            "coverage_market_data_available": None,
            "coverage_status": None,
            "coverage_reason": None,
        }
    ]


def test_price_plus_technicals_ticker_is_tracked_without_scorecard() -> None:
    rows = build_ticker_readiness_rows(
        registry_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame([{"ticker": "AAPL", "date": "2026-07-06"}]),
        technicals_frame=pd.DataFrame([{"ticker": "AAPL", "as_of_date": "2026-07-06"}]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
    )

    assert rows[0]["tracked_search_ready"] is True
    assert rows[0]["readiness_state"] == "tracked_without_scorecard"
    assert rows[0]["scorecard_available"] is False


def test_symbol_data_coverage_is_diagnostic_and_rebuildable_from_sources() -> None:
    assert SYMBOL_DATA_COVERAGE_SEMANTICS == "diagnostic"

    rows = rebuild_diagnostic_symbol_data_coverage_rows(
        prices_frame=pd.DataFrame([{"ticker": "AAPL", "date": "2026-07-06"}]),
        quotes_frame=pd.DataFrame(columns=["ticker", "quote_ts"]),
    )

    assert len(rows) == 1
    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["market_data_available"] is True
    assert rows[0]["coverage_status"] == "partial"
    assert rows[0]["reason"] == "missing_market_quote"


def test_residue_audit_catches_seeded_examples() -> None:
    audit = build_readiness_audit(
        registry_frame=pd.DataFrame(
            [
                {
                    "normalized_symbol": "ACTIVE_NOPRICE",
                    "status": "active",
                    "promotion_status": "validated_full",
                    "validation_status": "validated_full",
                },
                {
                    "normalized_symbol": "REJECTED_WITH_DATA",
                    "status": "rejected",
                    "promotion_status": "rejected",
                    "validation_status": "rejected",
                },
                {
                    "normalized_symbol": "2200963D",
                    "status": "active",
                    "promotion_status": "validated_full",
                    "validation_status": "validated_full",
                },
            ]
        ),
        prices_frame=pd.DataFrame(
            [
                {"ticker": "PRICE_ONLY", "date": "2026-07-06"},
                {"ticker": "READY", "date": "2026-07-06"},
                {"ticker": "REJECTED_WITH_DATA", "date": "2026-07-06"},
            ]
        ),
        technicals_frame=pd.DataFrame([{"ticker": "READY", "as_of_date": "2026-07-06"}]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(
            [
                {"ticker": "PRICE_ONLY", "market_data_available": False, "coverage_status": "fresh"},
                {"ticker": "ACTIVE_NOPRICE", "market_data_available": True, "coverage_status": "partial"},
            ]
        ),
    )

    issues = {(issue["issue_type"], issue["ticker"]) for issue in audit.issues}
    assert ("registry_active_validated_without_prices", "ACTIVE_NOPRICE") in issues
    assert ("prices_without_technicals", "PRICE_ONLY") in issues
    assert ("technicals_without_scorecard", "READY") in issues
    assert ("rejected_with_materialized_data", "REJECTED_WITH_DATA") in issues
    assert ("placeholder_like_symbol", "2200963D") in issues
    assert ("coverage_disagrees_with_materialized_rows", "PRICE_ONLY") in issues
    assert ("coverage_disagrees_with_materialized_rows", "ACTIVE_NOPRICE") in issues
    assert audit.as_dict()["tracked_tickers"] == ["READY"]
