from __future__ import annotations

import pandas as pd

from finance_data_ops.validation.readiness_cleanup import build_ticker_readiness_cleanup_plan


def test_cleanup_plan_classifies_invalid_placeholder() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "2200963D|us|default",
                    "input_symbol": "2200963D",
                    "normalized_symbol": "2200963D",
                    "status": "active",
                    "validation_status": "validated_full",
                    "promotion_status": "validated_full",
                }
            ]
        ),
        readiness_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame(columns=["ticker", "date"]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame([{"ticker": "2200963D", "market_data_available": True}]),
    )

    actions = plan["groups"]["invalid_placeholder"]
    assert [action["ticker"] for action in actions] == ["2200963D"]
    assert actions[0]["proposed_action"] == "reject_deactivate_registry_rows_and_clear_stale_coverage"
    assert any("update public.ticker_registry" in sql for sql in actions[0]["sql_preview"])
    assert any("delete from public.symbol_data_coverage" in sql for sql in actions[0]["sql_preview"])


def test_cleanup_plan_marks_cleaned_rejected_placeholder_non_actionable() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "2200963D|us|default",
                    "input_symbol": "2200963D",
                    "normalized_symbol": "2200963D",
                    "status": "rejected",
                    "validation_status": "rejected",
                    "promotion_status": "rejected",
                }
            ]
        ),
        readiness_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame(columns=["ticker", "date"]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(columns=["ticker", "market_data_available"]),
    )

    actions = plan["groups"]["already_cleaned"]
    assert [action["ticker"] for action in actions] == ["2200963D"]
    assert actions[0]["proposed_action"] == "no_action"
    assert actions[0]["sql_preview"] == []
    assert "already_cleaned" not in plan["issue_counts"]


def test_cleanup_plan_classifies_superseded_alias() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "700.HK|apac|default",
                    "input_symbol": "700.HK",
                    "normalized_symbol": "700.HK",
                    "status": "active",
                    "validation_status": "validated_full",
                    "promotion_status": "validated_full",
                }
            ]
        ),
        readiness_frame=pd.DataFrame([{"ticker": "0700.HK", "tracked_search_ready": True}]),
        prices_frame=pd.DataFrame([{"ticker": "0700.HK", "date": "2026-07-08"}]),
        technicals_frame=pd.DataFrame([{"ticker": "0700.HK", "as_of_date": "2026-07-08"}]),
        scorecard_frame=pd.DataFrame([{"ticker": "0700.HK", "as_of_date": "2026-07-08"}]),
        coverage_frame=pd.DataFrame([{"ticker": "700.HK", "market_data_available": False}]),
    )

    actions = plan["groups"]["superseded_alias"]
    assert [action["ticker"] for action in actions] == ["700.HK"]
    assert actions[0]["superseded_by"] == "0700.HK"
    assert actions[0]["canonical_tracked"] is True
    assert any("superseded_by:0700.HK" in sql for sql in actions[0]["sql_preview"])


def test_cleanup_plan_marks_cleaned_superseded_alias_non_actionable() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "700.HK|apac|default",
                    "input_symbol": "700.HK",
                    "normalized_symbol": "700.HK",
                    "status": "rejected",
                    "validation_status": "rejected",
                    "promotion_status": "rejected",
                    "validation_reason": "superseded_by:0700.HK",
                    "notes": {"superseded_by": "0700.HK"},
                }
            ]
        ),
        readiness_frame=pd.DataFrame([{"ticker": "0700.HK", "tracked_search_ready": True}]),
        prices_frame=pd.DataFrame([{"ticker": "0700.HK", "date": "2026-07-08"}]),
        technicals_frame=pd.DataFrame([{"ticker": "0700.HK", "as_of_date": "2026-07-08"}]),
        scorecard_frame=pd.DataFrame([{"ticker": "0700.HK", "as_of_date": "2026-07-08"}]),
        coverage_frame=pd.DataFrame(columns=["ticker", "market_data_available"]),
    )

    actions = plan["groups"]["already_cleaned"]
    assert [action["ticker"] for action in actions] == ["700.HK"]
    assert actions[0]["proposed_action"] == "no_action"
    assert actions[0]["sql_preview"] == []
    assert "superseded_alias" not in plan["issue_counts"]


def test_cleanup_plan_classifies_rejected_shadow_with_tracked_canonical() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "ADBE|us|default",
                    "input_symbol": "ADBE",
                    "normalized_symbol": "ADBE",
                    "status": "rejected",
                    "validation_status": "rejected",
                    "promotion_status": "rejected",
                    "notes": '{"superseded_by": "ADBE|us|NMS"}',
                },
                {
                    "registry_key": "ADBE|us|NMS",
                    "input_symbol": "ADBE",
                    "normalized_symbol": "ADBE",
                    "status": "pending_validation",
                    "validation_status": "pending_validation",
                    "promotion_status": "pending_validation",
                },
            ]
        ),
        readiness_frame=pd.DataFrame([{"ticker": "ADBE", "tracked_search_ready": True}]),
        prices_frame=pd.DataFrame([{"ticker": "ADBE", "date": "2026-07-08"}]),
        technicals_frame=pd.DataFrame([{"ticker": "ADBE", "as_of_date": "2026-07-08"}]),
        scorecard_frame=pd.DataFrame([{"ticker": "ADBE", "as_of_date": "2026-07-08"}]),
        coverage_frame=pd.DataFrame(),
    )

    actions = plan["groups"]["rejected_shadow_row_with_tracked_canonical"]
    assert [action["ticker"] for action in actions] == ["ADBE"]
    assert actions[0]["proposed_action"] == "suppress_from_readiness_error_audit"
    assert actions[0]["registry_keys"] == ["ADBE|us|default"]
    assert actions[0]["canonical_registry_keys"] == ["ADBE|us|NMS"]
    assert actions[0]["superseded_by"] == "ADBE|us|NMS"
    assert actions[0]["sql_preview"] == []


def test_cleanup_plan_classifies_active_validated_without_materialized_data() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(
            [
                {
                    "registry_key": "ARMN|us|default",
                    "input_symbol": "ARMN",
                    "normalized_symbol": "ARMN",
                    "status": "active",
                    "validation_status": "validated_market_only",
                    "promotion_status": "validated_market_only",
                }
            ]
        ),
        readiness_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame(columns=["ticker", "date"]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(),
    )

    actions = plan["groups"]["active_validated_without_materialized_data"]
    assert [action["ticker"] for action in actions] == ["ARMN"]
    assert actions[0]["reason"] == "no_materialized_data/provider_failed"
    assert actions[0]["registry_keys"] == ["ARMN|us|default"]


def test_cleanup_plan_classifies_missing_scorecard_repair() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(),
        readiness_frame=pd.DataFrame([{"ticker": "EA", "tracked_search_ready": True}]),
        prices_frame=pd.DataFrame([{"ticker": "EA", "date": "2026-07-08"}]),
        technicals_frame=pd.DataFrame([{"ticker": "EA", "as_of_date": "2026-07-08"}]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(),
    )

    actions = plan["groups"]["repairable_missing_scorecard"]
    assert [action["ticker"] for action in actions] == ["EA"]
    assert actions[0]["proposed_action"] == "trigger_targeted_scorecard_build"
    assert actions[0]["sql_preview"] == []


def test_cleanup_plan_classifies_partial_and_source_only_repair() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(),
        readiness_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame(
            [{"ticker": "SPCX", "date": f"2026-07-{day:02d}"} for day in range(1, 17)]
            + [{"ticker": "TTWO", "date": f"2026-06-{day:02d}"} for day in range(1, 31)]
        ),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame(
            [
                {"ticker": "SPCX", "market_data_available": True, "fundamentals_available": True, "earnings_available": True},
                {"ticker": "TTWO", "market_data_available": True, "fundamentals_available": True, "earnings_available": True},
            ]
        ),
    )

    partial = plan["groups"]["partial_keep_review"]
    repair = plan["groups"]["repairable_missing_technicals"]
    assert [action["ticker"] for action in partial] == ["SPCX"]
    assert partial[0]["evidence"]["source_price_rows"] == 16
    assert partial[0]["proposed_action"] == "leave_partial_source_only_for_manual_review"
    assert [action["ticker"] for action in repair] == ["TTWO"]
    assert repair[0]["proposed_action"] == "trigger_technical_backfill_then_scorecard_build"


def test_cleanup_plan_can_force_thin_source_only_repair_with_allowlist() -> None:
    plan = build_ticker_readiness_cleanup_plan(
        registry_frame=pd.DataFrame(),
        readiness_frame=pd.DataFrame(),
        prices_frame=pd.DataFrame([{"ticker": "SPCX", "date": f"2026-07-{day:02d}"} for day in range(1, 17)]),
        technicals_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        scorecard_frame=pd.DataFrame(columns=["ticker", "as_of_date"]),
        coverage_frame=pd.DataFrame([{"ticker": "SPCX", "market_data_available": True}]),
        repair_allowlist={"SPCX"},
        partial_price_row_threshold=100000,
    )

    repair = plan["groups"]["repairable_missing_technicals"]
    assert [action["ticker"] for action in repair] == ["SPCX"]
