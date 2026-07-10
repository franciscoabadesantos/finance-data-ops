from __future__ import annotations

from dataclasses import dataclass
import logging
import os

import pytest

from finance_data_ops.validation.ticker_registry import read_ticker_registry
from flows.prefect_dataops_daily import (
    DEFAULT_TICKER_BACKFILL_START_DATE,
    dataops_ticker_backfill_flow,
    dataops_ticker_onboarding_bulk_flow,
    dataops_ticker_onboarding_flow,
    dataops_ticker_remove_flow,
)


LIVE_TICKER_REGISTRY_STATUS_VALUES = {"pending_validation", "active", "rejected"}


@dataclass
class FakeDeploymentRun:
    id: str
    state_name: str


def test_onboarding_promotable_triggers_backfill(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_run_deployment(name: str, **kwargs):
        calls.append((name, dict(kwargs)))
        if "ticker_validation" in name:
            return FakeDeploymentRun(id="val-run", state_name="Completed")
        return FakeDeploymentRun(id="bf-run", state_name="Completed")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.fetch_registry_row_by_key",
        lambda **kwargs: {
            "status": "active",
            "promotion_status": "validated_full",
            "normalized_symbol": "ANZ.AX",
            "validation_reason": "all_domains_supported",
        },
    )

    result = dataops_ticker_onboarding_flow.fn(
        input_symbol="ANZ",
        region="apac",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["decision"] == "promoted"
    assert result["promoted_symbol"] == "ANZ.AX"
    assert result["backfill_flow_run_id"] == "bf-run"
    assert len(calls) == 2
    assert calls[0][0] == "dataops_ticker_validation/ticker-validation"
    assert calls[1][0] == "dataops_ticker_backfill/ticker-backfill"
    assert calls[1][1]["parameters"]["ticker"] == "ANZ.AX"
    assert calls[1][1]["parameters"]["history_limit"] == 100
    assert calls[1][1]["parameters"]["trigger_technical_features"] is True
    assert str(calls[0][1]["idempotency_key"]).startswith("ticker-validation:ANZ|apac|default:")
    assert calls[0][1]["idempotency_key"] != "ticker-validation:ANZ|apac|default"
    assert str(calls[1][1]["idempotency_key"]).startswith("ticker-backfill:ANZ|apac|default:ANZ.AX:")
    assert calls[1][1]["idempotency_key"] != "ticker-backfill:ANZ|apac|default:ANZ.AX"
    registry = read_ticker_registry(cache_root=tmp_path)
    row = registry.loc[registry["registry_key"] == "ANZ|apac|default"].iloc[-1].to_dict()
    assert row["status"] == "active"
    assert row["notes"]["lifecycle_state"] == "ready"
    assert row["notes"]["validation_flow_run_id"] == "val-run"
    assert row["notes"]["backfill_flow_run_id"] == "bf-run"


def test_onboarding_backfill_failure_fails_the_flow(monkeypatch, tmp_path) -> None:
    """A failed backfill child must fail the onboarding parent (never report ready without data)."""
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_run_deployment(name: str, **kwargs):
        if "ticker_validation" in name:
            return FakeDeploymentRun(id="val-run", state_name="Completed")
        return FakeDeploymentRun(id="bf-run", state_name="Failed")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.fetch_registry_row_by_key",
        lambda **kwargs: {
            "status": "active",
            "promotion_status": "validated_full",
            "normalized_symbol": "CELH",
            "validation_reason": "all_domains_supported",
        },
    )

    with pytest.raises(RuntimeError, match="backfill did not complete"):
        dataops_ticker_onboarding_flow.fn(
            input_symbol="CELH",
            region="us",
            cache_root=str(tmp_path),
            publish_enabled=False,
        )
    registry = read_ticker_registry(cache_root=tmp_path)
    row = registry.loc[registry["registry_key"] == "CELH|us|default"].iloc[-1].to_dict()
    assert row["status"] == "active"
    assert row["promotion_status"] == "validated_full"
    assert row["validation_reason"] == "backfill_flow_state_failed"
    assert row["notes"]["lifecycle_state"] == "pending_backfill"
    assert row["notes"]["backfill_flow_run_id"] == "bf-run"
    assert set(registry["status"]).issubset(LIVE_TICKER_REGISTRY_STATUS_VALUES)


def test_onboarding_retry_uses_existing_promotable_row_when_completed_validation_is_reused(
    monkeypatch,
    tmp_path,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    fetch_count = 0
    existing_promotable = {
        "registry_key": "GSAT|us|default",
        "input_symbol": "GSAT",
        "normalized_symbol": "GSAT",
        "region": "us",
        "exchange": None,
        "instrument_type": "equity",
        "status": "active",
        "market_supported": True,
        "fundamentals_supported": True,
        "earnings_supported": True,
        "validation_status": "validated_full",
        "validation_reason": "all_domains_supported",
        "promotion_status": "validated_full",
        "notes": {"lifecycle_state": "pending_backfill", "validation_flow_run_id": "old-val-run"},
    }
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_run_deployment(name: str, **kwargs):
        calls.append((name, dict(kwargs)))
        if "ticker_validation" in name:
            return FakeDeploymentRun(id="old-val-run", state_name="Completed")
        return FakeDeploymentRun(id="bf-run", state_name="Completed")

    def _fake_fetch(**kwargs):
        nonlocal fetch_count
        fetch_count += 1
        if fetch_count == 1:
            return dict(existing_promotable)
        registry = read_ticker_registry(cache_root=kwargs["cache_root"])
        matches = registry.loc[registry["registry_key"] == "GSAT|us|default"]
        return matches.iloc[-1].to_dict() if not matches.empty else None

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)
    monkeypatch.setattr("flows.prefect_dataops_daily.fetch_registry_row_by_key", _fake_fetch)

    result = dataops_ticker_onboarding_flow.fn(
        input_symbol="GSAT",
        region="us",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["decision"] == "promoted"
    assert result["promoted_symbol"] == "GSAT"
    assert [name for name, _ in calls] == [
        "dataops_ticker_validation/ticker-validation",
        "dataops_ticker_backfill/ticker-backfill",
    ]
    assert str(calls[0][1]["idempotency_key"]).startswith("ticker-validation:GSAT|us|default:")
    assert calls[0][1]["idempotency_key"] != "ticker-validation:GSAT|us|default"
    assert str(calls[1][1]["idempotency_key"]).startswith("ticker-backfill:GSAT|us|default:GSAT:")
    registry = read_ticker_registry(cache_root=tmp_path)
    row = registry.loc[registry["registry_key"] == "GSAT|us|default"].iloc[-1].to_dict()
    assert row["status"] == "active"
    assert row["promotion_status"] == "validated_full"
    assert row["validation_reason"] == "all_domains_supported"
    assert row["notes"]["lifecycle_state"] == "ready"
    assert row["notes"]["validation_flow_run_id"] == "old-val-run"
    assert row["notes"]["backfill_flow_run_id"] == "bf-run"
    assert set(registry["status"]).issubset(LIVE_TICKER_REGISTRY_STATUS_VALUES)


def test_onboarding_completed_validation_without_registry_update_fails_truthfully(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_deployment",
        lambda *args, **kwargs: FakeDeploymentRun(id="val-run", state_name="Completed"),
    )

    def _fake_fetch(**kwargs):
        registry = read_ticker_registry(cache_root=kwargs["cache_root"])
        matches = registry.loc[registry["registry_key"] == "GSAT|us|default"]
        return matches.iloc[-1].to_dict() if not matches.empty else None

    monkeypatch.setattr("flows.prefect_dataops_daily.fetch_registry_row_by_key", _fake_fetch)

    with pytest.raises(RuntimeError, match="remained pending_validation"):
        dataops_ticker_onboarding_flow.fn(
            input_symbol="GSAT",
            region="us",
            cache_root=str(tmp_path),
            publish_enabled=False,
        )

    registry = read_ticker_registry(cache_root=tmp_path)
    row = registry.loc[registry["registry_key"] == "GSAT|us|default"].iloc[-1].to_dict()
    assert row["status"] == "pending_validation"
    assert row["validation_reason"] == "validation_completed_without_registry_update"
    assert row["notes"]["lifecycle_state"] == "validation_incomplete"
    assert row["notes"]["validation_flow_run_id"] == "val-run"


def test_onboarding_rejected_does_not_trigger_backfill(monkeypatch, tmp_path) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_run_deployment(name: str, **kwargs):
        calls.append((name, dict(kwargs)))
        return FakeDeploymentRun(id="val-run", state_name="Completed")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.fetch_registry_row_by_key",
        lambda **kwargs: {
            "status": "rejected",
            "promotion_status": "rejected",
            "normalized_symbol": "EGPT",
            "validation_reason": "market_required_failed:market_daily_empty",
        },
    )

    result = dataops_ticker_onboarding_flow.fn(
        input_symbol="EGPT",
        region="apac",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["decision"] == "rejected"
    assert result["backfill_flow_run_id"] is None
    assert len(calls) == 1
    assert calls[0][0] == "dataops_ticker_validation/ticker-validation"


def test_onboarding_validation_failure_persists_rejected_row(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_deployment",
        lambda *args, **kwargs: FakeDeploymentRun(id="val-run", state_name="Failed"),
    )

    result = dataops_ticker_onboarding_flow.fn(
        input_symbol="XYZ",
        region="us",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["decision"] == "rejected"
    registry = read_ticker_registry(cache_root=tmp_path)
    row = registry.loc[registry["registry_key"] == "XYZ|us|default"].iloc[-1].to_dict()
    assert row["status"] == "rejected"
    assert row["validation_status"] == "rejected"
    assert str(row["validation_reason"]).startswith("validation_flow_state_failed")


def test_ticker_backfill_defaults_full_history_caps_earnings_and_triggers_features(
    monkeypatch,
    tmp_path,
) -> None:
    calls: dict[str, object] = {"deployments": []}
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_market(**kwargs):
        calls["market"] = dict(kwargs)
        return {"run_id": "market-run"}

    def _fake_earnings(**kwargs):
        calls["earnings"] = dict(kwargs)
        return {"run_id": "earnings-run"}

    def _fake_fundamentals(**kwargs):
        calls["fundamentals"] = dict(kwargs)
        return {"run_id": "fundamentals-run"}

    def _fake_run_deployment(name: str, **kwargs):
        deployment_calls = calls["deployments"]
        assert isinstance(deployment_calls, list)
        deployment_calls.append({"name": name, **dict(kwargs)})
        if name == "technical-feature-backfill/technical-feature-backfill":
            return FakeDeploymentRun(id="technical-run", state_name="Completed")
        return FakeDeploymentRun(id="scorecard-run", state_name="Scheduled")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_market_daily", _fake_market)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_earnings_daily", _fake_earnings)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_fundamentals_daily", _fake_fundamentals)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)

    result = dataops_ticker_backfill_flow.fn(
        ticker="aapl",
        end="2026-04-18",
        history_limit=120,
        cache_root=str(tmp_path),
        publish_enabled=True,
        isolated_cache=False,
    )

    market_call = calls["market"]
    earnings_call = calls["earnings"]
    deployment_calls = calls["deployments"]
    assert isinstance(market_call, dict)
    assert isinstance(earnings_call, dict)
    assert isinstance(deployment_calls, list)
    assert market_call["start"] == DEFAULT_TICKER_BACKFILL_START_DATE
    assert market_call["end"] == "2026-04-18"
    assert earnings_call["history_limit"] == 100
    assert result["requested_history_limit"] == 120
    assert result["history_limit"] == 100
    assert result["materialization_status"] == {
        "source_data_status": "complete",
        "technical_features_status": "triggered",
        "scorecard_build_status": "triggered",
    }
    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert result["steps"]["scorecard_build"]["status"] == "triggered"
    assert result["steps"]["scorecard_build"]["flow_run_id"] == "scorecard-run"
    assert len(deployment_calls) == 2
    assert deployment_calls[0]["name"] == "technical-feature-backfill/technical-feature-backfill"
    assert deployment_calls[0]["parameters"] == {
        "symbols": ["AAPL"],
        "start": DEFAULT_TICKER_BACKFILL_START_DATE,
        "end": "2026-04-18",
    }
    assert deployment_calls[1]["name"] == "scorecard-daily/scorecard-daily"
    assert deployment_calls[1]["parameters"] == {"symbols": ["AAPL"], "as_of_date": "2026-04-18"}


def test_ticker_backfill_isolated_cache_uses_temp_dir_and_cleans_up(monkeypatch, tmp_path) -> None:
    """Each onboarding backfill must run in its own cache_root (no shared parquet contention)."""
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    seen: dict[str, object] = {}

    def _fake_market(**kwargs):
        seen["cache_root"] = kwargs.get("cache_root")
        return {"run_id": "market-run"}

    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_market_daily", _fake_market)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_earnings_daily", lambda **k: {"run_id": "e"})
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_fundamentals_daily", lambda **k: {"run_id": "f"})
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_deployment",
        lambda name, **kwargs: FakeDeploymentRun(id="technical-run", state_name="Completed"),
    )

    result = dataops_ticker_backfill_flow.fn(
        ticker="aapl",
        end="2026-04-18",
        cache_root=str(tmp_path),
        publish_enabled=True,
        isolated_cache=True,
    )

    used = str(seen["cache_root"])
    assert used != str(tmp_path)  # ran in an isolated dir, not the shared cache
    assert "ticker-backfill-" in used
    assert not os.path.exists(used)  # temp cache cleaned up after the run
    assert result["status"] == "success"
    assert result["materialization_status"]["source_data_status"] == "complete"


def test_ticker_backfill_triggers_configured_scorecard_build_after_technicals(
    monkeypatch,
    tmp_path,
) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_market_daily",
        lambda **kwargs: {"run_id": "market-run"},
    )
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_earnings_daily",
        lambda **kwargs: {"run_id": "earnings-run"},
    )
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_fundamentals_daily",
        lambda **kwargs: {"run_id": "fundamentals-run"},
    )

    def _fake_run_deployment(name: str, **kwargs):
        calls.append({"name": name, **dict(kwargs)})
        if name == "technical-feature-backfill/technical-feature-backfill":
            return FakeDeploymentRun(id="technical-run", state_name="Completed")
        return FakeDeploymentRun(id="scorecard-run", state_name="Scheduled")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)

    result = dataops_ticker_backfill_flow.fn(
        ticker="aapl",
        end="2026-04-18",
        cache_root=str(tmp_path),
        publish_enabled=True,
        scorecard_build_deployment_name="scorecard-build/scorecard-only",
    )

    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert result["steps"]["scorecard_build"]["status"] == "triggered"
    assert result["steps"]["scorecard_build"]["flow_run_id"] == "scorecard-run"
    assert calls[0]["name"] == "technical-feature-backfill/technical-feature-backfill"
    assert calls[1]["name"] == "scorecard-build/scorecard-only"
    assert calls[1]["parameters"] == {"symbols": ["AAPL"], "as_of_date": "2026-04-18"}


def test_ticker_backfill_scorecard_trigger_failure_is_reported_not_raised(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_market_daily",
        lambda **kwargs: {"run_id": "market-run"},
    )
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_earnings_daily",
        lambda **kwargs: {"run_id": "earnings-run"},
    )
    monkeypatch.setattr(
        "flows.prefect_dataops_daily.run_dataops_fundamentals_daily",
        lambda **kwargs: {"run_id": "fundamentals-run"},
    )

    def _fake_run_deployment(name: str, **kwargs):
        if name == "scorecard-build/missing":
            raise RuntimeError("deployment not found")
        return FakeDeploymentRun(id="technical-run", state_name="Completed")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)

    result = dataops_ticker_backfill_flow.fn(
        ticker="aapl",
        end="2026-04-18",
        cache_root=str(tmp_path),
        publish_enabled=True,
        scorecard_build_deployment_name="scorecard-build/missing",
    )

    assert result["status"] == "success"
    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert result["materialization_status"]["scorecard_build_status"] == "failed"
    assert result["steps"]["scorecard_build"]["status"] == "failed"
    assert "FEATURE_SCORECARD_BUILD_DEPLOYMENT" in result["steps"]["scorecard_build"]["error"]


def test_ticker_backfill_earnings_empty_is_best_effort_and_still_triggers_technicals(
    monkeypatch,
    tmp_path,
) -> None:
    """A freshly listed ticker (recent IPO) with no earnings must not abort the backfill."""
    calls: dict[str, object] = {}
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))

    def _fake_market(**kwargs):
        calls["market"] = dict(kwargs)
        return {"run_id": "market-run"}

    def _fake_earnings(**kwargs):
        calls["earnings"] = dict(kwargs)
        raise RuntimeError("Data Ops earnings daily unhealthy status: earnings_empty")

    def _fake_fundamentals(**kwargs):
        calls["fundamentals"] = dict(kwargs)
        raise RuntimeError("fundamentals empty")

    def _fake_run_deployment(name: str, **kwargs):
        calls["deployment"] = {"name": name, **dict(kwargs)}
        return FakeDeploymentRun(id="technical-run", state_name="Completed")

    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_market_daily", _fake_market)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_earnings_daily", _fake_earnings)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_dataops_fundamentals_daily", _fake_fundamentals)
    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)

    result = dataops_ticker_backfill_flow.fn(
        ticker="spcx",
        end="2026-07-06",
        history_limit=100,
        cache_root=str(tmp_path),
        publish_enabled=True,
    )

    # Earnings + fundamentals failed, but the backfill still succeeded and triggered technicals.
    assert result["status"] == "success"
    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert result["steps"]["earnings"]["status"] == "skipped"
    assert result["steps"]["earnings"]["best_effort"] is True
    assert result["steps"]["fundamentals"]["status"] == "skipped"
    assert result["materialization_status"]["source_data_status"] == "partial"
    assert "deployment" in calls


def test_bulk_onboarding_schedules_runs_dedupes_and_skips_active(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    scheduled: list[tuple[str, object]] = []
    run_names: list[str] = []

    def _fake_run_deployment(name: str, **kwargs):
        symbol = kwargs["parameters"]["input_symbol"]
        scheduled.append((name, symbol))
        run_names.append(kwargs.get("flow_run_name"))
        return FakeDeploymentRun(id=f"run-{symbol}", state_name="Scheduled")

    def _fake_fetch(**kwargs):
        # AAA is already active -> should be skipped; others are absent.
        if str(kwargs["registry_key"]).startswith("AAA|"):
            return {
                "status": "active",
                "validation_status": "validated_full",
                "promotion_status": "validated_full",
                "normalized_symbol": "AAA",
            }
        return None

    monkeypatch.setattr("flows.prefect_dataops_daily.run_deployment", _fake_run_deployment)
    monkeypatch.setattr("flows.prefect_dataops_daily.fetch_registry_row_by_key", _fake_fetch)

    result = dataops_ticker_onboarding_bulk_flow.fn(
        tickers=["bbb", "ccc", "aaa", "bbb"],  # duplicate bbb collapses; aaa already active
        region="us",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["scheduled_count"] == 2
    assert {row["ticker"] for row in result["scheduled"]} == {"BBB", "CCC"}
    assert [row["ticker"] for row in result["skipped"]] == ["AAA"]
    # every scheduled run targeted the onboarding deployment
    assert {name for name, _ in scheduled} == {"dataops_ticker_onboarding/ticker-onboarding"}
    # runs are named with the backend's deterministic convention so /tickers/status can find them
    assert set(run_names) == {"onboard-bbb-us-default", "onboard-ccc-us-default"}


def test_ticker_remove_rejects_safe_lifecycle_state(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    existing = {
        "registry_key": "SPCX|us|default",
        "input_symbol": "SPCX",
        "normalized_symbol": "SPCX",
        "region": "us",
        "exchange": None,
        "instrument_type": "equity",
        "status": "pending_validation",
        "market_supported": False,
        "fundamentals_supported": False,
        "earnings_supported": False,
        "validation_status": "pending_validation",
        "validation_reason": "pending_validation",
        "promotion_status": "pending_validation",
        "notes": {"lifecycle_state": "pending_backfill"},
    }
    monkeypatch.setattr("flows.prefect_dataops_daily.fetch_registry_row_by_key", lambda **kwargs: existing)

    result = dataops_ticker_remove_flow.fn(
        input_symbol="SPCX",
        region="us",
        reason="user_cancelled",
        requested_by="backend",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["status"] == "removed"
    row = result["registry_row"]
    assert row["status"] == "rejected"
    assert row["validation_reason"] == "user_cancelled"
    assert row["notes"]["lifecycle_state"] == "rejected"
    assert row["notes"]["remove_requested_by"] == "backend"


def test_ticker_remove_blocks_ready_active_rows(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("flows.prefect_dataops_daily.get_run_logger", lambda: logging.getLogger("test"))
    existing = {
        "registry_key": "AAPL|us|default",
        "input_symbol": "AAPL",
        "normalized_symbol": "AAPL",
        "region": "us",
        "exchange": None,
        "status": "active",
        "validation_status": "validated_full",
        "promotion_status": "validated_full",
        "notes": {"lifecycle_state": "ready"},
    }
    monkeypatch.setattr("flows.prefect_dataops_daily.fetch_registry_row_by_key", lambda **kwargs: existing)

    result = dataops_ticker_remove_flow.fn(
        input_symbol="AAPL",
        region="us",
        reason="mistake",
        cache_root=str(tmp_path),
        publish_enabled=False,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "unsafe_lifecycle_state"
