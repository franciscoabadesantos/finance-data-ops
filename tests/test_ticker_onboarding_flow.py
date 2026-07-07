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
)


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


def test_ticker_backfill_defaults_full_history_caps_earnings_and_triggers_technicals(
    monkeypatch,
    tmp_path,
) -> None:
    calls: dict[str, object] = {}
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
        calls["deployment"] = {"name": name, **dict(kwargs)}
        return FakeDeploymentRun(id="technical-run", state_name="Completed")

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
    deployment_call = calls["deployment"]
    assert isinstance(market_call, dict)
    assert isinstance(earnings_call, dict)
    assert isinstance(deployment_call, dict)
    assert market_call["start"] == DEFAULT_TICKER_BACKFILL_START_DATE
    assert market_call["end"] == "2026-04-18"
    assert earnings_call["history_limit"] == 100
    assert result["requested_history_limit"] == 120
    assert result["history_limit"] == 100
    assert result["materialization_status"] == {
        "source_data_status": "complete",
        "technical_features_status": "triggered",
        "scorecard_build_status": "skipped",
    }
    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert result["steps"]["scorecard_build"]["status"] == "skipped"
    assert result["steps"]["scorecard_build"]["reason"] == "deployment_not_configured"
    assert deployment_call["name"] == "technical-feature-backfill/technical-feature-backfill"
    assert deployment_call["parameters"] == {
        "symbols": ["AAPL"],
        "start": DEFAULT_TICKER_BACKFILL_START_DATE,
        "end": "2026-04-18",
    }


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
