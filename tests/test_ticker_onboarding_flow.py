from __future__ import annotations

from dataclasses import dataclass
import logging

from finance_data_ops.validation.ticker_registry import read_ticker_registry
from flows.prefect_dataops_daily import (
    DEFAULT_TICKER_BACKFILL_START_DATE,
    dataops_ticker_backfill_flow,
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
    assert result["steps"]["technical_features"]["flow_run_id"] == "technical-run"
    assert deployment_call["name"] == "technical-feature-backfill/technical-feature-backfill"
    assert deployment_call["parameters"] == {
        "symbols": ["AAPL"],
        "start": DEFAULT_TICKER_BACKFILL_START_DATE,
        "end": "2026-04-18",
    }


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
    assert "deployment" in calls
