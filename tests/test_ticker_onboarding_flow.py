from __future__ import annotations

from dataclasses import dataclass
import logging

from finance_data_ops.validation.ticker_registry import read_ticker_registry
from flows.prefect_dataops_daily import dataops_ticker_onboarding_flow


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
