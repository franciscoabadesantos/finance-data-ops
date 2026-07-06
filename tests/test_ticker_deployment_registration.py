from __future__ import annotations

import inspect
from pathlib import Path

import yaml

from flows.prefect_dataops_daily import (
    dataops_ticker_backfill_flow,
    dataops_ticker_onboarding_bulk_flow,
    dataops_ticker_onboarding_flow,
    dataops_ticker_validation_flow,
)

PREFECT_YAML = Path(__file__).resolve().parents[1] / "prefect.yaml"


def _deployments() -> dict[str, dict]:
    data = yaml.safe_load(PREFECT_YAML.read_text())
    return {d["name"]: d for d in data["deployments"]}


def test_ticker_deployments_registered_with_matching_entrypoints() -> None:
    deployments = _deployments()
    expected = {
        "ticker-validation": "dataops_ticker_validation_flow",
        "ticker-backfill": "dataops_ticker_backfill_flow",
        "ticker-onboarding": "dataops_ticker_onboarding_flow",
        "ticker-onboarding-bulk": "dataops_ticker_onboarding_bulk_flow",
    }
    for dep_name, fn_name in expected.items():
        assert dep_name in deployments, f"missing deployment {dep_name}"
        dep = deployments[dep_name]
        assert dep["entrypoint"].endswith(f":{fn_name}"), dep["entrypoint"]
        assert dep["work_pool"]["name"] == "finance-ops-self-host"


def test_onboarding_flow_default_deployment_names_match_registered() -> None:
    """The deployment names the onboarding flow triggers must be the ones registered.

    This is the class of bug that already bit once (the technicals deployment-name mismatch).
    """
    deployments = _deployments()
    sig = inspect.signature(dataops_ticker_onboarding_flow.fn)
    validation_ref = sig.parameters["validation_deployment_name"].default
    backfill_ref = sig.parameters["backfill_deployment_name"].default

    assert validation_ref == f"{dataops_ticker_validation_flow.name}/ticker-validation"
    assert backfill_ref == f"{dataops_ticker_backfill_flow.name}/ticker-backfill"
    assert "ticker-validation" in deployments
    assert "ticker-backfill" in deployments


def test_bulk_flow_default_onboarding_deployment_name_matches_registered() -> None:
    deployments = _deployments()
    sig = inspect.signature(dataops_ticker_onboarding_bulk_flow.fn)
    ref = sig.parameters["onboarding_deployment_name"].default
    assert ref == f"{dataops_ticker_onboarding_flow.name}/ticker-onboarding"
    assert "ticker-onboarding" in deployments
