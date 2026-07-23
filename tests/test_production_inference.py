from __future__ import annotations

from typing import Any

import pytest

from flows import daily_production_inference
from finance_data_ops.production import inference


def _active_bundle() -> inference.ActiveProductionBundle:
    return inference.ActiveProductionBundle(
        active_pointer={
            "active_pointer_id": "pointer-1",
            "active_candidate_id": "candidate-1",
            "active_bundle_id": "bundle-1",
        },
        active_bundle={"bundle_manifest_json": {"model": "demo"}},
    )


def test_daily_production_inference_missing_active_pointer_skips_without_side_effects(monkeypatch) -> None:
    def fail_resolve(**kwargs: Any) -> inference.ActiveProductionBundle:
        raise inference.NoActiveProductionPointer(
            strategy_family=kwargs["strategy_family"],
            universe=kwargs["universe"],
            environment=kwargs["environment"],
        )

    def fail_orchestrator(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("orchestrator should not run when no active pointer exists")

    class FailingPublisher:
        def __init__(self, **_kwargs: Any) -> None:
            raise AssertionError("publisher should not be constructed when no active pointer exists")

    monkeypatch.setattr(inference, "resolve_active_production_bundle", fail_resolve)
    monkeypatch.setattr(inference, "run_orchestrator_command", fail_orchestrator)
    monkeypatch.setattr(inference, "PostgresPublisher", FailingPublisher)

    result = inference.run_daily_production_inference(
        database_dsn="postgresql://example",
        strategy_family="spy_signal",
        universe="spy",
        environment="production",
        target_date="2026-06-30",
    )

    assert result == {
        "status": "skipped",
        "reason": "no_active_pointer",
        "strategy_family": "spy_signal",
        "universe": "spy",
        "environment": "production",
        "signals_written": 0,
    }


def test_daily_production_inference_active_pointer_missing_orchestrator_command_raises(monkeypatch) -> None:
    monkeypatch.setattr(inference, "resolve_active_production_bundle", lambda **_kwargs: _active_bundle())

    with pytest.raises(ValueError, match="orchestrator_command is required"):
        inference.run_daily_production_inference(
            database_dsn="postgresql://example",
            strategy_family="spy_signal",
            universe="spy",
            environment="production",
            target_date="2026-06-30",
        )


def test_daily_production_inference_active_pointer_dry_run_returns_payload(monkeypatch) -> None:
    monkeypatch.setattr(inference, "resolve_active_production_bundle", lambda **_kwargs: _active_bundle())

    result = inference.run_daily_production_inference(
        database_dsn="postgresql://example",
        strategy_family="spy_signal",
        universe="spy",
        environment="production",
        target_date="2026-06-30",
        dry_run=True,
    )

    assert result["status"] == "dry_run"
    assert result["signals_written"] == 0
    assert result["payload"]["active_pointer"]["active_pointer_id"] == "pointer-1"
    assert result["payload"]["target_dates"] == ["2026-06-30"]


def test_daily_production_inference_flow_surfaces_skipped_result(monkeypatch) -> None:
    class FakeSettings:
        database_dsn = "postgresql://example"

        def require_database(self) -> None:
            return None

    monkeypatch.setattr(daily_production_inference, "load_settings", lambda **_kwargs: FakeSettings())
    monkeypatch.setattr(
        daily_production_inference,
        "run_daily_production_inference",
        lambda **kwargs: {
            "status": "skipped",
            "reason": "no_active_pointer",
            "strategy_family": kwargs["strategy_family"],
            "universe": kwargs["universe"],
            "environment": kwargs["environment"],
            "signals_written": 0,
        },
    )

    flow_callable = daily_production_inference.daily_production_inference_flow
    if hasattr(flow_callable, "fn"):
        from prefect.testing.utilities import prefect_test_harness

        with prefect_test_harness():
            result = flow_callable(
                strategy_family="spy_signal",
                universe="spy",
                environment="production",
                target_date="2026-06-30",
            )
    else:
        result = flow_callable(
            strategy_family="spy_signal",
            universe="spy",
            environment="production",
            target_date="2026-06-30",
        )

    assert result == {
        "status": "skipped",
        "reason": "no_active_pointer",
        "strategy_family": "spy_signal",
        "universe": "spy",
        "environment": "production",
        "signals_written": 0,
    }


def test_daily_production_inference_upsert_populates_production_signal_pk(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class FakePublisher:
        def __init__(self, *, database_dsn: str) -> None:
            captured["database_dsn"] = database_dsn

        def upsert(
            self,
            table: str,
            rows: list[dict[str, Any]],
            *,
            on_conflict: str | None = None,
        ) -> dict[str, Any]:
            captured["table"] = table
            captured["rows"] = rows
            captured["on_conflict"] = on_conflict
            return {"table": table, "status": "ok", "rows": len(rows)}

    monkeypatch.setattr(inference, "PostgresPublisher", FakePublisher)
    monkeypatch.setattr(inference, "resolve_active_production_bundle", lambda **_kwargs: _active_bundle())
    monkeypatch.setattr(
        inference,
        "run_orchestrator_command",
        lambda **_kwargs: {
            "signals": [
                {
                    "symbol": "spy",
                    "signal_date": "2026-06-30",
                    "score": "0.72",
                    "rank": "1",
                    "selected": "true",
                    "target_exposure": "0.35",
                    "created_at": "2026-06-30T20:00:00+00:00",
                    "updated_at": "2026-06-30T20:01:00+00:00",
                }
            ],
            "result_ref": "s3://signals/2026-06-30.json",
            "orchestrator_run_id": "orch-run-1",
        },
    )

    result = inference.run_daily_production_inference(
        database_dsn="postgresql://example",
        strategy_family="spy_signal",
        universe="spy",
        environment="production",
        target_date="2026-06-30",
        orchestrator_command="ignored",
    )

    assert result["signals_written"] == 1
    assert captured["table"] == "production_signals"
    assert captured["on_conflict"] == "signal_date,strategy_family,universe,environment,symbol"
    row = captured["rows"][0]
    assert tuple(row.keys()) == inference.PRODUCTION_SIGNAL_COLUMNS
    assert {column: row[column] for column in inference.PRODUCTION_SIGNAL_PK_COLUMNS} == {
        "signal_date": "2026-06-30",
        "strategy_family": "spy_signal",
        "universe": "spy",
        "environment": "production",
        "symbol": "SPY",
    }
    assert row["active_pointer_id"] == "pointer-1"
    assert row["candidate_id"] == "candidate-1"
    assert row["bundle_id"] == "bundle-1"
    assert row["target_exposure"] == 0.35
    assert row["result_ref"] == "s3://signals/2026-06-30.json"
    assert row["orchestrator_run_id"] == "orch-run-1"
