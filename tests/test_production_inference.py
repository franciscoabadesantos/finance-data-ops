from __future__ import annotations

from typing import Any

from finance_data_ops.production import inference


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
    monkeypatch.setattr(
        inference,
        "resolve_active_production_bundle",
        lambda **_kwargs: inference.ActiveProductionBundle(
            active_pointer={
                "active_pointer_id": "pointer-1",
                "active_candidate_id": "candidate-1",
                "active_bundle_id": "bundle-1",
            },
            active_bundle={"bundle_manifest_json": {"model": "demo"}},
        ),
    )
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
