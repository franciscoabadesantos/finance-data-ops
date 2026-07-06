from __future__ import annotations

from pathlib import Path
import sys
import threading
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

WORKER_ROOT = Path(__file__).resolve().parents[1] / "services" / "finance-jobs-worker"
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from app import executors as worker_executors  # type: ignore[import-not-found]
from app import main as worker_main  # type: ignore[import-not-found]
from app.executors import JobExecutor  # type: ignore[import-not-found]
from app.registry import parse_notes  # type: ignore[import-not-found]
from app.tasks import EnqueueResult  # type: ignore[import-not-found]


class MinimalRegistry:
    def __init__(self, *, completed_event: threading.Event) -> None:
        self.completed_event = completed_event
        self.async_job_runs: list[dict[str, Any]] = []
        self.registry_row: dict[str, Any] = {
            "registry_key": "NET|us|default",
            "input_symbol": "NET",
            "normalized_symbol": "NET",
            "region": "us",
            "exchange": None,
            "status": "pending_validation",
            "validation_status": "pending",
            "promotion_status": "pending_validation",
            "notes": {},
        }

    def get_by_key(self, registry_key: str) -> dict[str, Any] | None:
        if registry_key == self.registry_row["registry_key"]:
            return dict(self.registry_row)
        return None

    def patch_row(self, registry_key: str, patch: dict[str, Any]) -> dict[str, Any]:
        if registry_key != self.registry_row["registry_key"]:
            raise RuntimeError("ticker_registry row missing after update.")
        self.registry_row.update(dict(patch))
        return dict(self.registry_row)

    def reject(self, registry_key: str, reason: str) -> dict[str, Any]:
        return self.patch_row(
            registry_key,
            {
                "status": "rejected",
                "validation_status": "rejected",
                "promotion_status": "rejected",
                "validation_reason": str(reason),
            },
        )

    def record_async_job_run(
        self,
        *,
        job_id: str,
        job_type: str,
        registry_key: str | None,
        idempotency_key: str,
        status: str,
        attempt: int,
        payload: dict[str, Any],
        started_at: str | None = None,
        finished_at: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        row = {
            "job_id": job_id,
            "job_type": job_type,
            "registry_key": registry_key,
            "idempotency_key": idempotency_key,
            "status": status,
            "attempt": attempt,
            "payload": dict(payload),
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "metadata": metadata or {},
        }
        for existing in self.async_job_runs:
            if existing["job_id"] == job_id:
                existing.update(row)
                break
        else:
            self.async_job_runs.append(row)
        if status == "completed":
            self.completed_event.set()


class FakeTasks:
    def __init__(self) -> None:
        self.backfill_payloads: list[dict[str, Any]] = []

    def enqueue_backfill(self, payload: dict[str, Any], *, idempotency_key: str) -> EnqueueResult:
        self.backfill_payloads.append({"payload": dict(payload), "idempotency_key": idempotency_key})
        return EnqueueResult(job_id="job-backfill", state="QUEUED")


class FakeSettings:
    default_history_limit = 24
    default_backfill_years = 5


def test_ticker_execute_returns_202_and_completes_in_background(monkeypatch) -> None:
    validation_started = threading.Event()
    validation_release = threading.Event()
    job_completed = threading.Event()
    registry = MinimalRegistry(completed_event=job_completed)
    tasks = FakeTasks()
    executor = JobExecutor(
        settings=FakeSettings(),
        registry=registry,  # type: ignore[arg-type]
        tasks=tasks,  # type: ignore[arg-type]
    )

    def _fake_get_settings():
        return SimpleNamespace(worker_shared_token=None)

    def _fake_validation(**_kwargs: object) -> dict[str, Any]:
        validation_started.set()
        validation_release.wait(timeout=5.0)
        return {
            "registry_row": {
                "registry_key": "NET|us|default",
                "input_symbol": "NET",
                "normalized_symbol": "NET",
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
                "last_validated_at": "2026-07-06T12:00:00+00:00",
                "notes": "ok",
            },
            "selected": {"validation_status": "validated_full"},
        }

    monkeypatch.setattr(worker_main, "get_settings", _fake_get_settings)
    monkeypatch.setattr(worker_main.app.state, "executor", executor, raising=False)
    monkeypatch.setattr(worker_executors, "run_single_ticker_validation", _fake_validation)

    client = TestClient(worker_main.app)
    response_holder: dict[str, Any] = {}
    response_returned = threading.Event()

    def _post_job() -> None:
        response_holder["response"] = client.post(
            "/jobs/execute",
            headers={"X-CloudTasks-TaskName": "queues/ticker-jobs/tasks/job-net-validation"},
            json={
                "job_type": "ticker_validation",
                "registry_key": "NET|us|default",
                "ticker": "NET",
                "region": "us",
                "history_limit": 24,
                "idempotency_key": "validate:NET|us|default",
            },
        )
        response_returned.set()

    request_thread = threading.Thread(target=_post_job, daemon=True)
    request_thread.start()

    assert response_returned.wait(timeout=1.0)
    response = response_holder["response"]
    assert response.status_code == 202
    assert response.json()["job_id"] == "job-net-validation"
    assert response.json()["accepted"] is True
    assert validation_started.wait(timeout=1.0)
    assert not job_completed.is_set()

    validation_release.set()
    assert job_completed.wait(timeout=2.0)

    run = registry.async_job_runs[0]
    assert run["job_id"] == "job-net-validation"
    assert run["job_type"] == "ticker_validation"
    assert run["status"] == "completed"
    assert tasks.backfill_payloads[0]["payload"]["job_type"] == "ticker_backfill"
    assert parse_notes(registry.registry_row["notes"])["backfill_job_id"] == "job-backfill"


def test_backfill_earnings_empty_is_best_effort_and_completes(monkeypatch) -> None:
    """Executor backfill: earnings/fundamentals failure is non-fatal; technicals still run."""
    from app.models import ExecuteJobRequest  # type: ignore[import-not-found]

    registry = MinimalRegistry(completed_event=threading.Event())
    registry.registry_row["status"] = "pending_backfill"
    executor = JobExecutor(
        settings=FakeSettings(),
        registry=registry,  # type: ignore[arg-type]
        tasks=FakeTasks(),  # type: ignore[arg-type]
    )

    steps: dict[str, Any] = {}

    def _fake_market(**kwargs):
        steps["market"] = dict(kwargs)
        return {"run_id": "market-run"}

    def _fake_earnings(**kwargs):
        steps["earnings"] = dict(kwargs)
        raise RuntimeError("Data Ops earnings daily unhealthy status: earnings_empty")

    def _fake_fundamentals(**kwargs):
        steps["fundamentals"] = dict(kwargs)
        raise RuntimeError("fundamentals empty")

    def _fake_technicals(**kwargs):
        steps["technicals"] = dict(kwargs)
        return {"status": "completed", "flow_run_id": "tech-run"}

    monkeypatch.setattr(worker_executors, "run_dataops_market_daily", _fake_market)
    monkeypatch.setattr(worker_executors, "run_dataops_earnings_daily", _fake_earnings)
    monkeypatch.setattr(worker_executors, "run_dataops_fundamentals_daily", _fake_fundamentals)
    monkeypatch.setattr(worker_executors, "trigger_technical_feature_backfill", _fake_technicals)

    request = ExecuteJobRequest(
        job_type="ticker_backfill",
        registry_key="NET|us|default",
        ticker="NET",
        region="us",
        history_limit=100,
        end="2026-07-06",
        idempotency_key="backfill:NET|us|default",
    )
    result = executor._execute_backfill(request, job_id="job-bf", idempotency_key="backfill:NET|us|default")

    # Technicals ran despite earnings + fundamentals failing, and the backfill completed.
    assert "technicals" in steps
    assert result["status"] == "completed"
    assert result["steps"]["technical_features"]["flow_run_id"] == "tech-run"
    assert registry.registry_row["status"] == "active"
