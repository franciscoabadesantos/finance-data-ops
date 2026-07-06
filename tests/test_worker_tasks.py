from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Any

import pytest

WORKER_ROOT = Path(__file__).resolve().parents[1] / "services" / "finance-jobs-worker"
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from app.tasks import CloudTasksEnqueuer, EnqueueResult, _task_id_from_key


class FakeResponse:
    status = 200

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def getcode(self) -> int:
        return int(self.status)


def _settings(**overrides: Any) -> SimpleNamespace:
    values = {
        "cloud_tasks_enabled": False,
        "gcp_project_id": None,
        "gcp_location": "us-central1",
        "gcp_tasks_queue": "ticker-jobs",
        "worker_base_url": "http://127.0.0.1:18081",
        "worker_shared_token": "worker-token",
        "tasks_invoker_service_account_email": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_backfill_enqueue_posts_to_local_worker_when_cloud_tasks_disabled(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fake_urlopen(request, *, timeout: float):
        calls.append({"request": request, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr("app.tasks.urllib_request.urlopen", _fake_urlopen)
    queue = CloudTasksEnqueuer(_settings())
    payload = {
        "job_type": "ticker_backfill",
        "registry_key": "ISRG|us|default",
        "ticker": "ISRG",
        "region": "us",
        "idempotency_key": "backfill:ISRG|us|default",
    }

    result = queue.enqueue_backfill(payload, idempotency_key="backfill:ISRG|us|default")

    task_id = _task_id_from_key("backfill:ISRG|us|default")
    assert result == EnqueueResult(job_id=task_id, state="QUEUED")
    assert len(calls) == 1
    request = calls[0]["request"]
    assert request.full_url == "http://127.0.0.1:18081/jobs/execute"
    assert request.get_method() == "POST"
    assert request.headers["Content-type"] == "application/json"
    assert request.headers["Authorization"] == "Bearer worker-token"
    assert request.headers["X-cloudtasks-taskname"] == task_id
    assert request.headers["X-cloudtasks-taskretrycount"] == "0"
    assert json.loads(request.data.decode("utf-8")) == payload


def test_local_dispatch_requires_worker_base_url() -> None:
    with pytest.raises(RuntimeError, match="WORKER_BASE_URL is required"):
        CloudTasksEnqueuer(_settings(worker_base_url=""))


def test_dispatch_uses_cloud_task_creation_when_enabled() -> None:
    queue = CloudTasksEnqueuer.__new__(CloudTasksEnqueuer)
    queue.enabled = True
    created: list[tuple[dict[str, object], str]] = []

    def _fake_create(payload: dict[str, object], *, idempotency_key: str) -> EnqueueResult:
        created.append((dict(payload), idempotency_key))
        return EnqueueResult(job_id="cloud-task-1", state="QUEUED")

    queue._create_cloud_task = _fake_create  # type: ignore[method-assign]

    result = queue._dispatch(
        {"job_type": "ticker_backfill"},
        idempotency_key="backfill:ISRG|us|default",
    )

    assert result == EnqueueResult(job_id="cloud-task-1", state="QUEUED")
    assert created == [({"job_type": "ticker_backfill"}, "backfill:ISRG|us|default")]
