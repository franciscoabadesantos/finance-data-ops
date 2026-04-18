from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from typing import Any

try:
    from google.api_core.exceptions import AlreadyExists
    from google.cloud import tasks_v2
except Exception:  # pragma: no cover - optional for local dry runs
    AlreadyExists = Exception  # type: ignore[assignment]
    tasks_v2 = None  # type: ignore[assignment]

from app.config import WorkerSettings


@dataclass(slots=True)
class EnqueueResult:
    job_id: str
    state: str


def _task_id_from_key(idempotency_key: str) -> str:
    digest = hashlib.sha1(str(idempotency_key).encode("utf-8")).hexdigest()[:30]
    return f"job-{digest}"


class CloudTasksEnqueuer:
    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.enabled = bool(settings.cloud_tasks_enabled)
        if not self.enabled:
            self.client = None
            return

        if not settings.gcp_project_id:
            raise RuntimeError("GCP_PROJECT_ID is required when CLOUD_TASKS_ENABLED=true.")
        if not settings.worker_base_url:
            raise RuntimeError("WORKER_BASE_URL is required when CLOUD_TASKS_ENABLED=true.")
        if tasks_v2 is None:
            raise RuntimeError("google-cloud-tasks package is required when CLOUD_TASKS_ENABLED=true.")

        self.client = tasks_v2.CloudTasksClient()
        self.parent = self.client.queue_path(
            settings.gcp_project_id,
            settings.gcp_location,
            settings.gcp_tasks_queue,
        )
        self.target_url = f"{str(settings.worker_base_url).rstrip('/')}/jobs/execute"

    def enqueue_backfill(self, payload: dict[str, Any], *, idempotency_key: str) -> EnqueueResult:
        if not self.enabled:
            task_id = _task_id_from_key(idempotency_key)
            return EnqueueResult(job_id=task_id, state="QUEUED")

        assert self.client is not None
        assert tasks_v2 is not None
        task_id = _task_id_from_key(idempotency_key)
        body = json.dumps(payload, default=str).encode("utf-8")
        task_name = f"{self.parent}/tasks/{task_id}"

        headers = {"Content-Type": "application/json"}
        if self.settings.worker_shared_token:
            headers["Authorization"] = f"Bearer {self.settings.worker_shared_token}"

        task: dict[str, Any] = {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": self.target_url,
                "headers": headers,
                "body": body,
            },
        }
        if self.settings.tasks_invoker_service_account_email:
            task["http_request"]["oidc_token"] = {
                "service_account_email": self.settings.tasks_invoker_service_account_email,
                "audience": self.target_url,
            }

        try:
            created = self.client.create_task(request={"parent": self.parent, "task": task})
            task_job_id = str(created.name).split("/")[-1]
            return EnqueueResult(job_id=task_job_id, state="QUEUED")
        except AlreadyExists:
            return EnqueueResult(job_id=task_id, state="QUEUED")

    @staticmethod
    def now_iso() -> str:
        return datetime.now(UTC).isoformat()
