"""Rebuild progress persistence using analysis_jobs + analysis_results."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


class RebuildProgressStore:
    def __init__(self, *, registry: Any, job_id: str, analysis_type: str, domain: str) -> None:
        self.registry = registry
        self.job_id = str(job_id)
        self.analysis_type = str(analysis_type)
        self.domain = str(domain)

    def update(
        self,
        *,
        job_status: str | None = None,
        step: str,
        step_status: str,
        payload: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        result_json = dict(payload)
        result_json.setdefault("metadata", {})
        result_json["metadata"].update(
            {
                "job_id": self.job_id,
                "analysis_type": self.analysis_type,
                "domain": self.domain,
                "last_updated_at": now_iso(),
            }
        )
        result_json["progress"] = {
            "step": step,
            "step_status": step_status,
            "current_batch": payload.get("current_batch"),
            "total_batches": payload.get("total_batches"),
            "current_window": payload.get("current_window"),
            "rows_written_total": payload.get("rows_written_total", 0),
            "rows_deleted_total": payload.get("rows_deleted_total", 0),
            "health_checks": payload.get("health_checks", []),
            "abort_reason": payload.get("abort_reason"),
            "finalization_status": payload.get("finalization_status"),
            "last_updated_at": now_iso(),
        }
        summary = str(payload.get("summary") or f"Data ops rebuild {step_status}.")
        self.registry.upsert_analysis_result(
            job_id=self.job_id,
            analysis_type=self.analysis_type,
            result_json=result_json,
            summary_text=summary,
        )
        patch: dict[str, Any] = {"error_message": error_message}
        if job_status is not None:
            patch["status"] = str(job_status)
            if job_status in {"completed", "failed"}:
                patch["finished_at"] = now_iso()
        self.registry.patch_analysis_job(self.job_id, patch)


class NullRebuildProgressStore:
    def update(
        self,
        *,
        job_status: str | None = None,
        step: str,
        step_status: str,
        payload: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        return None
