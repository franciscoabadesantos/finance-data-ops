from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from typing import Any

try:
    from supabase import Client
except Exception:  # pragma: no cover - optional in local/test environments
    Client = Any  # type: ignore[assignment]


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def parse_notes(raw: object) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def payload_hash(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()


class WorkerRegistryStore:
    def __init__(self, client: Client) -> None:
        self.client = client

    def get_by_key(self, registry_key: str) -> dict[str, Any] | None:
        response = (
            self.client.table("ticker_registry")
            .select("*")
            .eq("registry_key", str(registry_key).strip())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def get_analysis_job(self, job_id: str) -> dict[str, Any] | None:
        response = (
            self.client.table("analysis_jobs")
            .select("*")
            .eq("job_id", str(job_id).strip())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def patch_analysis_job(self, job_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        update_payload = dict(patch)
        self.client.table("analysis_jobs").update(update_payload).eq("job_id", str(job_id).strip()).execute()
        row = self.get_analysis_job(job_id)
        if row is None:
            raise RuntimeError("analysis_jobs row missing after update.")
        return row

    def upsert_analysis_result(
        self,
        *,
        job_id: str,
        analysis_type: str,
        result_json: dict[str, Any],
        summary_text: str | None,
    ) -> None:
        row = {
            "job_id": str(job_id).strip(),
            "analysis_type": str(analysis_type).strip(),
            "result_json": dict(result_json),
            "summary_text": (str(summary_text).strip() if summary_text is not None else None),
            "created_at": now_iso(),
        }
        try:
            self.client.table("analysis_results").upsert(row, on_conflict="job_id").execute()
            return
        except Exception:
            pass

        existing_response = (
            self.client.table("analysis_results").select("*").eq("job_id", str(job_id).strip()).limit(1).execute()
        )
        existing = existing_response.data or []
        if existing:
            self.client.table("analysis_results").update(row).eq("job_id", str(job_id).strip()).execute()
        else:
            self.client.table("analysis_results").insert(row).execute()

    def patch_row(self, registry_key: str, patch: dict[str, Any]) -> dict[str, Any]:
        update_payload = dict(patch)
        update_payload["updated_at"] = now_iso()
        self.client.table("ticker_registry").update(update_payload).eq("registry_key", registry_key).execute()
        row = self.get_by_key(registry_key)
        if row is None:
            raise RuntimeError("ticker_registry row missing after update.")
        return row

    def resolve_registry_row(self, *, ticker: str, region: str, exchange: str | None) -> dict[str, Any] | None:
        query = (
            self.client.table("ticker_registry")
            .select("*")
            .eq("input_symbol", str(ticker).strip().upper())
            .eq("region", str(region).strip().lower())
        )
        if exchange:
            query = query.eq("exchange", str(exchange).strip().upper())
        response = query.execute()
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_market_snapshot(self, ticker: str) -> dict[str, Any] | None:
        response = (
            self.client.table("ticker_market_stats_snapshot")
            .select("*")
            .eq("ticker", str(ticker).strip().upper())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_symbol_coverage(self, ticker: str) -> dict[str, Any] | None:
        response = (
            self.client.table("symbol_data_coverage")
            .select("*")
            .eq("ticker", str(ticker).strip().upper())
            .limit(1)
            .execute()
        )
        data = response.data or []
        if not data:
            return None
        return dict(data[0])

    def fetch_data_asset_status(self) -> dict[str, dict[str, Any]]:
        response = self.client.table("data_asset_status").select("*").execute()
        rows = [dict(item) for item in (response.data or []) if isinstance(item, dict)]
        by_key: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = str(row.get("asset_key") or "").strip()
            if key:
                by_key[key] = row
        return by_key

    def merge_notes(self, registry_key: str, patch: dict[str, Any]) -> dict[str, Any]:
        row = self.get_by_key(registry_key)
        if row is None:
            raise RuntimeError("ticker_registry row not found.")
        notes = parse_notes(row.get("notes"))
        notes.update(patch)
        return self.patch_row(registry_key, {"notes": notes})

    def reject(self, registry_key: str, reason: str) -> dict[str, Any]:
        return self.patch_row(
            registry_key,
            {
                "status": "rejected",
                "validation_status": "rejected",
                "promotion_status": "rejected",
                "validation_reason": str(reason),
                "last_validated_at": now_iso(),
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
            "job_id": str(job_id),
            "job_type": str(job_type),
            "registry_key": str(registry_key or ""),
            "idempotency_key": str(idempotency_key),
            "status": str(status),
            "attempt": int(attempt),
            "payload_hash": payload_hash(payload),
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
            "metadata": metadata or {},
            "updated_at": now_iso(),
        }
        try:
            self.client.table("async_job_runs").upsert(row, on_conflict="job_id").execute()
        except Exception:
            # Keep job execution resilient when audit table migration is not present yet.
            return
