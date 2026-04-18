from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any

WORKER_ROOT = Path(__file__).resolve().parents[1] / "services" / "finance-jobs-worker"
if str(WORKER_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKER_ROOT))

from app.executors import JobExecutor  # type: ignore[import-not-found]
from app.models import ExecuteJobRequest  # type: ignore[import-not-found]
from app.registry import WorkerRegistryStore  # type: ignore[import-not-found]


class FakeSupabaseResponse:
    def __init__(self, data: list[dict[str, Any]] | None = None) -> None:
        self.data = data or []


class FakeSupabaseQuery:
    def __init__(self, client: "FakeSupabaseClient", table_name: str) -> None:
        self.client = client
        self.table_name = table_name
        self._operation = "select"
        self._payload: dict[str, Any] | None = None
        self._filters: list[tuple[str, Any]] = []
        self._limit: int | None = None
        self._on_conflict: str | None = None

    def select(self, _columns: str) -> "FakeSupabaseQuery":
        self._operation = "select"
        return self

    def eq(self, field: str, value: Any) -> "FakeSupabaseQuery":
        self._filters.append((str(field), value))
        return self

    def limit(self, count: int) -> "FakeSupabaseQuery":
        self._limit = int(count)
        return self

    def insert(self, payload: dict[str, Any]) -> "FakeSupabaseQuery":
        self._operation = "insert"
        self._payload = dict(payload)
        return self

    def update(self, payload: dict[str, Any]) -> "FakeSupabaseQuery":
        self._operation = "update"
        self._payload = dict(payload)
        return self

    def upsert(self, payload: dict[str, Any], on_conflict: str) -> "FakeSupabaseQuery":
        self._operation = "upsert"
        self._payload = dict(payload)
        self._on_conflict = str(on_conflict)
        return self

    def execute(self) -> FakeSupabaseResponse:
        rows = self.client.tables.setdefault(self.table_name, [])
        if self._operation == "select":
            selected = self._apply_filters(rows)
            if self._limit is not None:
                selected = selected[: self._limit]
            return FakeSupabaseResponse([dict(item) for item in selected])

        if self._operation == "insert":
            payload = dict(self._payload or {})
            rows.append(payload)
            return FakeSupabaseResponse([dict(payload)])

        if self._operation == "update":
            payload = dict(self._payload or {})
            matched = self._apply_filters(rows)
            for row in matched:
                row.update(payload)
            return FakeSupabaseResponse([dict(item) for item in matched])

        if self._operation == "upsert":
            payload = dict(self._payload or {})
            conflict_key = str(self._on_conflict or "job_id")
            conflict_value = payload.get(conflict_key)
            matched = [row for row in rows if row.get(conflict_key) == conflict_value]
            if matched:
                for row in matched:
                    row.update(payload)
            else:
                rows.append(payload)
            return FakeSupabaseResponse([dict(payload)])

        raise RuntimeError(f"Unsupported fake operation: {self._operation}")

    def _apply_filters(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self._filters:
            return rows
        out: list[dict[str, Any]] = []
        for row in rows:
            include = True
            for field, value in self._filters:
                if row.get(field) != value:
                    include = False
                    break
            if include:
                out.append(row)
        return out


class FakeSupabaseClient:
    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = {
            "analysis_jobs": [],
            "analysis_results": [],
            "ticker_market_stats_snapshot": [],
            "symbol_data_coverage": [],
            "data_asset_status": [],
            "ticker_registry": [],
        }

    def table(self, table_name: str) -> FakeSupabaseQuery:
        return FakeSupabaseQuery(self, table_name)


@dataclass
class FakeSettings:
    default_history_limit: int = 24
    default_backfill_years: int = 5


def test_analysis_job_transitions_to_completed_and_persists_result() -> None:
    supabase = FakeSupabaseClient()
    supabase.tables["analysis_jobs"].append(
        {
            "job_id": "analysis-123",
            "ticker": "AAPL",
            "region": "us",
            "exchange": None,
            "analysis_type": "ticker_snapshot",
            "status": "queued",
            "created_at": "2026-04-18T00:00:00+00:00",
            "error_message": None,
            "worker_job_id": None,
        }
    )
    supabase.tables["ticker_market_stats_snapshot"].append(
        {
            "ticker": "AAPL",
            "as_of_date": "2026-04-17",
            "last_price": 189.11,
            "return_1d_pct": 0.45,
            "return_1m_pct": 2.1,
            "return_1y_pct": 18.2,
        }
    )
    supabase.tables["symbol_data_coverage"].append(
        {
            "ticker": "AAPL",
            "market_data_available": True,
            "fundamentals_available": True,
            "earnings_available": True,
            "coverage_status": "fresh",
        }
    )
    supabase.tables["data_asset_status"].extend(
        [
            {"asset_key": "market_price_daily", "freshness_status": "fresh"},
            {"asset_key": "market_quotes", "freshness_status": "fresh"},
            {"asset_key": "fundamentals_daily", "freshness_status": "fresh"},
            {"asset_key": "earnings_daily", "freshness_status": "fresh"},
        ]
    )

    request = ExecuteJobRequest(
        job_type="analysis_job",
        job_id="analysis-123",
        ticker="AAPL",
        region="us",
        analysis_type="ticker_snapshot",
        idempotency_key="analysis:analysis-123",
    )
    executor = JobExecutor(
        settings=FakeSettings(),
        registry=WorkerRegistryStore(supabase),  # type: ignore[arg-type]
        tasks=object(),  # type: ignore[arg-type]
    )
    result = executor.execute(
        request,
        job_id="worker-job-1",
        attempt=1,
        task_name="projects/x/locations/y/queues/z/tasks/worker-job-1",
    )

    assert result["status"] == "completed"
    assert result["job_id"] == "analysis-123"

    analysis_job = supabase.tables["analysis_jobs"][0]
    assert analysis_job["status"] == "completed"
    assert analysis_job["worker_job_id"] == "worker-job-1"
    assert analysis_job["finished_at"] is not None
    assert analysis_job["error_message"] is None

    assert len(supabase.tables["analysis_results"]) == 1
    stored = supabase.tables["analysis_results"][0]
    assert stored["job_id"] == "analysis-123"
    assert stored["analysis_type"] == "ticker_snapshot"
    assert stored["summary_text"]
    payload = stored["result_json"]
    assert payload["metadata"]["analysis_type"] == "ticker_snapshot"
    assert payload["metadata"]["ticker"] == "AAPL"
    assert isinstance(payload["sections"], list)
