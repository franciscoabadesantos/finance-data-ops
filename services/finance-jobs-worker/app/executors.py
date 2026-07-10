from __future__ import annotations

from typing import Any

from finance_data_ops.analysis.coverage import build_coverage_report
from finance_data_ops.analysis.snapshots import build_ticker_snapshot_report
from finance_data_ops.analysis.ticker_signal_v1 import build_ticker_signal_v1_report

from app.config import WorkerSettings
from app.data_ops_jobs import run_data_ops_rebuild_job, run_data_ops_series_upsert_job
from app.models import ExecuteJobRequest
from app.registry import WorkerRegistryStore, now_iso


class JobExecutor:
    def __init__(
        self,
        *,
        settings: WorkerSettings,
        registry: WorkerRegistryStore,
        tasks: object | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry
        self.tasks = tasks

    def execute(
        self,
        request: ExecuteJobRequest,
        *,
        job_id: str,
        attempt: int,
        task_name: str | None = None,
    ) -> dict[str, Any]:
        try:
            return self._execute_analysis(request, worker_job_id=job_id)
        except Exception as exc:
            self._set_analysis_job_failed(request=request, worker_job_id=job_id, error=str(exc))
            raise

    def _execute_analysis(self, request: ExecuteJobRequest, *, worker_job_id: str) -> dict[str, Any]:
        analysis_job_id = str(request.job_id or "").strip()
        if not analysis_job_id:
            raise RuntimeError("analysis job_id is required")
        row = self.registry.get_analysis_job(analysis_job_id)
        if row is None:
            raise RuntimeError(f"analysis_jobs row not found for {analysis_job_id}")

        self.registry.patch_analysis_job(
            analysis_job_id,
            {
                "status": "running",
                "started_at": now_iso(),
                "finished_at": None,
                "error_message": None,
                "worker_job_id": worker_job_id,
            },
        )
        resolved_analysis_type = request.analysis_type or "ticker_snapshot"
        ticker = str(request.ticker).strip().upper()
        region = str(request.region or "us").strip().lower() or "us"
        exchange = request.exchange
        coverage = self.registry.fetch_symbol_coverage(ticker)
        assets = self.registry.fetch_data_asset_status()
        registry_row = self.registry.resolve_registry_row(
            ticker=ticker,
            region=region,
            exchange=exchange,
        )
        market_price_rows = self.registry.fetch_market_price_daily_rows(ticker, limit=252)
        fundamentals_rows = self.registry.fetch_fundamentals_rows(ticker)
        earnings_rows = self.registry.fetch_earnings_history_rows(ticker)
        market_snapshot = self.registry.fetch_market_snapshot(ticker)
        next_earnings_row = self.registry.fetch_next_earnings_row(ticker)
        if resolved_analysis_type == "ticker_snapshot":
            result_json = build_ticker_snapshot_report(
                ticker=ticker,
                region=region,
                exchange=exchange,
                analysis_type=resolved_analysis_type,
                market_snapshot=market_snapshot,
                coverage=coverage,
                asset_status_by_key=assets,
                registry_row=registry_row,
                market_price_rows=market_price_rows,
                fundamentals_rows=fundamentals_rows,
                earnings_rows=earnings_rows,
            )
        elif resolved_analysis_type == "coverage_report":
            result_json = build_coverage_report(
                ticker=ticker,
                region=region,
                exchange=exchange,
                analysis_type=resolved_analysis_type,
                coverage=coverage,
                asset_status_by_key=assets,
                registry_row=registry_row,
                market_price_rows=market_price_rows,
                fundamentals_rows=fundamentals_rows,
                earnings_rows=earnings_rows,
            )
        elif resolved_analysis_type == "ticker_signal_v1":
            result_json = build_ticker_signal_v1_report(
                ticker=ticker,
                region=region,
                exchange=exchange,
                analysis_type=resolved_analysis_type,
                market_snapshot=market_snapshot,
                coverage=coverage,
                asset_status_by_key=assets,
                registry_row=registry_row,
                market_price_rows=market_price_rows,
                earnings_rows=earnings_rows,
                next_earnings_row=next_earnings_row,
            )
        elif resolved_analysis_type == "data_ops_rebuild":
            output = run_data_ops_rebuild_job(
                settings=self.settings,
                registry=self.registry,
                params=dict(request.job_params or {}),
                job_id=analysis_job_id,
            )
            existing_result = self.registry.client.table("analysis_results").select("*").eq("job_id", analysis_job_id).limit(1).execute()
            existing_rows = existing_result.data or []
            result_json = dict(existing_rows[0].get("result_json") or {}) if existing_rows else {}
            result_json.setdefault("input", dict(request.job_params or {}))
            result_json["output"] = output
            result_json.setdefault("summary", f"Data ops rebuild {str((output or {}).get('status') or 'completed')}.")
            result_json.setdefault("metadata", {})
            result_json["metadata"].update(
                {
                    "analysis_type": resolved_analysis_type,
                    "generated_at": now_iso(),
                }
            )
        elif resolved_analysis_type == "data_ops_series_upsert":
            output = run_data_ops_series_upsert_job(
                settings=self.settings,
                registry=self.registry,
                params=dict(request.job_params or {}),
            )
            result_json = {
                "summary": f"Data ops series upsert {str((output or {}).get('status') or 'completed')}.",
                "input": dict(request.job_params or {}),
                "output": output,
                "metadata": {
                    "analysis_type": resolved_analysis_type,
                    "generated_at": now_iso(),
                },
            }
        else:
            raise RuntimeError(f"Unsupported analysis_type={resolved_analysis_type!r}")
        self.registry.upsert_analysis_result(
            job_id=analysis_job_id,
            analysis_type=resolved_analysis_type,
            result_json=(dict(result_json) if isinstance(result_json, dict) else {"result": result_json}),
            summary_text=(
                str(result_json.get("summary_text") or result_json.get("summary") or "")
                if isinstance(result_json, dict)
                else ""
            ),
        )
        self.registry.patch_analysis_job(
            analysis_job_id,
            {
                "status": "completed",
                "finished_at": now_iso(),
                "error_message": None,
                "worker_job_id": worker_job_id,
                "result_ref": analysis_job_id,
            },
        )
        return {
            "status": "completed",
            "job_id": analysis_job_id,
            "analysis_type": resolved_analysis_type,
        }

    def _set_analysis_job_failed(self, *, request: ExecuteJobRequest, worker_job_id: str, error: str) -> None:
        analysis_job_id = str(request.job_id or "").strip()
        if not analysis_job_id:
            return
        if self.registry.get_analysis_job(analysis_job_id) is None:
            return
        self.registry.patch_analysis_job(
            analysis_job_id,
            {
                "status": "failed",
                "finished_at": now_iso(),
                "error_message": str(error),
                "worker_job_id": worker_job_id,
            },
        )
