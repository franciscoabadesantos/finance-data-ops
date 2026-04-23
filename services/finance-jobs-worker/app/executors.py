from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from flows.dataops_earnings_daily import run_dataops_earnings_daily
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily
from flows.dataops_market_daily import run_dataops_market_daily
from finance_data_ops.analysis.coverage import build_coverage_report
from finance_data_ops.analysis.snapshots import build_ticker_snapshot_report
from finance_data_ops.analysis.ticker_signal_v1 import build_ticker_signal_v1_report
from finance_data_ops.validation.ticker_validation import run_single_ticker_validation

from app.config import WorkerSettings
from app.data_ops_jobs import run_data_ops_rebuild_job, run_data_ops_series_upsert_job
from app.models import ExecuteJobRequest
from app.registry import WorkerRegistryStore, now_iso, parse_notes
from app.tasks import CloudTasksEnqueuer

PROMOTABLE = {"validated_market_only", "validated_full"}


class JobExecutor:
    def __init__(
        self,
        *,
        settings: WorkerSettings,
        registry: WorkerRegistryStore,
        tasks: CloudTasksEnqueuer,
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
        started_at = now_iso()
        payload = request.model_dump()
        idem_key = request.resolved_idempotency_key()
        self._record_run(
            job_id=job_id,
            request=request,
            idempotency_key=idem_key,
            status="running",
            attempt=attempt,
            payload=payload,
            started_at=started_at,
            task_name=task_name,
        )
        try:
            if request.job_type == "ticker_validation":
                result = self._execute_validation(request, job_id=job_id, idempotency_key=idem_key)
            elif request.job_type == "ticker_backfill":
                result = self._execute_backfill(request, job_id=job_id, idempotency_key=idem_key)
            else:
                result = self._execute_analysis(request, worker_job_id=job_id)
        except Exception as exc:
            if request.job_type == "ticker_validation":
                self._set_job_state(
                    registry_key=request.registry_key or "",
                    stage="validation",
                    state="FAILED",
                    job_id=job_id,
                    idempotency_key=idem_key,
                    error=str(exc),
                )
                self.registry.reject(request.registry_key or "", f"validation_job_failed:{exc}")
            elif request.job_type == "ticker_backfill":
                self._set_job_state(
                    registry_key=request.registry_key or "",
                    stage="backfill",
                    state="FAILED",
                    job_id=job_id,
                    idempotency_key=idem_key,
                    error=str(exc),
                )
            else:
                self._set_analysis_job_failed(request=request, worker_job_id=job_id, error=str(exc))
            self._record_run(
                job_id=job_id,
                request=request,
                idempotency_key=idem_key,
                status="failed",
                attempt=attempt,
                payload=payload,
                started_at=started_at,
                finished_at=now_iso(),
                task_name=task_name,
                error_message=repr(exc),
            )
            raise

        self._record_run(
            job_id=job_id,
            request=request,
            idempotency_key=idem_key,
            status="completed",
            attempt=attempt,
            payload=payload,
            started_at=started_at,
            finished_at=now_iso(),
            task_name=task_name,
        )
        return result

    def _execute_validation(self, request: ExecuteJobRequest, *, job_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.registry.get_by_key(request.registry_key or "")
        if row is None:
            raise RuntimeError(f"ticker_registry row not found for {request.registry_key or ''}")
        if str(row.get("status") or "").strip().lower() == "rejected":
            return {"status": "ignored", "reason": "already_rejected"}

        self._set_job_state(
            registry_key=request.registry_key or "",
            stage="validation",
            state="RUNNING",
            job_id=job_id,
            idempotency_key=idempotency_key,
        )
        result = run_single_ticker_validation(
            input_symbol=str(row.get("input_symbol") or request.ticker).strip().upper(),
            region=str(row.get("region") or request.region).strip().lower(),
            exchange=(str(row.get("exchange")).strip().upper() if row.get("exchange") else request.exchange),
            instrument_type_hint=request.instrument_type_hint,
            history_limit=max(int(request.history_limit or self.settings.default_history_limit), 1),
        )
        registry_row = result.get("registry_row")
        if not isinstance(registry_row, dict):
            raise RuntimeError("validation result missing registry_row")

        current = self.registry.get_by_key(request.registry_key) or row
        notes = parse_notes(current.get("notes"))
        notes["validation_selected_status"] = str((result.get("selected") or {}).get("validation_status") or "")
        notes["data_ops_validation_notes"] = str(registry_row.get("notes") or "")

        patched = self.registry.patch_row(
            request.registry_key or "",
            {
                "normalized_symbol": registry_row.get("normalized_symbol"),
                "instrument_type": registry_row.get("instrument_type"),
                "status": registry_row.get("status"),
                "market_supported": bool(registry_row.get("market_supported")),
                "fundamentals_supported": bool(registry_row.get("fundamentals_supported")),
                "earnings_supported": bool(registry_row.get("earnings_supported")),
                "validation_status": registry_row.get("validation_status"),
                "validation_reason": registry_row.get("validation_reason"),
                "promotion_status": registry_row.get("promotion_status"),
                "last_validated_at": registry_row.get("last_validated_at"),
                "notes": notes,
            },
        )
        self._set_job_state(
            registry_key=request.registry_key or "",
            stage="validation",
            state="COMPLETED",
            job_id=job_id,
            idempotency_key=idempotency_key,
        )

        validation_status = str(patched.get("validation_status") or "").strip().lower()
        if validation_status != "validated_full":
            return {
                "status": "completed",
                "validation_status": validation_status,
                "next_action": None,
            }

        normalized_ticker = str(patched.get("normalized_symbol") or patched.get("input_symbol") or "").strip().upper()
        if not normalized_ticker:
            return {"status": "completed", "validation_status": validation_status, "next_action": None}

        backfill_payload = {
            "job_type": "ticker_backfill",
            "registry_key": request.registry_key,
            "ticker": normalized_ticker,
            "region": str(patched.get("region") or request.region).strip().lower() or "us",
            "exchange": patched.get("exchange"),
            "history_limit": int(request.history_limit or self.settings.default_history_limit),
            "start": request.start,
            "end": request.end,
            "requested_at": now_iso(),
            "idempotency_key": f"backfill:{request.registry_key or ''}",
        }
        enqueue = self.tasks.enqueue_backfill(backfill_payload, idempotency_key=f"backfill:{request.registry_key or ''}")
        refreshed = self.registry.get_by_key(request.registry_key or "")
        if refreshed is not None:
            refreshed_notes = parse_notes(refreshed.get("notes"))
            refreshed_notes["backfill_job_id"] = enqueue.job_id
            refreshed_notes["backfill_flow_run_id"] = enqueue.job_id
            refreshed_notes["backfill_job_state"] = enqueue.state
            refreshed_notes["backfill_idempotency_key"] = f"backfill:{request.registry_key or ''}"
            self.registry.patch_row(request.registry_key or "", {"notes": refreshed_notes})

        return {
            "status": "completed",
            "validation_status": validation_status,
            "next_action": "backfill_enqueued",
            "backfill_job_id": enqueue.job_id,
        }

    def _execute_backfill(self, request: ExecuteJobRequest, *, job_id: str, idempotency_key: str) -> dict[str, Any]:
        row = self.registry.get_by_key(request.registry_key or "")
        if row is None:
            raise RuntimeError(f"ticker_registry row not found for {request.registry_key or ''}")
        if str(row.get("status") or "").strip().lower() == "rejected":
            return {"status": "ignored", "reason": "already_rejected"}

        ticker = str(request.ticker or row.get("normalized_symbol") or row.get("input_symbol") or "").strip().upper()
        if not ticker:
            raise RuntimeError("Backfill ticker is missing.")

        self._set_job_state(
            registry_key=request.registry_key or "",
            stage="backfill",
            state="RUNNING",
            job_id=job_id,
            idempotency_key=idempotency_key,
        )

        end_date = _parse_iso_date(request.end) if request.end else datetime.now(UTC).date()
        start_date = _parse_iso_date(request.start) if request.start else _subtract_years(
            end_date, years=self.settings.default_backfill_years
        )
        if start_date > end_date:
            raise ValueError("Backfill start date must be on or before end date.")

        history_limit = int(request.history_limit or self.settings.default_history_limit)
        if history_limit <= 0:
            history_limit = self.settings.default_history_limit

        market = run_dataops_market_daily(
            symbols=[ticker],
            start=start_date.isoformat(),
            end=end_date.isoformat(),
            publish_enabled=True,
        )
        earnings = run_dataops_earnings_daily(
            symbols=[ticker],
            history_limit=history_limit,
            publish_enabled=True,
        )
        fundamentals = run_dataops_fundamentals_daily(
            symbols=[ticker],
            publish_enabled=True,
        )

        current = self.registry.get_by_key(request.registry_key) or row
        notes = parse_notes(current.get("notes"))
        notes["backfill_result_summary"] = {
            "ticker": ticker,
            "window": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "history_limit": history_limit,
        }
        patched = self.registry.patch_row(
            request.registry_key or "",
            {
                "status": "active",
                "promotion_status": str(current.get("promotion_status") or "validated_full"),
                "notes": notes,
            },
        )
        if str(patched.get("promotion_status") or "").strip().lower() not in PROMOTABLE:
            self.registry.patch_row(request.registry_key or "", {"promotion_status": "validated_full"})

        self._set_job_state(
            registry_key=request.registry_key or "",
            stage="backfill",
            state="COMPLETED",
            job_id=job_id,
            idempotency_key=idempotency_key,
        )
        return {
            "status": "completed",
            "ticker": ticker,
            "steps": {
                "market": market.get("run_id") if isinstance(market, dict) else None,
                "earnings": earnings.get("run_id") if isinstance(earnings, dict) else None,
                "fundamentals": fundamentals.get("run_id") if isinstance(fundamentals, dict) else None,
            },
        }

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
        # limit=252 is intentional: snapshot and coverage reports evaluate
        # completeness over a recent ~1-year window, not full canonical history.
        # Full history is available in market_price_daily but not fetched here.
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

    def _set_job_state(
        self,
        *,
        registry_key: str,
        stage: str,
        state: str,
        job_id: str,
        idempotency_key: str,
        error: str | None = None,
    ) -> None:
        row = self.registry.get_by_key(registry_key)
        if row is None:
            return
        notes = parse_notes(row.get("notes"))
        prefix = "validation" if stage == "validation" else "backfill"
        notes[f"{prefix}_job_id"] = job_id
        notes[f"{prefix}_flow_run_id"] = job_id
        notes[f"{prefix}_job_state"] = str(state).upper()
        notes[f"{prefix}_idempotency_key"] = idempotency_key
        if error:
            notes[f"{prefix}_job_error"] = str(error)
        self.registry.patch_row(registry_key, {"notes": notes})

    def _record_run(
        self,
        *,
        job_id: str,
        request: ExecuteJobRequest,
        idempotency_key: str,
        status: str,
        attempt: int,
        payload: dict[str, Any],
        started_at: str | None = None,
        finished_at: str | None = None,
        task_name: str | None = None,
        error_message: str | None = None,
    ) -> None:
        if request.job_type not in {"ticker_validation", "ticker_backfill"}:
            return
        metadata = {"task_name": task_name}
        self.registry.record_async_job_run(
            job_id=job_id,
            job_type=request.job_type,
            registry_key=request.registry_key,
            idempotency_key=idempotency_key,
            status=status,
            attempt=attempt,
            payload=payload,
            started_at=started_at,
            finished_at=finished_at,
            error_message=error_message,
            metadata=metadata,
        )


def _parse_iso_date(raw: str | None) -> date:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("date value is required")
    token = text[:10]
    return date.fromisoformat(token)


def _subtract_years(value: date, *, years: int) -> date:
    try:
        return value.replace(year=value.year - int(years))
    except ValueError:
        return value.replace(month=2, day=28, year=value.year - int(years))
