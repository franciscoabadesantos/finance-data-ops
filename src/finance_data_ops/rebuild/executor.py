"""Chunked historical rebuild executor."""

from __future__ import annotations

from dataclasses import asdict
import time
from typing import Any

from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.providers.macro import MacroDataProvider
from finance_data_ops.providers.release_calendar import EconomicReleaseCalendarProvider
from finance_data_ops.rebuild.finalize import finalize_rebuild
from finance_data_ops.rebuild.health import RebuildHealthGate
from finance_data_ops.rebuild.loaders.earnings import load_earnings_chunk
from finance_data_ops.rebuild.loaders.fundamentals import load_fundamentals_chunk
from finance_data_ops.rebuild.loaders.macro import load_macro_chunk
from finance_data_ops.rebuild.loaders.market import load_market_chunk
from finance_data_ops.rebuild.loaders.release_calendar import load_release_calendar_chunk
from finance_data_ops.rebuild.planner import RebuildPlan
from finance_data_ops.rebuild.progress import NullRebuildProgressStore, RebuildProgressStore


def execute_rebuild_plan(
    *,
    client: Any,
    publisher: SupabaseRestPublisher,
    cache_root: str,
    plan: RebuildPlan,
    progress: RebuildProgressStore | NullRebuildProgressStore | None,
    max_attempts: int = 3,
    history_limit: int = 120,
    symbol_batch_size: int = 100,
) -> dict[str, Any]:
    progress_store = progress or NullRebuildProgressStore()
    health_gate = RebuildHealthGate(client=client, policy=plan.policy)
    health_checks: list[dict[str, Any]] = []
    batch_results: list[dict[str, Any]] = []
    rows_written_total = 0
    touched_symbols: list[str] = []
    touched_series: list[str] = []
    consecutive_chunk_failures = 0

    try:
        preflight = health_gate.check()
    except Exception as exc:
        progress_store.update(
            job_status="failed",
            step="planning",
            step_status="failed",
            payload={
                "summary": "Rebuild aborted during pre-flight health check.",
                "plan": plan.dry_run_summary,
                "current_batch": 0,
                "total_batches": len(plan.chunks),
                "rows_written_total": 0,
                "rows_deleted_total": 0,
                "health_checks": health_checks,
                "abort_reason": f"preflight_exception:{exc}",
                "finalization_status": "skipped",
            },
            error_message=str(exc),
        )
        raise

    health_checks.append(asdict(preflight))
    progress_store.update(
        job_status="running",
        step="planning",
        step_status="completed",
        payload={
            "summary": "Rebuild plan resolved.",
            "plan": plan.dry_run_summary,
            "current_batch": 0,
            "total_batches": len(plan.chunks),
            "rows_written_total": 0,
            "rows_deleted_total": 0,
            "health_checks": health_checks,
            "finalization_status": "pending",
        },
    )
    if not preflight.ok and health_gate.should_abort(preflight):
        abort_reason = str(preflight.reason or "preflight_failed")
        progress_store.update(
            job_status="failed",
            step="planning",
            step_status="failed",
            payload={
                "summary": "Rebuild aborted during pre-flight health check.",
                "plan": plan.dry_run_summary,
                "current_batch": 0,
                "total_batches": len(plan.chunks),
                "rows_written_total": 0,
                "rows_deleted_total": 0,
                "health_checks": health_checks,
                "abort_reason": abort_reason,
                "finalization_status": "skipped",
            },
            error_message=abort_reason,
        )
        raise RuntimeError(abort_reason)

    total_batches = len(plan.chunks)
    for idx, chunk in enumerate(plan.chunks, start=1):
        if idx > 1 and plan.policy.inter_batch_health_check_every_n_batches > 0:
            if (idx - 1) % int(plan.policy.inter_batch_health_check_every_n_batches) == 0:
                health = health_gate.check()
                health_checks.append(asdict(health))
                if not health.ok and health_gate.should_abort(health):
                    abort_reason = str(health.reason or "health_check_failed")
                    progress_store.update(
                        job_status="failed",
                        step="loading",
                        step_status="failed",
                        payload={
                            "summary": "Rebuild aborted by health gate.",
                            "plan": plan.dry_run_summary,
                            "current_batch": idx,
                            "total_batches": total_batches,
                            "current_window": chunk.as_dict(),
                            "rows_written_total": rows_written_total,
                            "rows_deleted_total": 0,
                            "health_checks": health_checks,
                            "abort_reason": abort_reason,
                            "finalization_status": "skipped",
                        },
                        error_message=abort_reason,
                    )
                    raise RuntimeError(abort_reason)

        progress_store.update(
            job_status="running",
            step="loading",
            step_status="running",
            payload={
                "summary": "Rebuild batch running.",
                "plan": plan.dry_run_summary,
                "current_batch": idx,
                "total_batches": total_batches,
                "current_window": chunk.as_dict(),
                "rows_written_total": rows_written_total,
                "rows_deleted_total": 0,
                "health_checks": health_checks,
                "finalization_status": "pending",
            },
        )

        try:
            if plan.domain == "macro":
                batch_result = load_macro_chunk(
                    publisher=publisher,
                    provider=MacroDataProvider(),
                    cache_root=cache_root,
                    start_date=str(chunk.start_date),
                    end_date=str(chunk.end_date),
                    series_keys=chunk.series_batch,
                    max_attempts=max_attempts,
                    force_recompute=True,
                )
            elif plan.domain == "release-calendar":
                batch_result = load_release_calendar_chunk(
                    publisher=publisher,
                    provider=EconomicReleaseCalendarProvider(),
                    cache_root=cache_root,
                    start_date=str(chunk.start_date),
                    end_date=str(chunk.end_date),
                    series_keys=chunk.series_batch,
                    max_attempts=max_attempts,
                    force_recompute=True,
                    sleep_seconds=plan.policy.sleep_seconds,
                )
            elif plan.domain == "market":
                batch_result = load_market_chunk(
                    publisher=publisher,
                    provider=MarketDataProvider(),
                    cache_root=cache_root,
                    tickers=chunk.ticker_batch,
                    start_date=str(chunk.start_date),
                    end_date=str(chunk.end_date),
                    max_attempts=max_attempts,
                    symbol_batch_size=symbol_batch_size,
                )
            elif plan.domain == "fundamentals":
                batch_result = load_fundamentals_chunk(
                    publisher=publisher,
                    provider=FundamentalsDataProvider(),
                    cache_root=cache_root,
                    tickers=chunk.ticker_batch,
                    start_date=str(chunk.start_date),
                    end_date=str(chunk.end_date),
                    max_attempts=max_attempts,
                )
            elif plan.domain == "earnings":
                batch_result = load_earnings_chunk(
                    publisher=publisher,
                    provider=EarningsDataProvider(),
                    cache_root=cache_root,
                    tickers=chunk.ticker_batch,
                    start_date=str(chunk.start_date),
                    end_date=str(chunk.end_date),
                    max_attempts=max_attempts,
                    history_limit=history_limit,
                )
            else:
                raise ValueError(f"Unsupported rebuild domain: {plan.domain!r}")

            refresh_run = batch_result.get("refresh_run") or {}
            refresh_status = str(refresh_run.get("status") or "").strip().lower()
            if refresh_status in {"failed_hard", "failed_retrying"}:
                failure_detail = ",".join(str(v) for v in (refresh_run.get("error_messages") or []) if str(v).strip())
                suffix = f": {failure_detail}" if failure_detail else ""
                raise RuntimeError(f"refresh_status={refresh_status}{suffix}")
            consecutive_chunk_failures = 0
        except Exception as exc:
            consecutive_chunk_failures += 1
            abort_reason = f"chunk_failed:{exc}"
            progress_store.update(
                job_status="failed",
                step="loading",
                step_status="failed",
                payload={
                    "summary": "Rebuild batch failed.",
                    "plan": plan.dry_run_summary,
                    "current_batch": idx,
                    "total_batches": total_batches,
                    "current_window": chunk.as_dict(),
                    "rows_written_total": rows_written_total,
                    "rows_deleted_total": 0,
                    "health_checks": health_checks,
                    "abort_reason": abort_reason,
                    "consecutive_chunk_failures": consecutive_chunk_failures,
                    "finalization_status": "skipped",
                },
                error_message=str(exc),
            )
            if consecutive_chunk_failures >= max(int(plan.policy.max_consecutive_chunk_failures), 1):
                raise
            continue

        rows_written_total += int(batch_result.get("rows_written", 0))
        touched_symbols.extend(str(v).strip().upper() for v in batch_result.get("touched_symbols", []))
        touched_series.extend(str(v).strip() for v in batch_result.get("touched_series", []))
        batch_results.append(
            {
                "batch_index": idx,
                "current_window": batch_result.get("current_window") or chunk.as_dict(),
                "refresh_status": refresh_status,
                "provider_rows": batch_result.get("provider_rows"),
                "filtered_rows": batch_result.get("filtered_rows"),
                "symbol_breakdown": batch_result.get("symbol_breakdown"),
                "window_filter_field": batch_result.get("window_filter_field"),
                "rows_written": int(batch_result.get("rows_written", 0)),
                "publish_result": batch_result.get("publish_result"),
                "touched_symbols": list(batch_result.get("touched_symbols", [])),
                "touched_series": list(batch_result.get("touched_series", [])),
            }
        )

        progress_store.update(
            job_status="running",
            step="loading",
            step_status="running",
            payload={
                "summary": f"Completed batch {idx} of {total_batches}.",
                "plan": plan.dry_run_summary,
                "current_batch": idx,
                "total_batches": total_batches,
                "current_window": batch_result.get("current_window") or chunk.as_dict(),
                "batch_result": batch_result,
                "rows_written_total": rows_written_total,
                "rows_deleted_total": 0,
                "health_checks": health_checks,
                "finalization_status": "pending",
            },
        )
        if float(plan.policy.sleep_seconds) > 0:
            time.sleep(float(plan.policy.sleep_seconds))

    progress_store.update(
        job_status="running",
        step="finalizing",
        step_status="running",
        payload={
            "summary": "Running rebuild finalization.",
            "plan": plan.dry_run_summary,
            "current_batch": total_batches,
            "total_batches": total_batches,
            "rows_written_total": rows_written_total,
            "rows_deleted_total": 0,
            "health_checks": health_checks,
            "finalization_status": "running",
        },
    )

    try:
        finalization = finalize_rebuild(
            publisher=publisher,
            client=client,
            policy=plan.policy,
            domain=plan.domain,
            touched_symbols=touched_symbols,
            touched_series=touched_series,
        )
    except Exception as exc:
        progress_store.update(
            job_status="failed",
            step="finalizing",
            step_status="failed",
            payload={
                "summary": "Rebuild finalization failed.",
                "plan": plan.dry_run_summary,
                "current_batch": total_batches,
                "total_batches": total_batches,
                "rows_written_total": rows_written_total,
                "rows_deleted_total": 0,
                "health_checks": health_checks,
                "abort_reason": f"finalization_failed:{exc}",
                "finalization_status": "failed",
            },
            error_message=str(exc),
        )
        raise

    progress_store.update(
        job_status="completed",
        step="completed",
        step_status="completed",
        payload={
            "summary": "Rebuild completed.",
            "plan": plan.dry_run_summary,
            "current_batch": total_batches,
            "total_batches": total_batches,
            "rows_written_total": rows_written_total,
            "rows_deleted_total": 0,
            "health_checks": health_checks,
            "finalization_status": "completed",
            "batch_results": batch_results,
            "finalization": finalization,
        },
    )
    return {
        "rows_written_total": rows_written_total,
        "health_checks": health_checks,
        "batch_results": batch_results,
        "finalization": finalization,
        "touched_symbols": sorted(set(touched_symbols)),
        "touched_series": sorted(set(touched_series)),
    }
