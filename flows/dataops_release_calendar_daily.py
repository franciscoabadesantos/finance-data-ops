"""Main Data Ops economic release-calendar orchestration flow."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.ops.alerts import build_alert_payload, emit_alert, emit_alert_webhook
from finance_data_ops.ops.incidents import classify_failure
from finance_data_ops.providers.release_calendar import EconomicReleaseCalendarProvider
from finance_data_ops.publish.client import Publisher, RecordingPublisher, SupabaseRestPublisher
from finance_data_ops.publish.release_calendar import publish_release_calendar_surfaces
from finance_data_ops.publish.status import publish_status_surfaces
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.release_calendar_daily import refresh_release_calendar_daily
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness


def run_dataops_release_calendar_daily(
    *,
    start_date: str,
    end_date: str,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: EconomicReleaseCalendarProvider | None = None,
    publisher: Publisher | None = None,
    max_attempts: int = 3,
    raise_on_failed_hard: bool = True,
    force_recompute: bool = False,
    sleep_seconds: float = 0.0,
    official_start_year: int | None = None,
    official_end_year: int | None = None,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_release_calendar_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)

    provider_impl = provider or EconomicReleaseCalendarProvider()
    release_calendar_frame, release_run = refresh_release_calendar_daily(
        start_date=start_date,
        end_date=end_date,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        max_attempts=max_attempts,
        sleep_seconds=sleep_seconds,
        official_start_year=official_start_year,
        official_end_year=official_end_year,
        force_recompute=bool(force_recompute),
    )
    empty_window_ok = _is_valid_empty_release_window(release_calendar_frame, release_run)
    if empty_window_ok:
        release_run = replace(
            release_run,
            status=FreshnessState.FRESH,
            symbols_failed=[],
            error_messages=[],
        )
    late_anomaly = _extract_late_release_anomaly(release_calendar_frame)
    if late_anomaly is not None:
        payload = build_alert_payload(
            severity="warning",
            message="Economic release availability lag exceeds grace window.",
            run_id=flow_run_id,
            context=late_anomaly,
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)

    status_rows = _build_asset_status_rows(
        release_calendar=release_calendar_frame,
        release_run=release_run,
        flow_run_id=flow_run_id,
        empty_window_ok=empty_window_ok,
    )
    run_rows = [_refresh_run_to_row(release_run)]

    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_supabase()
            publisher_impl = SupabaseRestPublisher(
                supabase_url=settings.supabase_url,
                service_role_key=settings.supabase_secret_key,
            )
    else:
        publisher_impl = publisher or RecordingPublisher()

    publish_failures: list[dict[str, Any]] = []
    publish_results: dict[str, Any] = {}

    if publish_enabled or isinstance(publisher_impl, RecordingPublisher):
        release_result = _execute_publish_step(
            "release_calendar",
            lambda: publish_release_calendar_surfaces(
                publisher=publisher_impl,
                economic_release_calendar=release_calendar_frame,
                allow_empty=empty_window_ok,
            ),
            failures=publish_failures,
        )
        publish_results["release_calendar"] = release_result

    if publish_failures:
        for failure in publish_failures:
            run_rows.append(
                _flow_run_row(
                    run_id=f"{flow_run_id}_publish_{failure['step']}",
                    job_name=f"dataops_release_calendar_daily_publish_{failure['step']}",
                    source_type="publish",
                    scope=str(failure["step"]),
                    flow_started_at=flow_started_at,
                    status=failure["classification"]["code"],
                    context={
                        "step": failure["step"],
                        "error": failure["error"],
                        "retryable": failure["classification"]["retryable"],
                    },
                )
            )

    status_rows.append(
        _build_publish_pipeline_status_row(
            run_id=flow_run_id,
            has_publish_failures=bool(publish_failures),
            asset_key="data_ops_publish_pipeline_release_calendar",
        )
    )

    overall_state = _overall_status(status_rows)
    orchestration_status = _orchestration_run_status(
        overall_state=overall_state,
        has_publish_failures=bool(publish_failures),
    )
    run_rows.append(
        _flow_run_row(
            run_id=flow_run_id,
            job_name="dataops_release_calendar_daily",
            source_type="orchestration",
            scope=f"{start_date}:{end_date}",
            flow_started_at=flow_started_at,
            status=orchestration_status,
            context={
                "start_date": start_date,
                "end_date": end_date,
                "series_succeeded": release_run.symbols_succeeded,
                "series_failed": release_run.symbols_failed,
            },
        )
    )

    status_result = _execute_publish_step(
        "status",
        lambda: publish_status_surfaces(
            publisher=publisher_impl,
            data_source_runs=run_rows,
            data_asset_status=status_rows,
            symbol_data_coverage=[],
        ),
        failures=publish_failures,
    )
    publish_results["status"] = status_result

    hard_failure = bool(publish_failures) or overall_state in {
        FreshnessState.FAILED_HARD,
        FreshnessState.FAILED_RETRYING,
    }
    if raise_on_failed_hard and hard_failure:
        payload = build_alert_payload(
            severity="error",
            message="Data Ops release-calendar flow ended unhealthy.",
            run_id=flow_run_id,
            context={
                "overall_state": overall_state,
                "publish_failures": publish_failures,
                "status_rows": status_rows,
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        raise RuntimeError(
            "release-calendar flow unhealthy: "
            f"overall_state={overall_state} publish_failures={len(publish_failures)}"
        )

    return {
        "flow_run_id": flow_run_id,
        "window": {"start_date": start_date, "end_date": end_date},
        "release_calendar_daily": release_run.as_dict(),
        "availability_anomalies": late_anomaly,
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "economic_release_calendar": int(len(release_calendar_frame.index)),
        },
    }


def _build_asset_status_rows(
    *,
    release_calendar: pd.DataFrame,
    release_run: RefreshRunResult,
    flow_run_id: str,
    empty_window_ok: bool = False,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    latest_release_ts = _latest_release_effective_timestamp(release_calendar)
    if empty_window_ok:
        latest_release_ts = now
        release_state = FreshnessState.FRESH
        coverage_status = FreshnessState.FRESH
        reason = f"rows_written=0; run_id={release_run.run_id}; empty_window_ok=true"
        now_iso = now.isoformat()
        return [
            {
                "asset_key": "economic_release_calendar",
                "asset_type": "release_calendar",
                "provider": "mixed_alfred_fred",
                "last_success_at": now_iso,
                "last_available_date": now.date().isoformat(),
                "freshness_status": str(release_state),
                "coverage_status": coverage_status,
                "reason": reason,
                "updated_at": now_iso,
            },
            {
                "asset_key": "mv_latest_economic_release_calendar",
                "asset_type": "derived",
                "provider": "data_ops",
                "last_success_at": now_iso,
                "last_available_date": now.date().isoformat(),
                "freshness_status": str(release_state),
                "coverage_status": coverage_status,
                "reason": reason,
                "updated_at": now_iso,
            },
        ]

    release_state = classify_freshness(
        last_observed_at=latest_release_ts,
        now=now,
        fresh_within=timedelta(days=10),
        tolerance=timedelta(days=5),
        partial=str(release_run.status) == FreshnessState.PARTIAL,
        failure_state=release_run.status,
    )

    now_iso = now.isoformat()
    return [
        {
            "asset_key": "economic_release_calendar",
            "asset_type": "release_calendar",
            "provider": "mixed_alfred_fred",
            "last_success_at": _last_success_timestamp(latest_release_ts, release_state),
            "last_available_date": _date_or_none(latest_release_ts),
            "freshness_status": str(release_state),
            "coverage_status": str(release_run.status),
            "reason": _asset_reason(
                rows_written=int(len(release_calendar.index)),
                run_id=release_run.run_id,
                errors=release_run.error_messages,
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "mv_latest_economic_release_calendar",
            "asset_type": "derived",
            "provider": "data_ops",
            "last_success_at": _last_success_timestamp(latest_release_ts, release_state),
            "last_available_date": _date_or_none(latest_release_ts),
            "freshness_status": str(release_state),
            "coverage_status": str(release_run.status),
            "reason": _asset_reason(
                rows_written=int(len(release_calendar.index)),
                run_id=f"{flow_run_id}_mv_latest_economic_release_calendar",
                errors=release_run.error_messages,
            ),
            "updated_at": now_iso,
        },
    ]


def _extract_late_release_anomaly(release_calendar: pd.DataFrame) -> dict[str, Any] | None:
    if release_calendar.empty or "availability_status" not in release_calendar.columns:
        return None
    late = release_calendar[release_calendar["availability_status"].astype(str).str.strip() == "late_missing_observation"].copy()
    if late.empty:
        return None
    return {
        "late_row_count": int(len(late.index)),
        "series_keys": sorted(set(late["series_key"].astype(str).tolist())),
        "sample": late[
            [
                "series_key",
                "observation_period",
                "scheduled_release_timestamp_utc",
                "availability_status",
                "availability_source",
            ]
        ]
        .head(25)
        .to_dict(orient="records"),
    }


def _is_valid_empty_release_window(release_calendar: pd.DataFrame, release_run: RefreshRunResult) -> bool:
    if not release_calendar.empty:
        return False
    if str(release_run.status).strip().lower() != FreshnessState.FAILED_HARD:
        return False
    errors = [str(value).strip() for value in release_run.error_messages if str(value).strip()]
    return errors == ["provider returned zero release-calendar rows"]


def _latest_release_effective_timestamp(release_calendar: pd.DataFrame) -> datetime | None:
    if release_calendar.empty:
        return None
    local = release_calendar.copy()
    observed = pd.to_datetime(local.get("observed_first_available_at_utc"), utc=True, errors="coerce")
    scheduled = pd.to_datetime(
        local.get("scheduled_release_timestamp_utc", local.get("release_timestamp_utc")),
        utc=True,
        errors="coerce",
    )
    effective = observed.fillna(scheduled)
    effective = effective[effective.notna()]
    if effective.empty:
        return None
    now_utc = pd.Timestamp(datetime.now(UTC))
    effective_not_future = effective[effective <= now_utc]
    latest = effective_not_future.max() if not effective_not_future.empty else effective.max()
    if pd.isna(latest):
        return None
    return latest.to_pydatetime()


def _execute_publish_step(
    step_name: str,
    operation: Callable[[], dict[str, Any]],
    *,
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        return operation()
    except Exception as exc:
        classification = classify_failure(exc)
        failures.append(
            {
                "step": step_name,
                "error": repr(exc),
                "classification": {
                    "code": classification.code,
                    "retryable": classification.retryable,
                    "message": classification.message,
                },
            }
        )
        return {
            "status": classification.code,
            "error": repr(exc),
            "retryable": classification.retryable,
            "message": classification.message,
        }


def _count_items(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        return 0 if not value.strip() else 1
    try:
        return len(value)
    except TypeError:
        return 0


def _context_requested_count(context: dict[str, Any]) -> int:
    direct = context.get("symbols")
    if direct is None:
        direct = context.get("series")
    direct_count = _count_items(direct)
    if direct_count:
        return direct_count
    return _context_succeeded_count(context) + _context_failed_count(context)


def _context_succeeded_count(context: dict[str, Any]) -> int:
    value = context.get("symbols_succeeded")
    if value is None:
        value = context.get("series_succeeded")
    return _count_items(value)


def _context_failed_count(context: dict[str, Any]) -> int:
    value = context.get("symbols_failed")
    if value is None:
        value = context.get("series_failed")
    return _count_items(value)


def _refresh_run_to_row(result: RefreshRunResult) -> dict[str, Any]:
    failure_classification = (
        str(result.status)
        if str(result.status).strip().lower() in {"failed_hard", "failed_retrying"}
        else None
    )
    error_messages = [str(value) for value in result.error_messages if str(value).strip()]
    error_message = error_messages[0] if error_messages else None
    return {
        "run_id": result.run_id,
        "job_name": result.asset_name,
        "source_type": "refresh",
        "scope": "domain",
        "status": result.status,
        "started_at": result.started_at,
        "finished_at": result.ended_at,
        "rows_written": int(result.rows_written),
        "error_class": failure_classification,
        "error_message": error_message,
        "failure_classification": failure_classification,
        "symbols_requested": _count_items(result.symbols_requested),
        "symbols_succeeded": _count_items(result.symbols_succeeded),
        "symbols_failed": _count_items(result.symbols_failed),
        "error_messages": error_messages,
        "created_at": datetime.now(UTC).isoformat(),
    }


def _flow_run_row(
    *,
    run_id: str,
    job_name: str,
    source_type: str,
    scope: str,
    flow_started_at: datetime,
    status: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    normalized_status = str(status).strip().lower()
    failure_classification = (
        normalized_status if normalized_status in {"failed_hard", "failed_retrying", "failed"} else None
    )
    details = json.dumps(context, default=str)
    return {
        "run_id": str(run_id),
        "job_name": job_name,
        "source_type": source_type,
        "scope": scope,
        "status": status,
        "started_at": flow_started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "rows_written": int(context.get("rows_written", 0) or 0),
        "error_class": failure_classification,
        "error_message": details if failure_classification else None,
        "failure_classification": failure_classification,
        "symbols_requested": _context_requested_count(context),
        "symbols_succeeded": _context_succeeded_count(context),
        "symbols_failed": _context_failed_count(context),
        "error_messages": [details],
        "created_at": datetime.now(UTC).isoformat(),
    }


def _build_publish_pipeline_status_row(
    *,
    run_id: str,
    has_publish_failures: bool,
    asset_key: str,
) -> dict[str, Any]:
    if has_publish_failures:
        freshness_status = FreshnessState.FAILED_HARD
        coverage_status = FreshnessState.FAILED_HARD
        reason = f"publish_failure; run_id={run_id}"
    else:
        freshness_status = FreshnessState.FRESH
        coverage_status = FreshnessState.FRESH
        reason = f"success; run_id={run_id}"

    return {
        "asset_key": asset_key,
        "asset_type": "pipeline",
        "provider": "data_ops",
        "last_success_at": datetime.now(UTC).isoformat() if not has_publish_failures else None,
        "last_available_date": datetime.now(UTC).date().isoformat(),
        "freshness_status": freshness_status,
        "coverage_status": coverage_status,
        "reason": reason,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _overall_status(status_rows: list[dict[str, Any]]) -> str:
    statuses = {str(row.get("freshness_status", "")).strip().lower() for row in status_rows}
    if FreshnessState.FAILED_HARD in statuses:
        return FreshnessState.FAILED_HARD
    if FreshnessState.FAILED_RETRYING in statuses:
        return FreshnessState.FAILED_RETRYING
    if FreshnessState.PARTIAL in statuses:
        return FreshnessState.PARTIAL
    if FreshnessState.STALE_WITHIN_TOLERANCE in statuses:
        return FreshnessState.STALE_WITHIN_TOLERANCE
    if FreshnessState.DELAYED_EXPECTED in statuses:
        return FreshnessState.DELAYED_EXPECTED
    if FreshnessState.UNKNOWN in statuses:
        return FreshnessState.UNKNOWN
    return FreshnessState.FRESH


def _orchestration_run_status(*, overall_state: str, has_publish_failures: bool) -> str:
    if has_publish_failures:
        return FreshnessState.FAILED_HARD
    if overall_state in {FreshnessState.FAILED_HARD, FreshnessState.FAILED_RETRYING}:
        return overall_state
    if overall_state == FreshnessState.PARTIAL:
        return FreshnessState.PARTIAL
    return FreshnessState.FRESH


def _frame_datetime_max(frame: pd.DataFrame, column: str, *, utc: bool = False) -> datetime | None:
    if frame.empty or column not in frame.columns:
        return None
    parsed = pd.to_datetime(frame[column], errors="coerce", utc=utc)
    if parsed.isna().all():
        return None
    ts = parsed.max()
    if pd.isna(ts):
        return None
    out = ts.to_pydatetime()
    if out.tzinfo is None:
        return out.replace(tzinfo=UTC)
    return out.astimezone(UTC)


def _date_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.date().isoformat()


def _last_success_timestamp(value: datetime | None, state: str) -> str | None:
    if value is None:
        return None
    if str(state).strip().lower() in {FreshnessState.FAILED_HARD, FreshnessState.FAILED_RETRYING}:
        return None
    return value.astimezone(UTC).isoformat()


def _asset_reason(*, rows_written: int, run_id: str, errors: list[str]) -> str:
    error_text = ";".join([str(v).strip() for v in errors if str(v).strip()])
    if error_text:
        return f"rows_written={rows_written}; run_id={run_id}; errors={error_text}"
    return f"rows_written={rows_written}; run_id={run_id}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Data Ops release-calendar daily orchestration flow.")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD).")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--official-start-year", type=int, default=None)
    parser.add_argument("--official-end-year", type=int, default=None)
    parser.add_argument("--no-publish", action="store_true", help="Skip Supabase publish steps.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy completion.")
    parser.add_argument("--force-recompute", action="store_true", help="Rewrite release-calendar parquet output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = run_dataops_release_calendar_daily(
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        force_recompute=bool(args.force_recompute),
        sleep_seconds=float(args.sleep_seconds),
        official_start_year=args.official_start_year,
        official_end_year=args.official_end_year,
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
