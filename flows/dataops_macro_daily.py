"""Main Data Ops macro daily orchestration flow."""

from __future__ import annotations

import argparse
import json
import logging
import sys
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
from finance_data_ops.providers.macro import DEFAULT_REQUIRED_SERIES_KEYS, MacroDataProvider
from finance_data_ops.providers.macro import MacroSeriesSpec
from finance_data_ops.publish.client import Publisher, RecordingPublisher, SupabaseRestPublisher
from finance_data_ops.publish.macro import publish_macro_surfaces
from finance_data_ops.publish.status import publish_status_surfaces
from finance_data_ops.refresh.macro_daily import refresh_macro_daily
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.freshness import FreshnessState, classify_freshness

LOGGER = logging.getLogger("finance_data_ops.flows.dataops_macro_daily")


def run_dataops_macro_daily(
    *,
    start: str,
    end: str,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    provider: MacroDataProvider | None = None,
    publisher: Publisher | None = None,
    max_attempts: int = 3,
    raise_on_failed_hard: bool = True,
    force_recompute: bool = False,
    series_catalog: tuple[MacroSeriesSpec, ...] | None = None,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_macro_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)
    LOGGER.info(
        "Flow started (run_id=%s start=%s end=%s publish_enabled=%s cache_root=%s).",
        flow_run_id,
        start,
        end,
        bool(publish_enabled),
        settings.cache_root,
    )

    release_calendar = read_parquet_table(
        "economic_release_calendar",
        cache_root=settings.cache_root,
        required=False,
    )
    LOGGER.info("Loaded release calendar snapshot (rows=%s).", int(len(release_calendar.index)))

    provider_impl = provider or MacroDataProvider()
    catalog_frame, observations_frame, daily_frame, macro_run = refresh_macro_daily(
        start=start,
        end=end,
        provider=provider_impl,
        cache_root=str(settings.cache_root),
        release_calendar_frame=(release_calendar if not release_calendar.empty else None),
        series_catalog=series_catalog,
        max_attempts=max_attempts,
        force_recompute=bool(force_recompute),
    )
    LOGGER.info(
        "Refresh completed (status=%s succeeded=%s failed=%s rows_written=%s).",
        macro_run.status,
        len(macro_run.symbols_succeeded),
        len(macro_run.symbols_failed),
        macro_run.rows_written,
    )

    status_rows = _build_asset_status_rows(
        macro_observations=observations_frame,
        macro_daily=daily_frame,
        macro_run=macro_run,
        flow_run_id=flow_run_id,
    )
    run_rows = [_refresh_run_to_row(macro_run)]

    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_supabase()
            publisher_impl = SupabaseRestPublisher(
                supabase_url=settings.supabase_url,
                service_role_key=settings.supabase_secret_key,
            )
        LOGGER.info("Using Supabase publisher for macro/status surfaces.")
    else:
        publisher_impl = publisher or RecordingPublisher()
        LOGGER.info("Publish disabled; using recording publisher (dry-run mode).")

    publish_failures: list[dict[str, Any]] = []
    publish_results: dict[str, Any] = {}

    required_series_keys = (
        tuple(spec.key for spec in tuple(series_catalog or ()) if bool(spec.required_by_default))
        if series_catalog is not None
        else DEFAULT_REQUIRED_SERIES_KEYS
    )

    if publish_enabled or isinstance(publisher_impl, RecordingPublisher):
        macro_result = _execute_publish_step(
            "macro",
            lambda: publish_macro_surfaces(
                publisher=publisher_impl,
                series_catalog=catalog_frame,
                macro_observations=observations_frame,
                macro_daily=daily_frame,
                required_series_keys=required_series_keys,
            ),
            failures=publish_failures,
        )
        publish_results["macro"] = macro_result
        LOGGER.info("Macro publish step status=%s.", str(macro_result.get("status")))

    if publish_failures:
        for failure in publish_failures:
            run_rows.append(
                _flow_run_row(
                    run_id=f"{flow_run_id}_publish_{failure['step']}",
                    job_name=f"dataops_macro_daily_publish_{failure['step']}",
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
            asset_key="data_ops_publish_pipeline_macro",
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
            job_name="dataops_macro_daily",
            source_type="orchestration",
            scope=f"{start}:{end}",
            flow_started_at=flow_started_at,
            status=orchestration_status,
            context={
                "start": start,
                "end": end,
                "series_succeeded": macro_run.symbols_succeeded,
                "series_failed": macro_run.symbols_failed,
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
    LOGGER.info("Status publish step status=%s.", str(status_result.get("status")))

    hard_failure = bool(publish_failures) or overall_state in {
        FreshnessState.FAILED_HARD,
        FreshnessState.FAILED_RETRYING,
    }
    if raise_on_failed_hard and hard_failure:
        payload = build_alert_payload(
            severity="error",
            message="Data Ops macro daily flow ended unhealthy.",
            run_id=flow_run_id,
            context={
                "overall_state": overall_state,
                "publish_failures": publish_failures,
                "status_rows": status_rows,
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        LOGGER.error(
            "Flow ended unhealthy (overall_state=%s publish_failures=%s).",
            overall_state,
            len(publish_failures),
        )
        raise RuntimeError(
            "macro daily flow unhealthy: "
            f"overall_state={overall_state} publish_failures={len(publish_failures)}"
        )

    LOGGER.info(
        "Flow completed (run_id=%s overall_state=%s publish_failures=%s).",
        flow_run_id,
        overall_state,
        len(publish_failures),
    )
    return {
        "flow_run_id": flow_run_id,
        "window": {"start": start, "end": end},
        "macro_daily": macro_run.as_dict(),
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "macro_series_catalog": int(len(catalog_frame.index)),
            "macro_observations": int(len(observations_frame.index)),
            "macro_daily": int(len(daily_frame.index)),
        },
    }


def _build_asset_status_rows(
    *,
    macro_observations: pd.DataFrame,
    macro_daily: pd.DataFrame,
    macro_run: RefreshRunResult,
    flow_run_id: str,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    observations_last = _frame_datetime_max(macro_observations, "observation_date")
    daily_last = _frame_datetime_max(macro_daily, "as_of_date")

    observations_state = classify_freshness(
        last_observed_at=observations_last,
        now=now,
        fresh_within=timedelta(days=45),
        tolerance=timedelta(days=30),
        partial=str(macro_run.status) == FreshnessState.PARTIAL,
        failure_state=macro_run.status,
    )
    daily_state = classify_freshness(
        last_observed_at=daily_last,
        now=now,
        fresh_within=timedelta(days=2),
        tolerance=timedelta(days=2),
        partial=str(macro_run.status) == FreshnessState.PARTIAL,
        failure_state=macro_run.status,
    )

    provider = _provider_from_frame(macro_observations, fallback="mixed_fred_yfinance")
    now_iso = now.isoformat()

    return [
        {
            "asset_key": "macro_observations",
            "asset_type": "macro",
            "provider": provider,
            "last_success_at": _last_success_timestamp(observations_last, observations_state),
            "last_available_date": _date_or_none(observations_last),
            "freshness_status": str(observations_state),
            "coverage_status": str(macro_run.status),
            "reason": _asset_reason(
                rows_written=int(len(macro_observations.index)),
                run_id=macro_run.run_id,
                errors=macro_run.error_messages,
            ),
            "updated_at": now_iso,
        },
        {
            "asset_key": "macro_daily",
            "asset_type": "macro",
            "provider": "data_ops",
            "last_success_at": _last_success_timestamp(daily_last, daily_state),
            "last_available_date": _date_or_none(daily_last),
            "freshness_status": str(daily_state),
            "coverage_status": str(macro_run.status),
            "reason": _asset_reason(
                rows_written=int(len(macro_daily.index)),
                run_id=f"{flow_run_id}_macro_daily",
                errors=macro_run.error_messages,
            ),
            "updated_at": now_iso,
        },
    ]


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


def _provider_from_frame(frame: pd.DataFrame, *, fallback: str) -> str:
    if frame.empty:
        return fallback
    for column in ("source_provider", "source", "provider"):
        if column in frame.columns:
            values = frame[column].dropna().astype(str).str.strip()
            if not values.empty:
                return values.iloc[-1]
    return fallback


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
    parser = argparse.ArgumentParser(description="Run Data Ops macro daily orchestration flow.")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD).")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--no-publish", action="store_true", help="Skip Supabase publish steps.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy completion.")
    parser.add_argument("--force-recompute", action="store_true", help="Rewrite macro parquet outputs for selected range.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = run_dataops_macro_daily(
        start=str(args.start),
        end=str(args.end),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        force_recompute=bool(args.force_recompute),
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
