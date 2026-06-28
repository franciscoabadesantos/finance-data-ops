"""Main Data Ops exchange trading-calendar orchestration flow."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
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
from finance_data_ops.providers.exchange_calendar import SUPPORTED_MICS, trading_session_rows
from finance_data_ops.publish.client import Publisher, RecordingPublisher, PostgresPublisher
from finance_data_ops.publish.status import publish_status_surfaces
from finance_data_ops.publish.trading_calendar import publish_trading_calendar_surfaces
from finance_data_ops.settings import load_settings
from finance_data_ops.validation.freshness import FreshnessState

try:
    from prefect import flow
except Exception:  # pragma: no cover - CLI can run without prefect installed
    flow = None  # type: ignore[assignment]


def run_dataops_trading_calendar_daily(
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    cache_root: str | None = None,
    publish_enabled: bool = True,
    publisher: Publisher | None = None,
    raise_on_failed_hard: bool = True,
) -> dict[str, Any]:
    flow_started_at = datetime.now(UTC)
    flow_run_id = f"run_dataops_trading_calendar_daily_{uuid4().hex[:12]}"
    settings = load_settings(cache_root=cache_root)

    resolved_start = pd.Timestamp(start_date).date() if start_date else date(1990, 1, 1)
    resolved_end = pd.Timestamp(end_date).date() if end_date else datetime.now(UTC).date() + timedelta(days=366)
    if resolved_start > resolved_end:
        raise ValueError(
            f"Invalid window: start_date ({resolved_start.isoformat()}) is after end_date ({resolved_end.isoformat()})."
        )

    calendar_frame = _build_trading_calendar_frame(start=resolved_start, end=resolved_end)

    if publish_enabled:
        if publisher is not None:
            publisher_impl = publisher
        else:
            settings.require_database()
            publisher_impl = PostgresPublisher(database_dsn=settings.database_dsn)
    else:
        publisher_impl = publisher or RecordingPublisher()

    publish_failures: list[dict[str, Any]] = []
    publish_results: dict[str, Any] = {}
    if publish_enabled or isinstance(publisher_impl, RecordingPublisher):
        calendar_result = _execute_publish_step(
            "trading_calendar",
            lambda: publish_trading_calendar_surfaces(
                publisher=publisher_impl,
                trading_calendar=calendar_frame,
            ),
            failures=publish_failures,
        )
        publish_results["trading_calendar"] = calendar_result

    status_rows = _build_asset_status_rows(
        trading_calendar=calendar_frame,
        run_id=flow_run_id,
        has_publish_failures=bool(publish_failures),
    )
    run_rows = [
        _flow_run_row(
            run_id=flow_run_id,
            job_name="dataops_trading_calendar_daily",
            source_type="orchestration",
            scope=f"{resolved_start.isoformat()}:{resolved_end.isoformat()}",
            flow_started_at=flow_started_at,
            status=FreshnessState.FAILED_HARD if publish_failures else FreshnessState.FRESH,
            context={
                "start_date": resolved_start.isoformat(),
                "end_date": resolved_end.isoformat(),
                "mics": list(SUPPORTED_MICS),
                "rows_written": int(len(calendar_frame.index)),
            },
        )
    ]

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

    hard_failure = bool(publish_failures)
    if raise_on_failed_hard and hard_failure:
        payload = build_alert_payload(
            severity="error",
            message="Data Ops trading-calendar flow ended unhealthy.",
            run_id=flow_run_id,
            context={
                "publish_failures": publish_failures,
                "status_rows": status_rows,
            },
        )
        emit_alert(payload)
        emit_alert_webhook(payload, webhook_url=settings.alert_webhook_url)
        raise RuntimeError(f"trading-calendar flow unhealthy: publish_failures={len(publish_failures)}")

    return {
        "flow_run_id": flow_run_id,
        "window": {"start_date": resolved_start.isoformat(), "end_date": resolved_end.isoformat()},
        "mics": list(SUPPORTED_MICS),
        "asset_status": status_rows,
        "published": publish_results,
        "publish_failures": publish_failures,
        "rows": {
            "exchange_trading_calendar": int(len(calendar_frame.index)),
        },
    }


if flow is not None:

    @flow(name="dataops-trading-calendar-daily", retries=0, log_prints=True)
    def dataops_trading_calendar_daily_flow(
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        cache_root: str | None = None,
        publish_enabled: bool = True,
        raise_on_failed_hard: bool = True,
    ) -> dict[str, Any]:
        return run_dataops_trading_calendar_daily(
            start_date=start_date,
            end_date=end_date,
            cache_root=cache_root,
            publish_enabled=publish_enabled,
            raise_on_failed_hard=raise_on_failed_hard,
        )

else:

    def dataops_trading_calendar_daily_flow(**kwargs: Any) -> dict[str, Any]:
        return run_dataops_trading_calendar_daily(**kwargs)


def _build_trading_calendar_frame(*, start: date, end: date) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for mic in SUPPORTED_MICS:
        rows.extend(trading_session_rows(mic, start=start, end=end))
    if not rows:
        return pd.DataFrame(columns=["exchange_mic", "session_date", "is_half_day"])
    return pd.DataFrame(rows).sort_values(["exchange_mic", "session_date"]).reset_index(drop=True)


def _build_asset_status_rows(
    *,
    trading_calendar: pd.DataFrame,
    run_id: str,
    has_publish_failures: bool,
) -> list[dict[str, Any]]:
    now = datetime.now(UTC)
    latest_session = _latest_session_date(trading_calendar)
    state = FreshnessState.FAILED_HARD if has_publish_failures else FreshnessState.FRESH
    return [
        {
            "asset_key": "exchange_trading_calendar",
            "asset_type": "trading_calendar",
            "provider": "exchange_calendars",
            "last_success_at": now.isoformat() if not has_publish_failures else None,
            "last_available_date": latest_session.isoformat() if latest_session else None,
            "freshness_status": state,
            "coverage_status": state,
            "reason": f"rows_written={int(len(trading_calendar.index))}; run_id={run_id}",
            "updated_at": now.isoformat(),
        },
        {
            "asset_key": "data_ops_publish_pipeline_trading_calendar",
            "asset_type": "pipeline",
            "provider": "data_ops",
            "last_success_at": now.isoformat() if not has_publish_failures else None,
            "last_available_date": now.date().isoformat(),
            "freshness_status": state,
            "coverage_status": state,
            "reason": f"{'publish_failure' if has_publish_failures else 'success'}; run_id={run_id}",
            "updated_at": now.isoformat(),
        },
    ]


def _latest_session_date(trading_calendar: pd.DataFrame) -> date | None:
    if trading_calendar.empty or "session_date" not in trading_calendar.columns:
        return None
    parsed = pd.to_datetime(trading_calendar["session_date"], errors="coerce")
    parsed = parsed[parsed.notna()]
    if parsed.empty:
        return None
    return parsed.max().date()


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
    failure_classification = normalized_status if normalized_status in {"failed_hard", "failed_retrying", "failed"} else None
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
        "symbols_requested": len(SUPPORTED_MICS),
        "symbols_succeeded": 0 if failure_classification else len(SUPPORTED_MICS),
        "symbols_failed": len(SUPPORTED_MICS) if failure_classification else 0,
        "error_messages": [details],
        "created_at": datetime.now(UTC).isoformat(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Data Ops trading-calendar daily orchestration flow.")
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD), defaults to 1990-01-01.")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD), defaults to today + 366 days.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--no-publish", action="store_true", help="Skip Postgres publish steps.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy completion.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    summary = run_dataops_trading_calendar_daily(
        start_date=args.start_date,
        end_date=args.end_date,
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
