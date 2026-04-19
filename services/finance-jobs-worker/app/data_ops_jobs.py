from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from flows.dataops_earnings_daily import run_dataops_earnings_daily
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily
from flows.dataops_macro_daily import run_dataops_macro_daily
from flows.dataops_market_daily import run_dataops_market_daily
from flows.dataops_release_calendar_daily import run_dataops_release_calendar_daily
from finance_data_ops.providers.macro import MacroSeriesSpec
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.release_calendar import publish_release_calendar_surfaces
from finance_data_ops.rebuild.executor import execute_rebuild_plan
from finance_data_ops.rebuild.planner import build_rebuild_plan
from finance_data_ops.rebuild.progress import RebuildProgressStore
from finance_data_ops.settings import load_settings

from app.config import WorkerSettings
from app.registry import WorkerRegistryStore

_PROMOTABLE = {"validated_market_only", "validated_full"}
_VALID_DOMAINS = {"market", "fundamentals", "earnings", "macro", "release-calendar"}


def run_data_ops_rebuild_job(
    *,
    settings: WorkerSettings,
    registry: WorkerRegistryStore,
    params: dict[str, Any],
    job_id: str | None = None,
) -> dict[str, Any]:
    domain = str(params.get("domain") or "").strip().lower()
    if domain not in _VALID_DOMAINS:
        raise ValueError(f"Unsupported data ops domain: {domain!r}")

    mode = str(params.get("mode") or "rebuild_missing_only").strip().lower()
    if mode not in {"rebuild_missing_only", "rebuild_from_start_date", "wipe_rebuild"}:
        raise ValueError(f"Unsupported rebuild mode: {mode!r}")

    scope = dict(params.get("scope") or {})
    scope_type = str(scope.get("scope_type") or "whole_domain").strip().lower()
    dry_run = bool(params.get("dry_run"))
    start_date, end_date = _resolve_rebuild_window(params=params, scope=scope, domain=domain)
    symbols = _resolve_scope_symbols(registry=registry, scope=scope, domain=domain)

    if mode == "wipe_rebuild" and scope_type == "whole_domain" and not dry_run:
        confirm_phrase = str(params.get("confirm_phrase") or "").strip().upper()
        required_phrase = f"WIPE {domain}".upper()
        if confirm_phrase != required_phrase:
            raise ValueError(
                f"Destructive whole-domain wipe requires confirm_phrase={required_phrase!r}."
            )

    plan = build_rebuild_plan(
        client=registry.client,
        domain=domain,
        mode=mode,
        scope=scope,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    preview = plan.dry_run_summary
    if dry_run:
        return {
            "status": "completed",
            "dry_run": True,
            "domain": domain,
            "mode": mode,
            "preview": preview,
        }

    data_ops_settings = load_settings()
    publisher = SupabaseRestPublisher(
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    progress = None
    if job_id:
        progress = RebuildProgressStore(
            registry=registry,
            job_id=str(job_id),
            analysis_type="data_ops_rebuild",
            domain=domain,
        )

    delete_summary: dict[str, Any] | None = None
    if mode == "wipe_rebuild":
        if progress is not None:
            progress.update(
                job_status="running",
                step="deleting",
                step_status="running",
                payload={
                    "summary": "Deleting scoped rows before rebuild.",
                    "preview": preview,
                    "rows_written_total": 0,
                    "rows_deleted_total": 0,
                    "finalization_status": "pending",
                },
            )
        delete_summary = _execute_wipe(
            registry=registry,
            domain=domain,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        if progress is not None:
            progress.update(
                job_status="running",
                step="deleting",
                step_status="completed",
                payload={
                    "summary": "Scoped rows deleted.",
                    "preview": preview,
                    "delete_summary": delete_summary,
                    "rows_written_total": 0,
                    "rows_deleted_total": sum(
                        int((value or {}).get("deleted_rows_estimate") or 0)
                        for value in (delete_summary or {}).values()
                        if isinstance(value, dict)
                    ),
                    "finalization_status": "pending",
                },
            )

    run_summary = execute_rebuild_plan(
        client=registry.client,
        publisher=publisher,
        cache_root=str(data_ops_settings.cache_root),
        plan=plan,
        progress=progress,
        max_attempts=3,
        history_limit=120,
        symbol_batch_size=data_ops_settings.symbol_batch_size,
    )

    return {
        "status": "completed",
        "dry_run": False,
        "domain": domain,
        "mode": mode,
        "preview": preview,
        "delete_summary": delete_summary,
        "run_summary": run_summary,
    }


def run_data_ops_series_upsert_job(
    *,
    settings: WorkerSettings,
    registry: WorkerRegistryStore,
    params: dict[str, Any],
) -> dict[str, Any]:
    kind = str(params.get("kind") or "").strip().lower()
    dry_run = bool(params.get("dry_run"))
    if kind == "macro":
        return _run_macro_series_upsert(settings=settings, registry=registry, params=params, dry_run=dry_run)
    if kind == "release-calendar":
        return _run_release_calendar_upsert(settings=settings, registry=registry, params=params, dry_run=dry_run)
    raise ValueError(f"Unsupported series upsert kind: {kind!r}")


def _run_macro_series_upsert(
    *,
    settings: WorkerSettings,
    registry: WorkerRegistryStore,
    params: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    series = dict(params.get("series") or {})
    series_key = str(series.get("series_key") or "").strip()
    if not series_key:
        raise ValueError("series.series_key is required.")
    source_provider = str(series.get("source_provider") or "").strip().lower()
    source_code = str(series.get("source_code") or "").strip()
    frequency = str(series.get("frequency") or "").strip().lower()
    if source_provider not in {"fred", "yfinance"}:
        raise ValueError("series.source_provider must be one of: fred, yfinance.")
    if frequency not in {"daily", "weekly", "monthly"}:
        raise ValueError("series.frequency must be one of: daily, weekly, monthly.")
    if not source_code:
        raise ValueError("series.source_code is required.")

    start_date, end_date = _resolve_series_backfill_window(series=series)
    spec = MacroSeriesSpec(
        key=series_key,
        source=source_provider,  # type: ignore[arg-type]
        source_code=source_code,
        frequency=frequency,  # type: ignore[arg-type]
        required_by_default=bool(series.get("required_by_default")),
        optional=bool(series.get("optional", True)),
        description=str(series.get("description") or "").strip(),
    )

    row = {
        "series_key": series_key,
        "source_provider": source_provider,
        "source_code": source_code,
        "frequency": frequency,
        "required_by_default": bool(series.get("required_by_default")),
        "optional": bool(series.get("optional", True)),
        "staleness_max_bdays": int(series.get("staleness_max_bdays") or _staleness_for_frequency(frequency)),
        "release_calendar_source": str(series.get("release_calendar_source") or "").strip() or None,
        "description": str(series.get("description") or "").strip() or None,
        "updated_at": datetime.now(UTC).isoformat(),
    }

    if dry_run:
        return {
            "status": "completed",
            "dry_run": True,
            "kind": "macro",
            "preview": {
                "series_key": series_key,
                "source_provider": source_provider,
                "frequency": frequency,
                "backfill_start_date": start_date,
                "backfill_end_date": end_date,
                "catalog_row": row,
            },
        }

    run_summary = run_dataops_macro_daily(
        start=start_date,
        end=end_date,
        publish_enabled=True,
        force_recompute=True,
        series_catalog=(spec,),
    )

    registry.client.table("macro_series_catalog").upsert(row, on_conflict="series_key").execute()
    return {
        "status": "completed",
        "dry_run": False,
        "kind": "macro",
        "series_key": series_key,
        "run_summary": run_summary,
        "catalog_row": row,
    }


def _run_release_calendar_upsert(
    *,
    settings: WorkerSettings,
    registry: WorkerRegistryStore,
    params: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    row_input = dict(params.get("release_row") or {})
    series_key = str(row_input.get("series_key") or "").strip()
    observation_period = str(row_input.get("observation_period") or "").strip()
    observation_date = str(row_input.get("observation_date") or "").strip()
    scheduled_release_timestamp = str(row_input.get("scheduled_release_timestamp_utc") or "").strip()
    if not series_key or not observation_period or not observation_date or not scheduled_release_timestamp:
        raise ValueError("series_key, observation_period, observation_date, and scheduled_release_timestamp_utc are required.")

    scheduled_ts = pd.Timestamp(scheduled_release_timestamp)
    if scheduled_ts.tzinfo is None:
        scheduled_ts = scheduled_ts.tz_localize("UTC")
    else:
        scheduled_ts = scheduled_ts.tz_convert("UTC")
    observed_raw = str(row_input.get("observed_first_available_at_utc") or "").strip()
    observed_ts: pd.Timestamp | None = None
    if observed_raw:
        observed_ts = pd.Timestamp(observed_raw)
        if observed_ts.tzinfo is None:
            observed_ts = observed_ts.tz_localize("UTC")
        else:
            observed_ts = observed_ts.tz_convert("UTC")

    release_timezone = str(row_input.get("release_timezone") or "America/New_York").strip()
    tz = ZoneInfo(release_timezone)
    release_date_local = scheduled_ts.tz_convert(tz).date().isoformat()
    availability_source = f"{str(row_input.get('release_calendar_source') or 'manual_admin_entry_v1').strip()}_manual_v1"

    delay_seconds: int | None = None
    if observed_ts is not None:
        delay_seconds = int(round((observed_ts - scheduled_ts).total_seconds()))

    table_row = {
        "series_key": series_key,
        "observation_period": observation_period,
        "observation_date": observation_date,
        "release_timestamp_utc": scheduled_ts.isoformat(),
        "scheduled_release_timestamp_utc": scheduled_ts.isoformat(),
        "observed_first_available_at_utc": (observed_ts.isoformat() if observed_ts is not None else None),
        "availability_status": ("observed_available" if observed_ts is not None else "scheduled_provisional"),
        "availability_source": availability_source,
        "delay_vs_schedule_seconds": delay_seconds,
        "is_schedule_based_only": observed_ts is None,
        "release_timezone": release_timezone,
        "release_date_local": release_date_local,
        "release_calendar_source": str(row_input.get("release_calendar_source") or "manual_admin_entry_v1").strip(),
        "source": str(row_input.get("source") or "manual_admin_entry_v1").strip(),
        "provenance_class": str(row_input.get("provenance_class") or "manual").strip(),
        "ingested_at": datetime.now(UTC).isoformat(),
    }

    if dry_run:
        return {
            "status": "completed",
            "dry_run": True,
            "kind": "release-calendar",
            "preview": table_row,
        }

    publisher = SupabaseRestPublisher(
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_service_role_key,
    )
    frame = pd.DataFrame([table_row])
    publish_result = publish_release_calendar_surfaces(
        publisher=publisher,
        economic_release_calendar=frame,
    )
    return {
        "status": "completed",
        "dry_run": False,
        "kind": "release-calendar",
        "publish_result": publish_result,
        "row": table_row,
    }


def _resolve_rebuild_window(*, params: dict[str, Any], scope: dict[str, Any], domain: str) -> tuple[str, str]:
    end = _parse_iso_date_token(scope.get("end_date") or params.get("end_date")) or date.today()
    start = _parse_iso_date_token(scope.get("start_date") or params.get("start_date"))
    if start is None:
        if domain in {"market", "macro", "release-calendar"}:
            start = end - timedelta(days=90)
        else:
            start = end - timedelta(days=365)
    if start > end:
        raise ValueError("start_date must be on or before end_date.")
    return start.isoformat(), end.isoformat()


def _resolve_series_backfill_window(*, series: dict[str, Any]) -> tuple[str, str]:
    end = _parse_iso_date_token(series.get("backfill_end_date")) or date.today()
    start = _parse_iso_date_token(series.get("backfill_start_date")) or (end - timedelta(days=365))
    if start > end:
        raise ValueError("backfill_start_date must be on or before backfill_end_date.")
    return start.isoformat(), end.isoformat()


def _resolve_scope_symbols(*, registry: WorkerRegistryStore, scope: dict[str, Any], domain: str) -> list[str]:
    if domain in {"macro", "release-calendar"}:
        return []
    scope_type = str(scope.get("scope_type") or "whole_domain").strip().lower()
    ticker = str(scope.get("ticker") or "").strip().upper()
    if scope_type == "ticker":
        if not ticker:
            raise ValueError("scope.ticker is required when scope_type=ticker.")
        return [ticker]

    rows_response = registry.client.table("ticker_registry").select(
        "normalized_symbol,input_symbol,region,status,promotion_status"
    ).execute()
    rows = [dict(item) for item in (rows_response.data or []) if isinstance(item, dict)]
    region_filter = str(scope.get("region") or "").strip().lower()
    symbols: list[str] = []
    for row in rows:
        status = str(row.get("status") or "").strip().lower()
        promotion = str(row.get("promotion_status") or "").strip().lower()
        if status != "active" or promotion not in _PROMOTABLE:
            continue
        if scope_type == "region" and region_filter and str(row.get("region") or "").strip().lower() != region_filter:
            continue
        symbol = str(row.get("normalized_symbol") or row.get("input_symbol") or "").strip().upper()
        if symbol:
            symbols.append(symbol)
    deduped = sorted(set(symbols))
    if not deduped:
        raise RuntimeError("No active symbols resolved for requested scope.")
    return deduped


def _build_rebuild_preview(
    *,
    registry: WorkerRegistryStore,
    domain: str,
    mode: str,
    scope: dict[str, Any],
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    preview = {
        "domain": domain,
        "mode": mode,
        "scope": dict(scope),
        "resolved_symbols": symbols,
        "window": {"start_date": start_date, "end_date": end_date},
        "estimated_rows": {},
    }
    for table_name, date_col, ticker_col in _tables_for_domain(domain):
        estimated = _estimate_row_count(
            registry=registry,
            table_name=table_name,
            date_column=date_col,
            ticker_column=ticker_col,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        preview["estimated_rows"][table_name] = estimated
    return preview


def _execute_wipe(
    *,
    registry: WorkerRegistryStore,
    domain: str,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for table_name, date_col, ticker_col in _tables_for_domain(domain):
        deleted = _delete_rows(
            registry=registry,
            table_name=table_name,
            date_column=date_col,
            ticker_column=ticker_col,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        summary[table_name] = {"deleted_rows_estimate": deleted}
    return summary


def _execute_rebuild(
    *,
    domain: str,
    symbols: list[str],
    start_date: str,
    end_date: str,
    force_recompute: bool,
) -> dict[str, Any]:
    # Legacy fallback retained only for non-rebuild paths while the new executor rolls out.
    if domain == "market":
        return run_dataops_market_daily(
            symbols=symbols,
            start=start_date,
            end=end_date,
            publish_enabled=True,
        )
    if domain == "fundamentals":
        return run_dataops_fundamentals_daily(
            symbols=symbols,
            publish_enabled=True,
        )
    if domain == "earnings":
        return run_dataops_earnings_daily(
            symbols=symbols,
            publish_enabled=True,
            history_limit=120,
        )
    if domain == "macro":
        return run_dataops_macro_daily(
            start=start_date,
            end=end_date,
            publish_enabled=True,
            force_recompute=bool(force_recompute),
        )
    if domain == "release-calendar":
        return run_dataops_release_calendar_daily(
            start_date=start_date,
            end_date=end_date,
            publish_enabled=True,
            force_recompute=bool(force_recompute),
        )
    raise ValueError(f"Unsupported domain for rebuild: {domain!r}")


def _tables_for_domain(domain: str) -> list[tuple[str, str | None, str | None]]:
    if domain == "market":
        return [
            ("market_price_daily", "as_of_date", "symbol"),
            ("market_quotes", None, "ticker"),
            ("ticker_market_stats_snapshot", "as_of_date", "ticker"),
        ]
    if domain == "fundamentals":
        return [
            ("market_fundamentals_v2", "period_end", "ticker"),
            ("ticker_fundamental_summary", "latest_period_end", "ticker"),
        ]
    if domain == "earnings":
        return [
            ("market_earnings_events", "earnings_date", "ticker"),
            ("market_earnings_history", "earnings_date", "ticker"),
        ]
    if domain == "macro":
        return [
            ("macro_observations", "observation_date", None),
            ("macro_daily", "as_of_date", None),
            ("macro_series_catalog", None, None),
        ]
    if domain == "release-calendar":
        return [("economic_release_calendar", "observation_date", None)]
    return []


def _estimate_row_count(
    *,
    registry: WorkerRegistryStore,
    table_name: str,
    date_column: str | None,
    ticker_column: str | None,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> int:
    try:
        query = registry.client.table(table_name).select("*", count="exact")
        if ticker_column and symbols:
            query = query.in_(ticker_column, symbols)
        if date_column:
            query = query.gte(date_column, start_date).lte(date_column, end_date)
        response = query.limit(1).execute()
        count = getattr(response, "count", None)
        if count is not None:
            return int(count)
        data = response.data or []
        return int(len(data))
    except Exception:
        return 0


def _delete_rows(
    *,
    registry: WorkerRegistryStore,
    table_name: str,
    date_column: str | None,
    ticker_column: str | None,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> int:
    estimated = _estimate_row_count(
        registry=registry,
        table_name=table_name,
        date_column=date_column,
        ticker_column=ticker_column,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
    )
    query = registry.client.table(table_name).delete()
    if ticker_column and symbols:
        query = query.in_(ticker_column, symbols)
    if date_column:
        query = query.gte(date_column, start_date).lte(date_column, end_date)
    query.execute()
    return estimated


def _parse_iso_date_token(raw: object) -> date | None:
    token = str(raw or "").strip()
    if not token:
        return None
    return date.fromisoformat(token[:10])


def _staleness_for_frequency(frequency: str) -> int:
    token = str(frequency).strip().lower()
    if token == "weekly":
        return 10
    if token == "monthly":
        return 45
    return 5
