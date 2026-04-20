"""Chunk planning for historical rebuild jobs."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from finance_data_ops.providers.macro import MACRO_SERIES_CATALOG, _default_release_calendar_source
from finance_data_ops.rebuild.policies import DomainPolicy, get_domain_policy


@dataclass(frozen=True, slots=True)
class RebuildChunk:
    batch_index: int
    ticker_batch: tuple[str, ...] = ()
    series_batch: tuple[str, ...] = ()
    start_date: str | None = None
    end_date: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RebuildPlan:
    domain: str
    mode: str
    execution_mode: str
    scope: dict[str, Any]
    policy: DomainPolicy
    start_date: str
    end_date: str
    chunks: tuple[RebuildChunk, ...]
    series_start_dates: dict[str, str]
    resolved_symbols: tuple[str, ...]
    dry_run_summary: dict[str, Any]


def build_rebuild_plan(
    *,
    client: Any,
    domain: str,
    mode: str,
    scope: dict[str, Any],
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> RebuildPlan:
    policy = get_domain_policy(domain)
    scope_copy = dict(scope)
    start = date.fromisoformat(str(start_date))
    end = date.fromisoformat(str(end_date))
    if end < start:
        raise ValueError("Rebuild end date must be on or after start date.")

    series_start_dates = _resolve_series_start_dates(client=client, domain=domain, policy=policy)
    execution_mode = "wipe_rebuild" if str(mode).strip().lower() == "wipe_rebuild" else "historical_backfill"
    chunks: list[RebuildChunk] = []

    if domain in {"macro", "release-calendar"}:
        series_keys = sorted(series_start_dates.keys())
        for batch_index, series_key in enumerate(series_keys, start=1):
            effective_start = max(
                start,
                date.fromisoformat(series_start_dates.get(series_key, start.isoformat())),
            )
            if effective_start > end:
                continue
            chunks.append(
                RebuildChunk(
                    batch_index=batch_index,
                    series_batch=(series_key,),
                    start_date=effective_start.isoformat(),
                    end_date=end.isoformat(),
                )
            )
    else:
        tickers = tuple(sorted({str(value).strip().upper() for value in symbols if str(value).strip()}))
        batches = _chunk_values(list(tickers), size=policy.chunk_size)
        for batch_index, ticker_batch in enumerate(batches, start=1):
            chunks.append(
                RebuildChunk(
                    batch_index=batch_index,
                    ticker_batch=tuple(ticker_batch),
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                )
            )

    dry_run_summary = {
        "domain": domain,
        "mode": mode,
        "execution_mode": execution_mode,
        "scope": scope_copy,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "resolved_symbols": list(sorted({str(v).strip().upper() for v in symbols if str(v).strip()})),
        "series_start_dates": series_start_dates,
        "chunk_count": len(chunks),
        "chunks": [chunk.as_dict() for chunk in chunks[:50]],
        "upsert_batch_size": policy.chunk_size,
        "sleep_seconds": policy.sleep_seconds,
        "refresh_materialized_views": list(policy.refresh_materialized_views),
        "wipe_tables": list(policy.wipe_tables),
    }

    return RebuildPlan(
        domain=domain,
        mode=mode,
        execution_mode=execution_mode,
        scope=scope_copy,
        policy=policy,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        chunks=tuple(chunks),
        series_start_dates=series_start_dates,
        resolved_symbols=tuple(sorted({str(v).strip().upper() for v in symbols if str(v).strip()})),
        dry_run_summary=dry_run_summary,
    )


def _resolve_series_start_dates(*, client: Any, domain: str, policy: DomainPolicy) -> dict[str, str]:
    if domain not in {"macro", "release-calendar"}:
        return {}

    catalog_response = client.table("macro_series_catalog").select(
        "series_key,required_from_date,release_calendar_source"
    ).execute()
    rows = [dict(item) for item in (catalog_response.data or []) if isinstance(item, dict)]
    resolved: dict[str, str] = {}
    fallback_start = (
        policy.full_rebuild_start_date.isoformat()
        if policy.full_rebuild_start_date is not None
        else date(2000, 1, 1).isoformat()
    )
    if rows:
        for row in rows:
            key = str(row.get("series_key") or "").strip()
            if not key:
                continue
            release_calendar_source = str(row.get("release_calendar_source") or "").strip()
            if domain == "release-calendar" and not release_calendar_source:
                continue
            required_from = str(row.get("required_from_date") or "").strip()
            if required_from:
                resolved[key] = required_from[:10]
            else:
                resolved[key] = fallback_start

    if not resolved:
        for spec in MACRO_SERIES_CATALOG:
            if domain == "release-calendar" and not _default_release_calendar_source(spec.key):
                continue
            if spec.required_from_date is not None:
                resolved[spec.key] = spec.required_from_date.isoformat()
            else:
                resolved[spec.key] = fallback_start
    return dict(sorted(resolved.items()))


def _chunk_values(values: list[str], *, size: int) -> list[list[str]]:
    chunk_size = max(int(size), 1)
    return [values[idx : idx + chunk_size] for idx in range(0, len(values), chunk_size)]
