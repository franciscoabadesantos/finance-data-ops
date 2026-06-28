"""Gap-aware recurring execution planning based on canonical data watermarks."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from finance_data_ops.refresh.storage import read_parquet_table


ExecutionMode = Literal["normal", "catch-up", "gap-repair"]
Cadence = Literal["business", "calendar"]


@dataclass(frozen=True, slots=True)
class RecurringExecutionPlan:
    domain: str
    mode: ExecutionMode
    start_date: str
    end_date: str
    expected_end_date: str
    latest_complete_canonical_date: str | None
    gap_exists: bool
    missing_dates_count: int
    earliest_missing_date: str | None
    safety_overlap_days: int
    canonical_source: str
    table_name: str
    date_column: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def resolve_gap_aware_window(
    *,
    domain: str,
    table_name: str,
    date_column: str,
    cadence: Cadence,
    lookback_days: int,
    explicit_start: str | None,
    explicit_end: str | None,
    safety_overlap_days: int,
    cache_root: str | Path,
    database_dsn: str = "",
    **_legacy_kwargs: Any,
) -> RecurringExecutionPlan:
    target_end = _parse_date_or_today(explicit_end)
    baseline_start = (
        _parse_date(explicit_start)
        if explicit_start
        else (target_end - timedelta(days=max(int(lookback_days), 1)))
    )
    if baseline_start > target_end:
        raise ValueError(
            f"Invalid window: start ({baseline_start.isoformat()}) is after end ({target_end.isoformat()})."
        )

    available_dates, source = _load_canonical_dates(
        table_name=table_name,
        date_column=date_column,
        start_date=baseline_start,
        end_date=target_end,
        cache_root=cache_root,
        database_dsn=database_dsn,
    )
    latest_complete = max(available_dates) if available_dates else None

    if explicit_start:
        return RecurringExecutionPlan(
            domain=domain,
            mode="normal",
            start_date=baseline_start.isoformat(),
            end_date=target_end.isoformat(),
            expected_end_date=target_end.isoformat(),
            latest_complete_canonical_date=(latest_complete.isoformat() if latest_complete else None),
            gap_exists=False,
            missing_dates_count=0,
            earliest_missing_date=None,
            safety_overlap_days=max(int(safety_overlap_days), 0),
            canonical_source=source,
            table_name=table_name,
            date_column=date_column,
        )

    expected_dates = _expected_dates(start_date=baseline_start, end_date=target_end, cadence=cadence)
    missing_dates = sorted(expected_dates.difference(available_dates))
    overlap_days = max(int(safety_overlap_days), 0)

    if missing_dates:
        earliest_missing = missing_dates[0]
        mode: ExecutionMode
        if latest_complete is not None and earliest_missing <= latest_complete:
            mode = "gap-repair"
        else:
            mode = "catch-up"
        planned_start = max(baseline_start, earliest_missing - timedelta(days=overlap_days))
    else:
        earliest_missing = None
        mode = "normal"
        planned_start = max(baseline_start, target_end - timedelta(days=overlap_days))

    return RecurringExecutionPlan(
        domain=domain,
        mode=mode,
        start_date=planned_start.isoformat(),
        end_date=target_end.isoformat(),
        expected_end_date=target_end.isoformat(),
        latest_complete_canonical_date=(latest_complete.isoformat() if latest_complete else None),
        gap_exists=bool(missing_dates),
        missing_dates_count=int(len(missing_dates)),
        earliest_missing_date=(earliest_missing.isoformat() if earliest_missing else None),
        safety_overlap_days=overlap_days,
        canonical_source=source,
        table_name=table_name,
        date_column=date_column,
    )


def resolve_watermark_execution(
    *,
    domain: str,
    table_name: str,
    date_column: str,
    lookback_days: int,
    grace_days: int,
    safety_overlap_days: int,
    explicit_end: str | None,
    cache_root: str | Path,
    database_dsn: str = "",
    **_legacy_kwargs: Any,
) -> RecurringExecutionPlan:
    target_end = _parse_date_or_today(explicit_end)
    baseline_start = target_end - timedelta(days=max(int(lookback_days), 1))
    overlap_days = max(int(safety_overlap_days), 0)
    allowed_lag = timedelta(days=max(int(grace_days), 0))

    available_dates, source = _load_canonical_dates(
        table_name=table_name,
        date_column=date_column,
        start_date=baseline_start,
        end_date=target_end,
        cache_root=cache_root,
        database_dsn=database_dsn,
    )
    latest_complete = max(available_dates) if available_dates else None

    gap_exists = latest_complete is None or latest_complete < (target_end - allowed_lag)
    if gap_exists:
        mode: ExecutionMode = "catch-up"
        if latest_complete is None:
            earliest_missing = baseline_start
        else:
            earliest_missing = latest_complete + timedelta(days=1)
        planned_start = max(baseline_start, earliest_missing - timedelta(days=overlap_days))
        missing_count = max((target_end - earliest_missing).days + 1, 1)
    else:
        mode = "normal"
        earliest_missing = None
        planned_start = max(baseline_start, target_end - timedelta(days=overlap_days))
        missing_count = 0

    return RecurringExecutionPlan(
        domain=domain,
        mode=mode,
        start_date=planned_start.isoformat(),
        end_date=target_end.isoformat(),
        expected_end_date=target_end.isoformat(),
        latest_complete_canonical_date=(latest_complete.isoformat() if latest_complete else None),
        gap_exists=bool(gap_exists),
        missing_dates_count=int(missing_count),
        earliest_missing_date=(earliest_missing.isoformat() if earliest_missing else None),
        safety_overlap_days=overlap_days,
        canonical_source=source,
        table_name=table_name,
        date_column=date_column,
    )


def _load_canonical_dates(
    *,
    table_name: str,
    date_column: str,
    start_date: date,
    end_date: date,
    cache_root: str | Path,
    database_dsn: str,
) -> tuple[set[date], str]:
    remote_dates = _fetch_dates_from_postgres(
        table_name=table_name,
        date_column=date_column,
        start_date=start_date,
        end_date=end_date,
        database_dsn=database_dsn,
    )
    if remote_dates is not None:
        return remote_dates, "postgres"
    return _fetch_dates_from_parquet(
        table_name=table_name,
        date_column=date_column,
        start_date=start_date,
        end_date=end_date,
        cache_root=cache_root,
    ), "parquet"


def _fetch_dates_from_postgres(
    *,
    table_name: str,
    date_column: str,
    start_date: date,
    end_date: date,
    database_dsn: str,
) -> set[date] | None:
    dsn = str(database_dsn).strip()
    if not dsn:
        return None
    try:
        import psycopg
    except ImportError:
        return None
    try:
        schema_name, relation_name = _parse_relation_name(table_name)
        _validate_identifier(date_column)
        query = (
            f'select distinct "{date_column}" from "{schema_name}"."{relation_name}" '
            f'where "{date_column}" is not null and "{date_column}" >= %s and "{date_column}" <= %s'
        )
        out: set[date] = set()
        with psycopg.connect(dsn, connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (start_date, end_date))
                for (value,) in cur.fetchall():
                    parsed = _to_date(value)
                    if parsed is not None and start_date <= parsed <= end_date:
                        out.add(parsed)
        return out
    except Exception:
        return None


def _parse_relation_name(raw: str) -> tuple[str, str]:
    parts = str(raw).strip().split(".")
    if len(parts) == 1:
        schema_name, relation_name = "public", parts[0]
    elif len(parts) == 2:
        schema_name, relation_name = parts
    else:
        raise ValueError(f"Invalid table name: {raw!r}")
    _validate_identifier(schema_name)
    _validate_identifier(relation_name)
    return schema_name, relation_name


def _validate_identifier(value: str) -> None:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Invalid SQL identifier: {value!r}")


def _fetch_dates_from_parquet(
    *,
    table_name: str,
    date_column: str,
    start_date: date,
    end_date: date,
    cache_root: str | Path,
) -> set[date]:
    frame = read_parquet_table(table_name, cache_root=cache_root, required=False)
    if frame.empty or date_column not in frame.columns:
        return set()
    values = pd.to_datetime(frame[date_column], errors="coerce", utc=True)
    out: set[date] = set()
    for value in values:
        if pd.isna(value):
            continue
        parsed = pd.Timestamp(value).date()
        if start_date <= parsed <= end_date:
            out.add(parsed)
    return out


def _expected_dates(*, start_date: date, end_date: date, cadence: Cadence) -> set[date]:
    if end_date < start_date:
        return set()
    if cadence == "business":
        return {pd.Timestamp(value).date() for value in pd.bdate_range(start=start_date, end=end_date)}
    return {pd.Timestamp(value).date() for value in pd.date_range(start=start_date, end=end_date)}


def _to_date(raw: Any) -> date | None:
    if raw is None:
        return None
    parsed = pd.to_datetime(raw, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def _parse_date(raw: str) -> date:
    return pd.Timestamp(raw).date()


def _parse_date_or_today(raw: str | None) -> date:
    if raw:
        return _parse_date(raw)
    return datetime.now(UTC).date()
