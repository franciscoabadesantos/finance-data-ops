from __future__ import annotations

from datetime import UTC, date, timedelta
from typing import Any

import pandas as pd

DATE_KEYS_MARKET = ("date", "as_of_date")
DATE_KEYS_FUNDAMENTALS = ("period_end", "latest_period_end")
DATE_KEYS_EARNINGS = ("earnings_date",)
UPDATE_TS_KEYS = (
    "updated_at",
    "fetched_at",
    "created_at",
    "ingested_at",
    "available_at_utc",
    "published_at",
)

WINDOW_FIELDS = (
    "first_available_date",
    "last_available_date",
    "latest_update_timestamp_utc",
    "expected_rows",
    "actual_rows",
    "missing_days_count",
    "missing_periods_count",
    "completeness_pct",
    "largest_gap_summary",
)


def build_data_window_items(
    *,
    market_price_rows: list[dict[str, Any]] | None,
    fundamentals_rows: list[dict[str, Any]] | None,
    earnings_rows: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    market_stats = _compute_daily_window_stats(rows=market_price_rows or [], date_keys=DATE_KEYS_MARKET)
    fundamentals_stats = _compute_periodic_window_stats(rows=fundamentals_rows or [], date_keys=DATE_KEYS_FUNDAMENTALS)
    earnings_stats = _compute_quarterly_window_stats(rows=earnings_rows or [], date_keys=DATE_KEYS_EARNINGS)

    for domain, stats in (
        ("market_price_daily", market_stats),
        ("market_fundamentals_v2", fundamentals_stats),
        ("market_earnings_history", earnings_stats),
    ):
        for field in WINDOW_FIELDS:
            items.append({"key": f"{domain}.{field}", "value": stats.get(field)})
    return items


def _compute_daily_window_stats(*, rows: list[dict[str, Any]], date_keys: tuple[str, ...]) -> dict[str, Any]:
    unique_dates = sorted(set(_extract_dates(rows, date_keys=date_keys)))
    latest_update = _extract_latest_update(rows)
    if not unique_dates:
        return _empty_stats(latest_update)

    first_date = unique_dates[0]
    last_date = unique_dates[-1]
    expected_rows = _business_days_inclusive(first_date, last_date)
    actual_rows = len(unique_dates)
    missing_days = max(expected_rows - actual_rows, 0)
    completeness = round((actual_rows / expected_rows) * 100.0, 2) if expected_rows > 0 else None
    largest_gap_summary = _largest_business_gap_summary(unique_dates)
    return {
        "first_available_date": first_date.isoformat(),
        "last_available_date": last_date.isoformat(),
        "latest_update_timestamp_utc": latest_update,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "missing_days_count": missing_days,
        "missing_periods_count": None,
        "completeness_pct": completeness,
        "largest_gap_summary": largest_gap_summary,
    }


def _compute_periodic_window_stats(*, rows: list[dict[str, Any]], date_keys: tuple[str, ...]) -> dict[str, Any]:
    unique_dates = sorted(set(_extract_dates(rows, date_keys=date_keys)))
    latest_update = _extract_latest_update(rows)
    if not unique_dates:
        return _empty_stats(latest_update)

    first_date = unique_dates[0]
    last_date = unique_dates[-1]
    largest_gap_summary = _largest_calendar_gap_summary(unique_dates)
    return {
        "first_available_date": first_date.isoformat(),
        "last_available_date": last_date.isoformat(),
        "latest_update_timestamp_utc": latest_update,
        "expected_rows": None,
        "actual_rows": len(unique_dates),
        "missing_days_count": None,
        "missing_periods_count": None,
        "completeness_pct": None,
        "largest_gap_summary": largest_gap_summary,
    }


def _compute_quarterly_window_stats(*, rows: list[dict[str, Any]], date_keys: tuple[str, ...]) -> dict[str, Any]:
    unique_dates = sorted(set(_extract_dates(rows, date_keys=date_keys)))
    latest_update = _extract_latest_update(rows)
    if not unique_dates:
        return _empty_stats(latest_update)

    first_date = unique_dates[0]
    last_date = unique_dates[-1]
    expected_rows = (_quarter_index(last_date) - _quarter_index(first_date)) + 1
    actual_rows = len(unique_dates)
    missing_periods = max(expected_rows - actual_rows, 0)
    completeness = round((actual_rows / expected_rows) * 100.0, 2) if expected_rows > 0 else None
    largest_gap_summary = _largest_quarter_gap_summary(unique_dates)
    return {
        "first_available_date": first_date.isoformat(),
        "last_available_date": last_date.isoformat(),
        "latest_update_timestamp_utc": latest_update,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "missing_days_count": None,
        "missing_periods_count": missing_periods,
        "completeness_pct": completeness,
        "largest_gap_summary": largest_gap_summary,
    }


def _empty_stats(latest_update: str | None) -> dict[str, Any]:
    return {
        "first_available_date": None,
        "last_available_date": None,
        "latest_update_timestamp_utc": latest_update,
        "expected_rows": None,
        "actual_rows": 0,
        "missing_days_count": None,
        "missing_periods_count": None,
        "completeness_pct": None,
        "largest_gap_summary": None,
    }


def _extract_dates(rows: list[dict[str, Any]], *, date_keys: tuple[str, ...]) -> list[date]:
    out: list[date] = []
    for row in rows:
        for key in date_keys:
            parsed = _parse_date(row.get(key))
            if parsed is not None:
                out.append(parsed)
                break
    return out


def _extract_latest_update(rows: list[dict[str, Any]]) -> str | None:
    latest: pd.Timestamp | None = None
    for row in rows:
        for key in UPDATE_TS_KEYS:
            ts = _parse_timestamp(row.get(key))
            if ts is None:
                continue
            if latest is None or ts > latest:
                latest = ts
    if latest is None:
        return None
    return latest.isoformat()


def _parse_timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).tz_convert("UTC")


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return pd.Timestamp(ts).date()


def _is_business_day(value: date) -> bool:
    return value.weekday() < 5


def _business_days_inclusive(start: date, end: date) -> int:
    if end < start:
        return 0
    cursor = start
    count = 0
    while cursor <= end:
        if _is_business_day(cursor):
            count += 1
        cursor += timedelta(days=1)
    return count


def _largest_business_gap_summary(unique_dates: list[date]) -> str | None:
    if len(unique_dates) < 2:
        return None
    best_missing = 0
    best_pair: tuple[date, date] | None = None
    for previous, current in zip(unique_dates, unique_dates[1:]):
        gap = _business_days_between(previous, current)
        if gap > best_missing:
            best_missing = gap
            best_pair = (previous, current)
    if best_pair is None or best_missing <= 0:
        return None
    return (
        f"{best_pair[0].isoformat()} -> {best_pair[1].isoformat()} "
        f"(missing_business_days={best_missing})"
    )


def _largest_calendar_gap_summary(unique_dates: list[date]) -> str | None:
    if len(unique_dates) < 2:
        return None
    best_gap = 0
    best_pair: tuple[date, date] | None = None
    for previous, current in zip(unique_dates, unique_dates[1:]):
        gap_days = (current - previous).days - 1
        if gap_days > best_gap:
            best_gap = gap_days
            best_pair = (previous, current)
    if best_pair is None or best_gap <= 0:
        return None
    return (
        f"{best_pair[0].isoformat()} -> {best_pair[1].isoformat()} "
        f"(missing_calendar_days={best_gap})"
    )


def _largest_quarter_gap_summary(unique_dates: list[date]) -> str | None:
    if len(unique_dates) < 2:
        return None
    best_gap = 0
    best_pair: tuple[date, date] | None = None
    for previous, current in zip(unique_dates, unique_dates[1:]):
        gap_quarters = _quarter_index(current) - _quarter_index(previous) - 1
        if gap_quarters > best_gap:
            best_gap = gap_quarters
            best_pair = (previous, current)
    if best_pair is None or best_gap <= 0:
        return None
    return (
        f"{best_pair[0].isoformat()} -> {best_pair[1].isoformat()} "
        f"(missing_quarters={best_gap})"
    )


def _business_days_between(previous: date, current: date) -> int:
    if current <= previous:
        return 0
    start = previous + timedelta(days=1)
    end = current - timedelta(days=1)
    if end < start:
        return 0
    return _business_days_inclusive(start, end)


def _quarter_index(value: date) -> int:
    quarter = ((value.month - 1) // 3) + 1
    return value.year * 4 + quarter
