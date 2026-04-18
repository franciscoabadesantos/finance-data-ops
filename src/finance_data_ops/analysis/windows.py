from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

DATASET_LABELS: dict[str, str] = {
    "market_price_daily": "Market price history",
    "market_fundamentals_v2": "Fundamentals",
    "market_earnings_history": "Earnings history",
}

DATASET_KEYS: tuple[str, ...] = (
    "market_price_daily",
    "market_fundamentals_v2",
    "market_earnings_history",
)

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
    stats_by_domain = build_data_window_stats(
        market_price_rows=market_price_rows,
        fundamentals_rows=fundamentals_rows,
        earnings_rows=earnings_rows,
    )
    items: list[dict[str, Any]] = []
    for domain in DATASET_KEYS:
        label = DATASET_LABELS.get(domain, domain)
        stats = stats_by_domain.get(domain, {})
        for field in WINDOW_FIELDS:
            items.append({"key": f"{label}.{field}", "value": stats.get(field)})
    return items


def build_data_window_stats(
    *,
    market_price_rows: list[dict[str, Any]] | None,
    fundamentals_rows: list[dict[str, Any]] | None,
    earnings_rows: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    return {
        "market_price_daily": _compute_daily_window_stats(rows=market_price_rows or [], date_keys=DATE_KEYS_MARKET),
        "market_fundamentals_v2": _compute_periodic_window_stats(
            rows=fundamentals_rows or [],
            date_keys=DATE_KEYS_FUNDAMENTALS,
        ),
        "market_earnings_history": _compute_quarterly_window_stats(rows=earnings_rows or [], date_keys=DATE_KEYS_EARNINGS),
    }


def build_completeness_summary_lines(
    *,
    stats_by_domain: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    market_stats = stats_by_domain.get("market_price_daily", {})
    fundamentals_stats = stats_by_domain.get("market_fundamentals_v2", {})
    earnings_stats = stats_by_domain.get("market_earnings_history", {})
    return [
        {"key": DATASET_LABELS["market_price_daily"], "value": _format_market_completeness_summary(market_stats)},
        {"key": DATASET_LABELS["market_earnings_history"], "value": _format_earnings_completeness_summary(earnings_stats)},
        {"key": DATASET_LABELS["market_fundamentals_v2"], "value": _format_fundamentals_completeness_summary(fundamentals_stats)},
    ]


def coverage_summary_text(*, ticker: str, coverage: dict[str, Any] | None) -> str:
    coverage_row = coverage or {}
    available_labels = []
    if bool(coverage_row.get("market_data_available", False)):
        available_labels.append("market")
    if bool(coverage_row.get("fundamentals_available", False)):
        available_labels.append("fundamentals")
    if bool(coverage_row.get("earnings_available", False)):
        available_labels.append("earnings")
    if not available_labels:
        return f"{ticker} has no canonical market/fundamentals/earnings coverage currently available."
    if len(available_labels) == 1:
        joined = available_labels[0]
    elif len(available_labels) == 2:
        joined = f"{available_labels[0]} and {available_labels[1]}"
    else:
        joined = f"{available_labels[0]}, {available_labels[1]}, and {available_labels[2]}"
    return f"{ticker} has canonical {joined} data available."


def registry_summary_text(*, ticker: str, registry_row: dict[str, Any] | None) -> str:
    if registry_row is None:
        return f"{ticker} is available canonically but is not onboarded in ticker_registry for this scope."
    status = str(registry_row.get("status") or "unknown")
    validation = str(registry_row.get("validation_status") or "unknown")
    promotion = str(registry_row.get("promotion_status") or "unknown")
    return (
        f"{ticker} is onboarded in ticker_registry "
        f"(status={status}, validation={validation}, promotion={promotion})."
    )


def _format_market_completeness_summary(stats: dict[str, Any]) -> str:
    actual = _to_int(stats.get("actual_rows"))
    first_date = stats.get("first_available_date")
    last_date = stats.get("last_available_date")
    if actual <= 0 or not first_date or not last_date:
        return "Market price history is not available in canonical tables for the requested scope."

    expected_rows = _to_int(stats.get("expected_rows"))
    missing_days = _to_int(stats.get("missing_days_count"))
    completeness = _format_pct(stats.get("completeness_pct"))
    window = f"from {first_date} to {last_date}"
    if expected_rows > 0:
        sentence = (
            f"Market price history is {completeness} complete {window}, "
            f"with {missing_days} missing business days."
        )
    else:
        sentence = f"Market price history window is {window}."
    return f"{sentence} {_completeness_assessment_text(completeness_pct=stats.get('completeness_pct'), missing_count=missing_days)}"


def _format_earnings_completeness_summary(stats: dict[str, Any]) -> str:
    actual = _to_int(stats.get("actual_rows"))
    first_date = stats.get("first_available_date")
    last_date = stats.get("last_available_date")
    if actual <= 0 or not first_date or not last_date:
        return "Earnings history is not available in canonical tables for the requested scope."

    expected_rows = _to_int(stats.get("expected_rows"))
    missing_periods = _to_int(stats.get("missing_periods_count"))
    if expected_rows > 0:
        if missing_periods == 0:
            return f"Earnings history is complete: {actual} of {expected_rows} expected periods."
        return f"Earnings history has {actual} of {expected_rows} expected periods, with {missing_periods} missing periods."
    return f"Earnings history is present from {first_date} to {last_date}, but expected-period scoring is unavailable."


def _format_fundamentals_completeness_summary(stats: dict[str, Any]) -> str:
    actual = _to_int(stats.get("actual_rows"))
    first_date = stats.get("first_available_date")
    last_date = stats.get("last_available_date")
    if actual <= 0 or not first_date or not last_date:
        return "Fundamentals are not available in canonical tables for the requested scope."
    return f"Fundamentals are present from {first_date} to {last_date}, but completeness is not currently scored."


def _completeness_assessment_text(*, completeness_pct: Any, missing_count: int) -> str:
    pct = _to_float(completeness_pct)
    if missing_count == 0:
        return "Assessment: acceptable (no missing business days in range)."
    if pct is None:
        return "Assessment: needs review (completeness percentage unavailable)."
    if pct >= 99.0 and missing_count <= 3:
        return "Assessment: generally acceptable with minor gaps."
    if pct >= 95.0:
        return "Assessment: usable but gaps should be reviewed before strict analyses."
    return "Assessment: not acceptable for strict analyses until gaps are resolved."


def _to_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_pct(value: Any) -> str:
    parsed = _to_float(value)
    if parsed is None:
        return "unknown"
    return f"{parsed:.2f}%"


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
