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


class CanonicalWatermarkLookupError(RuntimeError):
    """A configured canonical Postgres watermark source could not be read."""


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
    symbols_expected_count: int = 0
    stale_symbols_count: int = 0
    missing_symbol_dates_count: int = 0

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
    symbols: list[str] | tuple[str, ...] | None = None,
    symbol_column: str = "symbol",
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

    expected_symbols = _normalize_symbols(symbols)
    if expected_symbols:
        dates_by_symbol, source = _load_canonical_dates_by_symbol(
            table_name=table_name,
            date_column=date_column,
            symbol_column=symbol_column,
            symbols=expected_symbols,
            start_date=baseline_start,
            end_date=target_end,
            cache_root=cache_root,
            database_dsn=database_dsn,
        )
        available_dates = set().union(*dates_by_symbol.values()) if dates_by_symbol else set()
        latest_by_symbol = {symbol: (max(values) if values else None) for symbol, values in dates_by_symbol.items()}
        complete_latest_values = [value for value in latest_by_symbol.values() if value is not None]
        latest_complete = min(complete_latest_values) if len(complete_latest_values) == len(expected_symbols) else None
    else:
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
            symbols_expected_count=len(expected_symbols),
        )

    expected_dates = _expected_dates(start_date=baseline_start, end_date=target_end, cadence=cadence)
    if expected_symbols:
        missing_by_symbol = {
            symbol: sorted(expected_dates.difference(dates_by_symbol.get(symbol, set())))
            for symbol in expected_symbols
        }
        missing_dates = sorted({value for values in missing_by_symbol.values() for value in values})
        stale_symbols_count = sum(1 for values in missing_by_symbol.values() if values)
        missing_symbol_dates_count = sum(len(values) for values in missing_by_symbol.values())
    else:
        missing_dates = sorted(expected_dates.difference(available_dates))
        stale_symbols_count = 0
        missing_symbol_dates_count = 0
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
        symbols_expected_count=len(expected_symbols),
        stale_symbols_count=int(stale_symbols_count),
        missing_symbol_dates_count=int(missing_symbol_dates_count),
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


def _load_canonical_dates_by_symbol(
    *,
    table_name: str,
    date_column: str,
    symbol_column: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
    cache_root: str | Path,
    database_dsn: str,
) -> tuple[dict[str, set[date]], str]:
    remote_dates = _fetch_dates_by_symbol_from_postgres(
        table_name=table_name,
        date_column=date_column,
        symbol_column=symbol_column,
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        database_dsn=database_dsn,
    )
    if remote_dates is not None:
        return remote_dates, "postgres"
    return _fetch_dates_by_symbol_from_parquet(
        table_name=table_name,
        date_column=date_column,
        symbol_column=symbol_column,
        symbols=symbols,
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
    except ImportError as exc:
        raise CanonicalWatermarkLookupError(
            "Postgres is configured as the canonical watermark source, but psycopg is unavailable."
        ) from exc
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
    except CanonicalWatermarkLookupError:
        raise
    except Exception as exc:
        raise CanonicalWatermarkLookupError(
            "Failed to read canonical Postgres watermark "
            f'for {table_name}.{date_column}; parquet fallback is disabled when database_dsn is configured.'
        ) from exc


def _fetch_dates_by_symbol_from_postgres(
    *,
    table_name: str,
    date_column: str,
    symbol_column: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
    database_dsn: str,
) -> dict[str, set[date]] | None:
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
        _validate_identifier(symbol_column)
        query = (
            f'select "{symbol_column}", "{date_column}" from "{schema_name}"."{relation_name}" '
            f'where "{symbol_column}" = any(%s) and "{date_column}" is not null '
            f'and "{date_column}" >= %s and "{date_column}" <= %s'
        )
        out: dict[str, set[date]] = {symbol: set() for symbol in symbols}
        with psycopg.connect(dsn, connect_timeout=30) as conn:
            with conn.cursor() as cur:
                cur.execute(query, (symbols, start_date, end_date))
                for symbol_value, date_value in cur.fetchall():
                    symbol = str(symbol_value or "").strip().upper()
                    parsed = _to_date(date_value)
                    if symbol in out and parsed is not None and start_date <= parsed <= end_date:
                        out[symbol].add(parsed)
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


def _fetch_dates_by_symbol_from_parquet(
    *,
    table_name: str,
    date_column: str,
    symbol_column: str,
    symbols: list[str],
    start_date: date,
    end_date: date,
    cache_root: str | Path,
) -> dict[str, set[date]]:
    frame = read_parquet_table(table_name, cache_root=cache_root, required=False)
    out: dict[str, set[date]] = {symbol: set() for symbol in symbols}
    if frame.empty or date_column not in frame.columns or symbol_column not in frame.columns:
        return out
    safe = frame[[symbol_column, date_column]].copy()
    safe[symbol_column] = safe[symbol_column].astype(str).str.strip().str.upper()
    safe[date_column] = pd.to_datetime(safe[date_column], errors="coerce", utc=True)
    mask = safe[symbol_column].isin(symbols) & safe[date_column].notna()
    for row in safe.loc[mask].itertuples(index=False):
        symbol = str(getattr(row, symbol_column)).strip().upper()
        parsed = pd.Timestamp(getattr(row, date_column)).date()
        if symbol in out and start_date <= parsed <= end_date:
            out[symbol].add(parsed)
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


def _normalize_symbols(raw: list[str] | tuple[str, ...] | None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in raw or []:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _parse_date(raw: str) -> date:
    return pd.Timestamp(raw).date()


def _parse_date_or_today(raw: str | None) -> date:
    if raw:
        return _parse_date(raw)
    return datetime.now(UTC).date()
