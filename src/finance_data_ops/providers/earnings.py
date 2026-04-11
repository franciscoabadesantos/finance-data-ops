"""Earnings provider wrappers for upcoming events and historical results."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
import re
from typing import Any

import pandas as pd


EARNINGS_EVENTS_COLUMNS = [
    "ticker",
    "earnings_date",
    "earnings_time",
    "fiscal_period",
    "estimate_eps",
    "estimate_revenue",
    "source",
    "fetched_at",
    "ingested_at",
]

EARNINGS_HISTORY_COLUMNS = [
    "ticker",
    "earnings_date",
    "fiscal_period",
    "actual_eps",
    "estimate_eps",
    "surprise_eps",
    "actual_revenue",
    "estimate_revenue",
    "surprise_revenue",
    "source",
    "fetched_at",
    "ingested_at",
]


class EarningsProviderError(RuntimeError):
    """Raised when earnings provider output cannot be normalized safely."""


class EarningsDataProvider:
    """Provider boundary for normalized earnings events and history in Data Ops v2."""

    def __init__(
        self,
        *,
        earnings_dates_fn: Callable[[str, int], pd.DataFrame] | None = None,
        calendar_fn: Callable[[str], Any] | None = None,
        provider_name: str = "yahoo_finance",
    ) -> None:
        self._earnings_dates_fn = earnings_dates_fn or self._default_earnings_dates_fn
        self._calendar_fn = calendar_fn or self._default_calendar_fn
        self.provider_name = str(provider_name).strip() or "unknown_provider"

    def fetch_earnings(
        self,
        symbols: Iterable[str],
        *,
        history_limit: int = 8,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        events_frames: list[pd.DataFrame] = []
        history_frames: list[pd.DataFrame] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            events, history = self.fetch_symbol_earnings(symbol, history_limit=history_limit)
            if not events.empty:
                events_frames.append(events)
            if not history.empty:
                history_frames.append(history)

        events_out = (
            pd.concat(events_frames, ignore_index=True)
            if events_frames
            else pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS)
        )
        history_out = (
            pd.concat(history_frames, ignore_index=True)
            if history_frames
            else pd.DataFrame(columns=EARNINGS_HISTORY_COLUMNS)
        )

        if not events_out.empty:
            events_out = events_out.sort_values(["ticker", "earnings_date", "fetched_at"])
            events_out = events_out.drop_duplicates(subset=["ticker", "earnings_date"], keep="last")
            events_out = events_out.reset_index(drop=True)

        if not history_out.empty:
            history_out = history_out.sort_values(["ticker", "earnings_date", "fiscal_period", "fetched_at"])
            history_out = history_out.drop_duplicates(
                subset=["ticker", "earnings_date", "fiscal_period"],
                keep="last",
            )
            history_out = history_out.reset_index(drop=True)

        return events_out, history_out

    def fetch_symbol_earnings(
        self,
        symbol: str,
        *,
        history_limit: int = 8,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = str(symbol).strip().upper()
        if not ticker:
            return (
                pd.DataFrame(columns=EARNINGS_EVENTS_COLUMNS),
                pd.DataFrame(columns=EARNINGS_HISTORY_COLUMNS),
            )

        ingested_at = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")

        dates_frame = _to_frame(self._earnings_dates_fn(ticker, max(int(history_limit), 1)))
        calendar_payload = self._calendar_fn(ticker)

        events_rows = self._normalize_upcoming_events(
            symbol=ticker,
            dates_frame=dates_frame,
            calendar_payload=calendar_payload,
            ingested_at=ingested_at,
        )
        history_rows = self._normalize_earnings_history(
            symbol=ticker,
            dates_frame=dates_frame,
            ingested_at=ingested_at,
        )

        events = pd.DataFrame(events_rows, columns=EARNINGS_EVENTS_COLUMNS)
        history = pd.DataFrame(history_rows, columns=EARNINGS_HISTORY_COLUMNS)
        return events, history

    def _normalize_upcoming_events(
        self,
        *,
        symbol: str,
        dates_frame: pd.DataFrame,
        calendar_payload: Any,
        ingested_at: pd.Timestamp,
    ) -> list[dict[str, Any]]:
        normalized_rows: list[dict[str, Any]] = []

        date_rows = _normalize_earnings_dates_rows(dates_frame)
        upcoming = [row for row in date_rows if row["actual_eps"] is None]
        if upcoming:
            next_row = sorted(upcoming, key=lambda row: row["earnings_date"])[0]
            normalized_rows.append(
                {
                    "ticker": symbol,
                    "earnings_date": next_row["earnings_date"],
                    "earnings_time": next_row.get("earnings_time"),
                    "fiscal_period": next_row.get("fiscal_period"),
                    "estimate_eps": next_row.get("estimate_eps"),
                    "estimate_revenue": next_row.get("estimate_revenue"),
                    "source": self.provider_name,
                    "fetched_at": ingested_at,
                    "ingested_at": ingested_at,
                }
            )

        calendar_row = _normalize_calendar_payload(calendar_payload)
        calendar_date = pd.to_datetime(calendar_row.get("earnings_date"), utc=True, errors="coerce")
        if not pd.isna(calendar_date):
            event_date = pd.Timestamp(calendar_date).date()
            has_existing = any(row.get("earnings_date") == event_date for row in normalized_rows)
            if not has_existing:
                normalized_rows.append(
                    {
                        "ticker": symbol,
                        "earnings_date": event_date,
                        "earnings_time": calendar_row.get("earnings_time"),
                        "fiscal_period": calendar_row.get("fiscal_period") or _fiscal_period(event_date),
                        "estimate_eps": _coerce_float(calendar_row.get("estimate_eps")),
                        "estimate_revenue": _coerce_float(calendar_row.get("estimate_revenue")),
                        "source": self.provider_name,
                        "fetched_at": ingested_at,
                        "ingested_at": ingested_at,
                    }
                )

        return normalized_rows

    def _normalize_earnings_history(
        self,
        *,
        symbol: str,
        dates_frame: pd.DataFrame,
        ingested_at: pd.Timestamp,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for record in _normalize_earnings_dates_rows(dates_frame):
            actual_eps = record.get("actual_eps")
            if actual_eps is None:
                continue
            estimate_eps = record.get("estimate_eps")
            actual_revenue = record.get("actual_revenue")
            estimate_revenue = record.get("estimate_revenue")
            rows.append(
                {
                    "ticker": symbol,
                    "earnings_date": record["earnings_date"],
                    "fiscal_period": record.get("fiscal_period") or _fiscal_period(record["earnings_date"]),
                    "actual_eps": actual_eps,
                    "estimate_eps": estimate_eps,
                    "surprise_eps": (
                        float(actual_eps - estimate_eps)
                        if actual_eps is not None and estimate_eps is not None
                        else record.get("surprise_eps")
                    ),
                    "actual_revenue": actual_revenue,
                    "estimate_revenue": estimate_revenue,
                    "surprise_revenue": (
                        float(actual_revenue - estimate_revenue)
                        if actual_revenue is not None and estimate_revenue is not None
                        else record.get("surprise_revenue")
                    ),
                    "source": self.provider_name,
                    "fetched_at": ingested_at,
                    "ingested_at": ingested_at,
                }
            )
        return rows

    def _default_earnings_dates_fn(self, symbol: str, limit: int) -> pd.DataFrame:
        ticker = _load_ticker(symbol)
        get_dates = getattr(ticker, "get_earnings_dates", None)
        if callable(get_dates):
            return _to_frame(get_dates(limit=max(int(limit), 1)))
        fallback = getattr(ticker, "earnings_dates", None)
        if callable(fallback):
            return _to_frame(fallback(limit=max(int(limit), 1)))
        if isinstance(fallback, pd.DataFrame):
            return fallback.copy()
        return pd.DataFrame()

    def _default_calendar_fn(self, symbol: str) -> Any:
        ticker = _load_ticker(symbol)
        payload = getattr(ticker, "calendar", None)
        return payload if payload is not None else {}


def _load_ticker(symbol: str) -> Any:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - runtime environment dependency
        raise RuntimeError("yfinance is required for live provider calls.") from exc
    return yf.Ticker(symbol)


def _normalize_earnings_dates_rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []

    local = frame.copy()
    if local.index.name is not None:
        local = local.reset_index()
    elif isinstance(local.index, pd.DatetimeIndex):
        local = local.reset_index().rename(columns={"index": "earnings_date"})

    normalized_columns = {_normalize_token(col): col for col in local.columns}

    date_column = _pick_first(normalized_columns, ["earningsdate", "date", "reportdate", "datetime"])
    if date_column is None:
        if isinstance(frame.index, pd.DatetimeIndex):
            local = local.copy()
            local["__index_date"] = frame.index
            date_column = "__index_date"
        else:
            raise EarningsProviderError("Unable to locate earnings date column in provider frame.")

    actual_eps_col = _pick_first(normalized_columns, ["reportedeps", "actualeps", "epsactual", "epsreported"])
    estimate_eps_col = _pick_first(normalized_columns, ["epsestimate", "estimateeps", "estimatedeps"])
    surprise_eps_col = _pick_first(
        normalized_columns,
        ["surpriseeps", "surprise", "surprisepercent", "surprisepct"],
    )
    actual_revenue_col = _pick_first(
        normalized_columns,
        ["reportedrevenue", "actualrevenue", "revenueactual"],
    )
    estimate_revenue_col = _pick_first(
        normalized_columns,
        ["revenueestimate", "estimaterevenue", "estimatedrevenue"],
    )
    surprise_revenue_col = _pick_first(
        normalized_columns,
        ["surpriserevenue"],
    )
    fiscal_period_col = _pick_first(
        normalized_columns,
        ["fiscalperiod", "fiscalquarter", "quarter", "period"],
    )
    earnings_time_col = _pick_first(
        normalized_columns,
        ["earningstime", "time"],
    )

    rows: list[dict[str, Any]] = []
    for _, record in local.iterrows():
        earnings_date = pd.to_datetime(record.get(date_column), utc=True, errors="coerce")
        if pd.isna(earnings_date):
            continue
        earnings_date_value = pd.Timestamp(earnings_date).date()

        actual_eps = _coerce_float(record.get(actual_eps_col)) if actual_eps_col else None
        estimate_eps = _coerce_float(record.get(estimate_eps_col)) if estimate_eps_col else None
        actual_revenue = _coerce_float(record.get(actual_revenue_col)) if actual_revenue_col else None
        estimate_revenue = _coerce_float(record.get(estimate_revenue_col)) if estimate_revenue_col else None

        fiscal_period_raw = str(record.get(fiscal_period_col) or "").strip() if fiscal_period_col else ""
        fiscal_period = fiscal_period_raw or _fiscal_period(earnings_date_value)

        earnings_time_raw = str(record.get(earnings_time_col) or "").strip() if earnings_time_col else ""
        if not earnings_time_raw:
            ts = pd.Timestamp(earnings_date)
            earnings_time_raw = ts.time().isoformat() if ts.time().isoformat() != "00:00:00" else ""

        rows.append(
            {
                "earnings_date": earnings_date_value,
                "earnings_time": earnings_time_raw or None,
                "fiscal_period": fiscal_period,
                "actual_eps": actual_eps,
                "estimate_eps": estimate_eps,
                "surprise_eps": _coerce_float(record.get(surprise_eps_col)) if surprise_eps_col else None,
                "actual_revenue": actual_revenue,
                "estimate_revenue": estimate_revenue,
                "surprise_revenue": (
                    _coerce_float(record.get(surprise_revenue_col)) if surprise_revenue_col else None
                ),
            }
        )
    return rows


def _normalize_calendar_payload(payload: Any) -> dict[str, Any]:
    if payload is None:
        return {}

    if isinstance(payload, dict):
        mapping = payload
    else:
        frame = _to_frame(payload)
        if frame.empty:
            return {}
        if frame.shape[1] == 1:
            mapping = {str(index): frame.iloc[idx, 0] for idx, index in enumerate(frame.index)}
        else:
            mapping = frame.iloc[0].to_dict()

    lookup = {_normalize_token(key): value for key, value in mapping.items()}
    earnings_date_value = _parse_calendar_date(
        lookup.get("earningsdate")
        or lookup.get("nextearningsdate")
        or lookup.get("nextearnings")
    )

    estimate_eps = (
        lookup.get("epsestimate")
        or lookup.get("earningsaverage")
        or lookup.get("estimateeps")
    )
    estimate_revenue = (
        lookup.get("revenueestimate")
        or lookup.get("revenueaverage")
        or lookup.get("estimaterevenue")
    )
    fiscal_period = lookup.get("fiscalperiod") or lookup.get("fiscalquarter") or lookup.get("period")
    earnings_time = lookup.get("earningstime") or lookup.get("time")

    return {
        "earnings_date": earnings_date_value,
        "estimate_eps": estimate_eps,
        "estimate_revenue": estimate_revenue,
        "fiscal_period": str(fiscal_period).strip() if fiscal_period is not None else None,
        "earnings_time": str(earnings_time).strip() if earnings_time is not None else None,
    }


def _parse_calendar_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        for candidate in value:
            parsed = pd.to_datetime(candidate, utc=True, errors="coerce")
            if not pd.isna(parsed):
                return pd.Timestamp(parsed).date()
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def _pick_first(mapping: dict[str, Any], candidates: list[str]) -> Any:
    for candidate in candidates:
        token = _normalize_token(candidate)
        if token in mapping:
            return mapping[token]
    return None


def _normalize_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _to_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if value is None:
        return pd.DataFrame()
    return pd.DataFrame(value)


def _coerce_float(value: Any) -> float | None:
    casted = pd.to_numeric(value, errors="coerce")
    if pd.isna(casted):
        return None
    return float(casted)


def _fiscal_period(earnings_date: date) -> str:
    quarter = ((int(earnings_date.month) - 1) // 3) + 1
    return f"{earnings_date.year}Q{quarter}"
