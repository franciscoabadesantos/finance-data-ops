"""Earnings-derived frontend summary helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd


NEXT_EARNINGS_COLUMNS = [
    "ticker",
    "earnings_date",
    "earnings_time",
    "fiscal_period",
    "estimate_eps",
    "estimate_revenue",
    "source",
    "fetched_at",
    "updated_at",
]


def compute_next_earnings(
    earnings_events_frame: pd.DataFrame,
    *,
    as_of_date: str | date | None = None,
) -> pd.DataFrame:
    if earnings_events_frame.empty:
        return pd.DataFrame(columns=NEXT_EARNINGS_COLUMNS)

    as_of = pd.Timestamp(as_of_date).date() if as_of_date is not None else datetime.now(UTC).date()

    frame = earnings_events_frame.copy()
    ticker = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    frame["ticker"] = ticker.astype(str).str.upper()
    frame["earnings_date"] = pd.to_datetime(frame.get("earnings_date"), errors="coerce").dt.date
    frame["estimate_eps"] = pd.to_numeric(frame.get("estimate_eps"), errors="coerce")
    frame["estimate_revenue"] = pd.to_numeric(frame.get("estimate_revenue"), errors="coerce")
    frame["fetched_at"] = pd.to_datetime(
        frame.get("fetched_at", frame.get("ingested_at")),
        utc=True,
        errors="coerce",
    )
    frame["source"] = frame.get("source", frame.get("provider", "unknown")).astype(str)

    frame = frame.dropna(subset=["ticker", "earnings_date"])
    if frame.empty:
        return pd.DataFrame(columns=NEXT_EARNINGS_COLUMNS)

    frame = frame.loc[frame["earnings_date"] >= as_of]
    if frame.empty:
        return pd.DataFrame(columns=NEXT_EARNINGS_COLUMNS)

    frame = frame.sort_values(["ticker", "earnings_date", "fetched_at"])
    selected = frame.groupby("ticker", as_index=False).first()
    selected["updated_at"] = datetime.now(UTC).isoformat()

    return selected[
        [
            "ticker",
            "earnings_date",
            "earnings_time",
            "fiscal_period",
            "estimate_eps",
            "estimate_revenue",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].reset_index(drop=True)
