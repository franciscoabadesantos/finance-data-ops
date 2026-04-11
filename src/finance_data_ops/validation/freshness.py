"""Freshness classification for Data Ops assets."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

import pandas as pd


class FreshnessState(StrEnum):
    FRESH = "fresh"
    STALE_WITHIN_TOLERANCE = "stale_within_tolerance"
    PARTIAL = "partial"
    FAILED_HARD = "failed_hard"
    FAILED_RETRYING = "failed_retrying"
    DELAYED_EXPECTED = "delayed_expected"
    UNKNOWN = "unknown"


def classify_freshness(
    *,
    last_observed_at: str | datetime | pd.Timestamp | None,
    now: datetime | pd.Timestamp | None = None,
    fresh_within: timedelta = timedelta(hours=26),
    tolerance: timedelta = timedelta(hours=24),
    delayed_expected: bool = False,
    partial: bool = False,
    failure_state: str | None = None,
) -> FreshnessState:
    failure_token = str(failure_state or "").strip().lower()
    if failure_token == FreshnessState.FAILED_HARD:
        return FreshnessState.FAILED_HARD
    if failure_token == FreshnessState.FAILED_RETRYING:
        return FreshnessState.FAILED_RETRYING
    if partial:
        return FreshnessState.PARTIAL

    observed = pd.to_datetime(last_observed_at, utc=True, errors="coerce")
    if pd.isna(observed):
        return FreshnessState.UNKNOWN

    current = pd.to_datetime(now or datetime.now(UTC), utc=True)
    age = current - observed
    if age <= pd.Timedelta(fresh_within):
        return FreshnessState.FRESH
    if age <= pd.Timedelta(fresh_within + tolerance):
        return FreshnessState.STALE_WITHIN_TOLERANCE
    if delayed_expected:
        return FreshnessState.DELAYED_EXPECTED
    return FreshnessState.FAILED_HARD
