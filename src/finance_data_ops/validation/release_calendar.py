"""Release-calendar contract validations and publish-safety gates."""

from __future__ import annotations

import pandas as pd


class ReleaseCalendarValidationError(ValueError):
    """Raised when release-calendar contract validation fails."""


_REQUIRED_COLUMNS = {
    "series_key",
    "observation_period",
    "observation_date",
    "release_timestamp_utc",
    "release_timezone",
    "release_date_local",
    "release_calendar_source",
    "source",
    "provenance_class",
    "ingested_at",
}


def validate_release_calendar_publish_contract(*, economic_release_calendar: pd.DataFrame) -> None:
    missing = sorted(_REQUIRED_COLUMNS.difference(economic_release_calendar.columns))
    if missing:
        raise ReleaseCalendarValidationError(f"economic_release_calendar missing required columns: {missing}")

    if economic_release_calendar.empty:
        raise ReleaseCalendarValidationError("economic_release_calendar cannot be empty before publish.")

    duplicated = economic_release_calendar.duplicated(subset=["series_key", "observation_period"], keep=False)
    if duplicated.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar contains duplicate (series_key, observation_period) rows; "
            f"duplicate_count={int(duplicated.sum())}."
        )

    timezone_text = economic_release_calendar["release_timezone"].astype(str).str.strip()
    timezone_missing = timezone_text.str.len() == 0
    if timezone_missing.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.release_timezone contains {int(timezone_missing.sum())} empty values."
        )

    release_ts = pd.to_datetime(economic_release_calendar["release_timestamp_utc"], utc=True, errors="coerce")
    invalid_release_ts = release_ts.isna()
    if invalid_release_ts.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.release_timestamp_utc contains invalid/NULL values; "
            f"invalid_count={int(invalid_release_ts.sum())}."
        )

    observation_date = pd.to_datetime(economic_release_calendar["observation_date"], errors="coerce")
    invalid_observation_date = observation_date.isna()
    if invalid_observation_date.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.observation_date contains {int(invalid_observation_date.sum())} invalid values."
        )

    release_date_local = pd.to_datetime(economic_release_calendar["release_date_local"], errors="coerce")
    invalid_release_local = release_date_local.isna()
    if invalid_release_local.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.release_date_local contains {int(invalid_release_local.sum())} invalid values."
        )

    empty_series = economic_release_calendar["series_key"].astype(str).str.strip().str.len() == 0
    if empty_series.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.series_key contains {int(empty_series.sum())} empty values."
        )

    empty_period = economic_release_calendar["observation_period"].astype(str).str.strip().str.len() == 0
    if empty_period.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.observation_period contains {int(empty_period.sum())} empty values."
        )

    ingested = pd.to_datetime(economic_release_calendar["ingested_at"], utc=True, errors="coerce")
    invalid_ingested = ingested.isna()
    if invalid_ingested.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.ingested_at contains {int(invalid_ingested.sum())} invalid values."
        )
