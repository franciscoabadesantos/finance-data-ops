"""Release-calendar contract validations and publish-safety gates."""

from __future__ import annotations

import pandas as pd


class ReleaseCalendarValidationError(ValueError):
    """Raised when release-calendar contract validation fails."""


_REQUIRED_COLUMNS = {
    "series_key",
    "observation_period",
    "observation_date",
    "scheduled_release_timestamp_utc",
    "observed_first_available_at_utc",
    "availability_status",
    "availability_source",
    "delay_vs_schedule_seconds",
    "is_schedule_based_only",
    "release_timestamp_utc",
    "release_timezone",
    "release_date_local",
    "release_calendar_source",
    "source",
    "provenance_class",
    "ingested_at",
}

_VALID_AVAILABILITY_STATUS = {
    "observed_available",
    "scheduled_provisional",
    "late_missing_observation",
    "schedule_only_unsupported_history",
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

    scheduled_release_ts = pd.to_datetime(
        economic_release_calendar["scheduled_release_timestamp_utc"], utc=True, errors="coerce", format="ISO8601"
    )
    invalid_release_ts = scheduled_release_ts.isna()
    if invalid_release_ts.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.scheduled_release_timestamp_utc contains invalid/NULL values; "
            f"invalid_count={int(invalid_release_ts.sum())}."
        )

    # Backward-compatible alias required by current downstream consumers.
    release_ts = pd.to_datetime(
        economic_release_calendar["release_timestamp_utc"], utc=True, errors="coerce", format="ISO8601"
    )
    invalid_release_alias = release_ts.isna()
    if invalid_release_alias.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.release_timestamp_utc contains invalid/NULL values; "
            f"invalid_count={int(invalid_release_alias.sum())}."
        )
    alias_mismatch = (
        release_ts.notna()
        & scheduled_release_ts.notna()
        & (release_ts != scheduled_release_ts)
    )
    if alias_mismatch.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.release_timestamp_utc must equal scheduled_release_timestamp_utc "
            f"for backward compatibility; mismatch_count={int(alias_mismatch.sum())}."
        )

    observed_release_ts = pd.to_datetime(
        economic_release_calendar["observed_first_available_at_utc"], utc=True, errors="coerce", format="ISO8601"
    )
    invalid_observed_ts = economic_release_calendar["observed_first_available_at_utc"].notna() & observed_release_ts.isna()
    if invalid_observed_ts.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.observed_first_available_at_utc contains invalid values; "
            f"invalid_count={int(invalid_observed_ts.sum())}."
        )

    status_text = economic_release_calendar["availability_status"].astype(str).str.strip()
    invalid_status = status_text.str.len() == 0
    if invalid_status.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.availability_status contains {int(invalid_status.sum())} empty values."
        )
    unknown_status = ~status_text.isin(_VALID_AVAILABILITY_STATUS)
    if unknown_status.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.availability_status contains unknown values: "
            + ",".join(sorted(set(status_text[unknown_status].tolist())))
        )

    source_text = economic_release_calendar["availability_source"].astype(str).str.strip()
    missing_source = source_text.str.len() == 0
    if missing_source.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.availability_source contains {int(missing_source.sum())} empty values."
        )

    schedule_only_raw = economic_release_calendar["is_schedule_based_only"]
    schedule_only = (
        schedule_only_raw.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False})
    )
    schedule_only_missing = schedule_only.isna()
    if schedule_only_missing.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.is_schedule_based_only contains {int(schedule_only_missing.sum())} invalid values."
        )

    observed_present = observed_release_ts.notna()
    if (observed_present & schedule_only.astype(bool)).any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar rows with observed_first_available_at_utc cannot be marked schedule-based-only."
        )
    if ((~observed_present) & (~schedule_only.astype(bool))).any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar rows without observed_first_available_at_utc must be marked schedule-based-only."
        )

    observed_status_mismatch = observed_present & (status_text != "observed_available")
    if observed_status_mismatch.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar observed rows must use availability_status='observed_available'; "
            f"mismatch_count={int(observed_status_mismatch.sum())}."
        )

    delay = pd.to_numeric(economic_release_calendar["delay_vs_schedule_seconds"], errors="coerce")
    invalid_delay = economic_release_calendar["delay_vs_schedule_seconds"].notna() & delay.isna()
    if invalid_delay.any():
        raise ReleaseCalendarValidationError(
            f"economic_release_calendar.delay_vs_schedule_seconds contains {int(invalid_delay.sum())} invalid numeric values."
        )

    expected_delay = (observed_release_ts - scheduled_release_ts).dt.total_seconds()
    delay_mismatch = observed_present & (
        delay.isna() | (delay.round().astype("Int64") != expected_delay.round().astype("Int64"))
    )
    if delay_mismatch.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.delay_vs_schedule_seconds must match observed-scheduled delta for observed rows; "
            f"mismatch_count={int(delay_mismatch.sum())}."
        )
    missing_delay_schedule_only = (~observed_present) & delay.notna()
    if missing_delay_schedule_only.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar schedule-only rows must not include delay_vs_schedule_seconds."
        )

    # Protect against accidentally asserting data availability in the future.
    now_ts = pd.Timestamp.now(tz="UTC")
    future_observed = observed_release_ts.notna() & (observed_release_ts > now_ts)
    if future_observed.any():
        raise ReleaseCalendarValidationError(
            "economic_release_calendar.observed_first_available_at_utc contains forward-dated values; "
            f"invalid_count={int(future_observed.sum())}."
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
