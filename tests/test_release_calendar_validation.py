from __future__ import annotations

import pandas as pd
import pytest

from finance_data_ops.validation.release_calendar import (
    ReleaseCalendarValidationError,
    validate_release_calendar_publish_contract,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_key": "UNRATE",
                "observation_period": "2025-12",
                "observation_date": "2025-12-01",
                "scheduled_release_timestamp_utc": "2026-01-09T13:30:00+00:00",
                "observed_first_available_at_utc": "2026-01-09T13:31:30+00:00",
                "availability_status": "observed_available",
                "availability_source": "alfred_graph_revision_dates_v1",
                "delay_vs_schedule_seconds": 90,
                "is_schedule_based_only": False,
                "release_timestamp_utc": "2026-01-09T13:30:00+00:00",
                "release_timezone": "America/New_York",
                "release_date_local": "2026-01-09",
                "release_calendar_source": "bls_unrate_release_calendar_v1",
                "source": "alfred_graph_revision_dates_v1",
                "provenance_class": "ALFRED-native",
                "ingested_at": "2026-01-10T00:00:00+00:00",
            },
            {
                "series_key": "ICSA",
                "observation_period": "2026-01-03",
                "observation_date": "2026-01-03",
                "scheduled_release_timestamp_utc": "2026-01-08T13:30:00+00:00",
                "observed_first_available_at_utc": None,
                "availability_status": "scheduled_provisional",
                "availability_source": "dol_icsa_release_calendar_v1_scheduled_calendar_v1",
                "delay_vs_schedule_seconds": None,
                "is_schedule_based_only": True,
                "release_timestamp_utc": "2026-01-08T13:30:00+00:00",
                "release_timezone": "America/New_York",
                "release_date_local": "2026-01-08",
                "release_calendar_source": "dol_icsa_release_calendar_v1",
                "source": "fred_release_calendar_rid180_v1",
                "provenance_class": "official release calendar",
                "ingested_at": "2026-01-10T00:00:00+00:00",
            },
        ]
    )


def test_validate_release_calendar_rejects_duplicate_series_period() -> None:
    frame = _frame()
    duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    with pytest.raises(ReleaseCalendarValidationError, match="duplicate"):
        validate_release_calendar_publish_contract(economic_release_calendar=duplicated)


def test_validate_release_calendar_rejects_invalid_timestamp() -> None:
    frame = _frame()
    frame.loc[0, "scheduled_release_timestamp_utc"] = "not-a-timestamp"
    with pytest.raises(ReleaseCalendarValidationError, match="scheduled_release_timestamp_utc"):
        validate_release_calendar_publish_contract(economic_release_calendar=frame)
