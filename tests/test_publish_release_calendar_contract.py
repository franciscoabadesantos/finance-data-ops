from __future__ import annotations

import pandas as pd
import pytest

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.publish.release_calendar import publish_release_calendar_surfaces
from finance_data_ops.validation.release_calendar import ReleaseCalendarValidationError


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_key": "UNRATE",
                "observation_period": "2025-12",
                "observation_date": "2025-12-01",
                "scheduled_release_timestamp_utc": "2026-01-09T13:30:00+00:00",
                "observed_first_available_at_utc": "2026-01-09T13:31:00+00:00",
                "availability_status": "observed_available",
                "availability_source": "alfred_graph_revision_dates_v1",
                "delay_vs_schedule_seconds": 60,
                "is_schedule_based_only": False,
                "release_timestamp_utc": "2026-01-09T13:30:00+00:00",
                "release_timezone": "America/New_York",
                "release_date_local": "2026-01-09",
                "release_calendar_source": "bls_unrate_release_calendar_v1",
                "source": "alfred_graph_revision_dates_v1",
                "provenance_class": "ALFRED-native",
                "ingested_at": "2026-01-10T00:00:00+00:00",
            }
        ]
    )


def test_publish_release_calendar_fails_before_upsert() -> None:
    publisher = RecordingPublisher()
    frame = _valid_frame()
    frame.loc[0, "release_timezone"] = ""

    with pytest.raises(ReleaseCalendarValidationError):
        publish_release_calendar_surfaces(
            publisher=publisher,
            economic_release_calendar=frame,
        )

    assert publisher.upserts == []
    assert publisher.rpcs == []
