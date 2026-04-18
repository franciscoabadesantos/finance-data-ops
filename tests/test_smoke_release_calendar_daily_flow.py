from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_release_calendar_daily import run_dataops_release_calendar_daily


class FakeReleaseCalendarProvider:
    def build_release_calendar(
        self,
        *,
        start_date: date,
        end_date: date,
        sleep_seconds: float = 0.0,
        official_start_year: int | None = None,
        official_end_year: int | None = None,
    ) -> pd.DataFrame:
        _ = sleep_seconds
        _ = official_start_year
        _ = official_end_year
        base_period = start_date.isoformat()
        release_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=UTC) + pd.Timedelta(
            hours=13, minutes=30
        )
        common = {
            "observation_period": base_period,
            "observation_date": base_period,
            "scheduled_release_timestamp_utc": release_dt.isoformat(),
            "observed_first_available_at_utc": release_dt.isoformat(),
            "availability_status": "observed_available",
            "availability_source": "fake_release_calendar_source_v1",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_dt.isoformat(),
            "release_timezone": "America/New_York",
            "release_date_local": start_date.isoformat(),
            "source": "fake_release_calendar_source_v1",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        }
        rows = [
            {**common, "series_key": "CPI_Headline", "release_calendar_source": "bls_cpi_release_calendar_v1"},
            {**common, "series_key": "CPI_Core", "release_calendar_source": "bls_cpi_release_calendar_v1"},
            {**common, "series_key": "UNRATE", "release_calendar_source": "bls_unrate_release_calendar_v1"},
            {**common, "series_key": "U6RATE", "release_calendar_source": "bls_unrate_release_calendar_v1"},
            {**common, "series_key": "CIVPART", "release_calendar_source": "bls_unrate_release_calendar_v1"},
            {**common, "series_key": "ICSA", "release_calendar_source": "dol_icsa_release_calendar_v1"},
        ]
        frame = pd.DataFrame(rows)
        frame = frame[(pd.to_datetime(frame["observation_date"]).dt.date >= start_date)].copy()
        frame = frame[(pd.to_datetime(frame["observation_date"]).dt.date <= end_date)].copy()
        return frame


def test_smoke_release_calendar_refresh_publish_status(tmp_path) -> None:
    publisher = RecordingPublisher()
    today = datetime.now(UTC).date()
    start_date = today.isoformat()
    end_date = today.isoformat()
    summary = run_dataops_release_calendar_daily(
        start_date=start_date,
        end_date=end_date,
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeReleaseCalendarProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
        force_recompute=True,
    )

    assert table_path("economic_release_calendar", cache_root=tmp_path).exists()
    assert summary["publish_failures"] == []
    assert summary["rows"]["economic_release_calendar"] == 6

    release_upsert = next(call for call in publisher.upserts if call["table"] == "economic_release_calendar")
    assert release_upsert["on_conflict"] == "series_key,observation_period"
    assert len(release_upsert["rows"]) == 6

    status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    keys = {row["asset_key"] for row in status_upsert["rows"]}
    assert {
        "economic_release_calendar",
        "mv_latest_economic_release_calendar",
        "data_ops_publish_pipeline_release_calendar",
    }.issubset(keys)

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_release_calendar_daily")
    assert orchestration["status"] == "fresh"
    assert orchestration["scope"] == f"{start_date}:{end_date}"
