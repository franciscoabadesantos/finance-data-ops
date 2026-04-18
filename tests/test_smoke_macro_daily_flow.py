from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path, write_parquet_table
from flows.dataops_macro_daily import run_dataops_macro_daily


class FakeMacroProvider:
    def fetch_series(self, spec, *, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        start_ts = pd.Timestamp(start).normalize()
        end_ts = pd.Timestamp(end).normalize()
        frequency = str(spec.frequency).strip().lower()

        if frequency == "monthly":
            index = pd.date_range(start=start_ts, end=end_ts, freq="MS")
        elif frequency == "weekly":
            index = pd.date_range(start=start_ts, end=end_ts, freq="W-SAT")
        else:
            index = pd.bdate_range(start=start_ts, end=end_ts)

        if len(index) == 0:
            index = pd.DatetimeIndex([start_ts])

        values = pd.Series(
            [float(i + 1) for i in range(len(index))],
            index=index,
            dtype="float64",
        )
        return values


def _seed_release_calendar(cache_root: str, *, run_date: date) -> None:
    month_obs = run_date.replace(day=1)
    month_release_date = run_date - timedelta(days=1)
    release_ts = datetime.combine(month_release_date, datetime.min.time(), tzinfo=UTC) + pd.Timedelta(hours=12, minutes=30)
    release_ts_iso = release_ts.isoformat()
    icsa_obs = run_date - timedelta(days=(run_date.weekday() - 5) % 7)
    icsa_release = icsa_obs + timedelta(days=(3 - icsa_obs.weekday()) % 7)
    rows = [
        {
            "series_key": "CPI_Headline",
            "observation_period": str(pd.Timestamp(month_obs).to_period("M")),
            "observation_date": month_obs.isoformat(),
            "scheduled_release_timestamp_utc": release_ts_iso,
            "observed_first_available_at_utc": release_ts_iso,
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_ts_iso,
            "release_timezone": "America/New_York",
            "release_date_local": month_release_date.isoformat(),
            "release_calendar_source": "bls_cpi_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        {
            "series_key": "CPI_Core",
            "observation_period": str(pd.Timestamp(month_obs).to_period("M")),
            "observation_date": month_obs.isoformat(),
            "scheduled_release_timestamp_utc": release_ts_iso,
            "observed_first_available_at_utc": release_ts_iso,
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_ts_iso,
            "release_timezone": "America/New_York",
            "release_date_local": month_release_date.isoformat(),
            "release_calendar_source": "bls_cpi_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        {
            "series_key": "UNRATE",
            "observation_period": str(pd.Timestamp(month_obs).to_period("M")),
            "observation_date": month_obs.isoformat(),
            "scheduled_release_timestamp_utc": release_ts_iso,
            "observed_first_available_at_utc": release_ts_iso,
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_ts_iso,
            "release_timezone": "America/New_York",
            "release_date_local": month_release_date.isoformat(),
            "release_calendar_source": "bls_unrate_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        {
            "series_key": "U6RATE",
            "observation_period": str(pd.Timestamp(month_obs).to_period("M")),
            "observation_date": month_obs.isoformat(),
            "scheduled_release_timestamp_utc": release_ts_iso,
            "observed_first_available_at_utc": release_ts_iso,
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_ts_iso,
            "release_timezone": "America/New_York",
            "release_date_local": month_release_date.isoformat(),
            "release_calendar_source": "bls_unrate_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        {
            "series_key": "CIVPART",
            "observation_period": str(pd.Timestamp(month_obs).to_period("M")),
            "observation_date": month_obs.isoformat(),
            "scheduled_release_timestamp_utc": release_ts_iso,
            "observed_first_available_at_utc": release_ts_iso,
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": release_ts_iso,
            "release_timezone": "America/New_York",
            "release_date_local": month_release_date.isoformat(),
            "release_calendar_source": "bls_unrate_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        {
            "series_key": "ICSA",
            "observation_period": icsa_obs.isoformat(),
            "observation_date": icsa_obs.isoformat(),
            "scheduled_release_timestamp_utc": (datetime.combine(icsa_release, datetime.min.time(), tzinfo=UTC) + pd.Timedelta(hours=12, minutes=30)).isoformat(),
            "observed_first_available_at_utc": (datetime.combine(icsa_release, datetime.min.time(), tzinfo=UTC) + pd.Timedelta(hours=12, minutes=30)).isoformat(),
            "availability_status": "observed_available",
            "availability_source": "test_release_calendar",
            "delay_vs_schedule_seconds": 0,
            "is_schedule_based_only": False,
            "release_timestamp_utc": (datetime.combine(icsa_release, datetime.min.time(), tzinfo=UTC) + pd.Timedelta(hours=12, minutes=30)).isoformat(),
            "release_timezone": "America/New_York",
            "release_date_local": icsa_release.isoformat(),
            "release_calendar_source": "dol_icsa_release_calendar_v1",
            "source": "test_release_calendar",
            "provenance_class": "official",
            "ingested_at": datetime.now(UTC).isoformat(),
        },
    ]
    write_parquet_table(
        "economic_release_calendar",
        pd.DataFrame(rows),
        cache_root=cache_root,
        mode="replace",
        dedupe_subset=["series_key", "observation_period"],
    )


def test_smoke_macro_daily_refresh_publish_status(tmp_path) -> None:
    publisher = RecordingPublisher()
    run_date = (pd.Timestamp(datetime.now(UTC).date()) - pd.offsets.BDay(1)).date()
    _seed_release_calendar(str(tmp_path), run_date=run_date)
    run_date_iso = run_date.isoformat()
    summary = run_dataops_macro_daily(
        start=run_date_iso,
        end=run_date_iso,
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMacroProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert table_path("macro_series_catalog", cache_root=tmp_path).exists()
    assert table_path("macro_observations", cache_root=tmp_path).exists()
    assert table_path("macro_daily", cache_root=tmp_path).exists()

    assert summary["publish_failures"] == []
    assert summary["rows"]["macro_series_catalog"] > 0
    assert summary["rows"]["macro_observations"] > 0
    assert summary["rows"]["macro_daily"] > 0

    macro_upsert = next(call for call in publisher.upserts if call["table"] == "macro_daily")
    assert macro_upsert["on_conflict"] == "as_of_date,series_key"
    assert macro_upsert["rows"]

    status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    keys = {row["asset_key"] for row in status_upsert["rows"]}
    assert {"macro_observations", "macro_daily", "data_ops_publish_pipeline_macro"}.issubset(keys)

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_macro_daily")
    assert orchestration["status"] == "fresh"
    assert orchestration["scope"] == f"{run_date_iso}:{run_date_iso}"


def test_smoke_macro_daily_force_recompute_idempotent_write(tmp_path) -> None:
    publisher = RecordingPublisher()
    run_date = (pd.Timestamp(datetime.now(UTC).date()) - pd.offsets.BDay(1)).date()
    _seed_release_calendar(str(tmp_path), run_date=run_date)
    run_date_iso = run_date.isoformat()

    run_dataops_macro_daily(
        start=run_date_iso,
        end=run_date_iso,
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMacroProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
        force_recompute=True,
    )
    first = pd.read_parquet(table_path("macro_daily", cache_root=tmp_path))

    run_dataops_macro_daily(
        start=run_date_iso,
        end=run_date_iso,
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeMacroProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
        force_recompute=True,
    )
    second = pd.read_parquet(table_path("macro_daily", cache_root=tmp_path))

    first_sorted = first.sort_values(["as_of_date", "series_key"]).reset_index(drop=True)
    second_sorted = second.sort_values(["as_of_date", "series_key"]).reset_index(drop=True)
    assert not first_sorted.duplicated(subset=["as_of_date", "series_key"]).any()
    assert not second_sorted.duplicated(subset=["as_of_date", "series_key"]).any()
    pd.testing.assert_frame_equal(
        first_sorted.drop(columns=["ingested_at"], errors="ignore"),
        second_sorted.drop(columns=["ingested_at"], errors="ignore"),
        check_dtype=False,
    )
