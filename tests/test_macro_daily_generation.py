from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.providers.macro import MacroSeriesSpec
from finance_data_ops.publish.macro import build_macro_daily_payload
from finance_data_ops.refresh.macro_daily import _build_macro_daily_frame


def _build_frame(
    *,
    observations: pd.DataFrame,
    catalog: tuple[MacroSeriesSpec, ...],
    start: str,
    end: str,
) -> pd.DataFrame:
    return _build_macro_daily_frame(
        observations=observations,
        catalog=catalog,
        start_date=pd.Timestamp(start),
        end_date=pd.Timestamp(end),
        alignment_mode="release_timed",
        ingested_at=pd.Timestamp(datetime.now(UTC)),
    )


def test_macro_daily_stale_rows_preserve_carried_value() -> None:
    observations = pd.DataFrame(
        [
            {
                "series_key": "WTI",
                "observation_period": "2026-01-02",
                "observation_date": "2026-01-02",
                "value": 70.0,
                "scheduled_release_timestamp_utc": None,
                "observed_first_available_at_utc": None,
                "release_timezone": None,
            }
        ]
    )
    catalog = (
        MacroSeriesSpec(
            key="WTI",
            source="fred",
            source_code="DCOILWTICO",
            frequency="daily",
            required_by_default=False,
            staleness_max_bdays=2,
        ),
    )

    frame = _build_frame(observations=observations, catalog=catalog, start="2026-01-02", end="2026-01-08")
    stale_row = frame.loc[frame["as_of_date"] == datetime(2026, 1, 7).date()].iloc[0]

    assert stale_row["value"] == 70.0
    assert bool(stale_row["is_stale"]) is True
    assert int(stale_row["staleness_bdays"]) == 3
    assert stale_row["source_observation_date"].isoformat() == "2026-01-02"


def test_macro_daily_monthly_release_timed_rows_carry_forward_when_stale() -> None:
    observations = pd.DataFrame(
        [
            {
                "series_key": "CPI_Headline",
                "observation_period": "2026-04",
                "observation_date": "2026-04-01",
                "value": 300.0,
                "scheduled_release_timestamp_utc": "2026-05-13T12:30:00+00:00",
                "observed_first_available_at_utc": "2026-05-13T12:30:00+00:00",
                "release_timezone": "America/New_York",
            }
        ]
    )
    catalog = (
        MacroSeriesSpec(
            key="CPI_Headline",
            source="fred",
            source_code="CPIAUCSL",
            frequency="monthly",
            required_by_default=False,
            staleness_max_bdays=10,
        ),
    )

    frame = _build_frame(observations=observations, catalog=catalog, start="2026-05-13", end="2026-05-29")
    stale_row = frame.loc[frame["as_of_date"] == datetime(2026, 5, 29).date()].iloc[0]

    assert stale_row["value"] == 300.0
    assert bool(stale_row["is_stale"]) is True
    assert int(stale_row["staleness_bdays"]) > 10
    assert stale_row["source_observation_date"].isoformat() == "2026-04-01"


def test_macro_daily_payload_omits_leading_rows_before_first_eligible_observation() -> None:
    observations = pd.DataFrame(
        [
            {
                "series_key": "ICSA",
                "observation_period": "2026-01-02",
                "observation_date": "2026-01-02",
                "value": 210000.0,
                "scheduled_release_timestamp_utc": None,
                "observed_first_available_at_utc": None,
                "release_timezone": None,
            }
        ]
    )
    catalog = (
        MacroSeriesSpec(
            key="ICSA",
            source="fred",
            source_code="ICSA",
            frequency="weekly",
            required_by_default=False,
            staleness_max_bdays=10,
        ),
    )

    frame = _build_frame(observations=observations, catalog=catalog, start="2025-12-30", end="2026-01-05")
    payload = build_macro_daily_payload(frame)

    assert payload
    assert min(row["as_of_date"].isoformat() for row in payload) == "2026-01-02"
    assert all(row["value"] is not None for row in payload)
