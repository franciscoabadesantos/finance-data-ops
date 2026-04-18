from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.publish.macro import publish_macro_surfaces
from finance_data_ops.validation.macro import MacroValidationError


def _catalog(now: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_key": "VIX",
                "source_provider": "yfinance",
                "source_code": "^VIX",
                "frequency": "daily",
                "required_by_default": True,
                "optional": False,
                "staleness_max_bdays": 5,
                "release_calendar_source": None,
                "description": "CBOE Volatility Index close.",
                "updated_at": now.isoformat(),
            }
        ]
    )


def _observations(now: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_key": "VIX",
                "observation_period": "2026-01-02",
                "observation_date": "2026-01-02",
                "frequency": "daily",
                "value": 15.0,
                "source_provider": "yfinance",
                "source_code": "^VIX",
                "release_timestamp_utc": None,
                "release_timezone": None,
                "release_date_local": None,
                "release_calendar_source": None,
                "source": "yfinance_api_v1",
                "fetched_at": now.isoformat(),
                "ingested_at": now.isoformat(),
            }
        ]
    )


def _daily(now: datetime) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": "2026-01-09",
                "series_key": "VIX",
                "value": 15.0,
                "source_observation_period": "2026-01-02",
                "source_observation_date": "2026-01-02",
                "available_at_utc": "2026-01-02T00:00:00+00:00",
                "staleness_bdays": 5,
                "is_stale": False,
                "alignment_mode": "release_timed",
                "ingested_at": now.isoformat(),
            }
        ]
    )


def test_publish_macro_surfaces_fails_before_upsert_when_validation_fails() -> None:
    now = datetime.now(UTC)
    publisher = RecordingPublisher()

    catalog = _catalog(now)
    observations = _observations(now)
    observations["release_timestamp_utc"] = (now + timedelta(days=1)).isoformat()
    daily = _daily(now)

    with pytest.raises(MacroValidationError):
        publish_macro_surfaces(
            publisher=publisher,
            series_catalog=catalog,
            macro_observations=observations,
            macro_daily=daily,
        )

    assert publisher.upserts == []
    assert publisher.rpcs == []

