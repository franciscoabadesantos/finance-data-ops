from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from finance_data_ops.validation.macro import MacroValidationError, validate_macro_publish_contract


def _catalog() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "series_key": "VIX",
                "source_provider": "yfinance",
                "source_code": "^VIX",
                "frequency": "daily",
                "required_by_default": True,
                "staleness_max_bdays": 5,
            },
            {
                "series_key": "UNRATE",
                "source_provider": "fred",
                "source_code": "UNRATE",
                "frequency": "monthly",
                "required_by_default": True,
                "staleness_max_bdays": 45,
            },
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
            },
            {
                "series_key": "UNRATE",
                "observation_period": "2025-12",
                "observation_date": "2025-12-01",
                "frequency": "monthly",
                "value": 4.0,
                "source_provider": "fred",
                "source_code": "UNRATE",
                "release_timestamp_utc": "2026-01-09T13:30:00+00:00",
                "release_timezone": "America/New_York",
                "release_date_local": "2026-01-09",
                "release_calendar_source": "bls_unrate_release_calendar_v1",
                "source": "fred_api_v1",
                "fetched_at": now.isoformat(),
                "ingested_at": now.isoformat(),
            },
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
            },
            {
                "as_of_date": "2026-01-09",
                "series_key": "UNRATE",
                "value": 4.0,
                "source_observation_period": "2025-12",
                "source_observation_date": "2025-12-01",
                "available_at_utc": "2026-01-09T13:30:00+00:00",
                "staleness_bdays": 29,
                "is_stale": False,
                "alignment_mode": "release_timed",
                "ingested_at": now.isoformat(),
            },
        ]
    )


def test_validate_macro_publish_contract_rejects_forward_release_timestamp() -> None:
    now = datetime.now(UTC)
    catalog = _catalog()
    observations = _observations(now)
    observations.loc[observations["series_key"] == "UNRATE", "release_timestamp_utc"] = (
        now + timedelta(days=1)
    ).isoformat()
    daily = _daily(now)

    with pytest.raises(MacroValidationError, match="forward-dated release_timestamp_utc"):
        validate_macro_publish_contract(
            series_catalog=catalog,
            macro_observations=observations,
            macro_daily=daily,
            now_utc=now,
        )


def test_validate_macro_publish_contract_rejects_null_required_latest_value() -> None:
    now = datetime.now(UTC)
    catalog = _catalog()
    observations = _observations(now)
    daily = _daily(now)
    daily.loc[daily["series_key"] == "UNRATE", "value"] = None

    with pytest.raises(MacroValidationError, match="NULL required series values"):
        validate_macro_publish_contract(
            series_catalog=catalog,
            macro_observations=observations,
            macro_daily=daily,
            now_utc=now,
        )

