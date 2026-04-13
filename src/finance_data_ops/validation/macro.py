"""Macro contract validations and publish-safety gates."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Iterable

import pandas as pd


class MacroValidationError(ValueError):
    """Raised when macro contract validation fails."""


_REQUIRED_CATALOG_COLUMNS = {
    "series_key",
    "source_provider",
    "source_code",
    "frequency",
    "required_by_default",
    "staleness_max_bdays",
}

_REQUIRED_OBSERVATION_COLUMNS = {
    "series_key",
    "observation_period",
    "observation_date",
    "frequency",
    "value",
    "source_provider",
    "source_code",
    "fetched_at",
    "ingested_at",
}

_REQUIRED_DAILY_COLUMNS = {
    "as_of_date",
    "series_key",
    "value",
    "source_observation_period",
    "source_observation_date",
    "available_at_utc",
    "staleness_bdays",
    "is_stale",
    "alignment_mode",
    "ingested_at",
}


def validate_macro_publish_contract(
    *,
    series_catalog: pd.DataFrame,
    macro_observations: pd.DataFrame,
    macro_daily: pd.DataFrame,
    required_series_keys: Iterable[str] | None = None,
    now_utc: datetime | None = None,
) -> None:
    now = now_utc or datetime.now(UTC)
    now_ts = pd.Timestamp(now)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize("UTC")
    else:
        now_ts = now_ts.tz_convert("UTC")
    required_set = {str(v).strip() for v in (required_series_keys or []) if str(v).strip()}
    if not required_set and "required_by_default" in series_catalog.columns and "series_key" in series_catalog.columns:
        required_set = {
            str(v).strip()
            for v in series_catalog.loc[
                series_catalog["required_by_default"].astype(bool),
                "series_key",
            ].astype(str)
            if str(v).strip()
        }

    _assert_columns("macro_series_catalog", series_catalog, _REQUIRED_CATALOG_COLUMNS)
    _assert_columns("macro_observations", macro_observations, _REQUIRED_OBSERVATION_COLUMNS)
    _assert_columns("macro_daily", macro_daily, _REQUIRED_DAILY_COLUMNS)

    if macro_observations.empty:
        raise MacroValidationError("macro_observations cannot be empty before publish.")
    if macro_daily.empty:
        raise MacroValidationError("macro_daily cannot be empty before publish.")

    _assert_no_duplicates(
        "macro_observations",
        macro_observations,
        ["series_key", "observation_period"],
    )
    _assert_no_duplicates("macro_daily", macro_daily, ["as_of_date", "series_key"])

    _assert_valid_timestamp_column(macro_observations, "fetched_at", table_name="macro_observations", allow_null=False)
    _assert_valid_timestamp_column(macro_observations, "ingested_at", table_name="macro_observations", allow_null=False)
    # Leading coverage windows for some series can be intentionally unavailable (NULL available_at_utc).
    # Non-NULL timestamps must still parse correctly.
    _assert_valid_timestamp_column(macro_daily, "available_at_utc", table_name="macro_daily", allow_null=True)
    _assert_valid_timestamp_column(macro_daily, "ingested_at", table_name="macro_daily", allow_null=False)

    _assert_valid_date_column(macro_observations, "observation_date", table_name="macro_observations")
    _assert_valid_date_column(macro_daily, "as_of_date", table_name="macro_daily")
    # Leading coverage windows for some series can be intentionally unavailable.
    _assert_valid_date_column(macro_daily, "source_observation_date", table_name="macro_daily", allow_null=True)

    if "release_timestamp_utc" in macro_observations.columns:
        _assert_valid_timestamp_column(
            macro_observations,
            "release_timestamp_utc",
            table_name="macro_observations",
            allow_null=True,
        )
        parsed_release = pd.to_datetime(macro_observations["release_timestamp_utc"], utc=True, errors="coerce")
        forward_mask = parsed_release.notna() & (parsed_release > now_ts)
        if forward_mask.any():
            count = int(forward_mask.sum())
            raise MacroValidationError(f"macro_observations contains {count} forward-dated release_timestamp_utc rows.")

    obs_values = pd.to_numeric(macro_observations.get("value"), errors="coerce")
    obs_null = obs_values.isna()
    if obs_null.any():
        raise MacroValidationError(
            f"macro_observations contains {int(obs_null.sum())} NULL/non-numeric values in required value column."
        )

    if required_set:
        latest_as_of = pd.to_datetime(macro_daily["as_of_date"], errors="coerce").max()
        if pd.isna(latest_as_of):
            raise MacroValidationError("macro_daily has invalid as_of_date values.")
        latest_rows = macro_daily[pd.to_datetime(macro_daily["as_of_date"], errors="coerce") == latest_as_of].copy()
        latest_rows["series_key"] = latest_rows["series_key"].astype(str)

        missing_required = sorted(required_set.difference(set(latest_rows["series_key"].tolist())))
        if missing_required:
            raise MacroValidationError(
                "macro_daily latest as_of_date is missing required series: "
                + ",".join(missing_required)
            )

        req_rows = latest_rows[latest_rows["series_key"].isin(required_set)].copy()
        req_values = pd.to_numeric(req_rows.get("value"), errors="coerce")
        null_required = req_rows.loc[req_values.isna(), "series_key"].astype(str).tolist()
        if null_required:
            raise MacroValidationError(
                "macro_daily latest as_of_date has NULL required series values: "
                + ",".join(sorted(set(null_required)))
            )


def _assert_columns(table_name: str, frame: pd.DataFrame, required_columns: set[str]) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise MacroValidationError(f"{table_name} missing required columns: {missing}")


def _assert_no_duplicates(table_name: str, frame: pd.DataFrame, keys: list[str]) -> None:
    duplicated = frame.duplicated(subset=keys, keep=False)
    if duplicated.any():
        raise MacroValidationError(
            f"{table_name} contains duplicate key rows for {keys}; duplicate_count={int(duplicated.sum())}."
        )


def _assert_valid_timestamp_column(
    frame: pd.DataFrame,
    column: str,
    *,
    table_name: str,
    allow_null: bool,
) -> None:
    values = frame[column]
    parsed = pd.to_datetime(values, utc=True, errors="coerce")
    invalid_mask = values.notna() & parsed.isna()
    if invalid_mask.any():
        raise MacroValidationError(
            f"{table_name}.{column} contains {int(invalid_mask.sum())} invalid timestamp values."
        )
    if not allow_null:
        null_mask = parsed.isna()
        if null_mask.any():
            raise MacroValidationError(
                f"{table_name}.{column} contains {int(null_mask.sum())} NULL timestamp values."
            )


def _assert_valid_date_column(
    frame: pd.DataFrame,
    column: str,
    *,
    table_name: str,
    allow_null: bool = False,
) -> None:
    parsed = pd.to_datetime(frame[column], errors="coerce")
    invalid_mask = frame[column].notna() & parsed.isna()
    if invalid_mask.any():
        raise MacroValidationError(f"{table_name}.{column} contains {int(invalid_mask.sum())} invalid date values.")
    if not allow_null:
        null_mask = parsed.isna()
        if null_mask.any():
            raise MacroValidationError(f"{table_name}.{column} contains {int(null_mask.sum())} NULL date values.")
