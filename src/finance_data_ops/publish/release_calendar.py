"""Publish economic release-calendar canonical surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher
from finance_data_ops.validation.release_calendar import validate_release_calendar_publish_contract


def build_economic_release_calendar_payload(economic_release_calendar: pd.DataFrame) -> list[dict[str, Any]]:
    if economic_release_calendar.empty:
        return []

    frame = economic_release_calendar.copy()
    payload = pd.DataFrame(
        {
            "series_key": frame.get("series_key", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "observation_period": frame.get("observation_period", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "observation_date": pd.to_datetime(frame.get("observation_date"), errors="coerce").dt.date,
            "scheduled_release_timestamp_utc": pd.to_datetime(
                frame.get("scheduled_release_timestamp_utc", frame.get("release_timestamp_utc")),
                utc=True,
                errors="coerce",
            ),
            "observed_first_available_at_utc": pd.to_datetime(
                frame.get("observed_first_available_at_utc"),
                utc=True,
                errors="coerce",
            ),
            "availability_status": frame.get("availability_status", pd.Series(index=frame.index, dtype=object))
            .astype(str)
            .str.strip(),
            "availability_source": frame.get("availability_source", pd.Series(index=frame.index, dtype=object))
            .astype(str)
            .str.strip(),
            "delay_vs_schedule_seconds": pd.to_numeric(
                frame.get("delay_vs_schedule_seconds"),
                errors="coerce",
            ).astype("Int64"),
            "is_schedule_based_only": frame.get("is_schedule_based_only", pd.Series(index=frame.index, dtype=object))
            .astype(str)
            .str.strip()
            .str.lower()
            .map({"true": True, "false": False, "1": True, "0": False}),
            "release_timezone": frame.get("release_timezone", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "release_date_local": pd.to_datetime(frame.get("release_date_local"), errors="coerce").dt.date,
            "release_calendar_source": frame.get("release_calendar_source", pd.Series(index=frame.index, dtype=object)),
            "source": frame.get("source", pd.Series(index=frame.index, dtype=object)),
            "provenance_class": frame.get("provenance_class", pd.Series(index=frame.index, dtype=object)),
            "ingested_at": pd.to_datetime(frame.get("ingested_at"), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["release_timestamp_utc"] = payload["scheduled_release_timestamp_utc"]
    payload["ingested_at"] = payload["ingested_at"].fillna(pd.Timestamp.now(tz="UTC"))
    payload = payload.sort_values(["series_key", "observation_period", "scheduled_release_timestamp_utc", "ingested_at"])
    payload = payload.drop_duplicates(subset=["series_key", "observation_period"], keep="last")
    return payload.to_dict(orient="records")


def publish_release_calendar_surfaces(
    *,
    publisher: Publisher,
    economic_release_calendar: pd.DataFrame,
    refresh_materialized_view: bool = True,
    allow_empty: bool = False,
) -> dict[str, Any]:
    validate_release_calendar_publish_contract(
        economic_release_calendar=economic_release_calendar,
        allow_empty=allow_empty,
    )

    if economic_release_calendar.empty:
        return {
            "economic_release_calendar": {"table": "economic_release_calendar", "status": "skipped", "rows": 0},
            "mv_latest_economic_release_calendar": None,
        }

    rows = build_economic_release_calendar_payload(economic_release_calendar)
    table_result = publisher.upsert(
        "economic_release_calendar",
        rows,
        on_conflict="series_key,observation_period",
    )

    mv_result: dict[str, Any] | None = None
    if refresh_materialized_view:
        mv_result = publisher.rpc("refresh_mv_latest_economic_release_calendar", {})

    return {
        "economic_release_calendar": table_result,
        "mv_latest_economic_release_calendar": mv_result,
    }
