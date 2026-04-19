"""Publish macro canonical surfaces with pre-publish contract gates."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable

import pandas as pd

from finance_data_ops.publish.client import Publisher
from finance_data_ops.validation.macro import validate_macro_publish_contract


def build_macro_series_catalog_payload(series_catalog: pd.DataFrame) -> list[dict[str, Any]]:
    if series_catalog.empty:
        return []
    frame = series_catalog.copy()
    payload = pd.DataFrame(
        {
            "series_key": frame.get("series_key", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "source_provider": frame.get("source_provider", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "source_code": frame.get("source_code", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "frequency": frame.get("frequency", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "required_by_default": frame.get("required_by_default", False).fillna(False).astype(bool),
            "optional": frame.get("optional", False).fillna(False).astype(bool),
            "staleness_max_bdays": pd.to_numeric(frame.get("staleness_max_bdays"), errors="coerce").astype("Int64"),
            "release_calendar_source": frame.get("release_calendar_source", pd.Series(index=frame.index, dtype=object)),
            "description": frame.get("description", pd.Series(index=frame.index, dtype=object)),
            "updated_at": pd.to_datetime(frame.get("updated_at"), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["updated_at"] = payload["updated_at"].fillna(pd.Timestamp.now(tz="UTC"))
    payload = payload.dropna(subset=["series_key", "source_provider", "source_code", "frequency", "staleness_max_bdays"])
    payload = payload.sort_values(["series_key", "updated_at"]).drop_duplicates(subset=["series_key"], keep="last")
    return payload.to_dict(orient="records")


def build_macro_observations_payload(macro_observations: pd.DataFrame) -> list[dict[str, Any]]:
    if macro_observations.empty:
        return []
    frame = macro_observations.copy()
    payload = pd.DataFrame(
        {
            "series_key": frame.get("series_key", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "observation_period": frame.get("observation_period", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "observation_date": pd.to_datetime(frame.get("observation_date"), errors="coerce").dt.date,
            "frequency": frame.get("frequency", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "source_provider": frame.get("source_provider", pd.Series(index=frame.index, dtype=object)),
            "source_code": frame.get("source_code", pd.Series(index=frame.index, dtype=object)),
            "release_timestamp_utc": pd.to_datetime(frame.get("release_timestamp_utc"), utc=True, errors="coerce"),
            "release_timezone": frame.get("release_timezone", pd.Series(index=frame.index, dtype=object)),
            "release_date_local": pd.to_datetime(frame.get("release_date_local"), errors="coerce").dt.date,
            "release_calendar_source": frame.get("release_calendar_source", pd.Series(index=frame.index, dtype=object)),
            "source": frame.get("source", pd.Series(index=frame.index, dtype=object)),
            "fetched_at": pd.to_datetime(frame.get("fetched_at"), utc=True, errors="coerce"),
            "ingested_at": pd.to_datetime(frame.get("ingested_at"), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["ingested_at"] = payload["ingested_at"].fillna(now_utc)
    payload = payload.dropna(subset=["series_key", "observation_period", "observation_date", "value"])
    payload = payload.sort_values(["series_key", "observation_period", "fetched_at"]).drop_duplicates(
        subset=["series_key", "observation_period"],
        keep="last",
    )
    return payload.to_dict(orient="records")


def build_macro_daily_payload(macro_daily: pd.DataFrame) -> list[dict[str, Any]]:
    if macro_daily.empty:
        return []
    frame = macro_daily.copy()
    payload = pd.DataFrame(
        {
            "as_of_date": pd.to_datetime(frame.get("as_of_date"), errors="coerce").dt.date,
            "series_key": frame.get("series_key", pd.Series(index=frame.index, dtype=object)).astype(str).str.strip(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "source_observation_period": frame.get("source_observation_period", pd.Series(index=frame.index, dtype=object)),
            "source_observation_date": pd.to_datetime(frame.get("source_observation_date"), errors="coerce").dt.date,
            "available_at_utc": pd.to_datetime(frame.get("available_at_utc"), utc=True, errors="coerce"),
            "staleness_bdays": pd.to_numeric(frame.get("staleness_bdays"), errors="coerce").astype("Int64"),
            "is_stale": frame.get("is_stale", False).fillna(False).astype(bool),
            "alignment_mode": frame.get("alignment_mode", "release_timed").astype(str).str.strip(),
            "ingested_at": pd.to_datetime(frame.get("ingested_at"), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["ingested_at"] = payload["ingested_at"].fillna(pd.Timestamp.now(tz="UTC"))
    payload = payload.dropna(subset=["as_of_date", "series_key", "source_observation_period", "source_observation_date", "available_at_utc"])
    payload = payload.sort_values(["as_of_date", "series_key", "ingested_at"]).drop_duplicates(
        subset=["as_of_date", "series_key"],
        keep="last",
    )
    return payload.to_dict(orient="records")


def publish_macro_surfaces(
    *,
    publisher: Publisher,
    series_catalog: pd.DataFrame,
    macro_observations: pd.DataFrame,
    macro_daily: pd.DataFrame,
    refresh_materialized_views: bool = True,
    required_series_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    validate_macro_publish_contract(
        series_catalog=series_catalog,
        macro_observations=macro_observations,
        macro_daily=macro_daily,
        required_series_keys=required_series_keys,
        now_utc=datetime.now(UTC),
    )

    catalog_rows = build_macro_series_catalog_payload(series_catalog)
    observation_rows = build_macro_observations_payload(macro_observations)
    daily_rows = build_macro_daily_payload(macro_daily)

    catalog_result = publisher.upsert("macro_series_catalog", catalog_rows, on_conflict="series_key")
    observations_result = _chunked_upsert(
        publisher=publisher,
        table="macro_observations",
        rows=observation_rows,
        on_conflict="series_key,observation_period",
        chunk_size=2500,
    )
    daily_result = _chunked_upsert(
        publisher=publisher,
        table="macro_daily",
        rows=daily_rows,
        on_conflict="as_of_date,series_key",
        chunk_size=2500,
    )

    mv_latest_obs_result: dict[str, Any] | None = None
    mv_latest_rel_result: dict[str, Any] | None = None
    if refresh_materialized_views:
        mv_latest_obs_result = publisher.rpc("refresh_mv_latest_macro_observations", {})
        mv_latest_rel_result = publisher.rpc("refresh_mv_latest_economic_release_calendar", {})

    return {
        "macro_series_catalog": catalog_result,
        "macro_observations": observations_result,
        "macro_daily": daily_result,
        "mv_latest_macro_observations": mv_latest_obs_result,
        "mv_latest_economic_release_calendar": mv_latest_rel_result,
    }


def _chunked_upsert(
    *,
    publisher: Publisher,
    table: str,
    rows: list[dict[str, Any]],
    on_conflict: str,
    chunk_size: int,
) -> dict[str, Any]:
    if not rows:
        return {"table": table, "status": "skipped", "rows": 0, "chunks": 0}

    size = max(int(chunk_size), 1)
    chunk_responses: list[dict[str, Any]] = []
    total_rows = 0
    for start in range(0, len(rows), size):
        batch = rows[start : start + size]
        response = publisher.upsert(table, batch, on_conflict=on_conflict)
        chunk_responses.append(response)
        total_rows += len(batch)

    status_code = None
    if chunk_responses:
        status_code = chunk_responses[-1].get("status_code")
    return {
        "table": table,
        "status_code": status_code,
        "rows": total_rows,
        "chunks": len(chunk_responses),
        "chunk_responses": chunk_responses,
    }
