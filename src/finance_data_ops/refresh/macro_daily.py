"""Daily macro refresh job with release-timed alignment and staleness policy."""

from __future__ import annotations

from datetime import UTC, datetime, time as clock_time
from uuid import uuid4
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.macro import (
    DEFAULT_REQUIRED_SERIES_KEYS,
    MACRO_SERIES_CATALOG,
    RELEASE_TIMED_SERIES,
    STALENESS_MAX_BDAYS_BY_FREQUENCY,
    MacroDataProvider,
    MacroSeriesSpec,
    catalog_frame,
)
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table

_DEFAULT_RELEASE_TIMEZONE = "America/New_York"
_MARKET_CLOSE_LOCAL_TIME = clock_time(hour=16, minute=0)


def refresh_macro_daily(
    *,
    start: str,
    end: str,
    provider: MacroDataProvider,
    cache_root: str,
    release_calendar_frame: pd.DataFrame | None = None,
    series_catalog: tuple[MacroSeriesSpec, ...] | None = None,
    max_attempts: int = 3,
    alignment_mode: str = "release_timed",
    force_recompute: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_macro_daily_{uuid4().hex[:12]}"

    start_ts = pd.Timestamp(start).tz_localize(None)
    end_ts = pd.Timestamp(end).tz_localize(None)
    if end_ts < start_ts:
        raise ValueError("macro refresh end date must be on/after start date")

    catalog = tuple(series_catalog or MACRO_SERIES_CATALOG)
    catalog_df = catalog_frame(catalog)

    observations_rows: list[dict[str, object]] = []
    symbols_succeeded: list[str] = []
    symbols_failed: list[str] = []
    retry_exhausted_symbols: list[str] = []
    error_messages: list[str] = []

    ingested_at = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")

    for spec in catalog:

        def _fetch_one() -> pd.Series:
            return provider.fetch_series(spec, start=start_ts, end=end_ts)

        result, error, _, exhausted_retry_path = run_with_retry(
            _fetch_one,
            max_attempts=max_attempts,
            sleep_seconds=0.0,
        )

        if error is not None or result is None:
            symbols_failed.append(spec.key)
            if exhausted_retry_path:
                retry_exhausted_symbols.append(spec.key)
            classification = classify_failure(error or RuntimeError("unknown macro refresh failure"))
            error_messages.append(f"{spec.key}: {classification.message}")
            continue

        series = pd.to_numeric(result, errors="coerce").dropna()
        if series.empty:
            if spec.required_by_default:
                symbols_failed.append(spec.key)
                error_messages.append(f"{spec.key}: provider returned zero rows for required series")
            continue

        rows = _series_to_observation_rows(spec=spec, series=series, ingested_at=ingested_at)
        observations_rows.extend(rows)
        symbols_succeeded.append(spec.key)

    observations = pd.DataFrame(observations_rows)
    if observations.empty:
        observations = pd.DataFrame(
            columns=[
                "series_key",
                "observation_period",
                "observation_date",
                "frequency",
                "value",
                "source_provider",
                "source_code",
                "release_timestamp_utc",
                "release_timezone",
                "release_date_local",
                "release_calendar_source",
                "source",
                "fetched_at",
                "ingested_at",
            ]
        )

    observations = _attach_release_calendar(
        observations=observations,
        release_calendar_frame=release_calendar_frame,
    )

    macro_daily = _build_macro_daily_frame(
        observations=observations,
        catalog=catalog,
        start_date=start_ts,
        end_date=end_ts,
        alignment_mode=alignment_mode,
        ingested_at=ingested_at,
    )

    write_parquet_table(
        "macro_series_catalog",
        catalog_df,
        cache_root=cache_root,
        mode="replace",
        dedupe_subset=["series_key"],
    )
    write_parquet_table(
        "macro_observations",
        observations,
        cache_root=cache_root,
        mode="replace" if force_recompute else "append",
        dedupe_subset=["series_key", "observation_period"],
    )
    write_parquet_table(
        "macro_daily",
        macro_daily,
        cache_root=cache_root,
        mode="replace" if force_recompute else "append",
        dedupe_subset=["as_of_date", "series_key"],
    )

    required_failed = sorted(set(DEFAULT_REQUIRED_SERIES_KEYS).intersection(set(symbols_failed)))
    if required_failed and symbols_succeeded:
        status = "partial"
    elif required_failed and not symbols_succeeded:
        status = "failed_retrying" if retry_exhausted_symbols else "failed_hard"
    elif symbols_failed:
        status = "partial"
    else:
        status = "fresh"

    refresh_result = RefreshRunResult(
        run_id=run_id,
        asset_name="macro_daily",
        status=status,
        started_at=started_at.isoformat(),
        ended_at=datetime.now(UTC).isoformat(),
        symbols_requested=[spec.key for spec in catalog],
        symbols_succeeded=sorted(set(symbols_succeeded)),
        symbols_failed=sorted(set(symbols_failed)),
        retry_exhausted_symbols=sorted(set(retry_exhausted_symbols)),
        rows_written=int(len(catalog_df.index) + len(observations.index) + len(macro_daily.index)),
        error_messages=error_messages,
    )
    return catalog_df, observations, macro_daily, refresh_result


def _series_to_observation_rows(*, spec: MacroSeriesSpec, series: pd.Series, ingested_at: pd.Timestamp) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    fetched_at = pd.Timestamp.now(tz="UTC")

    idx = pd.to_datetime(series.index, errors="coerce")
    valid_mask = ~pd.isna(idx)
    series = series.loc[valid_mask]
    idx = pd.DatetimeIndex(idx[valid_mask]).tz_localize(None)
    series.index = idx
    series = series.sort_index()
    series = series[~series.index.duplicated(keep="last")]

    for obs_ts, value in series.items():
        obs_ts = pd.Timestamp(obs_ts).normalize()
        observation_date = _normalize_observation_date(obs_ts, spec.frequency)
        observation_period = _to_observation_period(observation_date, spec.frequency)
        rows.append(
            {
                "series_key": spec.key,
                "observation_period": observation_period,
                "observation_date": observation_date.isoformat(),
                "frequency": spec.frequency,
                "value": float(value),
                "source_provider": spec.source,
                "source_code": spec.source_code,
                "release_timestamp_utc": None,
                "release_timezone": None,
                "release_date_local": None,
                "release_calendar_source": _default_release_calendar_source(spec.key),
                "source": f"{spec.source}_api_v1",
                "fetched_at": fetched_at,
                "ingested_at": ingested_at,
            }
        )
    return rows


def _normalize_observation_date(observation_ts: pd.Timestamp, frequency: str) -> pd.Timestamp:
    ts = pd.Timestamp(observation_ts).normalize()
    if str(frequency).strip().lower() == "weekly":
        days_back_to_saturday = (int(ts.dayofweek) - 5) % 7
        return (ts - pd.Timedelta(days=days_back_to_saturday)).normalize()
    return ts


def _to_observation_period(observation_date: pd.Timestamp, frequency: str) -> str:
    freq = str(frequency).strip().lower()
    if freq == "monthly":
        return str(pd.Timestamp(observation_date).to_period("M"))
    return str(pd.Timestamp(observation_date).date())


def _default_release_calendar_source(series_key: str) -> str | None:
    key = str(series_key).strip()
    if key in {"CPI_Headline", "CPI_Core"}:
        return "bls_cpi_release_calendar_v1"
    if key in {"UNRATE", "U6RATE", "CIVPART"}:
        return "bls_unrate_release_calendar_v1"
    if key == "ICSA":
        return "dol_icsa_release_calendar_v1"
    return None


def _attach_release_calendar(
    *,
    observations: pd.DataFrame,
    release_calendar_frame: pd.DataFrame | None,
) -> pd.DataFrame:
    if observations.empty:
        return observations
    out = observations.copy()
    for col in ["release_timestamp_utc", "release_timezone", "release_date_local", "release_calendar_source"]:
        if col not in out.columns:
            out[col] = None

    if release_calendar_frame is None or release_calendar_frame.empty:
        return out

    required_cols = {
        "series_key",
        "observation_period",
        "release_timestamp_utc",
        "release_timezone",
        "release_date_local",
        "release_calendar_source",
    }
    if not required_cols.issubset(release_calendar_frame.columns):
        return out

    release = release_calendar_frame.copy()
    release = release.drop_duplicates(subset=["series_key", "observation_period"], keep="last")
    release_map = release.set_index(["series_key", "observation_period"])

    for idx, row in out.iterrows():
        series_key = str(row.get("series_key", "")).strip()
        if series_key not in RELEASE_TIMED_SERIES:
            continue
        key = (series_key, str(row.get("observation_period", "")).strip())
        if key not in release_map.index:
            continue
        release_row = release_map.loc[key]
        out.at[idx, "release_timestamp_utc"] = release_row.get("release_timestamp_utc")
        out.at[idx, "release_timezone"] = release_row.get("release_timezone")
        out.at[idx, "release_date_local"] = release_row.get("release_date_local")
        out.at[idx, "release_calendar_source"] = release_row.get("release_calendar_source")
    return out


def _build_macro_daily_frame(
    *,
    observations: pd.DataFrame,
    catalog: tuple[MacroSeriesSpec, ...],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    alignment_mode: str,
    ingested_at: pd.Timestamp,
) -> pd.DataFrame:
    business_index = pd.bdate_range(start=start_date.date().isoformat(), end=end_date.date().isoformat())
    if len(business_index) == 0:
        return pd.DataFrame(
            columns=[
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
            ]
        )

    rows: list[pd.DataFrame] = []
    for spec in catalog:
        local = observations[observations["series_key"].astype(str) == spec.key].copy()
        if local.empty:
            continue
        local["observation_date"] = pd.to_datetime(local["observation_date"], errors="coerce")
        local["value"] = pd.to_numeric(local["value"], errors="coerce")
        local = local.dropna(subset=["observation_date", "value"]).sort_values("observation_date")
        if local.empty:
            continue

        effective_rows: list[dict[str, object]] = []
        for _, row in local.iterrows():
            release_ts = pd.to_datetime(row.get("release_timestamp_utc"), utc=True, errors="coerce")
            release_tz = str(row.get("release_timezone") or _DEFAULT_RELEASE_TIMEZONE).strip() or _DEFAULT_RELEASE_TIMEZONE
            obs_date = pd.Timestamp(row["observation_date"]).normalize()

            if str(alignment_mode).strip().lower() == "release_timed" and pd.notna(release_ts):
                eligible_date = _eligible_trading_date_from_release(
                    release_timestamp_utc=release_ts,
                    release_timezone=release_tz,
                )
                available_at_utc = pd.Timestamp(release_ts).tz_convert("UTC")
            else:
                eligible_date = obs_date
                if int(eligible_date.dayofweek) >= 5:
                    eligible_date = pd.Timestamp(eligible_date + pd.offsets.BDay(1)).normalize()
                available_at_utc = pd.Timestamp(eligible_date).tz_localize("UTC")

            effective_rows.append(
                {
                    "effective_date": eligible_date,
                    "value": float(row["value"]),
                    "source_observation_period": str(row.get("observation_period") or ""),
                    "source_observation_date": obs_date,
                    "available_at_utc": available_at_utc,
                }
            )

        effective = pd.DataFrame(effective_rows)
        if effective.empty:
            continue
        effective["effective_date"] = pd.to_datetime(effective["effective_date"], errors="coerce").dt.normalize()
        effective = effective.dropna(subset=["effective_date"]) 
        effective = effective.sort_values("effective_date").drop_duplicates(subset=["effective_date"], keep="last")
        effective = effective.set_index("effective_date")

        aligned = pd.DataFrame(index=business_index)
        aligned["value"] = effective["value"].reindex(business_index, method="ffill")
        aligned["source_observation_period"] = effective["source_observation_period"].reindex(business_index, method="ffill")
        aligned["source_observation_date"] = pd.to_datetime(
            effective["source_observation_date"],
            errors="coerce",
        ).reindex(business_index, method="ffill")
        aligned["available_at_utc"] = pd.to_datetime(
            effective["available_at_utc"],
            utc=True,
            errors="coerce",
        ).reindex(business_index, method="ffill")

        idx_days = business_index.normalize().to_numpy(dtype="datetime64[D]")
        src_days = pd.to_datetime(aligned["source_observation_date"], errors="coerce").dt.normalize().to_numpy(dtype="datetime64[D]")
        valid = ~pd.isna(src_days)
        staleness = np.full(len(business_index), np.nan)
        if bool(np.any(valid)):
            staleness_valid = np.busday_count(src_days[valid], idx_days[valid])
            staleness[valid] = staleness_valid

        limit = int(STALENESS_MAX_BDAYS_BY_FREQUENCY[spec.frequency])
        stale_mask = (~valid) | (staleness > limit)
        aligned.loc[stale_mask, "value"] = np.nan

        aligned = aligned.reset_index().rename(columns={"index": "as_of_date"})
        aligned["as_of_date"] = pd.to_datetime(aligned["as_of_date"], errors="coerce").dt.date
        aligned["source_observation_date"] = pd.to_datetime(aligned["source_observation_date"], errors="coerce").dt.date
        aligned["available_at_utc"] = pd.to_datetime(aligned["available_at_utc"], utc=True, errors="coerce")
        aligned["series_key"] = spec.key
        aligned["staleness_bdays"] = pd.Series(staleness).round().astype("Int64")
        aligned["is_stale"] = stale_mask
        aligned["alignment_mode"] = str(alignment_mode).strip().lower() or "release_timed"
        aligned["ingested_at"] = ingested_at

        rows.append(
            aligned[
                [
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
                ]
            ]
        )

    if not rows:
        return pd.DataFrame(
            columns=[
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
            ]
        )

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["as_of_date", "series_key", "ingested_at"]).drop_duplicates(
        subset=["as_of_date", "series_key"],
        keep="last",
    )
    return out.reset_index(drop=True)


def _eligible_trading_date_from_release(*, release_timestamp_utc: pd.Timestamp, release_timezone: str) -> pd.Timestamp:
    tz = ZoneInfo(str(release_timezone or _DEFAULT_RELEASE_TIMEZONE).strip() or _DEFAULT_RELEASE_TIMEZONE)
    release_local = pd.Timestamp(release_timestamp_utc).tz_convert(tz)
    release_day = pd.Timestamp(release_local.date())

    if int(release_day.dayofweek) >= 5:
        return pd.Timestamp(release_day + pd.offsets.BDay(1)).normalize()
    if release_local.timetz().replace(tzinfo=None) > _MARKET_CLOSE_LOCAL_TIME:
        return pd.Timestamp(release_day + pd.offsets.BDay(1)).normalize()
    return release_day.normalize()
