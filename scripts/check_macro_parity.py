#!/usr/bin/env python3
"""Parity checks between Data Ops canonical macro outputs and legacy Finance outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

RELEASE_TIMED_SERIES = frozenset({"CPI_Headline", "CPI_Core", "UNRATE", "U6RATE", "CIVPART", "ICSA"})
DEFAULT_STALENESS_MAX_BDAYS_BY_FREQUENCY: dict[str, int] = {
    "daily": 5,
    "weekly": 10,
    "monthly": 45,
}


def _parse_iso_date(raw: str | None) -> pd.Timestamp | None:
    token = str(raw or "").strip()
    if not token:
        return None
    parsed = pd.to_datetime(token, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _normalize_utc_ts(value: object) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).isoformat()


def _load_legacy_macro(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    if not isinstance(frame.index, pd.DatetimeIndex):
        frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame.loc[~pd.isna(frame.index)].copy()
    if frame.index.tz is not None:
        frame.index = frame.index.tz_localize(None)
    frame.index = frame.index.normalize()
    return frame.sort_index()


def _fallback_series_frequency(series_key: str) -> str:
    key = str(series_key).strip()
    if key in {"CPI_Headline", "CPI_Core", "UNRATE", "U6RATE", "CIVPART"}:
        return "monthly"
    if key == "ICSA":
        return "weekly"
    if key == "Gasoline_US_Regular":
        return "weekly"
    return "daily"


def _load_staleness_limits(
    *,
    shared_series: list[str],
    canonical_series_catalog_path: Path,
) -> dict[str, int]:
    limits: dict[str, int] = {}
    if canonical_series_catalog_path.exists():
        catalog = pd.read_parquet(canonical_series_catalog_path)
        if not catalog.empty and {"series_key", "staleness_max_bdays"}.issubset(set(catalog.columns)):
            catalog_local = catalog.copy()
            catalog_local["series_key"] = catalog_local["series_key"].astype(str)
            catalog_local["staleness_max_bdays"] = pd.to_numeric(
                catalog_local["staleness_max_bdays"], errors="coerce"
            )
            for _, row in catalog_local.iterrows():
                series_key = str(row["series_key"]).strip()
                if series_key not in shared_series:
                    continue
                value = row.get("staleness_max_bdays")
                if pd.notna(value):
                    limits[series_key] = int(value)

    for series_key in shared_series:
        if series_key in limits:
            continue
        freq = _fallback_series_frequency(series_key)
        limits[series_key] = int(DEFAULT_STALENESS_MAX_BDAYS_BY_FREQUENCY[freq])
    return limits


def _compute_legacy_staleness(
    *,
    merged: pd.DataFrame,
    audit: pd.DataFrame,
    shared_series: list[str],
    staleness_limits: dict[str, int],
) -> pd.DataFrame:
    audit_local = audit.copy()
    audit_local["series_key"] = audit_local["series_key"].astype(str)
    audit_local["observation_period"] = audit_local["observation_period"].astype(str)
    audit_local["observation_date"] = pd.to_datetime(audit_local["observation_date"], errors="coerce").dt.normalize()
    audit_local["first_eligible_trading_date"] = pd.to_datetime(
        audit_local["first_eligible_trading_date"], errors="coerce"
    ).dt.normalize()
    audit_local = audit_local.drop_duplicates(subset=["series_key", "observation_period"], keep="last")

    out_frames: list[pd.DataFrame] = []
    for series_key in shared_series:
        local = merged[merged["series_key"] == series_key][["as_of_date", "series_key", "legacy_value"]].copy()
        local = local.sort_values("as_of_date").reset_index(drop=True)
        if local.empty:
            continue

        source_dates = pd.Series(pd.NaT, index=local.index, dtype="datetime64[ns]")
        if series_key in RELEASE_TIMED_SERIES:
            phase = audit_local[audit_local["series_key"] == series_key][
                ["first_eligible_trading_date", "observation_date"]
            ].copy()
            phase = phase.dropna(subset=["first_eligible_trading_date"])
            phase = phase.sort_values("first_eligible_trading_date")
            if not phase.empty:
                points = phase.rename(
                    columns={
                        "first_eligible_trading_date": "effective_date",
                        "observation_date": "source_observation_date",
                    }
                )
                left = local[["as_of_date"]].copy()
                left["as_of_date"] = pd.to_datetime(left["as_of_date"], errors="coerce").astype("datetime64[ns]")
                left = left.sort_values("as_of_date")
                points["effective_date"] = pd.to_datetime(points["effective_date"], errors="coerce").astype(
                    "datetime64[ns]"
                )
                points = points.sort_values("effective_date")
                mapped = pd.merge_asof(
                    left,
                    points,
                    left_on="as_of_date",
                    right_on="effective_date",
                    direction="backward",
                )
                source_dates = pd.to_datetime(mapped["source_observation_date"], errors="coerce").dt.normalize()
        else:
            legacy_values = pd.to_numeric(local["legacy_value"], errors="coerce")
            non_null_mask = legacy_values.notna()
            source_dates.loc[non_null_mask] = local.loc[non_null_mask, "as_of_date"].to_numpy()
            source_dates = source_dates.ffill()

        idx_days = pd.to_datetime(local["as_of_date"], errors="coerce").dt.normalize().to_numpy(dtype="datetime64[D]")
        src_days = pd.to_datetime(source_dates, errors="coerce").to_numpy(dtype="datetime64[D]")
        valid = ~pd.isna(src_days)
        staleness = np.full(len(local.index), np.nan)
        if bool(np.any(valid)):
            staleness_valid = np.busday_count(src_days[valid], idx_days[valid])
            staleness[valid] = staleness_valid

        limit = int(staleness_limits.get(series_key, DEFAULT_STALENESS_MAX_BDAYS_BY_FREQUENCY["daily"]))
        stale_mask = (~valid) | (staleness > limit)
        local["legacy_source_observation_date"] = pd.to_datetime(source_dates, errors="coerce").dt.date
        local["legacy_staleness_bdays"] = pd.Series(staleness).round().astype("Int64")
        local["legacy_is_stale_true"] = stale_mask.astype(bool)
        out_frames.append(local[["as_of_date", "series_key", "legacy_source_observation_date", "legacy_staleness_bdays", "legacy_is_stale_true"]])

    if not out_frames:
        return pd.DataFrame(
            columns=[
                "as_of_date",
                "series_key",
                "legacy_source_observation_date",
                "legacy_staleness_bdays",
                "legacy_is_stale_true",
            ]
        )
    out = pd.concat(out_frames, ignore_index=True)
    return out.drop_duplicates(subset=["as_of_date", "series_key"], keep="last")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check macro parity against legacy Finance outputs.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--value-tolerance", type=float, default=1e-5, help="Absolute numeric tolerance.")
    parser.add_argument(
        "--canonical-macro-daily-path",
        default="data_cache/macro_daily.parquet",
        help="Path to Data Ops macro_daily parquet.",
    )
    parser.add_argument(
        "--canonical-series-catalog-path",
        default="data_cache/macro_series_catalog.parquet",
        help="Path to Data Ops macro_series_catalog parquet.",
    )
    parser.add_argument(
        "--legacy-macro-processed-path",
        default="../Finance/data_cache/macro_data_processed.parquet",
        help="Path to legacy Finance macro_data_processed parquet.",
    )
    parser.add_argument(
        "--legacy-release-timing-audit-path",
        default="../Finance/data_cache/audits/macro_release_timing_audit.parquet",
        help="Path to legacy Finance macro_release_timing_audit parquet.",
    )
    parser.add_argument("--json-output", default=None, help="Optional output path for JSON summary.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    start_ts = _parse_iso_date(args.start_date)
    end_ts = _parse_iso_date(args.end_date)
    if start_ts is None or end_ts is None:
        raise ValueError("start-date and end-date must be valid ISO dates.")
    if end_ts < start_ts:
        raise ValueError("end-date must be on/after start-date.")

    canonical_path = Path(args.canonical_macro_daily_path).resolve()
    canonical_series_catalog_path = Path(args.canonical_series_catalog_path).resolve()
    legacy_path = Path(args.legacy_macro_processed_path).resolve()
    audit_path = Path(args.legacy_release_timing_audit_path).resolve()

    canonical = pd.read_parquet(canonical_path)
    canonical["as_of_date"] = pd.to_datetime(canonical["as_of_date"], errors="coerce").dt.normalize()
    canonical = canonical.loc[(canonical["as_of_date"] >= start_ts) & (canonical["as_of_date"] <= end_ts)].copy()
    canonical["series_key"] = canonical["series_key"].astype(str)

    legacy = _load_legacy_macro(legacy_path)
    legacy = legacy.loc[(legacy.index >= start_ts) & (legacy.index <= end_ts)].copy()

    canonical_series = sorted(set(canonical["series_key"].astype(str)))
    legacy_series = sorted([str(c) for c in legacy.columns])
    shared_series = sorted(set(canonical_series).intersection(set(legacy_series)))

    legacy_long = (
        legacy.reset_index(names="as_of_date")
        .melt(id_vars=["as_of_date"], value_vars=shared_series, var_name="series_key", value_name="legacy_value")
        .copy()
    )
    legacy_long["as_of_date"] = pd.to_datetime(legacy_long["as_of_date"], errors="coerce").dt.normalize()

    canonical_cmp = canonical[
        [
            "as_of_date",
            "series_key",
            "value",
            "is_stale",
            "source_observation_period",
            "available_at_utc",
            "source_observation_date",
            "staleness_bdays",
        ]
    ].copy()
    canonical_cmp["canonical_value"] = pd.to_numeric(canonical_cmp["value"], errors="coerce")
    canonical_cmp["canonical_is_stale"] = canonical_cmp["is_stale"].fillna(False).astype(bool)
    canonical_cmp["canonical_staleness_bdays"] = pd.to_numeric(canonical_cmp["staleness_bdays"], errors="coerce").astype("Int64")
    canonical_cmp["canonical_source_observation_date"] = pd.to_datetime(
        canonical_cmp["source_observation_date"], errors="coerce"
    ).dt.date
    canonical_cmp["canonical_available_at_utc"] = canonical_cmp["available_at_utc"].map(_normalize_utc_ts)

    merged = canonical_cmp.merge(legacy_long, on=["as_of_date", "series_key"], how="left")

    legacy_values = pd.to_numeric(merged["legacy_value"], errors="coerce")
    canonical_values = pd.to_numeric(merged["canonical_value"], errors="coerce")
    both_present = canonical_values.notna() & legacy_values.notna()
    abs_diff = (canonical_values - legacy_values).abs()
    value_mismatch_mask = both_present & (abs_diff > float(args.value_tolerance))
    canonical_only_mask = canonical_values.notna() & legacy_values.isna()
    legacy_only_mask = canonical_values.isna() & legacy_values.notna()

    audit = pd.read_parquet(audit_path)
    audit["series_key"] = audit["series_key"].astype(str)
    audit["observation_period"] = audit["observation_period"].astype(str)
    audit["legacy_first_known_at_utc"] = audit["first_known_at_timestamp_utc"].map(_normalize_utc_ts)
    audit_map = audit.drop_duplicates(subset=["series_key", "observation_period"], keep="last")[
        ["series_key", "observation_period", "legacy_first_known_at_utc"]
    ]

    availability = merged[merged["series_key"].isin(RELEASE_TIMED_SERIES)].copy()
    availability["observation_period"] = availability["source_observation_period"].astype(str)
    availability = availability.merge(audit_map, on=["series_key", "observation_period"], how="left")
    availability_missing_mask = availability["legacy_first_known_at_utc"].isna() & availability[
        "canonical_available_at_utc"
    ].notna()
    availability_mismatch_mask = (
        availability["legacy_first_known_at_utc"].notna()
        & availability["canonical_available_at_utc"].notna()
        & (availability["legacy_first_known_at_utc"] != availability["canonical_available_at_utc"])
    )

    staleness_limits = _load_staleness_limits(
        shared_series=shared_series,
        canonical_series_catalog_path=canonical_series_catalog_path,
    )
    legacy_staleness = _compute_legacy_staleness(
        merged=merged,
        audit=audit,
        shared_series=shared_series,
        staleness_limits=staleness_limits,
    )
    merged = merged.merge(
        legacy_staleness,
        on=["as_of_date", "series_key"],
        how="left",
    )
    merged["legacy_is_stale_true"] = merged["legacy_is_stale_true"].fillna(True).astype(bool)
    stale_mismatch_mask = merged["canonical_is_stale"] != merged["legacy_is_stale_true"]

    summary = {
        "window": {"start_date": start_ts.date().isoformat(), "end_date": end_ts.date().isoformat()},
        "inputs": {
            "canonical_macro_daily_path": str(canonical_path),
            "canonical_series_catalog_path": str(canonical_series_catalog_path),
            "legacy_macro_processed_path": str(legacy_path),
            "legacy_release_timing_audit_path": str(audit_path),
            "value_tolerance": float(args.value_tolerance),
        },
        "contract": {
            "value": {
                "fields": ["canonical.value", "legacy.value"],
                "comparison": "absolute_difference <= value_tolerance",
                "value_tolerance": float(args.value_tolerance),
            },
            "availability_timing": {
                "scope_series": sorted(RELEASE_TIMED_SERIES),
                "fields": ["canonical.available_at_utc", "legacy.first_known_at_timestamp_utc"],
                "key": ["series_key", "source_observation_period"],
                "comparison": "exact_utc_timestamp_equality",
            },
            "staleness": {
                "fields": ["canonical.is_stale", "legacy_is_stale_true"],
                "legacy_computation": (
                    "release_timed: audit(first_eligible_trading_date -> observation_date) replay; "
                    "non_release: last_non_null_observation_date ffill; "
                    "stale when source_observation_date missing or busday_count(source_observation_date, as_of_date) > staleness_max_bdays"
                ),
                "staleness_limits_by_series": staleness_limits,
            },
        },
        "series": {
            "canonical_count": len(canonical_series),
            "legacy_count": len(legacy_series),
            "shared_count": len(shared_series),
            "missing_in_legacy": sorted(set(canonical_series).difference(set(legacy_series))),
            "missing_in_canonical": sorted(set(legacy_series).difference(set(canonical_series))),
        },
        "value_parity": {
            "rows_compared": int(len(merged.index)),
            "both_present_rows": int(both_present.sum()),
            "value_mismatch_rows": int(value_mismatch_mask.sum()),
            "canonical_present_legacy_missing_rows": int(canonical_only_mask.sum()),
            "legacy_present_canonical_missing_rows": int(legacy_only_mask.sum()),
            "max_abs_diff": (float(abs_diff[value_mismatch_mask].max()) if value_mismatch_mask.any() else 0.0),
        },
        "availability_parity": {
            "rows_compared": int(len(availability.index)),
            "missing_legacy_timing_rows": int(availability_missing_mask.sum()),
            "timing_mismatch_rows": int(availability_mismatch_mask.sum()),
        },
        "staleness_parity": {
            "rows_compared": int(len(merged.index)),
            "mismatch_rows": int(stale_mismatch_mask.sum()),
            "legacy_method": "computed_legacy_is_stale_true",
        },
        "samples": {
            "value_mismatch": (
                merged.loc[value_mismatch_mask, ["as_of_date", "series_key", "canonical_value", "legacy_value"]]
                .head(25)
                .assign(as_of_date=lambda x: x["as_of_date"].dt.date.astype(str))
                .to_dict(orient="records")
            ),
            "availability_mismatch": (
                availability.loc[
                    availability_mismatch_mask,
                    ["as_of_date", "series_key", "observation_period", "canonical_available_at_utc", "legacy_first_known_at_utc"],
                ]
                .head(25)
                .assign(as_of_date=lambda x: pd.to_datetime(x["as_of_date"]).dt.date.astype(str))
                .to_dict(orient="records")
            ),
            "staleness_mismatch": (
                merged.loc[
                    stale_mismatch_mask,
                    [
                        "as_of_date",
                        "series_key",
                        "canonical_is_stale",
                        "legacy_is_stale_true",
                        "canonical_staleness_bdays",
                        "legacy_staleness_bdays",
                        "canonical_source_observation_date",
                        "legacy_source_observation_date",
                    ],
                ]
                .head(25)
                .assign(as_of_date=lambda x: x["as_of_date"].dt.date.astype(str))
                .to_dict(orient="records")
            ),
        },
    }

    has_failure = any(
        [
            summary["value_parity"]["value_mismatch_rows"] > 0,
            summary["value_parity"]["canonical_present_legacy_missing_rows"] > 0,
            summary["value_parity"]["legacy_present_canonical_missing_rows"] > 0,
            summary["availability_parity"]["missing_legacy_timing_rows"] > 0,
            summary["availability_parity"]["timing_mismatch_rows"] > 0,
            summary["staleness_parity"]["mismatch_rows"] > 0,
        ]
    )
    summary["status"] = "fail" if has_failure else "ok"

    rendered = json.dumps(summary, indent=2, default=str)
    print(rendered)
    if args.json_output:
        out_path = Path(args.json_output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rendered + "\n", encoding="utf-8")
    return 1 if has_failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
