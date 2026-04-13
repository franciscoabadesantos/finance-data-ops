#!/usr/bin/env python3
"""Parity checks between Data Ops canonical release calendar and legacy Finance registry tables."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _normalize_ts(value: object) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).isoformat()


def _normalize_date(value: object) -> str | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date().isoformat()


def _load_legacy_rows(finance_repo_root: Path, registry_path: Path) -> pd.DataFrame:
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    tables = payload.get("tables")
    if not isinstance(tables, list):
        raise ValueError("Legacy release calendar registry missing 'tables'.")

    rows: list[dict[str, Any]] = []
    for item in tables:
        if not isinstance(item, dict):
            continue
        if item.get("active") is False:
            continue
        rel_path = str(item.get("path") or "").strip()
        if not rel_path:
            continue
        csv_path = (finance_repo_root / rel_path).resolve()
        if not csv_path.exists():
            raise FileNotFoundError(f"Legacy release table not found: {csv_path}")
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for raw in reader:
                if not isinstance(raw, dict):
                    continue
                series_key = str(raw.get("series_key") or "").strip()
                observation_period = str(raw.get("observation_period") or "").strip()
                if not series_key or not observation_period:
                    continue
                rows.append(
                    {
                        "series_key": series_key,
                        "observation_period": observation_period,
                        "observation_date": _normalize_date(raw.get("observation_date")),
                        "release_timestamp_utc": _normalize_ts(raw.get("release_timestamp_utc")),
                        "release_timezone": str(raw.get("release_timezone") or "").strip() or None,
                        "release_date_local": _normalize_date(raw.get("release_date_local")),
                        "release_calendar_source": str(raw.get("release_calendar_source") or "").strip() or None,
                        "source": str(raw.get("source") or "").strip() or None,
                    }
                )

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame.sort_values(["series_key", "observation_period", "release_timestamp_utc"]).drop_duplicates(
        subset=["series_key", "observation_period"],
        keep="last",
    )
    return frame.reset_index(drop=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check release-calendar parity against legacy Finance registry.")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--canonical-release-path",
        default="data_cache/economic_release_calendar.parquet",
        help="Path to Data Ops economic_release_calendar parquet.",
    )
    parser.add_argument(
        "--finance-repo-root",
        default="../Finance",
        help="Path to Finance repository root.",
    )
    parser.add_argument(
        "--legacy-registry-path",
        default="../Finance/configs/release_calendars/registry.json",
        help="Path to legacy registry.json.",
    )
    parser.add_argument("--json-output", default=None, help="Optional output path for JSON summary.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    start_ts = pd.to_datetime(args.start_date, errors="coerce")
    end_ts = pd.to_datetime(args.end_date, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        raise ValueError("start-date and end-date must be valid ISO dates.")
    start_ts = pd.Timestamp(start_ts).normalize()
    end_ts = pd.Timestamp(end_ts).normalize()
    if end_ts < start_ts:
        raise ValueError("end-date must be on/after start-date.")

    canonical_path = Path(args.canonical_release_path).resolve()
    finance_root = Path(args.finance_repo_root).resolve()
    registry_path = Path(args.legacy_registry_path).resolve()

    canonical = pd.read_parquet(canonical_path)
    canonical["observation_date"] = pd.to_datetime(canonical["observation_date"], errors="coerce").dt.normalize()
    canonical = canonical.loc[
        (canonical["observation_date"] >= start_ts) & (canonical["observation_date"] <= end_ts)
    ].copy()
    canonical["series_key"] = canonical["series_key"].astype(str)
    canonical["observation_period"] = canonical["observation_period"].astype(str)
    canonical["release_timestamp_utc_norm"] = canonical["release_timestamp_utc"].map(_normalize_ts)
    canonical = canonical.sort_values(["series_key", "observation_period", "release_timestamp_utc_norm"]).drop_duplicates(
        subset=["series_key", "observation_period"],
        keep="last",
    )

    legacy = _load_legacy_rows(finance_root, registry_path)
    if not legacy.empty:
        legacy["observation_date"] = pd.to_datetime(legacy["observation_date"], errors="coerce").dt.normalize()
        legacy = legacy.loc[(legacy["observation_date"] >= start_ts) & (legacy["observation_date"] <= end_ts)].copy()
        legacy["release_timestamp_utc_norm"] = legacy["release_timestamp_utc"].map(_normalize_ts)

    canonical_series = set(canonical["series_key"].astype(str))
    legacy_series = set(legacy["series_key"].astype(str)) if not legacy.empty else set()

    canonical_counts = canonical.groupby("series_key")["observation_period"].count().to_dict()
    legacy_counts = legacy.groupby("series_key")["observation_period"].count().to_dict() if not legacy.empty else {}
    all_series = sorted(canonical_series.union(legacy_series))
    per_series_count_delta = {
        series: {
            "canonical": int(canonical_counts.get(series, 0)),
            "legacy": int(legacy_counts.get(series, 0)),
            "delta": int(canonical_counts.get(series, 0) - legacy_counts.get(series, 0)),
        }
        for series in all_series
        if int(canonical_counts.get(series, 0)) != int(legacy_counts.get(series, 0))
    }

    merged = canonical.merge(
        legacy[["series_key", "observation_period", "release_timestamp_utc_norm"]],
        on=["series_key", "observation_period"],
        how="outer",
        suffixes=("_canonical", "_legacy"),
        indicator=True,
    )
    missing_in_legacy = merged[merged["_merge"] == "left_only"][["series_key", "observation_period"]]
    missing_in_canonical = merged[merged["_merge"] == "right_only"][["series_key", "observation_period"]]
    shared = merged[merged["_merge"] == "both"].copy()
    ts_mismatch = shared[
        shared["release_timestamp_utc_norm_canonical"].fillna("") != shared["release_timestamp_utc_norm_legacy"].fillna("")
    ]

    summary = {
        "window": {"start_date": start_ts.date().isoformat(), "end_date": end_ts.date().isoformat()},
        "inputs": {
            "canonical_release_path": str(canonical_path),
            "finance_repo_root": str(finance_root),
            "legacy_registry_path": str(registry_path),
        },
        "series": {
            "canonical_series": sorted(canonical_series),
            "legacy_series": sorted(legacy_series),
            "missing_in_legacy": sorted(canonical_series.difference(legacy_series)),
            "missing_in_canonical": sorted(legacy_series.difference(canonical_series)),
        },
        "counts": {
            "canonical_total_events": int(len(canonical.index)),
            "legacy_total_events": int(len(legacy.index)),
            "per_series_count_delta": per_series_count_delta,
        },
        "timestamp_parity": {
            "shared_rows": int(len(shared.index)),
            "timestamp_mismatch_rows": int(len(ts_mismatch.index)),
        },
        "key_parity": {
            "missing_keys_in_legacy": int(len(missing_in_legacy.index)),
            "missing_keys_in_canonical": int(len(missing_in_canonical.index)),
        },
        "samples": {
            "timestamp_mismatch": ts_mismatch[
                ["series_key", "observation_period", "release_timestamp_utc_norm_canonical", "release_timestamp_utc_norm_legacy"]
            ]
            .head(25)
            .to_dict(orient="records"),
            "missing_keys_in_legacy": missing_in_legacy.head(25).to_dict(orient="records"),
            "missing_keys_in_canonical": missing_in_canonical.head(25).to_dict(orient="records"),
        },
    }

    has_failure = any(
        [
            bool(summary["series"]["missing_in_legacy"]),
            bool(summary["series"]["missing_in_canonical"]),
            bool(summary["counts"]["per_series_count_delta"]),
            summary["timestamp_parity"]["timestamp_mismatch_rows"] > 0,
            summary["key_parity"]["missing_keys_in_legacy"] > 0,
            summary["key_parity"]["missing_keys_in_canonical"] > 0,
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
