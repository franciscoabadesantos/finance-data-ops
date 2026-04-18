#!/usr/bin/env python3
"""Run macro + release-calendar historical backfill in one idempotent command."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from flows.dataops_macro_daily import run_dataops_macro_daily
from flows.dataops_release_calendar_daily import run_dataops_release_calendar_daily


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run macro + release-calendar idempotent backfill.")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD.")
    parser.add_argument("--cache-root", default=None, help="Optional cache root override.")
    parser.add_argument("--max-attempts", type=int, default=3, help="Provider retry attempts.")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Release-calendar provider pacing.")
    parser.add_argument("--official-start-year", type=int, default=None)
    parser.add_argument("--official-end-year", type=int, default=None)
    parser.add_argument("--no-publish", action="store_true", help="Skip canonical table upserts.")
    parser.add_argument("--allow-unhealthy", action="store_true", help="Do not raise on unhealthy status.")
    parser.add_argument(
        "--force-recompute",
        action="store_true",
        help="Rewrite local parquet slices before publish while keeping DB writes as deterministic upserts.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    macro_summary = run_dataops_macro_daily(
        start=str(args.start_date),
        end=str(args.end_date),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        force_recompute=bool(args.force_recompute),
    )
    release_summary = run_dataops_release_calendar_daily(
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        force_recompute=bool(args.force_recompute),
        sleep_seconds=float(args.sleep_seconds),
        official_start_year=args.official_start_year,
        official_end_year=args.official_end_year,
    )
    print(json.dumps({"macro": macro_summary, "release_calendar": release_summary}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
