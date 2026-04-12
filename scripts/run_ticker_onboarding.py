#!/usr/bin/env python3
"""Scheduler-friendly script wrapper for Data Ops ticker onboarding flow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from flows.prefect_dataops_daily import dataops_ticker_onboarding_flow


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ticker onboarding flow locally.")
    parser.add_argument("input_symbol", type=str, help="Raw ticker symbol to onboard.")
    parser.add_argument("--region", type=str, default="us", help="Region key (us, eu, apac).")
    parser.add_argument("--exchange", type=str, default=None, help="Optional exchange hint.")
    parser.add_argument("--instrument-type-hint", type=str, default=None, help="Optional instrument type hint.")
    parser.add_argument("--start", type=str, default=None, help="Optional market backfill start YYYY-MM-DD.")
    parser.add_argument("--end", type=str, default=None, help="Optional market backfill end YYYY-MM-DD.")
    parser.add_argument("--history-limit", type=int, default=24, help="Backfill earnings history limit.")
    parser.add_argument("--cache-root", type=str, default=None, help="Override local cache root.")
    parser.add_argument("--no-publish", action="store_true", help="Skip remote Supabase publishes.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    result = dataops_ticker_onboarding_flow.fn(
        input_symbol=str(args.input_symbol).strip(),
        region=str(args.region).strip().lower(),
        exchange=args.exchange,
        instrument_type_hint=args.instrument_type_hint,
        start=args.start,
        end=args.end,
        history_limit=int(args.history_limit),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
    )
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
