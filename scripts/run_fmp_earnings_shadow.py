#!/usr/bin/env python3
"""Run manual FMP earnings shadow ingestion; never changes canonical earnings."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finance_data_ops.settings import load_settings
from finance_data_ops.shadow.fmp_earnings import PostgresFmpEarningsShadowRepository, run_fmp_earnings_shadow


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run FMP earnings in manual shadow mode only.")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker symbols, for example AAPL MSFT.")
    parser.add_argument("--dry-run", action="store_true", help="Emit a plan without HTTP calls or DB writes.")
    parser.add_argument("--refresh", action="store_true", help="Bypass successful/not-found raw cache entries.")
    parser.add_argument("--request-sleep-seconds", type=float, default=0.25, help="Pause between live FMP requests.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = load_settings()
    repository = None
    # The runner returns a clean skipped payload before touching the database if
    # the key/allowlist gate is not open.
    if not args.dry_run and settings.database_dsn:
        repository = PostgresFmpEarningsShadowRepository(database_dsn=settings.database_dsn)
    report = run_fmp_earnings_shadow(
        symbols=args.symbols,
        repository=repository,
        dry_run=bool(args.dry_run),
        refresh=bool(args.refresh),
        request_sleep_seconds=max(0.0, float(args.request_sleep_seconds)),
    )
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
