#!/usr/bin/env python3
"""Run manual Tiingo ETF/fund distribution shadow ingestion."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))

from finance_data_ops.settings import load_settings
from finance_data_ops.shadow.fund_distributions import PostgresFundDistributionRepository, run_fund_distributions_shadow

parser = argparse.ArgumentParser(description="Run ETF/fund distributions in manual shadow mode only.")
parser.add_argument("--symbols", nargs="+", required=True)
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--refresh", action="store_true")
parser.add_argument("--request-sleep-seconds", type=float, default=0.25)

def main(argv: list[str] | None = None) -> int:
    args = parser.parse_args(argv)
    settings = load_settings()
    repository = None if args.dry_run or not settings.database_dsn else PostgresFundDistributionRepository(database_dsn=settings.database_dsn)
    print(json.dumps(run_fund_distributions_shadow(symbols=args.symbols, repository=repository, dry_run=args.dry_run, refresh=args.refresh, request_sleep_seconds=max(0.0, args.request_sleep_seconds)), indent=2, sort_keys=True, default=str))
    return 0
if __name__ == "__main__": raise SystemExit(main())
