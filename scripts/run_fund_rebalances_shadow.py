#!/usr/bin/env python3
"""Run manual public-index-announcement rebalance shadow ingestion."""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0, str(ROOT))
from finance_data_ops.settings import load_settings
from finance_data_ops.shadow.fund_rebalances import PostgresFundRebalanceRepository, run_fund_rebalances_shadow
parser = argparse.ArgumentParser(description="Run fund rebalances in manual shadow mode only.")
parser.add_argument("--sources", type=Path, default=ROOT / "data" / "fund_rebalance_sources.json")
parser.add_argument("--dry-run", action="store_true")
parser.add_argument("--refresh", action="store_true")
parser.add_argument("--request-sleep-seconds", type=float, default=0.25)
def main(argv: list[str] | None = None) -> int:
    args = parser.parse_args(argv); settings = load_settings(); repository = None if args.dry_run or not settings.database_dsn else PostgresFundRebalanceRepository(database_dsn=settings.database_dsn)
    print(json.dumps(run_fund_rebalances_shadow(repository=repository, source_config_path=args.sources, dry_run=args.dry_run, refresh=args.refresh, request_sleep_seconds=max(0.0, args.request_sleep_seconds)), indent=2, sort_keys=True, default=str)); return 0
if __name__ == "__main__": raise SystemExit(main())
