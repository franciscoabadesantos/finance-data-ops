#!/usr/bin/env python3
"""Run manual SEC-only equity-capital-events shadow ingestion."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finance_data_ops.settings import load_settings
from finance_data_ops.shadow.equity_capital_events import PostgresEquityCapitalEventsRepository, run_equity_capital_events_shadow


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run equity-capital events in manual shadow mode only.")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    settings = load_settings()
    dsn = os.environ.get("DATA_OPS_DATABASE_URL") or settings.database_dsn
    repository = PostgresEquityCapitalEventsRepository(database_dsn=dsn) if not args.dry_run and dsn else None
    print(json.dumps(run_equity_capital_events_shadow(symbols=args.symbols, repository=repository, dry_run=args.dry_run), indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
