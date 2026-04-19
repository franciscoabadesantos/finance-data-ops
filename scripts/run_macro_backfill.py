#!/usr/bin/env python3
"""Historical macro backfill runner (idempotent)."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from flows.dataops_macro_daily import run_dataops_macro_daily


def _configure_logging(level: str) -> None:
    numeric_level = getattr(logging, str(level).strip().upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run idempotent historical macro backfill.")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD.")
    parser.add_argument("--cache-root", default=None, help="Optional cache root override.")
    parser.add_argument("--max-attempts", type=int, default=3, help="Provider retry attempts.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console log level.",
    )
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
    _configure_logging(args.log_level)
    logger = logging.getLogger("finance_data_ops.scripts.run_macro_backfill")
    logger.info(
        "Starting macro backfill (start=%s end=%s publish_enabled=%s force_recompute=%s max_attempts=%s).",
        args.start_date,
        args.end_date,
        not bool(args.no_publish),
        bool(args.force_recompute),
        int(args.max_attempts),
    )
    summary = run_dataops_macro_daily(
        start=str(args.start_date),
        end=str(args.end_date),
        cache_root=args.cache_root,
        publish_enabled=not bool(args.no_publish),
        max_attempts=int(args.max_attempts),
        raise_on_failed_hard=not bool(args.allow_unhealthy),
        force_recompute=bool(args.force_recompute),
    )
    logger.info(
        "Macro backfill completed (flow_run_id=%s status=%s publish_failures=%s).",
        summary.get("flow_run_id"),
        (summary.get("macro_daily") or {}).get("status"),
        len(summary.get("publish_failures") or []),
    )
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
