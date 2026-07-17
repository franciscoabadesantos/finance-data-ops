#!/usr/bin/env python
"""Read-only entity-aware dedup audit for frontier/onboarding candidates."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finance_data_ops.identity.frontier_dedup import (
    DEFAULT_FRONTIER_DEDUP_SCOPE_KEY,
    FrontierDedupOptions,
    run_frontier_entity_dedup_audit,
)
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    symbols = tuple(_parse_symbols(args.symbols))
    if args.refresh_cache_misses and not args.refresh_live:
        raise SystemExit("--refresh-cache-misses requires --refresh-live.")
    options = FrontierDedupOptions(
        source=args.source,
        scope_key=args.scope_key,
        symbols=symbols,
        candidates_file=args.candidates_file,
        candidate_source=args.candidate_source,
        refresh_live=bool(args.refresh_live),
        refresh_cache_misses=bool(args.refresh_cache_misses),
        cache_root=args.cache_root,
        curated_identity_file=args.curated_identity_file,
        batch_size=args.batch_size,
        request_sleep_seconds=args.request_sleep_seconds,
        gleif_page_size=args.gleif_page_size,
        gleif_request_sleep_seconds=args.gleif_request_sleep_seconds,
        gleif_lei_isin_max_retries=args.gleif_lei_isin_max_retries,
        gleif_retry_backoff_seconds=args.gleif_retry_backoff_seconds,
        gleif_retry_jitter_seconds=args.gleif_retry_jitter_seconds,
    )
    settings = load_settings(cache_root=args.cache_root)
    result = run_frontier_entity_dedup_audit(options=options, settings=settings)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit frontier candidates against the current tracked Entity Layer.")
    parser.add_argument("--source", choices=["postgres", "fixtures"], default="postgres")
    parser.add_argument("--scope-key", default=DEFAULT_FRONTIER_DEDUP_SCOPE_KEY)
    parser.add_argument("--symbols", action="append", default=[], help="Comma-separated frontier candidate symbols.")
    parser.add_argument("--candidates-file", default=None, help="JSON or CSV candidate file.")
    parser.add_argument(
        "--candidate-source",
        choices=["symbols", "etf-holding-identity"],
        default="symbols",
        help="Postgres frontier candidate source when --symbols/--candidates-file are not supplied.",
    )
    parser.add_argument("--cache-root", default=None)
    parser.add_argument(
        "--curated-identity-file",
        default=None,
        help="Version-controlled reviewed identity decisions JSON. Defaults to data/entity_identity_curated.json.",
    )
    parser.add_argument("--refresh-live", action="store_true", help="Allow live provider refresh through existing clients.")
    parser.add_argument(
        "--refresh-cache-misses",
        action="store_true",
        help="With --refresh-live, call providers only for raw-cache misses.",
    )
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--request-sleep-seconds", type=float, default=6.5)
    parser.add_argument("--gleif-page-size", type=int, default=200)
    parser.add_argument("--gleif-request-sleep-seconds", type=float, default=0.5)
    parser.add_argument("--gleif-lei-isin-max-retries", type=int, default=3)
    parser.add_argument("--gleif-retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--gleif-retry-jitter-seconds", type=float, default=0.25)
    return parser


def _parse_symbols(values: list[str]) -> list[str]:
    symbols: list[str] = []
    for value in values:
        symbols.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return symbols


if __name__ == "__main__":
    main()
