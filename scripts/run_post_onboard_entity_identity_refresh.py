#!/usr/bin/env python
"""Refresh side-by-side Entity Layer identity after onboarding completes.

Dry-run is the default. Entity table writes require --apply-entities, and raw
cache writes require --apply-caches. The Postgres mode always targets the
current tracked universe from feature_store.ticker_readiness.
"""

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

from finance_data_ops.identity.post_onboard_refresh import (
    DEFAULT_POST_ONBOARD_ENTITY_SCOPE_KEY,
    PostOnboardEntityIdentityRefreshOptions,
    run_post_onboard_entity_identity_refresh,
)
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    if args.refresh_cache_misses and not args.refresh_live:
        raise SystemExit("--refresh-cache-misses requires --refresh-live.")
    options = PostOnboardEntityIdentityRefreshOptions(
        source=args.source,
        scope_key=args.scope_key,
        batch_id=args.batch_id,
        apply_caches=bool(args.apply_caches),
        apply_entities=bool(args.apply_entities),
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
    result = run_post_onboard_entity_identity_refresh(options=options, settings=settings)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    if args.apply_entities and result.get("status") == "blocked":
        raise SystemExit(2)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run post-onboard Entity Layer refresh over the tracked universe.")
    parser.add_argument("--source", choices=["postgres", "fixtures"], default="postgres")
    parser.add_argument("--scope-key", default=DEFAULT_POST_ONBOARD_ENTITY_SCOPE_KEY)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--cache-root", default=None)
    parser.add_argument(
        "--curated-identity-file",
        default=None,
        help="Version-controlled reviewed identity decisions JSON. Defaults to data/entity_identity_curated.json.",
    )
    parser.add_argument("--apply-caches", action="store_true", help="Upsert raw fact cache tables.")
    parser.add_argument("--apply-entities", action="store_true", help="Publish side-by-side entity tables if the gate passes.")
    parser.add_argument("--refresh-live", action="store_true", help="Allow live provider calls through the existing clients.")
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


if __name__ == "__main__":
    main()
