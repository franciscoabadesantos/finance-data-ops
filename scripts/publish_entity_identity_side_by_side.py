#!/usr/bin/env python
"""Controlled side-by-side Entity Layer publication.

Dry-run is the default. Cache writes and entity table writes require separate
explicit flags, and entity writes are blocked unless the publication gate is
green for the measured scope.
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

from finance_data_ops.identity.publisher import publish_entity_identity_controlled
from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.settings import load_settings
from scripts.measure_entity_identity_chain import build_entity_identity_measurement


def main() -> None:
    args = _parser().parse_args()
    if not args.refresh_live:
        args.offline = True
    settings = load_settings(cache_root=args.cache_root)
    measurement = build_entity_identity_measurement(args=args, settings=settings)

    if args.apply_caches or args.apply_entities:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
    else:
        publisher = RecordingPublisher()

    result = publish_entity_identity_controlled(
        publisher=publisher,
        measurement=measurement,
        apply_caches=args.apply_caches,
        apply_entities=args.apply_entities,
        batch_id=args.batch_id,
        scope_key=args.scope_key,
    )
    result["source"] = args.source
    result["symbols"] = _parse_symbols(args.symbols)
    print(json.dumps(result, indent=2, sort_keys=True, default=str))

    if args.apply_entities and result.get("status") == "blocked":
        raise SystemExit(2)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Entity Layer side-by-side tables after a green audit gate.")
    parser.add_argument("--symbols", action="append", default=[], help="Optional comma-separated symbol subset.")
    parser.add_argument("--source", choices=["postgres", "fixtures"], default="fixtures")
    parser.add_argument("--offline", action="store_true", help="Do not call live OpenFIGI/yfinance/GLEIF APIs.")
    parser.add_argument("--refresh-live", action="store_true", help="Explicitly allow live OpenFIGI/yfinance/GLEIF refresh.")
    parser.add_argument("--apply-caches", action="store_true", help="Upsert raw fact cache tables before entity publication.")
    parser.add_argument("--apply-entities", action="store_true", help="Upsert side-by-side entity tables. Requires green gate.")
    parser.add_argument("--batch-id", default=None, help="Optional operator-provided publication batch id.")
    parser.add_argument("--scope-key", default="default", help="Current-publication pointer scope.")
    parser.add_argument("--cache-root", default=None)
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
