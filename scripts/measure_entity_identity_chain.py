#!/usr/bin/env python
"""Measure listing -> ISIN -> LEI -> entity identity chain.

This command is dry-run first. `--apply-cache` writes only raw cache tables and
never writes feature_store.entity_master or feature_store.entity_listing.
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

from finance_data_ops.identity.chain import (
    acceptance_fixture_candidates,
    acceptance_gleif_fixtures,
    acceptance_isin_fixtures,
    acceptance_openfigi_fixtures,
    acceptance_pairs_for_symbols,
    measure_entity_identity_chain,
)
from finance_data_ops.identity.gleif import GleifIsinLeiClient
from finance_data_ops.identity.isin import YFinanceIsinClient
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity_raw_caches
from finance_data_ops.identity.universe import read_postgres_candidate_universe
from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    symbols = _parse_symbols(args.symbols)

    if args.source == "fixtures":
        candidates = acceptance_fixture_candidates(symbols=symbols)
        openfigi_fixtures = acceptance_openfigi_fixtures()
        isin_fixtures = acceptance_isin_fixtures()
        gleif_fixtures = acceptance_gleif_fixtures()
    else:
        candidates = read_postgres_candidate_universe(database_dsn=settings.database_dsn, symbols=symbols)
        openfigi_fixtures = None
        isin_fixtures = None
        gleif_fixtures = None

    selected_symbols = [candidate.symbol for candidate in candidates]
    openfigi_client = OpenFigiClient(
        fixture_mappings=openfigi_fixtures,
        dry_run=args.offline,
        batch_size=args.batch_size,
        request_sleep_seconds=args.request_sleep_seconds,
    )
    openfigi_mappings = openfigi_client.map_candidates(candidates)
    isin_client = YFinanceIsinClient(fixture_isins=isin_fixtures, offline=args.offline)
    isin_records = isin_client.enrich_candidates(candidates)
    gleif_client = GleifIsinLeiClient(fixture_mappings=gleif_fixtures, offline=args.offline)
    gleif_records = gleif_client.lookup_isins([record.isin for record in isin_records if record.isin])
    measurement = measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        pairs=acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=openfigi_client.batch_split_retries,
    )
    output = measurement.as_dict()
    output["mode"] = "apply_cache" if args.apply_cache else "dry_run"
    output["source"] = args.source
    if args.apply_cache:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        output["publish"] = publish_entity_identity_raw_caches(publisher=publisher, measurement=measurement)
    else:
        publisher = RecordingPublisher()
        output["planned_cache_publish"] = publish_entity_identity_raw_caches(
            publisher=publisher,
            measurement=measurement,
        )

    print(json.dumps(output, indent=2, sort_keys=True, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure Entity Layer V0.1 ISIN-to-LEI identity chain.")
    parser.add_argument("--symbols", action="append", default=[], help="Optional comma-separated symbol subset.")
    parser.add_argument("--source", choices=["postgres", "fixtures"], default="fixtures")
    parser.add_argument("--offline", action="store_true", help="Do not call live OpenFIGI/yfinance/GLEIF APIs.")
    parser.add_argument("--apply-cache", action="store_true", help="Write only raw cache tables. Never writes entity tables.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--request-sleep-seconds", type=float, default=6.5)
    return parser


def _parse_symbols(values: list[str]) -> list[str]:
    symbols: list[str] = []
    for value in values:
        symbols.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return symbols


if __name__ == "__main__":
    main()
