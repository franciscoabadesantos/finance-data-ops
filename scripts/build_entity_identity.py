#!/usr/bin/env python
"""Build Entity Layer V0 identity tables side-by-side.

Dry-run is the default. Use --apply to write only the new entity identity tables.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity
from finance_data_ops.identity.resolver import build_entity_identity
from finance_data_ops.identity.universe import (
    fixture_candidate_universe,
    read_local_candidate_universe,
    read_postgres_candidate_universe,
)
from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    symbols = _parse_symbols(args.symbols)

    if args.source == "postgres":
        candidates = read_postgres_candidate_universe(database_dsn=settings.database_dsn, symbols=symbols)
        fixture_mappings = None
    elif args.source == "local":
        candidates = read_local_candidate_universe(cache_root=settings.cache_root, symbols=symbols)
        fixture_mappings = None
    else:
        candidates = fixture_candidate_universe(symbols=symbols)
        fixture_mappings = _example_openfigi_fixtures()

    client = OpenFigiClient(
        fixture_mappings=fixture_mappings,
        dry_run=bool(args.offline),
        batch_size=args.batch_size,
        request_sleep_seconds=args.request_sleep_seconds,
    )
    mappings = client.map_candidates(candidates)
    result = build_entity_identity(candidates=candidates, mappings=mappings)
    summary = result.summary()
    summary["candidate_symbols"] = len(candidates)
    summary["openfigi_cache_rows"] = len(result.openfigi_cache_rows)
    summary["mode"] = "apply" if args.apply else "dry_run"
    summary["source"] = args.source

    if args.apply:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        summary["publish"] = publish_entity_identity(publisher=publisher, result=result)
    else:
        publisher = RecordingPublisher()
        summary["planned_writes"] = {
            "source_cache.openfigi_mapping_raw": len(result.openfigi_cache_rows),
            "feature_store.entity_master": len(result.entities),
            "feature_store.entity_listing": len(result.listings),
            "feature_store.entity_identity_audit": len(result.audits),
        }
        # Exercise payload construction in dry-run without mutating external state.
        publish_entity_identity(publisher=publisher, result=result)

    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build side-by-side Entity Layer V0 identity rows.")
    parser.add_argument("--apply", action="store_true", help="Write new entity identity tables. Omitted means dry-run.")
    parser.add_argument("--symbols", action="append", default=[], help="Optional comma-separated symbol subset.")
    parser.add_argument("--source", choices=["fixtures", "postgres", "local"], default="fixtures")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--offline", action="store_true", help="Do not call OpenFIGI when source is postgres/local.")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--request-sleep-seconds", type=float, default=6.5)
    return parser


def _parse_symbols(values: list[str]) -> list[str]:
    symbols: list[str] = []
    for value in values:
        symbols.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return symbols


def _example_openfigi_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "SAP": _fixture("SAP", "SAP SE", "SAP-LEGAL", "BBG000BB5373", "BBG000BB53Q9", "US", "DE", "USD", "XNYS"),
        "SAP.DE": _fixture("SAP", "SAP SE", "SAP-LEGAL", "BBG000BB5373", "BBG000BB53Q9", "DE", "DE", "EUR", "XETR"),
        "ASML": _fixture("ASML", "ASML HOLDING NV", "ASML-LEGAL", "BBG000D6VW15", "BBG000D6VW24", "US", "NL", "USD", "XNAS"),
        "ASML.AS": _fixture("ASML", "ASML HOLDING NV", "ASML-LEGAL", "BBG000D6VW15", "BBG000D6VW24", "NL", "NL", "EUR", "XAMS"),
        "NVO": _fixture("NVO", "NOVO NORDISK A/S", "NOVO-LEGAL", "BBG000Q1X7V1", "BBG000Q1X7W0", "US", "DK", "USD", "XNYS"),
        "NOVO-B.CO": _fixture("NOVOB", "NOVO NORDISK A/S", "NOVO-LEGAL", "BBG000Q1X7V1", "BBG000Q1X7W0", "DK", "DK", "DKK", "XCSE"),
        "TLS": _fixture("TLS", "TELOS CORP", "TELOS-LEGAL", "BBG00TLSUS01", "BBG00TLSUS02", "US", "US", "USD", "XNYS"),
        "TLS.AX": _fixture("TLS", "TELSTRA GROUP LTD", "TELSTRA-LEGAL", "BBG00TLSAU01", "BBG00TLSAU02", "AU", "AU", "AUD", "XASX"),
        "GOOG": _fixture("GOOG", "ALPHABET INC", "ALPHABET-LEGAL", "BBG009S3NB21", "BBG009S3NB30", "US", "US", "USD", "XNAS"),
        "GOOGL": _fixture("GOOGL", "ALPHABET INC", "ALPHABET-LEGAL", "BBG009S39JX6", "BBG009S39JY5", "US", "US", "USD", "XNAS"),
        "LEN": _fixture("LEN", "LENNAR CORP", "LENNAR-LEGAL", "BBG000C3FGH9", "BBG000C3FGJ8", "US", "US", "USD", "XNYS"),
        "LENB": _fixture("LENB", "LENNAR CORP", "LENNAR-LEGAL", "BBG000C3FHK1", "BBG000C3FHL0", "US", "US", "USD", "XNYS"),
    }


def _fixture(
    ticker: str,
    name: str,
    legal_entity_id: str,
    share_class_figi: str,
    composite_figi: str,
    country: str,
    home_country: str,
    currency: str,
    mic: str,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "name": name,
        "legalEntityId": legal_entity_id,
        "figi": f"{composite_figi}F",
        "compositeFIGI": composite_figi,
        "shareClassFIGI": share_class_figi,
        "country": country,
        "homeCountry": home_country,
        "currency": currency,
        "micCode": mic,
        "securityType2": "Common Stock",
    }


if __name__ == "__main__":
    main()
