#!/usr/bin/env python3
"""Recompute and optionally publish canonical entity attribute regions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_backfill_payload
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    source = read_parquet_table("entity_attributes_static", cache_root=settings.cache_root, required=True)
    payload = build_entity_attributes_static_backfill_payload(source.to_dict(orient="records"))
    output = pd.DataFrame(payload)

    if args.write_cache:
        write_parquet_table(
            "entity_attributes_static",
            output,
            cache_root=settings.cache_root,
            mode="replace",
            dedupe_subset=["entity_id"],
        )

    publish_result: dict[str, object] = {"status": "skipped"}
    if args.publish:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        publish_result = publisher.upsert(
            "feature_store.entity_attributes_static",
            payload,
            on_conflict="entity_id",
        )

    summary = {
        "source_rows": int(len(source.index)),
        "backfill_rows": int(len(payload)),
        "regions": output["region"].value_counts(dropna=False).sort_index().to_dict() if not output.empty else {},
        "write_cache": bool(args.write_cache),
        "publish": publish_result,
    }
    print(json.dumps(summary, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill feature_store.entity_attributes_static with canonical country/region mapping."
    )
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--write-cache", action="store_true", help="Replace the cached entity_attributes_static parquet.")
    parser.add_argument("--publish", action="store_true", help="Upsert corrected rows to Postgres.")
    return parser


if __name__ == "__main__":
    main()
