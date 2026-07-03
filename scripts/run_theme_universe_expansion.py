#!/usr/bin/env python3
"""Prepare a capped wave of thematic ETF constituents for ticker registry expansion."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.theme_etfs.universe import build_wave_universe_additions
from finance_data_ops.validation.ticker_registry import upsert_ticker_registry_rows


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    holdings = read_parquet_table("etf_holdings", cache_root=settings.cache_root, required=True)
    themes = read_parquet_table("etf_themes", cache_root=settings.cache_root, required=True)
    ramp_table = f"theme_etf_universe_wave{int(args.wave)}_ramp"
    existing_ramp = read_parquet_table(ramp_table, cache_root=settings.cache_root, required=False)
    batch_offset = (
        int(existing_ramp["theme_ramp_batch"].max())
        if not existing_ramp.empty and "theme_ramp_batch" in existing_ramp.columns
        else 0
    )
    registry_rows, entity_rows, summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        cache_root=settings.cache_root,
        wave=int(args.wave),
        max_new_tickers=int(args.max_new_tickers),
        batch_size=int(args.batch_size),
        batch_offset=batch_offset,
    )

    if not registry_rows.empty:
        # theme_ramp_batch is operational metadata, not part of ticker_registry.
        rows = registry_rows.drop(columns=["theme_ramp_batch"], errors="ignore").to_dict(orient="records")
        upsert_ticker_registry_rows(cache_root=settings.cache_root, rows=rows)
        write_parquet_table(
            ramp_table,
            registry_rows,
            cache_root=settings.cache_root,
            mode="append",
            dedupe_subset=["registry_key"],
        )
    if not entity_rows.empty:
        write_parquet_table(
            "entity_attributes_static",
            entity_rows,
            cache_root=settings.cache_root,
            mode="append",
            dedupe_subset=["entity_id"],
        )

    publish_result: dict[str, object] = {"status": "skipped"}
    if args.publish and not registry_rows.empty:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        publish_result = publish_ticker_registry(
            publisher=publisher,
            rows=registry_rows.drop(columns=["theme_ramp_batch"], errors="ignore").to_dict(orient="records"),
        )

    summary["new_tickers"] = registry_rows["normalized_symbol"].tolist() if not registry_rows.empty else []
    summary["publish"] = publish_result
    print(json.dumps(summary, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare thematic ETF constituent ticker registry additions.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--wave", type=int, default=1, choices=[1, 2])
    parser.add_argument("--max-new-tickers", type=int, default=125)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--publish", action="store_true", help="Publish registry/entity rows to Postgres.")
    return parser


if __name__ == "__main__":
    main()
