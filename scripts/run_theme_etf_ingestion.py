#!/usr/bin/env python3
"""Ingest curated thematic ETF holdings into the canonical ETF holdings cache."""

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
from finance_data_ops.publish.fundamentals import build_etf_holdings_payload
from finance_data_ops.settings import load_settings
from finance_data_ops.theme_etfs.holdings import fetch_theme_etf_holdings, write_theme_etf_outputs


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    holdings, themes, failures = fetch_theme_etf_holdings()
    outputs = write_theme_etf_outputs(holdings=holdings, themes=themes, cache_root=str(settings.cache_root))

    publish_result: dict[str, object] = {"status": "skipped"}
    if args.publish:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        publish_result = {
            "etf_holdings": publisher.upsert(
                "etf_holdings",
                build_etf_holdings_payload(holdings),
                on_conflict="etf_ticker,holding_symbol,as_of",
            ),
            "etf_themes": publisher.upsert(
                "etf_themes",
                themes.to_dict(orient="records"),
                on_conflict="etf_ticker",
            ),
        }

    summary = {
        "surviving_themes": themes[
            [
                "theme",
                "etf_ticker",
                "wave",
                "source_type",
                "holdings_source_depth",
                "holdings_shallow",
                "holdings_count",
                "holdings_as_of",
            ]
        ].to_dict(orient="records"),
        "holdings_rows": int(len(holdings.index)),
        "theme_count": int(themes["theme"].nunique()) if not themes.empty else 0,
        "failures": failures,
        "outputs": outputs,
        "publish": publish_result,
    }
    print(json.dumps(summary, indent=2, default=str))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest thematic ETF holdings.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--publish", action="store_true", help="Publish to Postgres using DATA_OPS_DATABASE_URL.")
    return parser


if __name__ == "__main__":
    main()
