#!/usr/bin/env python3
"""Backfill foreign holding symbols and entity names without ingesting new prices."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.geography import country_from_source_or_symbol, infer_country_from_symbol, normalize_country
from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.fundamentals import build_etf_holdings_payload
from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_backfill_payload
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings
from finance_data_ops.symbology import is_placeholder_identifier, normalize_listing_symbol, normalize_symbol_with_country


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)

    holdings = read_parquet_table("etf_holdings", cache_root=settings.cache_root, required=True)
    entities = read_parquet_table("entity_attributes_static", cache_root=settings.cache_root, required=False)
    prices = read_parquet_table("market_price_daily", cache_root=settings.cache_root, required=False)

    normalized_holdings, holding_summary = _normalize_holdings_table(holdings)
    name_by_entity = _holding_name_lookup(normalized_holdings)
    country_by_entity = _holding_country_lookup(normalized_holdings)
    entity_rows = _entity_backfill_source_rows(
        entities=entities,
        name_by_entity=name_by_entity,
        country_by_entity=country_by_entity,
    )
    entity_payload = build_entity_attributes_static_backfill_payload(entity_rows, name_by_entity=name_by_entity)
    counts = _backfill_counts(
        original_holdings=holdings,
        normalized_holdings=normalized_holdings,
        existing_entities=entities,
        entity_payload=entity_payload,
        prices=prices,
    )

    if args.write_cache:
        write_parquet_table(
            "etf_holdings",
            normalized_holdings,
            cache_root=settings.cache_root,
            mode="replace",
            dedupe_subset=["etf_ticker", "holding_symbol", "as_of"],
        )
        write_parquet_table(
            "entity_attributes_static",
            pd.DataFrame(entity_payload),
            cache_root=settings.cache_root,
            mode="replace",
            dedupe_subset=["entity_id"],
        )

    publish_result: dict[str, object] = {"status": "skipped"}
    if args.publish:
        settings.require_database()
        publisher = PostgresPublisher(database_dsn=settings.database_dsn)
        affected_etfs = sorted({str(value).strip().upper() for value in normalized_holdings["etf_ticker"].dropna()})
        _delete_published_etf_holdings(database_dsn=settings.database_dsn, etf_tickers=affected_etfs)
        holdings_result = publisher.upsert(
            "etf_holdings",
            build_etf_holdings_payload(normalized_holdings),
            on_conflict="etf_ticker,holding_symbol,as_of",
        )
        entity_result = publisher.upsert(
            "feature_store.entity_attributes_static",
            entity_payload,
            on_conflict="entity_id",
        )
        publish_result = {"etf_holdings": holdings_result, "feature_store.entity_attributes_static": entity_result}

    summary = {
        **holding_summary,
        **counts,
        "write_cache": bool(args.write_cache),
        "publish": publish_result,
    }
    print(json.dumps(summary, indent=2, default=str))


def _normalize_holdings_table(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if frame.empty:
        return frame.copy(), {"source_holding_rows": 0, "normalized_holding_rows": 0, "placeholder_rows_dropped": 0}

    out = frame.copy()
    original = out["holding_symbol"].astype(str).str.upper()
    source_countries = out.get("holding_country", pd.Series(index=out.index, dtype=object)).map(normalize_country)
    normalized = pd.Series(
        [
            normalize_symbol_with_country(symbol, country)
            for symbol, country in zip(original.tolist(), source_countries.tolist(), strict=False)
        ],
        index=out.index,
    )
    placeholder_mask = original.map(is_placeholder_identifier) | normalized.map(is_placeholder_identifier)
    changed_pairs = sorted(
        {
            (str(before).strip().upper(), str(after).strip().upper())
            for before, after in zip(original.tolist(), normalized.tolist(), strict=False)
            if str(before).strip().upper() != str(after).strip().upper()
        }
    )
    out = out.loc[~placeholder_mask].copy()
    normalized = normalized.loc[~placeholder_mask]
    source_countries = source_countries.loc[~placeholder_mask]
    out["holding_symbol"] = normalized
    out["holding_country"] = [
        country_from_source_or_symbol(country, symbol)
        for country, symbol in zip(source_countries.tolist(), normalized.tolist(), strict=False)
    ]
    out = out.loc[out["holding_symbol"].astype(str).str.strip().ne("")]
    out = out.drop_duplicates(subset=["etf_ticker", "holding_symbol", "as_of"], keep="last").reset_index(drop=True)
    return out, {
        "source_holding_rows": int(len(frame.index)),
        "normalized_holding_rows": int(len(out.index)),
        "holding_symbols_normalized": len(changed_pairs),
        "holding_symbols_normalized_examples": [f"{before}->{after}" for before, after in changed_pairs[:20]],
        "placeholder_rows_dropped": int(placeholder_mask.sum()),
    }


def _holding_name_lookup(holdings: pd.DataFrame) -> dict[str, str]:
    if holdings.empty:
        return {}
    out: dict[str, str] = {}
    for _, row in holdings.iterrows():
        symbol = str(row.get("holding_symbol") or "").strip().upper()
        name = _text(row.get("holding_name"))
        if symbol and name and symbol not in out:
            out[symbol] = name
    return out


def _holding_country_lookup(holdings: pd.DataFrame) -> dict[str, str]:
    if holdings.empty or "holding_country" not in holdings.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in holdings.iterrows():
        symbol = str(row.get("holding_symbol") or "").strip().upper()
        country = normalize_country(row.get("holding_country"))
        if symbol and country and symbol not in out:
            out[symbol] = country
    return out


def _entity_backfill_source_rows(
    *,
    entities: pd.DataFrame,
    name_by_entity: dict[str, str],
    country_by_entity: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    entity_ids: set[str] = set()
    if not entities.empty:
        sortable_rows: list[tuple[bool, bool, dict[str, Any]]] = []
        for raw in entities.to_dict(orient="records"):
            original_id = str(raw.get("entity_id") or "").strip().upper()
            entity_id = normalize_symbol_with_country(
                original_id,
                raw.get("country") or raw.get("holding_country") or raw.get("home_country"),
            )
            if not entity_id or is_placeholder_identifier(entity_id):
                continue
            row = dict(raw)
            row["entity_id"] = entity_id
            if entity_id in country_by_entity:
                row["holding_country"] = country_by_entity[entity_id]
            sortable_rows.append((original_id == entity_id, bool(_text(row.get("name"))), row))
        for _, _, row in sorted(sortable_rows, key=lambda item: (item[0], item[1])):
            entity_id = str(row["entity_id"]).strip().upper()
            entity_ids.add(entity_id)
            rows.append(row)

    for entity_id, name in sorted(name_by_entity.items()):
        if entity_id in entity_ids:
            continue
        country = country_by_entity.get(entity_id) or infer_country_from_symbol(entity_id)
        rows.append({"entity_id": entity_id, "country": country, "holding_country": country, "name": name})
    return rows


def _backfill_counts(
    *,
    original_holdings: pd.DataFrame,
    normalized_holdings: pd.DataFrame,
    existing_entities: pd.DataFrame,
    entity_payload: list[dict[str, Any]],
    prices: pd.DataFrame,
) -> dict[str, Any]:
    priced_symbols = _priced_symbols(prices)
    original_symbols = {
        str(value).strip().upper()
        for value in original_holdings.get("holding_symbol", pd.Series(dtype=object)).dropna().tolist()
        if str(value).strip()
    }
    normalized_symbols = {
        str(value).strip().upper()
        for value in normalized_holdings.get("holding_symbol", pd.Series(dtype=object)).dropna().tolist()
        if str(value).strip()
    }
    reconciled = sorted(
        symbol
        for symbol in normalized_symbols
        if symbol in priced_symbols and symbol not in original_symbols and "." in symbol
    )

    existing_name_by_entity = {}
    if not existing_entities.empty and "entity_id" in existing_entities.columns:
        for _, row in existing_entities.iterrows():
            entity_id = normalize_listing_symbol(row.get("entity_id"))
            name = _text(row.get("name"))
            if entity_id and name:
                existing_name_by_entity[entity_id] = name
    newly_named = sorted(
        str(row.get("entity_id")).strip().upper()
        for row in entity_payload
        if _text(row.get("name")) and not existing_name_by_entity.get(str(row.get("entity_id")).strip().upper())
    )
    existing_country_by_entity = {}
    existing_home_country_by_entity = {}
    if not existing_entities.empty and "entity_id" in existing_entities.columns:
        for _, row in existing_entities.iterrows():
            entity_id = normalize_symbol_with_country(
                row.get("entity_id"),
                row.get("country") or row.get("holding_country") or row.get("home_country"),
            )
            if entity_id:
                existing_country_by_entity[entity_id] = normalize_country(row.get("country"))
                existing_home_country_by_entity[entity_id] = normalize_country(row.get("home_country"))
    us_listing_corrected = sorted(
        str(row.get("entity_id")).strip().upper()
        for row in entity_payload
        if existing_country_by_entity.get(str(row.get("entity_id")).strip().upper()) == "US"
        and normalize_country(row.get("country")) not in {"", "US"}
    )
    us_home_corrected = sorted(
        str(row.get("entity_id")).strip().upper()
        for row in entity_payload
        if existing_country_by_entity.get(str(row.get("entity_id")).strip().upper()) == "US"
        and existing_home_country_by_entity.get(str(row.get("entity_id")).strip().upper()) in {"", "US"}
        and normalize_country(row.get("home_country")) not in {"", "US"}
    )
    return {
        "foreign_symbols_reconciled_to_priced_tickers": len(reconciled),
        "foreign_symbols_reconciled_examples": reconciled[:20],
        "entities_that_gained_name": len(newly_named),
        "entities_that_gained_name_examples": newly_named[:20],
        "us_misclassified_listing_country_corrected": len(us_listing_corrected),
        "us_misclassified_listing_country_corrected_examples": us_listing_corrected[:20],
        "us_listed_home_country_corrected": len(us_home_corrected),
        "us_listed_home_country_corrected_examples": us_home_corrected[:20],
    }


def _priced_symbols(prices: pd.DataFrame) -> set[str]:
    if prices.empty:
        return set()
    column = "ticker" if "ticker" in prices.columns else "symbol" if "symbol" in prices.columns else None
    if column is None:
        return set()
    return {normalize_listing_symbol(value) for value in prices[column].dropna().tolist() if normalize_listing_symbol(value)}


def _delete_published_etf_holdings(*, database_dsn: str, etf_tickers: list[str]) -> None:
    if not etf_tickers:
        return
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres publishing.") from exc
    placeholders = ", ".join(["%s"] * len(etf_tickers))
    with psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops") as conn:
        with conn.cursor() as cur:
            cur.execute(f"delete from public.etf_holdings where etf_ticker in ({placeholders})", etf_tickers)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NONE", "NULL", "NAN"}:
        return None
    return text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill foreign ETF holding symbols and entity names from cached holdings."
    )
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--write-cache", action="store_true", help="Replace cached etf_holdings/entity attributes.")
    parser.add_argument("--publish", action="store_true", help="Replace published ETF holdings and upsert entity names.")
    return parser


if __name__ == "__main__":
    main()
