#!/usr/bin/env python3
"""Backfill foreign holding symbols and entity names without ingesting new prices."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
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
    country_by_entity = _holding_country_lookup(original_holdings=holdings, normalized_holdings=normalized_holdings)
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
        cleanup_result = _delete_published_replaced_bare_entities(database_dsn=settings.database_dsn)
        publish_result = {
            "etf_holdings": holdings_result,
            "feature_store.entity_attributes_static": entity_result,
            "feature_store.entity_attributes_static_bare_cleanup": cleanup_result,
        }

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


def _holding_country_lookup(*, original_holdings: pd.DataFrame, normalized_holdings: pd.DataFrame) -> dict[str, str]:
    if normalized_holdings.empty or "holding_country" not in normalized_holdings.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in normalized_holdings.iterrows():
        symbol = str(row.get("holding_symbol") or "").strip().upper()
        country = normalize_country(row.get("holding_country"))
        if symbol and country and symbol not in out:
            out[symbol] = country
    if original_holdings.empty or "holding_symbol" not in original_holdings.columns:
        return out
    source_countries = original_holdings.get("holding_country", pd.Series(index=original_holdings.index, dtype=object)).map(
        normalize_country
    )
    for symbol, country in zip(original_holdings["holding_symbol"].tolist(), source_countries.tolist(), strict=False):
        raw_symbol = str(symbol or "").strip().upper()
        if raw_symbol and country and raw_symbol not in out:
            out[raw_symbol] = country
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
            country_hint = (
                country_by_entity.get(original_id)
                or raw.get("country")
                or raw.get("holding_country")
                or raw.get("home_country")
            )
            entity_id = normalize_symbol_with_country(
                original_id,
                country_hint,
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
    bare_before = _bare_numeric_entities(existing_entities)
    bare_after = _bare_numeric_ids(row.get("entity_id") for row in entity_payload)
    duplicate_pairs_before = _replaced_bare_entity_pairs(
        existing_entities.get("entity_id", pd.Series(dtype=object)).dropna().tolist()
        if not existing_entities.empty
        else []
    )
    duplicate_pairs_after = _replaced_bare_entity_pairs(row.get("entity_id") for row in entity_payload)
    placeholder_before = _placeholder_entity_ids(
        existing_entities.get("entity_id", pd.Series(dtype=object)).dropna().tolist()
        if not existing_entities.empty
        else []
    )
    placeholder_after = _placeholder_entity_ids(row.get("entity_id") for row in entity_payload)
    non_iso_before = _raw_non_iso_country_count(existing_entities, columns=("country", "home_country"))
    non_iso_after = _raw_non_iso_country_count(pd.DataFrame(entity_payload), columns=("country", "home_country"))
    null_home_before = _missing_home_country_count(existing_entities)
    null_home_after = _missing_home_country_count(pd.DataFrame(entity_payload))
    return {
        "bare_numeric_entities_before": len(bare_before),
        "bare_numeric_entities_before_examples": bare_before[:20],
        "bare_numeric_entities_after": len(bare_after),
        "bare_numeric_entities_after_examples": bare_after[:20],
        "safe_duplicate_bare_entities_removable": len(duplicate_pairs_before),
        "safe_duplicate_bare_entities_removable_examples": [
            f"{bare}->{normalized}" for bare, normalized in duplicate_pairs_before[:20]
        ],
        "duplicate_bare_entities_after": len(duplicate_pairs_after),
        "duplicate_bare_entities_after_examples": [
            f"{bare}->{normalized}" for bare, normalized in duplicate_pairs_after[:20]
        ],
        "placeholder_entities_before": len(placeholder_before),
        "placeholder_entities_before_examples": placeholder_before[:20],
        "placeholder_entities_after": len(placeholder_after),
        "placeholder_entities_after_examples": placeholder_after[:20],
        "non_iso_country_values_before": non_iso_before,
        "non_iso_country_values_after": non_iso_after,
        "missing_home_country_before": null_home_before,
        "missing_home_country_after": null_home_after,
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


def _delete_published_replaced_bare_entities(*, database_dsn: str) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres publishing.") from exc

    with psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops") as conn:
        with conn.cursor() as cur:
            cur.execute("select entity_id from feature_store.entity_attributes_static")
            entity_ids = [str(row[0]).strip().upper() for row in cur.fetchall() if str(row[0]).strip()]
            pairs = _replaced_bare_entity_pairs(entity_ids)
            placeholder_ids = _placeholder_entity_ids(entity_ids)
            if not pairs and not placeholder_ids:
                return {"status": "ok", "rows": 0, "duplicate_examples": [], "placeholder_examples": []}
            bare_ids = [bare for bare, _ in pairs]
            delete_ids = sorted(set(bare_ids) | set(placeholder_ids))
            placeholders = ", ".join(["%s"] * len(delete_ids))
            cur.execute(
                f"delete from feature_store.entity_attributes_static where entity_id in ({placeholders})",
                delete_ids,
            )
    return {
        "status": "ok",
        "rows": len(delete_ids),
        "duplicate_rows": len(pairs),
        "placeholder_rows": len(placeholder_ids),
        "duplicate_examples": [f"{bare}->{normalized}" for bare, normalized in pairs[:20]],
        "placeholder_examples": placeholder_ids[:20],
    }


def _bare_numeric_entities(frame: pd.DataFrame) -> list[str]:
    if frame.empty or "entity_id" not in frame.columns:
        return []
    return sorted(_bare_numeric_ids(frame["entity_id"].dropna().tolist()))


def _bare_numeric_ids(values: Any) -> list[str]:
    out = {
        str(value).strip().upper()
        for value in values
        if re.fullmatch(r"(?:600\d{3}|000\d{3}|002\d{3}|300\d{3}|\d{1,4})", str(value).strip().upper())
    }
    return sorted(out)


def _placeholder_entity_ids(values: Any) -> list[str]:
    return sorted(
        {
            str(value).strip().upper()
            for value in values
            if is_placeholder_identifier(str(value).strip().upper())
        }
    )


def _replaced_bare_entity_pairs(values: Any) -> list[tuple[str, str]]:
    symbols = {str(value).strip().upper() for value in values if str(value).strip()}
    pairs = {
        (symbol, normalize_listing_symbol(symbol))
        for symbol in symbols
        if _is_safe_entity_cleanup_candidate(symbol)
        and normalize_listing_symbol(symbol)
        and normalize_listing_symbol(symbol) != symbol
        and normalize_listing_symbol(symbol) in symbols
    }
    return sorted(pairs)


def _is_safe_entity_cleanup_candidate(symbol: str) -> bool:
    token = str(symbol or "").strip().upper()
    return token in _bare_numeric_ids([token]) or bool(re.fullmatch(r"\d{1,3}\.HK", token))


def _raw_non_iso_country_count(frame: pd.DataFrame, *, columns: tuple[str, ...]) -> int:
    if frame.empty:
        return 0
    count = 0
    for column in columns:
        if column not in frame.columns:
            continue
        for value in frame[column].dropna().tolist():
            country = str(value).strip().upper()
            if country and not re.fullmatch(r"[A-Z]{2}", country):
                count += 1
    return count


def _missing_home_country_count(frame: pd.DataFrame) -> int:
    if frame.empty or "home_country" not in frame.columns:
        return int(len(frame.index)) if not frame.empty else 0
    normalized = frame["home_country"].map(normalize_country)
    return int(normalized.eq("").sum())


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
