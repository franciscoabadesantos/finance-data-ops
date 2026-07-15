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
    acceptance_gleif_legal_name_fixtures,
    acceptance_gleif_lei_isin_fixtures,
    acceptance_isin_fixtures,
    acceptance_openfigi_fixtures,
    acceptance_pairs_for_symbols,
    measure_entity_identity_chain,
)
from finance_data_ops.identity.gleif import GleifIsinLeiClient
from finance_data_ops.identity.isin import YFinanceIsinClient, isin_prefix_policy_for_listing
from finance_data_ops.identity.names import legal_name_query_variants_from_listing, normalize_legal_name_conservative
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity_raw_caches
from finance_data_ops.identity.raw_cache import (
    missing_candidate_symbols_from_isins,
    missing_candidate_symbols_from_openfigi,
    missing_record_keys,
    merge_records_by_key,
    read_postgres_raw_cache_snapshot,
)
from finance_data_ops.identity.universe import read_postgres_candidate_universe
from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.settings import load_settings

_PREFIX_MISMATCH_ISIN_REASONS = {
    "provider_returned_alternate_market_instrument",
    "provider_listing_mismatch",
    "isin_prefix_mismatch",
}


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    measurement = build_entity_identity_measurement(args=args, settings=settings)
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


def build_entity_identity_measurement(*, args, settings=None):
    settings = settings or load_settings(cache_root=getattr(args, "cache_root", None))
    symbols = _parse_symbols(args.symbols)

    if args.source == "fixtures":
        candidates = acceptance_fixture_candidates(symbols=symbols)
        openfigi_fixtures = acceptance_openfigi_fixtures()
        isin_fixtures = acceptance_isin_fixtures()
        gleif_fixtures = acceptance_gleif_fixtures()
        gleif_lei_isin_fixtures = acceptance_gleif_lei_isin_fixtures()
        gleif_legal_name_fixtures = acceptance_gleif_legal_name_fixtures()
    else:
        candidates = read_postgres_candidate_universe(
            database_dsn=settings.database_dsn,
            symbols=symbols,
            tracked_only=bool(getattr(args, "tracked_only", False)),
        )
        openfigi_fixtures = None
        isin_fixtures = None
        gleif_fixtures = None
        gleif_lei_isin_fixtures = None
        gleif_legal_name_fixtures = None

    selected_symbols = [candidate.symbol for candidate in candidates]
    if getattr(args, "use_raw_cache", False):
        return _build_measurement_with_raw_cache(
            args=args,
            candidates=candidates,
            selected_symbols=selected_symbols,
            settings=settings,
        )

    openfigi_client = OpenFigiClient(
        fixture_mappings=openfigi_fixtures,
        dry_run=args.offline,
        batch_size=args.batch_size,
        request_sleep_seconds=args.request_sleep_seconds,
    )
    openfigi_mappings = openfigi_client.map_candidates(candidates)
    isin_client = YFinanceIsinClient(fixture_isins=isin_fixtures, offline=args.offline)
    isin_records = isin_client.enrich_candidates(candidates)
    gleif_fixture_mappings = {}
    if gleif_fixtures:
        gleif_fixture_mappings.update(gleif_fixtures)
    if gleif_lei_isin_fixtures:
        gleif_fixture_mappings.update(gleif_lei_isin_fixtures)
    if gleif_legal_name_fixtures:
        gleif_fixture_mappings.update(gleif_legal_name_fixtures)
    gleif_client = GleifIsinLeiClient(
        fixture_mappings=gleif_fixture_mappings or None,
        offline=args.offline,
        page_size=args.gleif_page_size,
        request_sleep_seconds=args.gleif_request_sleep_seconds,
        lei_isin_max_retries=args.gleif_lei_isin_max_retries,
        retry_backoff_seconds=args.gleif_retry_backoff_seconds,
        retry_jitter_seconds=args.gleif_retry_jitter_seconds,
    )
    gleif_records = gleif_client.lookup_isins(_gleif_lookup_isins(isin_records))
    direct_lei_by_isin = {record.isin: record.lei for record in gleif_records if record.lei and record.status == "success"}
    openfigi_by_symbol = {mapping.symbol: mapping for mapping in openfigi_mappings}
    candidates_by_symbol = {candidate.symbol: candidate for candidate in candidates}
    direct_lei_symbols = {
        record.symbol
        for record in isin_records
        if _direct_isin_can_skip_legal_name(
            candidate=candidates_by_symbol.get(record.symbol),
            record=record,
            direct_lei_by_isin=direct_lei_by_isin,
        )
    }
    legal_name_records = gleif_client.search_legal_names(
        [
            query
            for candidate in candidates
            if candidate.symbol not in direct_lei_symbols
            for query in legal_name_query_variants_from_listing(
                (openfigi_by_symbol.get(candidate.symbol).name if openfigi_by_symbol.get(candidate.symbol) else ""),
                candidate.name,
            )
        ]
    )
    legal_name_candidate_leis = [
        candidate["lei"]
        for record in legal_name_records
        for candidate in record.candidates
        if candidate.get("lei")
    ]
    gleif_lei_expansion_plan = _gleif_lei_expansion_lookup_plan(
        isin_records=isin_records,
        candidates_by_symbol=candidates_by_symbol,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=legal_name_candidate_leis,
    )
    gleif_lei_isin_records = gleif_client.lookup_lei_isins(gleif_lei_expansion_plan["request_leis"])
    measurement = measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        gleif_lei_isin_records=gleif_lei_isin_records,
        gleif_legal_name_records=legal_name_records,
        gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
        gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
        gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
        pairs=acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=openfigi_client.batch_split_retries,
    )
    audit_forward_isins = _audit_forward_lookup_isins(measurement, gleif_records)
    if audit_forward_isins:
        gleif_records = _merge_gleif_records(gleif_records, gleif_client.lookup_isins(audit_forward_isins))
        measurement = measure_entity_identity_chain(
            candidates=candidates,
            openfigi_mappings=openfigi_mappings,
            isin_records=isin_records,
            gleif_records=gleif_records,
            gleif_lei_isin_records=gleif_lei_isin_records,
            gleif_legal_name_records=legal_name_records,
            gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
            gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
            gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
            pairs=acceptance_pairs_for_symbols(selected_symbols),
            batch_split_retries=openfigi_client.batch_split_retries,
        )
    return measurement


def _build_measurement_with_raw_cache(*, args, candidates, selected_symbols: list[str], settings):
    raw_cache = read_postgres_raw_cache_snapshot(database_dsn=settings.database_dsn, candidates=candidates)
    live_miss_refresh = bool(getattr(args, "refresh_cache_misses", False)) and not bool(args.offline)

    openfigi_mappings = raw_cache.openfigi_mappings_for_candidates(candidates)
    batch_split_retries = 0
    if live_miss_refresh:
        missing_symbols = missing_candidate_symbols_from_openfigi(openfigi_mappings)
        live_candidates = [candidate for candidate in candidates if candidate.symbol in missing_symbols]
        live_openfigi_client = OpenFigiClient(
            dry_run=False,
            batch_size=args.batch_size,
            request_sleep_seconds=args.request_sleep_seconds,
        )
        openfigi_mappings = merge_records_by_key(
            openfigi_mappings,
            live_openfigi_client.map_candidates(live_candidates),
            "symbol",
        )
        batch_split_retries = live_openfigi_client.batch_split_retries

    isin_records = raw_cache.isin_records_for_candidates(candidates)
    if live_miss_refresh:
        missing_symbols = missing_candidate_symbols_from_isins(isin_records)
        live_candidates = [candidate for candidate in candidates if candidate.symbol in missing_symbols]
        isin_records = merge_records_by_key(
            isin_records,
            YFinanceIsinClient(offline=False).enrich_candidates(live_candidates),
            "symbol",
        )

    gleif_client = GleifIsinLeiClient(
        offline=not live_miss_refresh,
        page_size=args.gleif_page_size,
        request_sleep_seconds=args.gleif_request_sleep_seconds,
        lei_isin_max_retries=args.gleif_lei_isin_max_retries,
        retry_backoff_seconds=args.gleif_retry_backoff_seconds,
        retry_jitter_seconds=args.gleif_retry_jitter_seconds,
    )
    gleif_records = raw_cache.gleif_records_for_isins(_gleif_lookup_isins(isin_records))
    if live_miss_refresh:
        missing_isins = missing_record_keys(gleif_records, "isin")
        gleif_records = merge_records_by_key(gleif_records, gleif_client.lookup_isins(missing_isins), "isin")

    direct_lei_by_isin = {record.isin: record.lei for record in gleif_records if record.lei and record.status == "success"}
    openfigi_by_symbol = {mapping.symbol: mapping for mapping in openfigi_mappings}
    candidates_by_symbol = {candidate.symbol: candidate for candidate in candidates}
    direct_lei_symbols = {
        record.symbol
        for record in isin_records
        if _direct_isin_can_skip_legal_name(
            candidate=candidates_by_symbol.get(record.symbol),
            record=record,
            direct_lei_by_isin=direct_lei_by_isin,
        )
    }
    legal_name_queries = [
        query
        for candidate in candidates
        if candidate.symbol not in direct_lei_symbols
        for query in legal_name_query_variants_from_listing(
            (openfigi_by_symbol.get(candidate.symbol).name if openfigi_by_symbol.get(candidate.symbol) else ""),
            candidate.name,
        )
    ]
    legal_name_records = raw_cache.legal_name_records_for_queries(legal_name_queries)
    if live_miss_refresh:
        successful_names = {record.normalized_query_name for record in legal_name_records if record.status == "success"}
        live_queries = [
            query
            for query in legal_name_queries
            if query and normalize_legal_name_conservative(query) not in successful_names
        ]
        legal_name_records = merge_records_by_key(
            legal_name_records,
            gleif_client.search_legal_names(live_queries),
            "normalized_query_name",
        )

    legal_name_candidate_leis = [
        candidate["lei"]
        for record in legal_name_records
        for candidate in record.candidates
        if candidate.get("lei")
    ]
    gleif_lei_expansion_plan = _gleif_lei_expansion_lookup_plan(
        isin_records=isin_records,
        candidates_by_symbol=candidates_by_symbol,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=legal_name_candidate_leis,
    )
    gleif_lei_isin_records = raw_cache.gleif_lei_isin_records_for_leis(gleif_lei_expansion_plan["request_leis"])
    if live_miss_refresh:
        missing_leis = missing_record_keys(gleif_lei_isin_records, "lei")
        gleif_lei_isin_records = merge_records_by_key(
            gleif_lei_isin_records,
            gleif_client.lookup_lei_isins(missing_leis),
            "lei",
        )

    measurement = measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        gleif_lei_isin_records=gleif_lei_isin_records,
        gleif_legal_name_records=legal_name_records,
        gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
        gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
        gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
        pairs=acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=batch_split_retries,
    )
    audit_forward_isins = _audit_forward_lookup_isins(measurement, gleif_records)
    if audit_forward_isins:
        audit_records = raw_cache.gleif_records_for_isins(audit_forward_isins)
        if live_miss_refresh:
            missing_isins = missing_record_keys(audit_records, "isin")
            audit_records = merge_records_by_key(audit_records, gleif_client.lookup_isins(missing_isins), "isin")
        gleif_records = _merge_gleif_records(gleif_records, audit_records)
        measurement = measure_entity_identity_chain(
            candidates=candidates,
            openfigi_mappings=openfigi_mappings,
            isin_records=isin_records,
            gleif_records=gleif_records,
            gleif_lei_isin_records=gleif_lei_isin_records,
            gleif_legal_name_records=legal_name_records,
            gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
            gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
            gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
            pairs=acceptance_pairs_for_symbols(selected_symbols),
            batch_split_retries=batch_split_retries,
        )
    measurement.summary.update(
        raw_cache.coverage_summary(
            candidates=candidates,
            openfigi_mappings=openfigi_mappings,
            isin_records=isin_records,
            gleif_records=gleif_records,
            gleif_lei_isin_records=gleif_lei_isin_records,
            legal_name_records=legal_name_records,
            tracked_only=bool(getattr(args, "tracked_only", False)),
        )
    )
    measurement.summary["raw_cache_refresh_misses_enabled"] = live_miss_refresh
    return measurement


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Measure Entity Layer V0.2 anchor ISIN-to-LEI expansion identity chain.")
    parser.add_argument("--symbols", action="append", default=[], help="Optional comma-separated symbol subset.")
    parser.add_argument("--source", choices=["postgres", "fixtures"], default="fixtures")
    parser.add_argument("--offline", action="store_true", help="Do not call live OpenFIGI/yfinance/GLEIF APIs.")
    parser.add_argument("--use-raw-cache", action="store_true", help="Read source_cache raw facts before using live providers.")
    parser.add_argument(
        "--refresh-cache-misses",
        action="store_true",
        help="With --use-raw-cache and live mode, call providers only for cache misses.",
    )
    parser.add_argument(
        "--tracked-only",
        action="store_true",
        help="For --source postgres, use feature_store.ticker_readiness rows where is_tracked is true.",
    )
    parser.add_argument("--apply-cache", action="store_true", help="Write only raw cache tables. Never writes entity tables.")
    parser.add_argument("--cache-root", default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--request-sleep-seconds", type=float, default=6.5)
    parser.add_argument("--gleif-page-size", type=int, default=200, help="GLEIF page[size] for LEI-to-ISIN expansion.")
    parser.add_argument(
        "--gleif-request-sleep-seconds",
        type=float,
        default=0.5,
        help="Sleep between GLEIF LEI-to-ISIN expansion requests.",
    )
    parser.add_argument("--gleif-lei-isin-max-retries", type=int, default=3)
    parser.add_argument("--gleif-retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--gleif-retry-jitter-seconds", type=float, default=0.25)
    return parser


def _parse_symbols(values: list[str]) -> list[str]:
    symbols: list[str] = []
    for value in values:
        symbols.extend(part.strip().upper() for part in str(value).split(",") if part.strip())
    return symbols


def _direct_isin_can_skip_legal_name(*, candidate, record, direct_lei_by_isin: dict[str, str]) -> bool:
    if not candidate or not record.isin or record.status != "success" or not direct_lei_by_isin.get(record.isin):
        return False
    allowed_prefixes = set(isin_prefix_policy_for_listing(candidate)["allowed_isin_prefixes"])
    return bool(allowed_prefixes and record.isin[:2] in allowed_prefixes)


def _gleif_lookup_isins(isin_records) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for record in isin_records:
        if not record.isin:
            continue
        should_lookup = record.status == "success" or (
            record.status == "suspect" and record.error_message in _PREFIX_MISMATCH_ISIN_REASONS
        )
        if should_lookup and record.isin not in seen:
            out.append(record.isin)
            seen.add(record.isin)
    return out


def _audit_forward_lookup_isins(measurement, gleif_records) -> list[str]:
    existing = {str(record.isin or "").strip().upper() for record in gleif_records if str(record.isin or "").strip()}
    out: list[str] = []
    seen: set[str] = set(existing)
    for audit_row in measurement.heuristic_attach_audit:
        if audit_row.get("attach_audit_kind") != "heuristic":
            continue
        candidate_isins = []
        candidate_isins.extend(audit_row.get("matched_compatible_isins") or [])
        raw_isin = audit_row.get("raw_isin")
        if raw_isin:
            candidate_isins.append(raw_isin)
        listing_isin = audit_row.get("isin")
        if listing_isin:
            candidate_isins.append(listing_isin)
        for isin in candidate_isins:
            normalized = str(isin or "").strip().upper()
            if normalized and normalized not in seen:
                out.append(normalized)
                seen.add(normalized)
    return out


def _merge_gleif_records(existing_records, new_records):
    by_isin = {
        str(record.isin or "").strip().upper(): record
        for record in existing_records
        if str(record.isin or "").strip()
    }
    for record in new_records:
        isin = str(record.isin or "").strip().upper()
        if isin:
            by_isin[isin] = record
    return list(by_isin.values())


def _gleif_lei_expansion_lookup_leis(
    *,
    isin_records,
    candidates_by_symbol,
    direct_lei_by_isin: dict[str, str],
    legal_name_candidate_leis: list[str],
) -> list[str]:
    return _gleif_lei_expansion_lookup_plan(
        isin_records=isin_records,
        candidates_by_symbol=candidates_by_symbol,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=legal_name_candidate_leis,
    )["request_leis"]


def _gleif_lei_expansion_lookup_plan(
    *,
    isin_records,
    candidates_by_symbol,
    direct_lei_by_isin: dict[str, str],
    legal_name_candidate_leis: list[str],
) -> dict[str, object]:
    out: list[str] = []
    seen: set[str] = set()
    origin_leis = {
        "prefix_compatible_direct_anchor": [],
        "legal_name_candidate": [],
        "prefix_mismatch_isolated_candidate": [],
    }
    excluded_origin_leis = {"prefix_mismatch_isolated_candidate": []}
    for record in isin_records:
        if not _direct_isin_can_skip_legal_name(
            candidate=candidates_by_symbol.get(record.symbol),
            record=record,
            direct_lei_by_isin=direct_lei_by_isin,
        ):
            if record.status == "suspect" and record.error_message in _PREFIX_MISMATCH_ISIN_REASONS:
                mismatch_lei = direct_lei_by_isin.get(record.isin)
                if mismatch_lei and mismatch_lei not in excluded_origin_leis["prefix_mismatch_isolated_candidate"]:
                    excluded_origin_leis["prefix_mismatch_isolated_candidate"].append(mismatch_lei)
            continue
        lei = direct_lei_by_isin.get(record.isin)
        if lei and lei not in seen:
            out.append(lei)
            seen.add(lei)
            origin_leis["prefix_compatible_direct_anchor"].append(lei)
    for lei in legal_name_candidate_leis:
        if lei and lei not in origin_leis["legal_name_candidate"]:
            origin_leis["legal_name_candidate"].append(lei)
        if lei and lei not in seen:
            out.append(lei)
            seen.add(lei)
    return {"request_leis": out, "origin_leis": origin_leis, "excluded_origin_leis": excluded_origin_leis}


if __name__ == "__main__":
    main()
