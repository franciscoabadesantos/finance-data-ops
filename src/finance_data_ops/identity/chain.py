"""Measure listing -> ISIN -> LEI -> canonical entity identity chain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from finance_data_ops.identity.gleif import (
    GleifIsinLeiRecord,
    GleifLegalNameRecord,
    GleifLeiIsinRecord,
    gleif_cache_rows,
    gleif_lei_isin_cache_rows,
)
from finance_data_ops.identity.isin import (
    IsinRecord,
    allowed_isin_prefixes_for_listing,
    isin_cache_rows,
    isin_prefix_policy_for_listing,
)
from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping
from finance_data_ops.identity.names import (
    legal_name_query_from_listing,
    legal_name_query_variants_from_listing,
    normalize_legal_name_conservative,
)
from finance_data_ops.identity.openfigi import openfigi_cache_rows

ACCEPTANCE_PAIRS = [
    ("SAP", "SAP.DE", "adr_home"),
    ("ASML", "ASML.AS", "adr_home"),
    ("NVO", "NOVO-B.CO", "adr_home"),
    ("RIO", "RIO.L", "adr_home"),
    ("0005.HK", "HSBA.L", "cross_listing"),
    ("GOOG", "GOOGL", "share_class"),
    ("LEN", "LENB", "share_class"),
]

MAX_ISIN_OUTPUT_SAMPLE = 10

TAIL_DECISION_BUCKETS = {
    "fixable_free",
    "requires_provider_or_curated_identity",
    "needs_manual_review",
}


@dataclass(frozen=True, slots=True)
class EntityChainMeasurement:
    symbol_rows: list[dict[str, Any]]
    pair_rows: list[dict[str, Any]]
    summary: dict[str, Any]
    name_anchor_precision_audit: list[dict[str, Any]]
    openfigi_cache_rows: list[dict[str, Any]]
    isin_cache_rows: list[dict[str, Any]]
    gleif_cache_rows: list[dict[str, Any]]
    gleif_lei_isin_cache_rows: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "symbols": self.symbol_rows,
            "pairs": self.pair_rows,
            "name_anchor_precision_audit": self.name_anchor_precision_audit,
            "planned_cache_writes": {
                "source_cache.openfigi_mapping_raw": len(self.openfigi_cache_rows),
                "source_cache.listing_isin_raw": len(self.isin_cache_rows),
                "source_cache.gleif_isin_lei_raw": len(self.gleif_cache_rows),
                "source_cache.gleif_lei_isin_raw": len(self.gleif_lei_isin_cache_rows),
            },
        }


def measure_entity_identity_chain(
    *,
    candidates: list[ListingCandidate],
    openfigi_mappings: list[OpenFigiMapping],
    isin_records: list[IsinRecord],
    gleif_records: list[GleifIsinLeiRecord],
    gleif_lei_isin_records: list[GleifLeiIsinRecord] | None = None,
    gleif_legal_name_records: list[GleifLegalNameRecord] | None = None,
    gleif_lei_expansion_request_leis: list[str] | None = None,
    gleif_lei_expansion_request_origin_leis: dict[str, list[str]] | None = None,
    gleif_lei_expansion_excluded_origin_leis: dict[str, list[str]] | None = None,
    pairs: list[tuple[str, str, str]] | None = None,
    batch_split_retries: int = 0,
) -> EntityChainMeasurement:
    candidates_by_symbol = {_symbol(candidate.symbol): candidate for candidate in candidates}
    openfigi_by_symbol = {_symbol(mapping.symbol): mapping for mapping in openfigi_mappings}
    isin_by_symbol = {_symbol(record.symbol): record for record in isin_records}
    gleif_by_isin = {_symbol(record.isin): record for record in gleif_records}
    lei_expansions = {_symbol(record.lei): record for record in (gleif_lei_isin_records or [])}
    legal_name_records = {
        _symbol(record.normalized_query_name): record for record in (gleif_legal_name_records or [])
    }
    lei_expansion_request_leis = [_symbol(lei) for lei in (gleif_lei_expansion_request_leis or []) if _symbol(lei)]
    lei_expansion_request_set = set(lei_expansion_request_leis)
    selected_pairs = pairs or _pairs_for_symbols(sorted(candidates_by_symbol))

    base_rows = []
    for symbol in sorted(candidates_by_symbol):
        base_rows.append(
            _symbol_row(
                symbol=symbol,
                candidate=candidates_by_symbol[symbol],
                openfigi=openfigi_by_symbol.get(symbol),
                isin_record=isin_by_symbol.get(symbol),
                gleif_by_isin=gleif_by_isin,
            )
        )
    symbol_rows = _attach_rows_via_lei_expansion(
        base_rows=base_rows,
        candidates_by_symbol=candidates_by_symbol,
        openfigi_by_symbol=openfigi_by_symbol,
        gleif_by_isin=gleif_by_isin,
        lei_expansions=lei_expansions,
        lei_expansion_request_set=lei_expansion_request_set,
        legal_name_records=legal_name_records,
    )
    rows_by_symbol = {row["symbol"]: row for row in symbol_rows}
    pair_rows = [_pair_row(left, right, pair_type, rows_by_symbol) for left, right, pair_type in selected_pairs]
    symbol_rows = _annotate_listing_group_profiles(symbol_rows, pair_rows)
    lei_expanded_isin_count = len(
        {
            isin
            for record in lei_expansions.values()
            if record.status == "success"
            for isin in record.isin_list
            if isin
        }
    )
    precision_audit = _name_anchor_precision_audit(symbol_rows)
    summary = _summary(
        symbol_rows=symbol_rows,
        pair_rows=pair_rows,
        batch_split_retries=batch_split_retries,
        lei_expanded_isin_count=lei_expanded_isin_count,
        lei_expansions=lei_expansions,
        lei_expansion_request_leis=lei_expansion_request_leis,
        lei_expansion_request_origin_leis=gleif_lei_expansion_request_origin_leis or {},
        lei_expansion_excluded_origin_leis=gleif_lei_expansion_excluded_origin_leis or {},
        name_anchor_precision_audit=precision_audit,
    )
    return EntityChainMeasurement(
        symbol_rows=symbol_rows,
        pair_rows=pair_rows,
        summary=summary,
        name_anchor_precision_audit=precision_audit,
        openfigi_cache_rows=openfigi_cache_rows(openfigi_mappings),
        isin_cache_rows=isin_cache_rows(isin_records),
        gleif_cache_rows=gleif_cache_rows(gleif_records),
        gleif_lei_isin_cache_rows=gleif_lei_isin_cache_rows(gleif_lei_isin_records or []),
    )


def acceptance_fixture_candidates(symbols: list[str] | None = None) -> list[ListingCandidate]:
    rows = [
        ("SAP", "US", "USD", "NYQ", "SAP SE-SPONSORED ADR"),
        ("SAP.DE", "DE", "EUR", "", "SAP SE"),
        ("ASML", "US", "USD", "NMS", "ASML HOLDING NV-NY REG SHS"),
        ("ASML.AS", "NL", "EUR", "", "ASML HOLDING NV"),
        ("NVO", "US", "USD", "NYQ", "NOVO-NORDISK A/S-SPONS ADR"),
        ("NOVO-B.CO", "DK", "DKK", "", "NOVO NORDISK A/S-B"),
        ("RIO", "US", "USD", "NYQ", "RIO TINTO PLC-SPON ADR"),
        ("RIO.L", "GB", "GBP", "", "RIO TINTO PLC"),
        ("0005.HK", "HK", "HKD", "", "HSBC HOLDINGS PLC"),
        ("HSBA.L", "GB", "GBP", "", "HSBC HOLDINGS PLC"),
        ("GOOG", "US", "USD", "NMS", "ALPHABET INC-CL C"),
        ("GOOGL", "US", "USD", "NMS", "ALPHABET INC-CL A"),
        ("LEN", "US", "USD", "NYQ", "LENNAR CORP-A"),
        ("LENB", "US", "USD", "NYQ", "LENNAR CORP-B"),
    ]
    selected = {_symbol(symbol) for symbol in (symbols or []) if _symbol(symbol)}
    return [
        ListingCandidate(
            symbol=symbol,
            provider_symbol=symbol,
            country=country,
            currency=currency,
            exchange=exchange,
            name=name,
            source="fixtures",
            has_prices=True,
            has_technicals=True,
        )
        for symbol, country, currency, exchange, name in rows
        if not selected or symbol in selected
    ]


def acceptance_openfigi_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "SAP": _openfigi_fixture("SAP", "SAP SE-SPONSORED ADR", "BBG000BDSLD7", "BBG001S6RD41", "US", "USD", "ADR"),
        "SAP.DE": _openfigi_fixture("SAP", "SAP SE", "BBG000BG7DY8", "BBG001S6RK27", "DE", "EUR", "Common Stock"),
        "ASML": _openfigi_fixture("ASML", "ASML HOLDING NV-NY REG SHS", "BBG000K6MRN4", "BBG001SCG0R3", "US", "USD", "Depositary Receipt"),
        "ASML.AS": _openfigi_fixture("ASML", "ASML HOLDING NV", "BBG000C1HSN8", "BBG001S7Q066", "NL", "EUR", "Common Stock"),
        "NVO": _openfigi_fixture("NVO", "NOVO-NORDISK A/S-SPONS ADR", "BBG000BQBKR3", "BBG001S5TSK0", "US", "USD", "ADR"),
        "NOVO-B.CO": _openfigi_fixture("NOVOB", "NOVO NORDISK A/S-B", "BBG000F8TYC6", "BBG001S6RN12", "DK", "DKK", "Common Stock"),
        "RIO": _openfigi_fixture("RIO", "RIO TINTO PLC-SPON ADR", "BBG000BT4K93", "BBG001S5VRT2", "US", "USD", "ADR"),
        "RIO.L": _openfigi_fixture("RIO", "RIO TINTO PLC", "BBG000C2R2C5", "BBG001S6P3Q3", "GB", "GBP", "Common Stock"),
        "0005.HK": _openfigi_fixture("5", "HSBC HOLDINGS PLC", "BBG000BLNNH6", "BBG001S6J4S8", "HK", "HKD", "Common Stock"),
        "HSBA.L": _openfigi_fixture("HSBA", "HSBC HOLDINGS PLC", "BBG000C05BD1", "BBG001S61XF2", "GB", "GBP", "Common Stock"),
        "GOOG": _openfigi_fixture("GOOG", "ALPHABET INC-CL C", "BBG009S3NB30", "BBG009S3NB21", "US", "USD", "Common Stock"),
        "GOOGL": _openfigi_fixture("GOOGL", "ALPHABET INC-CL A", "BBG009S39JY5", "BBG009S39JX6", "US", "USD", "Common Stock"),
        "LEN": _openfigi_fixture("LEN", "LENNAR CORP-A", "BBG000C3FGJ8", "BBG000C3FGH9", "US", "USD", "Common Stock"),
        "LENB": {"error": "No identifier found."},
    }


def acceptance_isin_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "SAP": {"isin": "US8030542042", "source": "fixture_yfinance"},
        "SAP.DE": {"isin": "-", "source": "fixture_yfinance"},
        "ASML": {"isin": "USN070592100", "source": "fixture_yfinance"},
        "ASML.AS": {"isin": "AR0725224551", "source": "fixture_yfinance"},
        "NVO": {"isin": "US6701002056", "source": "fixture_yfinance"},
        "NOVO-B.CO": {"isin": "-", "source": "fixture_yfinance"},
        "RIO": {"isin": "US7672041008", "source": "fixture_yfinance"},
        "RIO.L": {"isin": "-", "source": "fixture_yfinance"},
        "0005.HK": {"isin": "ARDEUT112257", "source": "fixture_yfinance"},
        "HSBA.L": {"isin": "-", "source": "fixture_yfinance"},
        "GOOG": {"isin": "CA02080M1005", "source": "fixture_yfinance"},
        "GOOGL": {"isin": "CA02080M1005", "source": "fixture_yfinance"},
        "LEN": {"isin": "-", "source": "fixture_yfinance"},
        "LENB": {"isin": "-", "source": "fixture_yfinance"},
    }


def acceptance_gleif_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "US8030542042": {"lei": "529900D6BF99LW9R2E68", "legal_name": "SAP SE", "source": "fixture_gleif"},
        "USN070592100": {"lei": "724500Y6DUVHQD8W8H93", "legal_name": "ASML HOLDING N.V.", "source": "fixture_gleif"},
        "US6701002056": {"lei": "549300DAQ1CVT6CXN342", "legal_name": "NOVO NORDISK A/S", "source": "fixture_gleif"},
        "US7672041008": {"lei": "213800YOEO5OQ72G2R82", "legal_name": "RIO TINTO PLC", "source": "fixture_gleif"},
    }


def acceptance_gleif_legal_name_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "NAME:ALPHABET": {
            "candidates": [
                {
                    "lei": "5493006MHB84DD0ZWV18",
                    "legal_name": "ALPHABET INC.",
                    "legal_country": "US",
                    "headquarters_country": "US",
                    "jurisdiction": "US-DE",
                    "entity_status": "ACTIVE",
                    "registration_status": "ISSUED",
                    "conformity_flag": "CONFORMING",
                },
                {
                    "lei": "984500443D78642F8280",
                    "legal_name": "ALPHABET ENERGY INC",
                    "legal_country": "IN",
                    "headquarters_country": "IN",
                    "jurisdiction": "IN",
                    "entity_status": "ACTIVE",
                    "registration_status": "ISSUED",
                },
            ]
        },
        "NAME:LENNAR": {
            "candidates": [
                {
                    "lei": "529900G61XVRLX5TJX09",
                    "legal_name": "LENNAR CORPORATION",
                    "legal_country": "US",
                    "headquarters_country": "US",
                    "jurisdiction": "US-DE",
                    "entity_status": "ACTIVE",
                    "registration_status": "LAPSED",
                    "conformity_flag": "NON_CONFORMING",
                }
            ]
        },
        "NAME:HSBC HOLDINGS": {
            "candidates": [
                {
                    "lei": "MP6I5ZYZBEU3UXPYFY54",
                    "legal_name": "HSBC HOLDINGS PLC",
                    "legal_country": "GB",
                    "headquarters_country": "GB",
                    "jurisdiction": "GB",
                    "entity_status": "ACTIVE",
                    "registration_status": "ISSUED",
                }
            ]
        },
    }


def acceptance_gleif_lei_isin_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "LEI:529900D6BF99LW9R2E68": {
            "legal_name": "SAP SE",
            "isin_list": ["US8030542042", "DE0007164600"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:724500Y6DUVHQD8W8H93": {
            "legal_name": "ASML HOLDING N.V.",
            "isin_list": ["USN070592100", "NL0010273215"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:549300DAQ1CVT6CXN342": {
            "legal_name": "NOVO NORDISK A/S",
            "isin_list": ["US6701002056", "DK0062498333"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:213800YOEO5OQ72G2R82": {
            "legal_name": "RIO TINTO PLC",
            "isin_list": ["US7672041008", "GB0007188757"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:MP6I5ZYZBEU3UXPYFY54": {
            "legal_name": "HSBC HOLDINGS PLC",
            "isin_list": ["GB0005405286"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:5493006MHB84DD0ZWV18": {
            "legal_name": "ALPHABET INC.",
            "isin_list": ["US02079K1079", "US02079K3059"],
            "source": "fixture_gleif_lei_expansion",
        },
        "LEI:529900G61XVRLX5TJX09": {
            "legal_name": "LENNAR CORPORATION",
            "isin_list": ["US5260571048", "US5260573028"],
            "source": "fixture_gleif_lei_expansion",
        },
    }


def acceptance_pairs_for_symbols(symbols: list[str]) -> list[tuple[str, str, str]]:
    selected = {_symbol(symbol) for symbol in symbols}
    return [pair for pair in ACCEPTANCE_PAIRS if pair[0] in selected and pair[1] in selected]


def _symbol_row(
    *,
    symbol: str,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    isin_record: IsinRecord | None,
    gleif_by_isin: dict[str, GleifIsinLeiRecord],
) -> dict[str, Any]:
    request_payload = dict((openfigi.payload if openfigi else {}) or {})
    raw_isin = _raw_isin_from_record(isin_record)
    isin_status = isin_record.status if isin_record else "missing"
    isin = _symbol(isin_record.isin) if isin_record and isin_status == "success" else ""
    gleif = gleif_by_isin.get(isin)
    direct_lei = gleif.lei if gleif else ""
    direct_legal_name = gleif.legal_name if gleif else ""
    listing_name = _best_listing_name(candidate, openfigi)
    listing_name_queries = _listing_name_search_queries(candidate, openfigi)
    listing_name_query = listing_name_queries[0] if listing_name_queries else legal_name_query_from_listing(listing_name)
    prefix_policy = isin_prefix_policy_for_listing(candidate)
    allowed_prefixes = list(prefix_policy["allowed_isin_prefixes"])
    isin_prefix_match = bool(isin and allowed_prefixes and isin[:2] in set(allowed_prefixes))
    direct_name_confirmed = _direct_isin_name_confirmed(
        candidate=candidate,
        openfigi=openfigi,
        legal_name=direct_legal_name,
    )
    direct_attach_method = ""
    direct_attach_reason = ""
    direct_attach_reasons: list[str] = []
    direct_entity_lei = ""
    direct_entity_legal_name = ""
    direct_lei_value = ""
    direct_gate_status = "not_evaluated"
    direct_gate_reject_reason = ""
    direct_reject_reason = ""
    if direct_lei:
        if isin_prefix_match:
            direct_attach_method = "direct_isin"
            direct_attach_reason = "direct_isin_to_lei"
            direct_attach_reasons = ["direct_isin_to_lei"]
            direct_entity_lei = direct_lei
            direct_entity_legal_name = direct_legal_name
            direct_lei_value = direct_lei
            direct_gate_status = "passed"
        else:
            direct_reject_reason = "direct_isin_prefix_mismatch_name_unconfirmed"
            direct_gate_status = "rejected"
            direct_gate_reject_reason = direct_reject_reason
    return {
        "symbol": symbol,
        "provider_symbol": candidate.provider_symbol or symbol,
        "listing_country": candidate.country or "",
        "derived_listing_country": prefix_policy["derived_listing_country"],
        "listing_currency": candidate.currency or "",
        "openfigi_ticker": request_payload.get("idValue") or "",
        "openfigi_exchange": request_payload.get("micCode") or request_payload.get("exchCode") or "",
        "openfigi_status": openfigi.status if openfigi else "missing",
        "openfigi_name": openfigi.name if openfigi else "",
        "internal_candidate_name": candidate.name or "",
        "listing_name_used_for_legal_name_search": listing_name_query,
        "listing_name_search_queries": listing_name_queries,
        "figi": openfigi.figi if openfigi else "",
        "compositeFIGI": openfigi.composite_figi if openfigi else "",
        "shareClassFIGI": openfigi.share_class_figi if openfigi else "",
        "security_type": openfigi.security_type if openfigi else "",
        "isin_source": isin_record.source if isin_record else "",
        "isin_status": isin_status,
        "isin_error_reason": isin_record.error_message if isin_record else "",
        "isin_prefix_match": isin_prefix_match,
        "isin_prefix_diagnostic": "" if isin_prefix_match or not isin else "isin_prefix_mismatch",
        "raw_isin": raw_isin,
        "isin": isin,
        "lei_source": gleif.source if gleif and gleif.lei else "",
        "lei_status": gleif.status if gleif else ("not_requested" if not isin else "missing"),
        "direct_lei": direct_lei,
        "lei": direct_lei_value,
        "legal_name": direct_legal_name,
        "lei_role": _lei_role(gleif),
        "entity_lei": direct_entity_lei,
        "entity_legal_name": direct_entity_legal_name,
        "entity_attach_method": direct_attach_method,
        "entity_attach_reason": direct_attach_reason,
        "entity_attach_reasons": direct_attach_reasons,
        "decision_bucket": "attached" if direct_entity_lei else "",
        "direct_isin_name_confirmed": direct_name_confirmed,
        "direct_isin_attach_reject_reason": direct_reject_reason,
        "expanded_candidate_isins": [],
        "lei_expanded_isins": [],
        "lei_expanded_isin_count": 0,
        "compatible_expanded_isin_count": 0,
        "matched_compatible_isins": [isin] if direct_entity_lei else [],
        "allowed_isin_prefixes": allowed_prefixes,
        "compatible_isin_gate_status": direct_gate_status,
        "compatible_isin_gate_reject_reason": direct_gate_reject_reason,
        "expected_listing_country_prefix_present": isin_prefix_match,
        "compatible_isin_candidate_count": 1 if direct_entity_lei and isin_prefix_match else 0,
        "compatible_isin_candidate_sample": [isin] if direct_entity_lei and isin_prefix_match else [],
        "legal_name_anchor_status": "not_requested_direct_isin" if direct_entity_lei else "not_evaluated",
        "legal_name_anchor_reject_reason": "",
        "candidate_lei": "",
        "candidate_legal_name": "",
        "candidate_legal_country": "",
        "candidate_headquarters_country": "",
        "candidate_entity_status": "",
        "candidate_registration_status": "",
        "legal_name_candidate_lei": "",
        "legal_name_candidate_lei_in_expansion_request": False,
        "legal_name_candidate_lei_expansion_status": "",
        "legal_name_candidate_lei_expansion_error": "",
        "legal_name_candidate_lei_expansion_isin_count": 0,
        "legal_name_candidate_lei_expansion_isin_sample": [],
        "legal_name_candidate_compatible_isin_count": 0,
        "legal_name_candidate_compatible_isin_sample": [],
        "direct_prefix_mismatch_candidate_status": "not_requested",
        "direct_prefix_mismatch_candidate_reject_reason": "",
        "direct_prefix_mismatch_status": "not_requested",
        "direct_prefix_mismatch_reject_reason": "",
        "direct_prefix_mismatch_lei": "",
        "direct_prefix_mismatch_legal_name": "",
    }


def _attach_rows_via_lei_expansion(
    *,
    base_rows: list[dict[str, Any]],
    candidates_by_symbol: dict[str, ListingCandidate],
    openfigi_by_symbol: dict[str, OpenFigiMapping],
    gleif_by_isin: dict[str, GleifIsinLeiRecord],
    lei_expansions: dict[str, GleifLeiIsinRecord],
    lei_expansion_request_set: set[str],
    legal_name_records: dict[str, GleifLegalNameRecord],
) -> list[dict[str, Any]]:
    direct_lei_names: dict[str, str] = {}
    for row in base_rows:
        if row.get("entity_attach_method") != "direct_isin":
            continue
        lei = _symbol(row.get("direct_lei"))
        if lei:
            direct_lei_names.setdefault(lei, str(row.get("legal_name") or ""))

    out: list[dict[str, Any]] = []
    for row in base_rows:
        attached = dict(row)
        if attached.get("entity_lei"):
            expansion = lei_expansions.get(_symbol(attached.get("entity_lei")))
            if expansion and expansion.status == "success":
                compatible_isins = _compatible_isins_for_listing(
                    candidate=candidates_by_symbol[_symbol(attached.get("symbol"))],
                    isins=expansion.isin_list,
                )
                attached["lei_expanded_isins"] = _sample_isins(expansion.isin_list)
                attached["lei_expanded_isin_count"] = len(expansion.isin_list)
                attached["compatible_expanded_isin_count"] = len(compatible_isins)
                attached["matched_compatible_isins"] = _sample_isins(compatible_isins)
                attached["expected_listing_country_prefix_present"] = bool(compatible_isins)
                attached["compatible_isin_candidate_sample"] = _sample_isins(compatible_isins)
                attached["compatible_isin_candidate_count"] = len(compatible_isins)
                direct_isin = _symbol(attached.get("isin"))
                if direct_isin and direct_isin in set(expansion.isin_list):
                    attached["matched_compatible_isins"] = [direct_isin]
            method = str(attached.get("entity_attach_method") or "")
            attached["attachment_provenance"] = (
                "isin_direct_prefix_mismatch_name_confirmed"
                if method == "isin_direct_prefix_mismatch_name_confirmed"
                else "isin_direct"
            )
            attached["attachment_confidence"] = "high"
            attached["decision_bucket"] = "attached"
            out.append(attached)
            continue

        symbol = _symbol(attached.get("symbol"))
        candidate = candidates_by_symbol[symbol]
        openfigi = openfigi_by_symbol.get(symbol)
        matches = _lei_expansion_matches(
            candidate=candidate,
            openfigi=openfigi,
            lei_expansions=lei_expansions,
            direct_lei_names=direct_lei_names,
        )
        if len(matches) == 1:
            record, candidate_isins = matches[0]
            legal_name = record.legal_name or direct_lei_names.get(record.lei, "")
            attached.update(
                {
                    "lei": record.lei,
                    "legal_name": legal_name,
                    "entity_lei": record.lei,
                    "entity_legal_name": legal_name,
                    "entity_attach_method": "lei_expansion",
                    "entity_attach_reason": "single_compatible_expanded_lei",
                    "entity_attach_reasons": ["single_compatible_expanded_lei"],
                    "decision_bucket": "attached",
                    "expanded_candidate_isins": _sample_isins(candidate_isins),
                    "lei_expanded_isins": _sample_isins(record.isin_list),
                    "lei_expanded_isin_count": len(record.isin_list),
                    "compatible_expanded_isin_count": len(candidate_isins),
                    "matched_compatible_isins": _sample_isins(candidate_isins),
                    "compatible_isin_gate_status": "passed",
                    "compatible_isin_gate_reject_reason": "",
                    "expected_listing_country_prefix_present": True,
                    "compatible_isin_candidate_sample": _sample_isins(candidate_isins),
                    "compatible_isin_candidate_count": len(candidate_isins),
                    "lei_source": record.source,
                    "lei_status": record.status,
                    "attachment_provenance": "lei_expansion",
                    "attachment_confidence": "high",
                }
            )
        elif len(matches) > 1:
            attached.update(
                {
                    "entity_attach_method": "unattached_ambiguous",
                    "entity_attach_reason": "multiple_compatible_expanded_lei",
                    "entity_attach_reasons": ["multiple_compatible_expanded_lei"],
                    "decision_bucket": "needs_manual_review",
                    "expanded_candidate_isins": _sample_isins(sorted({isin for _, isins in matches for isin in isins})),
                    "lei_expanded_isins": _sample_isins(sorted({isin for record, _ in matches for isin in record.isin_list})),
                    "lei_expanded_isin_count": len({isin for record, _ in matches for isin in record.isin_list}),
                    "compatible_expanded_isin_count": len({isin for _, isins in matches for isin in isins}),
                    "matched_compatible_isins": _sample_isins(sorted({isin for _, isins in matches for isin in isins})),
                    "compatible_isin_gate_status": "ambiguous",
                    "compatible_isin_gate_reject_reason": "multiple_compatible_expanded_lei",
                    "expected_listing_country_prefix_present": True,
                    "attachment_provenance": "needs_review",
                    "attachment_confidence": "review",
                }
            )
        else:
            name_evaluation = _evaluate_name_anchor(
                candidate=candidate,
                openfigi=openfigi,
                lei_expansions=lei_expansions,
                lei_expansion_request_set=lei_expansion_request_set,
                legal_name_records=legal_name_records,
            )
            _apply_name_anchor_diagnostics(attached, name_evaluation)
            name_matches = name_evaluation["matches"]
            if len(name_matches) == 1:
                name_candidate, record, candidate_isins = name_matches[0]
                attached.update(
                    {
                        "lei": record.lei,
                        "legal_name": name_candidate.get("legal_name") or record.legal_name,
                        "entity_lei": record.lei,
                        "entity_legal_name": name_candidate.get("legal_name") or record.legal_name,
                        "entity_attach_method": "name_anchor_confirmed",
                        "entity_attach_reason": "legal_name_candidate_confirmed_by_lei_isin_expansion",
                        "entity_attach_reasons": ["legal_name_candidate_confirmed_by_lei_isin_expansion"],
                        "decision_bucket": "attached",
                        "expanded_candidate_isins": _sample_isins(candidate_isins),
                        "lei_expanded_isins": _sample_isins(record.isin_list),
                        "lei_expanded_isin_count": len(record.isin_list),
                        "compatible_expanded_isin_count": len(candidate_isins),
                        "matched_compatible_isins": _sample_isins(candidate_isins),
                        "compatible_isin_gate_status": "passed",
                        "compatible_isin_gate_reject_reason": "",
                        "expected_listing_country_prefix_present": True,
                        "compatible_isin_candidate_sample": _sample_isins(candidate_isins),
                        "compatible_isin_candidate_count": len(candidate_isins),
                        "lei_source": "gleif_legal_name_plus_lei_expansion",
                        "lei_status": record.status,
                        "candidate_lei": record.lei,
                        "candidate_legal_name": name_candidate.get("legal_name") or "",
                        "candidate_legal_country": name_candidate.get("legal_country") or "",
                        "candidate_headquarters_country": name_candidate.get("headquarters_country") or "",
                        "candidate_entity_status": name_candidate.get("entity_status") or "",
                        "candidate_registration_status": name_candidate.get("registration_status") or "",
                        "attachment_provenance": "name_anchor_confirmed",
                        "attachment_confidence": "medium",
                        "legal_name_anchor_status": "confirmed",
                        "legal_name_anchor_reject_reason": "",
                    }
                )
            elif len(name_matches) > 1:
                candidate_isins = sorted({isin for _, _, isins in name_matches for isin in isins})
                attached.update(
                    {
                        "entity_attach_method": "unattached_ambiguous",
                        "entity_attach_reason": "legal_name_search_ambiguous",
                        "entity_attach_reasons": _unattached_reasons(attached, openfigi, name_evaluation),
                        "decision_bucket": "needs_manual_review",
                        "candidate_lei": ",".join(sorted({record.lei for _, record, _ in name_matches})),
                        "expanded_candidate_isins": _sample_isins(candidate_isins),
                        "compatible_expanded_isin_count": len(candidate_isins),
                        "matched_compatible_isins": _sample_isins(candidate_isins),
                        "attachment_provenance": "needs_review",
                        "attachment_confidence": "review",
                        "legal_name_anchor_status": "ambiguous",
                        "legal_name_anchor_reject_reason": "legal_name_search_ambiguous",
                    }
                )
            else:
                reasons = _unattached_reasons(attached, openfigi, name_evaluation)
                reason = _primary_unattached_reason(reasons)
                attached.update(
                    {
                        "entity_attach_method": "unattached_no_anchor",
                        "entity_attach_reason": reason,
                        "entity_attach_reasons": reasons,
                        "decision_bucket": _decision_bucket_for_reasons(reasons),
                        "attachment_provenance": "needs_review",
                        "attachment_confidence": "review",
                    }
                )
        if not attached.get("entity_lei") and attached.get("entity_attach_method") == "unattached_no_anchor":
            _apply_direct_prefix_mismatch_candidate(
                attached=attached,
                candidate=candidate,
                openfigi=openfigi,
                gleif_by_isin=gleif_by_isin,
            )
        out.append(attached)
    return out


def _apply_direct_prefix_mismatch_candidate(
    *,
    attached: dict[str, Any],
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    gleif_by_isin: dict[str, GleifIsinLeiRecord],
) -> None:
    """Try the isolated prefix-mismatch direct path after baseline paths fail."""

    mismatch_reasons = {
        "provider_returned_alternate_market_instrument",
        "provider_listing_mismatch",
        "isin_prefix_mismatch",
    }
    if attached.get("isin_status") != "suspect" or attached.get("isin_error_reason") not in mismatch_reasons:
        return

    if not _is_us_listing(candidate=candidate, openfigi=openfigi):
        attached["direct_prefix_mismatch_candidate_status"] = "rejected"
        attached["direct_prefix_mismatch_candidate_reject_reason"] = "direct_prefix_mismatch_non_us_listing"
        attached["direct_prefix_mismatch_status"] = "rejected"
        attached["direct_prefix_mismatch_reject_reason"] = "direct_prefix_mismatch_non_us_listing"
        return

    raw_isin = _symbol(attached.get("raw_isin"))
    if not raw_isin:
        attached["direct_prefix_mismatch_candidate_status"] = "rejected"
        attached["direct_prefix_mismatch_candidate_reject_reason"] = "missing_raw_isin"
        attached["direct_prefix_mismatch_status"] = "rejected"
        attached["direct_prefix_mismatch_reject_reason"] = "missing_raw_isin"
        return

    gleif = gleif_by_isin.get(raw_isin)
    if not gleif or gleif.status != "success" or not gleif.lei:
        attached["direct_prefix_mismatch_candidate_status"] = "rejected"
        attached["direct_prefix_mismatch_candidate_reject_reason"] = "direct_prefix_mismatch_no_gleif_lei"
        attached["direct_prefix_mismatch_status"] = "rejected"
        attached["direct_prefix_mismatch_reject_reason"] = "direct_prefix_mismatch_no_gleif_lei"
        return

    attached["direct_prefix_mismatch_lei"] = gleif.lei
    attached["direct_prefix_mismatch_legal_name"] = gleif.legal_name
    if not _direct_prefix_mismatch_name_confirmed(candidate=candidate, openfigi=openfigi, legal_name=gleif.legal_name):
        attached["direct_prefix_mismatch_candidate_status"] = "rejected"
        attached["direct_prefix_mismatch_candidate_reject_reason"] = "direct_prefix_mismatch_name_unconfirmed"
        attached["direct_prefix_mismatch_status"] = "rejected"
        attached["direct_prefix_mismatch_reject_reason"] = "direct_prefix_mismatch_name_unconfirmed"
        attached["decision_bucket"] = "needs_manual_review"
        return

    attached.update(
        {
            "isin": raw_isin,
            "lei_source": gleif.source,
            "lei_status": gleif.status,
            "direct_lei": gleif.lei,
            "lei": gleif.lei,
            "legal_name": gleif.legal_name,
            "lei_role": _lei_role(gleif),
            "entity_lei": gleif.lei,
            "entity_legal_name": gleif.legal_name,
            "entity_attach_method": "isin_direct_prefix_mismatch_name_confirmed",
            "entity_attach_reason": "direct_isin_prefix_mismatch_name_confirmed",
            "entity_attach_reasons": ["direct_isin_prefix_mismatch_name_confirmed"],
            "decision_bucket": "attached",
            "matched_compatible_isins": [raw_isin],
            "compatible_isin_gate_status": "diagnostic_prefix_mismatch",
            "compatible_isin_gate_reject_reason": "",
            "expected_listing_country_prefix_present": False,
            "attachment_provenance": "isin_direct_prefix_mismatch_name_confirmed",
            "attachment_confidence": "high",
            "direct_isin_name_confirmed": True,
            "direct_isin_attach_reject_reason": "",
            "candidate_lei": gleif.lei,
            "candidate_legal_name": gleif.legal_name,
            "direct_prefix_mismatch_candidate_status": "confirmed",
            "direct_prefix_mismatch_candidate_reject_reason": "",
            "direct_prefix_mismatch_status": "confirmed",
            "direct_prefix_mismatch_reject_reason": "",
            "direct_prefix_mismatch_lei": gleif.lei,
            "direct_prefix_mismatch_legal_name": gleif.legal_name,
        }
    )


def _lei_expansion_matches(
    *,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    lei_expansions: dict[str, GleifLeiIsinRecord],
    direct_lei_names: dict[str, str],
) -> list[tuple[GleifLeiIsinRecord, list[str]]]:
    matches = []
    allowed_prefixes = allowed_isin_prefixes_for_listing(candidate)
    if not allowed_prefixes:
        return matches
    listing_name = _best_listing_name(candidate, openfigi)
    for lei, record in sorted(lei_expansions.items()):
        if lei not in direct_lei_names:
            continue
        if record.status != "success":
            continue
        candidate_isins = _compatible_isins_for_listing(candidate=candidate, isins=record.isin_list)
        if not candidate_isins:
            continue
        legal_name = record.legal_name or direct_lei_names.get(lei, "")
        if not _names_compatible(listing_name, legal_name):
            continue
        matches.append((record, candidate_isins))
    return matches


def _evaluate_name_anchor(
    *,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    lei_expansions: dict[str, GleifLeiIsinRecord],
    lei_expansion_request_set: set[str],
    legal_name_records: dict[str, GleifLegalNameRecord],
) -> dict[str, Any]:
    listing_name = _best_listing_name(candidate, openfigi)
    query_names = _listing_name_search_queries(candidate, openfigi)
    query_name = query_names[0] if query_names else legal_name_query_from_listing(listing_name)
    normalized_listing_name = normalize_legal_name_conservative(query_name or listing_name)
    prefix_policy = isin_prefix_policy_for_listing(candidate)
    allowed_prefixes = set(prefix_policy["allowed_isin_prefixes"])
    base: dict[str, Any] = {
        "listing_name_used_for_legal_name_search": query_name,
        "listing_name_search_queries": query_names,
        "status": "not_requested" if not normalized_listing_name else "not_found",
        "reject_reason": "",
        "matches": [],
        "derived_listing_country": prefix_policy["derived_listing_country"],
        "allowed_isin_prefixes": list(prefix_policy["allowed_isin_prefixes"]),
        "candidate_lei": "",
        "candidate_legal_name": "",
        "candidate_legal_country": "",
        "candidate_headquarters_country": "",
        "candidate_entity_status": "",
        "candidate_registration_status": "",
        "legal_name_candidate_lei": "",
        "legal_name_candidate_lei_in_expansion_request": False,
        "legal_name_candidate_lei_expansion_status": "",
        "legal_name_candidate_lei_expansion_error": "",
        "legal_name_candidate_lei_expansion_isin_count": 0,
        "legal_name_candidate_lei_expansion_isin_sample": [],
        "legal_name_candidate_compatible_isin_count": 0,
        "legal_name_candidate_compatible_isin_sample": [],
        "compatible_expanded_isin_count": 0,
        "matched_compatible_isins": [],
        "compatible_isin_gate_status": "not_evaluated",
        "compatible_isin_gate_reject_reason": "",
        "expected_listing_country_prefix_present": False,
        "compatible_isin_candidate_count": 0,
        "compatible_isin_candidate_sample": [],
    }
    if not normalized_listing_name:
        base["reject_reason"] = "no_listing_name_for_legal_name_search"
        return base

    record = None
    for candidate_query in query_names or [query_name]:
        normalized_query = normalize_legal_name_conservative(candidate_query)
        candidate_record = legal_name_records.get(_symbol(normalized_query))
        if candidate_record and candidate_record.status == "success":
            record = candidate_record
            query_name = candidate_query
            normalized_listing_name = normalized_query
            base["listing_name_used_for_legal_name_search"] = candidate_query
            break
    if not record:
        base["status"] = "not_found"
        base["reject_reason"] = "legal_name_search_no_match"
        return base

    exact_candidates = [
        name_candidate
        for name_candidate in record.candidates
        if _symbol(name_candidate.get("normalized_legal_name")) == _symbol(normalized_listing_name)
    ]
    if not exact_candidates:
        base["status"] = "not_found"
        base["reject_reason"] = "legal_name_search_no_match"
        return base

    status_candidates = [
        name_candidate
        for name_candidate in exact_candidates
        if _legal_name_candidate_status_acceptable(name_candidate)
    ]
    if not status_candidates:
        _copy_name_candidate_diagnostics(base, exact_candidates[0])
        base["status"] = "rejected"
        base["reject_reason"] = "legal_name_search_no_match"
        return base

    if not allowed_prefixes:
        _copy_name_candidate_diagnostics(base, status_candidates[0])
        base["status"] = "rejected"
        base["reject_reason"] = "missing_listing_country_for_isin_gate"
        base["compatible_isin_gate_status"] = "rejected"
        base["compatible_isin_gate_reject_reason"] = "missing_listing_country_for_isin_gate"
        return base

    country_candidates = [
        name_candidate
        for name_candidate in status_candidates
        if _legal_name_candidate_country_compatible(
            name_candidate=name_candidate,
            allowed_prefixes=allowed_prefixes,
        )
    ]
    if not country_candidates:
        _copy_name_candidate_diagnostics(base, status_candidates[0])
        base["status"] = "rejected"
        base["reject_reason"] = "legal_name_match_country_incompatible"
        base["compatible_isin_gate_status"] = "rejected"
        base["compatible_isin_gate_reject_reason"] = "legal_name_match_country_incompatible"
        return base

    matches: list[tuple[dict[str, Any], GleifLeiIsinRecord, list[str]]] = []
    for name_candidate in country_candidates:
        lei = _symbol(name_candidate.get("lei"))
        expansion = lei_expansions.get(lei)
        _copy_legal_name_candidate_expansion_diagnostics(
            base,
            candidate=candidate,
            name_candidate=name_candidate,
            expansion=expansion,
            requested=lei in lei_expansion_request_set,
        )
        if not expansion or expansion.status != "success":
            continue
        candidate_isins = _compatible_isins_for_listing(candidate=candidate, isins=expansion.isin_list)
        if not candidate_isins:
            continue
        matches.append((name_candidate, expansion, candidate_isins))

    if len(matches) == 1:
        name_candidate, _, candidate_isins = matches[0]
        _copy_name_candidate_diagnostics(base, name_candidate)
        base.update(
            {
                "status": "confirmed",
                "reject_reason": "",
                "matches": matches,
                "compatible_expanded_isin_count": len(candidate_isins),
                "matched_compatible_isins": _sample_isins(candidate_isins),
                "compatible_isin_gate_status": "passed",
                "compatible_isin_gate_reject_reason": "",
                "expected_listing_country_prefix_present": True,
                "compatible_isin_candidate_sample": _sample_isins(candidate_isins),
                "compatible_isin_candidate_count": len(candidate_isins),
            }
        )
        return base
    if len(matches) > 1:
        all_isins = sorted({isin for _, _, candidate_isins in matches for isin in candidate_isins})
        base.update(
            {
                "status": "ambiguous",
                "reject_reason": "legal_name_search_ambiguous",
                "matches": matches,
                "candidate_lei": ",".join(sorted({record.lei for _, record, _ in matches})),
                "compatible_expanded_isin_count": len(all_isins),
                "matched_compatible_isins": _sample_isins(all_isins),
                "compatible_isin_gate_status": "ambiguous",
                "compatible_isin_gate_reject_reason": "legal_name_search_ambiguous",
                "expected_listing_country_prefix_present": bool(all_isins),
            }
        )
        return base

    first_country_candidate = country_candidates[0]
    first_lei = _symbol(first_country_candidate.get("lei"))
    _copy_legal_name_candidate_expansion_diagnostics(
        base,
        candidate=candidate,
        name_candidate=first_country_candidate,
        expansion=lei_expansions.get(first_lei),
        requested=first_lei in lei_expansion_request_set,
    )
    _copy_name_candidate_diagnostics(base, first_country_candidate)
    base["status"] = "rejected"
    base["reject_reason"] = "gleif_lei_found_but_no_compatible_isin"
    base["compatible_isin_gate_status"] = "rejected"
    base["compatible_isin_gate_reject_reason"] = "no_compatible_expanded_isin_for_listing_country"
    return base


def _apply_name_anchor_diagnostics(row: dict[str, Any], evaluation: dict[str, Any]) -> None:
    for key in (
        "listing_name_used_for_legal_name_search",
        "candidate_lei",
        "candidate_legal_name",
        "candidate_legal_country",
        "candidate_headquarters_country",
        "candidate_entity_status",
        "candidate_registration_status",
        "legal_name_candidate_lei",
        "legal_name_candidate_lei_in_expansion_request",
        "legal_name_candidate_lei_expansion_status",
        "legal_name_candidate_lei_expansion_error",
        "legal_name_candidate_lei_expansion_isin_count",
        "legal_name_candidate_lei_expansion_isin_sample",
        "legal_name_candidate_compatible_isin_count",
        "legal_name_candidate_compatible_isin_sample",
        "compatible_expanded_isin_count",
        "matched_compatible_isins",
        "derived_listing_country",
        "allowed_isin_prefixes",
        "listing_name_search_queries",
        "compatible_isin_gate_status",
        "compatible_isin_gate_reject_reason",
        "expected_listing_country_prefix_present",
        "compatible_isin_candidate_count",
        "compatible_isin_candidate_sample",
    ):
        if evaluation.get(key) or key in {
            "compatible_expanded_isin_count",
            "compatible_isin_candidate_count",
            "expected_listing_country_prefix_present",
            "legal_name_candidate_lei_in_expansion_request",
            "legal_name_candidate_lei_expansion_isin_count",
            "legal_name_candidate_compatible_isin_count",
            "legal_name_candidate_lei_expansion_isin_sample",
            "legal_name_candidate_compatible_isin_sample",
        }:
            row[key] = evaluation[key]
    row["legal_name_anchor_status"] = evaluation.get("status") or ""
    row["legal_name_anchor_reject_reason"] = evaluation.get("reject_reason") or ""


def _copy_name_candidate_diagnostics(target: dict[str, Any], name_candidate: dict[str, Any]) -> None:
    target.update(
        {
            "candidate_lei": _symbol(name_candidate.get("lei")),
            "candidate_legal_name": str(name_candidate.get("legal_name") or ""),
            "candidate_legal_country": _symbol(name_candidate.get("legal_country")),
            "candidate_headquarters_country": _symbol(name_candidate.get("headquarters_country")),
            "candidate_entity_status": _symbol(name_candidate.get("entity_status")),
            "candidate_registration_status": _symbol(name_candidate.get("registration_status")),
        }
    )


def _copy_legal_name_candidate_expansion_diagnostics(
    target: dict[str, Any],
    *,
    candidate: ListingCandidate,
    name_candidate: dict[str, Any],
    expansion: GleifLeiIsinRecord | None,
    requested: bool,
) -> None:
    compatible_isins = (
        _compatible_isins_for_listing(candidate=candidate, isins=expansion.isin_list)
        if expansion and expansion.status == "success"
        else []
    )
    target.update(
        {
            "legal_name_candidate_lei": _symbol(name_candidate.get("lei")),
            "legal_name_candidate_lei_in_expansion_request": bool(requested),
            "legal_name_candidate_lei_expansion_status": (
                expansion.status if expansion else ("missing_response" if requested else "not_requested")
            ),
            "legal_name_candidate_lei_expansion_error": expansion.error_message if expansion else "",
            "legal_name_candidate_lei_expansion_isin_count": len(expansion.isin_list) if expansion else 0,
            "legal_name_candidate_lei_expansion_isin_sample": _sample_isins(expansion.isin_list if expansion else []),
            "legal_name_candidate_compatible_isin_count": len(compatible_isins),
            "legal_name_candidate_compatible_isin_sample": _sample_isins(compatible_isins),
        }
    )


def _legal_name_candidate_status_acceptable(name_candidate: dict[str, Any]) -> bool:
    return _symbol(name_candidate.get("entity_status")) == "ACTIVE" and _symbol(
        name_candidate.get("registration_status")
    ) in {"ISSUED", "LAPSED"}


def _legal_name_candidate_country_compatible(
    *,
    name_candidate: dict[str, Any],
    allowed_prefixes: set[str],
) -> bool:
    if not allowed_prefixes:
        return True
    countries = {
        _symbol(name_candidate.get("legal_country")),
        _symbol(name_candidate.get("headquarters_country")),
        _symbol(name_candidate.get("jurisdiction_country")),
    }
    countries.discard("")
    return bool(countries & allowed_prefixes)


def _best_listing_name(candidate: ListingCandidate, openfigi: OpenFigiMapping | None) -> str:
    return str((openfigi.name if openfigi and openfigi.name else "") or candidate.name or "").strip()


def _listing_name_search_queries(candidate: ListingCandidate, openfigi: OpenFigiMapping | None) -> list[str]:
    names = []
    if openfigi and openfigi.name:
        names.append(openfigi.name)
    if candidate.name:
        names.append(candidate.name)
    return legal_name_query_variants_from_listing(*names)


def _raw_isin_from_record(record: IsinRecord | None) -> str:
    if not record:
        return ""
    payload = record.response_payload if isinstance(record.response_payload, dict) else {}
    return _symbol(payload.get("isin") or payload.get("get_isin") or record.isin)


def _pair_row(left: str, right: str, pair_type: str, rows_by_symbol: dict[str, dict[str, Any]]) -> dict[str, Any]:
    left_row = rows_by_symbol.get(_symbol(left), {})
    right_row = rows_by_symbol.get(_symbol(right), {})
    left_lei = _symbol(left_row.get("entity_lei") or left_row.get("lei"))
    right_lei = _symbol(right_row.get("entity_lei") or right_row.get("lei"))
    grouped = bool(left_lei and right_lei and left_lei == right_lei)
    left_method = left_row.get("entity_attach_method") or ""
    right_method = right_row.get("entity_attach_method") or ""
    return {
        "pair": [left, right],
        "pair_type": pair_type,
        "grouped": grouped,
        "reason": "shared_lei" if grouped else _not_grouped_reason(left_row, right_row),
        "grouping_path": _grouping_path(grouped=grouped, left_method=left_method, right_method=right_method),
        "left_lei": left_lei,
        "right_lei": right_lei,
        "left_isin": left_row.get("isin") or "",
        "right_isin": right_row.get("isin") or "",
        "left_attach_method": left_method,
        "right_attach_method": right_method,
    }


def _summary(
    *,
    symbol_rows: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
    batch_split_retries: int,
    lei_expanded_isin_count: int,
    lei_expansions: dict[str, GleifLeiIsinRecord],
    lei_expansion_request_leis: list[str],
    lei_expansion_request_origin_leis: dict[str, list[str]],
    lei_expansion_excluded_origin_leis: dict[str, list[str]],
    name_anchor_precision_audit: list[dict[str, Any]],
) -> dict[str, Any]:
    candidate_count = len(symbol_rows)
    isin_found = len([row for row in symbol_rows if row.get("isin") and row.get("isin_status") == "success"])
    isin_suspect_rows = [row for row in symbol_rows if row.get("isin_status") == "suspect"]
    lei_found = len([row for row in symbol_rows if row.get("direct_lei")])
    lei_groups: dict[str, list[str]] = {}
    for row in symbol_rows:
        lei = _symbol(row.get("entity_lei") or row.get("lei"))
        if lei:
            lei_groups.setdefault(lei, []).append(row["symbol"])
    grouped_pairs = [row for row in pair_rows if row.get("grouped")]
    direct_methods = {"direct_isin", "isin_direct_prefix_mismatch_name_confirmed"}
    direct_attached = [row for row in symbol_rows if row.get("entity_attach_method") in direct_methods]
    direct_prefix_mismatch_attached = [
        row
        for row in symbol_rows
        if row.get("entity_attach_method") == "isin_direct_prefix_mismatch_name_confirmed"
    ]
    expansion_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "lei_expansion"]
    name_anchor_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "name_anchor_confirmed"]
    attached = direct_attached + expansion_attached + name_anchor_attached
    ambiguous_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_ambiguous"]
    no_anchor_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_no_anchor"]
    decision_bucket_counts = _decision_bucket_counts(symbol_rows)
    fixable_free_count = decision_bucket_counts.get("fixable_free", 0)
    provider_or_curated_count = decision_bucket_counts.get("requires_provider_or_curated_identity", 0)
    review_count = decision_bucket_counts.get("needs_manual_review", 0)
    accepted_pair_rows = [row for row in pair_rows if not _is_guardrail_pair(row)]
    guardrail_pair_rows = [row for row in pair_rows if _is_guardrail_pair(row)]
    legal_name_candidate_leis = _unique_ordered(
        [_symbol(lei) for lei in lei_expansion_request_origin_leis.get("legal_name_candidate", [])]
    )
    expansion_request_set = set(lei_expansion_request_leis)
    return {
        "candidate_count": candidate_count,
        "attached_count": len(attached),
        "attached_rate": _rate(len(attached), candidate_count),
        "isin_found_count": isin_found,
        "isin_found_rate": _rate(isin_found, candidate_count),
        "isin_suspect_count": len(isin_suspect_rows),
        "isin_suspect_reasons": _reason_counts(isin_suspect_rows),
        "lei_found_count": lei_found,
        "lei_found_rate_among_isins": _rate(lei_found, isin_found),
        "anchor_isin_count": isin_found,
        "anchor_lei_count": len({_symbol(row.get("direct_lei")) for row in direct_attached if row.get("direct_lei")}),
        "lei_expanded_isin_count": int(lei_expanded_isin_count),
        "gleif_lei_expansion_requested_count": len(lei_expansion_request_leis),
        "gleif_lei_expansion_requested_by_origin": {
            origin: len(_unique_ordered([_symbol(lei) for lei in leis]))
            for origin, leis in sorted(lei_expansion_request_origin_leis.items())
        },
        "gleif_lei_expansion_excluded_by_origin": {
            origin: len(_unique_ordered([_symbol(lei) for lei in leis]))
            for origin, leis in sorted(lei_expansion_excluded_origin_leis.items())
        },
        "gleif_lei_expansion_excluded_lei_sample_by_origin": {
            origin: _sample_text(_unique_ordered([_symbol(lei) for lei in leis]))
            for origin, leis in sorted(lei_expansion_excluded_origin_leis.items())
        },
        "gleif_lei_expansion_requested_lei_sample": _sample_text(lei_expansion_request_leis),
        "gleif_lei_expansion_legal_name_candidate_requested_count": len(
            [lei for lei in legal_name_candidate_leis if lei in expansion_request_set]
        ),
        "gleif_lei_expansion_legal_name_candidate_requested_sample": _sample_text(
            [lei for lei in legal_name_candidate_leis if lei in expansion_request_set]
        ),
        "gleif_lei_expansion_legal_name_candidate_not_requested_count": len(
            [lei for lei in legal_name_candidate_leis if lei not in expansion_request_set]
        ),
        "gleif_lei_expansion_legal_name_candidate_not_requested_sample": _sample_text(
            [lei for lei in legal_name_candidate_leis if lei not in expansion_request_set]
        ),
        "gleif_lei_expansion_status_counts": _record_status_counts(list(lei_expansions.values())),
        "listings_attached_direct_isin": len(direct_attached),
        "listings_attached_direct_isin_prefix_mismatch_name_confirmed": len(direct_prefix_mismatch_attached),
        "listings_attached_via_lei_expansion": len(expansion_attached),
        "listings_attached_name_anchor_confirmed": len(name_anchor_attached),
        "listings_unattached_no_anchor": len(no_anchor_unattached),
        "listings_unattached_ambiguous": len(ambiguous_unattached),
        "acceptance_pairs_grouped": len(grouped_pairs),
        "pairs_grouped_direct_anchor_plus_lei_expansion": len(
            [row for row in grouped_pairs if row.get("grouping_path") == "direct_anchor_plus_lei_expansion"]
        ),
        "pairs_grouped_direct_lei": len([row for row in grouped_pairs if row.get("grouping_path") == "direct_lei"]),
        "pairs_grouped_name_anchor_confirmed": len(
            [row for row in grouped_pairs if row.get("grouping_path") == "name_anchor_confirmed"]
        ),
        "pairs_blocked_no_valid_anchor": len(
            [row for row in pair_rows if not row.get("grouped") and row.get("reason") == "no_valid_anchor_isin"]
        ),
        "tail_without_anchor_count": len(no_anchor_unattached),
        "tail_without_anchor_examples": _tail_without_anchor_examples(no_anchor_unattached),
        "entity_attach_reason_counts": _field_counts(symbol_rows, "entity_attach_reason"),
        "entity_attach_reasons_counts": _multi_reason_counts(symbol_rows, "entity_attach_reasons"),
        "compatible_isin_gate_status_counts": _field_counts(symbol_rows, "compatible_isin_gate_status"),
        "compatible_isin_gate_reject_reason_counts": _field_counts(
            symbol_rows,
            "compatible_isin_gate_reject_reason",
        ),
        "decision_bucket_counts": decision_bucket_counts,
        "provider_or_curated_by_listing_group_kind": _field_counts(
            [
                row
                for row in symbol_rows
                if row.get("decision_bucket") == "requires_provider_or_curated_identity"
            ],
            "listing_group_kind",
        ),
        "manual_review_by_listing_group_kind": _field_counts(
            [row for row in symbol_rows if row.get("decision_bucket") == "needs_manual_review"],
            "listing_group_kind",
        ),
        "tail_without_anchor_by_listing_group_kind": _field_counts(no_anchor_unattached, "listing_group_kind"),
        "fixable_free": fixable_free_count,
        "fixable_free_count": fixable_free_count,
        "fixable_free_rate": _rate(fixable_free_count, candidate_count),
        "requires_provider_or_curated_identity": provider_or_curated_count,
        "provider_or_curated_count": provider_or_curated_count,
        "provider_or_curated_rate": _rate(provider_or_curated_count, candidate_count),
        "needs_manual_review": review_count,
        "review_count": review_count,
        "review_rate": _rate(review_count, candidate_count),
        "name_anchor_precision_audit_count": len(name_anchor_precision_audit),
        "precision_audit_count": len(name_anchor_precision_audit),
        "accepted_pairs_passed": bool(accepted_pair_rows) and all(row.get("grouped") for row in accepted_pair_rows),
        "guardrail_pairs_unmerged": all(not row.get("grouped") for row in guardrail_pair_rows),
        "unresolved_percentage": _rate(len(no_anchor_unattached) + len(ambiguous_unattached), candidate_count),
        "entity_groups_formed": len([symbols for symbols in lei_groups.values() if len(symbols) > 1]),
        "entity_group_symbols": {lei: symbols for lei, symbols in sorted(lei_groups.items()) if len(symbols) > 1},
        "adr_home_pairs_grouped": len([row for row in grouped_pairs if row.get("pair_type") == "adr_home"]),
        "share_class_pairs_grouped": len([row for row in grouped_pairs if row.get("pair_type") == "share_class"]),
        "adrs_mapping_to_depositary_or_ambiguous_lei_count": len(
            [
                row
                for row in symbol_rows
                if _is_adr_like(row) and _symbol(row.get("lei_role")) in {"DEPOSITARY", "AMBIGUOUS"}
            ]
        ),
        "unresolved_no_isin_count": len([row for row in symbol_rows if not row.get("isin") and row.get("isin_status") != "suspect"]),
        "unresolved_no_lei_count": len([row for row in symbol_rows if row.get("isin") and not row.get("entity_lei")]),
        "openfigi_not_found_count": len([row for row in symbol_rows if row.get("openfigi_status") == "not_found"]),
        "batch_split_retries": int(batch_split_retries),
    }


def _is_guardrail_pair(row: dict[str, Any]) -> bool:
    symbols = {_symbol(symbol) for symbol in row.get("pair", [])}
    return row.get("pair_type") == "bare_collision" or symbols == {"TLS", "TLS.AX"}


def _not_grouped_reason(left: dict[str, Any], right: dict[str, Any]) -> str:
    if not left or not right:
        return "symbol_missing_from_measurement"
    left_entity_lei = _symbol(left.get("entity_lei") or left.get("lei"))
    right_entity_lei = _symbol(right.get("entity_lei") or right.get("lei"))
    if not left_entity_lei or not right_entity_lei:
        if not left.get("direct_lei") and not right.get("direct_lei"):
            return "no_valid_anchor_isin"
        methods = {_symbol(left.get("entity_attach_method")), _symbol(right.get("entity_attach_method"))}
        if "UNATTACHED_AMBIGUOUS" in methods:
            return "ambiguous_lei_expansion_attach"
        if not left.get("isin") or not right.get("isin"):
            return "missing_isin_or_anchor"
        return "missing_lei"
    if left_entity_lei != right_entity_lei:
        roles = {_symbol(left.get("lei_role")), _symbol(right.get("lei_role"))}
        if "DEPOSITARY" in roles or "AMBIGUOUS" in roles:
            return "different_lei_depositary_or_ambiguous"
        return "different_lei"
    return "unknown"


def _grouping_path(*, grouped: bool, left_method: str, right_method: str) -> str:
    if not grouped:
        return ""
    methods = {left_method, right_method}
    if "name_anchor_confirmed" in methods:
        return "name_anchor_confirmed"
    if "lei_expansion" in methods:
        return "direct_anchor_plus_lei_expansion"
    direct_methods = {"direct_isin", "isin_direct_prefix_mismatch_name_confirmed"}
    if methods <= direct_methods:
        return "direct_lei"
    return "other"


def _pairs_for_symbols(symbols: list[str]) -> list[tuple[str, str, str]]:
    return acceptance_pairs_for_symbols(symbols)


def _openfigi_fixture(
    ticker: str,
    name: str,
    composite_figi: str,
    share_class_figi: str,
    country: str,
    currency: str,
    security_type: str,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "name": name,
        "figi": f"{composite_figi}F",
        "compositeFIGI": composite_figi,
        "shareClassFIGI": share_class_figi,
        "country": country,
        "currency": currency,
        "securityType2": security_type,
    }


def _lei_role(record: GleifIsinLeiRecord | None) -> str:
    if not record or not isinstance(record.response_payload, dict):
        return ""
    return _symbol(record.response_payload.get("lei_role") or record.response_payload.get("role"))


def _direct_isin_name_confirmed(
    *,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    legal_name: str,
) -> bool:
    normalized_legal_name = normalize_legal_name_conservative(legal_name)
    if not normalized_legal_name:
        return False
    listing_names = []
    if openfigi and openfigi.name:
        listing_names.append(openfigi.name)
    if candidate.name:
        listing_names.append(candidate.name)
    if not listing_names:
        listing_names.append(_best_listing_name(candidate, openfigi))
    for query in legal_name_query_variants_from_listing(*listing_names):
        if normalize_legal_name_conservative(query) == normalized_legal_name:
            return True
    return False


def _direct_prefix_mismatch_name_confirmed(
    *,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    legal_name: str,
) -> bool:
    if _direct_isin_name_confirmed(candidate=candidate, openfigi=openfigi, legal_name=legal_name):
        return True
    normalized_legal_name = _normalize_direct_prefix_mismatch_name(legal_name)
    if not normalized_legal_name:
        return False
    listing_names = []
    if openfigi and openfigi.name:
        listing_names.append(openfigi.name)
    if candidate.name:
        listing_names.append(candidate.name)
    if not listing_names:
        listing_names.append(_best_listing_name(candidate, openfigi))
    return any(_normalize_direct_prefix_mismatch_name(query) == normalized_legal_name for query in listing_names)


def _normalize_direct_prefix_mismatch_name(value: Any) -> str:
    normalized = normalize_legal_name_conservative(value)
    if normalized.endswith(" PUBLIC"):
        return normalized[: -len(" PUBLIC")]
    return normalized


def _is_us_listing(*, candidate: ListingCandidate, openfigi: OpenFigiMapping | None) -> bool:
    listing_country = _symbol(candidate.country)
    if listing_country == "US":
        return True
    openfigi_country = _symbol(openfigi.country if openfigi else "")
    if openfigi_country == "US":
        return True
    exchange = _symbol((openfigi.payload if openfigi else {}).get("exchCode") if openfigi else "")
    return exchange == "US"


def _annotate_listing_group_profiles(
    rows: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups_by_symbol: dict[str, set[str]] = {str(row.get("symbol") or ""): set() for row in rows}
    row_by_symbol = {str(row.get("symbol") or ""): row for row in rows}

    for pair in pair_rows:
        symbols = {_symbol(symbol) for symbol in pair.get("pair", []) if _symbol(symbol) in row_by_symbol}
        if len(symbols) > 1:
            for symbol in symbols:
                groups_by_symbol.setdefault(symbol, set()).update(symbols)

    lei_groups: dict[str, set[str]] = {}
    name_groups: dict[str, set[str]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "")
        lei = _symbol(row.get("entity_lei"))
        if lei:
            lei_groups.setdefault(f"LEI:{lei}", set()).add(symbol)
        name_key = _listing_group_name_key(row)
        if name_key:
            name_groups.setdefault(f"NAME:{name_key}", set()).add(symbol)

    for grouped in list(lei_groups.values()) + list(name_groups.values()):
        if len(grouped) <= 1:
            continue
        for symbol in grouped:
            groups_by_symbol.setdefault(symbol, set()).update(grouped)

    out = []
    for row in rows:
        symbol = str(row.get("symbol") or "")
        symbols = sorted(groups_by_symbol.get(symbol) or {symbol})
        annotated = dict(row)
        annotated["listing_group_kind"] = "multi_listing_candidate" if len(symbols) > 1 else "single_listing"
        annotated["listing_group_size_in_measurement"] = len(symbols)
        annotated["listing_group_symbols_in_measurement"] = symbols
        out.append(annotated)
    return out


def _listing_group_name_key(row: dict[str, Any]) -> str:
    for value in (
        row.get("entity_legal_name"),
        row.get("candidate_legal_name"),
        row.get("legal_name"),
        row.get("openfigi_name"),
        row.get("internal_candidate_name"),
        row.get("listing_name_used_for_legal_name_search"),
    ):
        normalized = normalize_legal_name_conservative(value)
        if normalized:
            return normalized
    return ""


def _is_adr_like(row: dict[str, Any]) -> bool:
    text = f"{row.get('security_type') or ''} {row.get('symbol') or ''}".upper()
    return "ADR" in text or "DEPOSITARY" in text


def _unattached_reasons(
    row: dict[str, Any],
    openfigi: OpenFigiMapping | None,
    name_evaluation: dict[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if openfigi and openfigi.status == "not_found":
        reasons.append("openfigi_not_found")

    reject_reason = str(name_evaluation.get("reject_reason") or "")
    if reject_reason:
        reasons.append(reject_reason)
    gate_reject_reason = str(name_evaluation.get("compatible_isin_gate_reject_reason") or "")
    if gate_reject_reason:
        reasons.append(gate_reject_reason)
    direct_reject_reason = str(row.get("direct_isin_attach_reject_reason") or "")
    if direct_reject_reason:
        reasons.append(direct_reject_reason)

    isin_status = str(row.get("isin_status") or "")
    if isin_status == "success" and not row.get("direct_lei"):
        reasons.append("valid_isin_no_gleif_lei")
    elif isin_status == "suspect":
        reasons.append("yfinance_isin_suspect")
    elif isin_status in {"missing", "not_found"}:
        reasons.append("provider_isin_missing")
    elif isin_status == "error":
        reasons.append("provider_isin_error")

    if not reasons:
        reasons.append("needs_manual_review")
    return _unique_ordered(reasons)


def _primary_unattached_reason(reasons: list[str]) -> str:
    priority = [
        "legal_name_search_ambiguous",
        "legal_name_match_country_incompatible",
        "missing_listing_country_for_isin_gate",
        "gleif_lei_found_but_no_compatible_isin",
        "no_compatible_expanded_isin_for_listing_country",
        "direct_isin_prefix_mismatch_name_unconfirmed",
        "valid_isin_no_gleif_lei",
        "openfigi_not_found",
        "no_listing_name_for_legal_name_search",
        "legal_name_search_no_match",
        "yfinance_isin_suspect",
        "provider_isin_missing",
        "provider_isin_error",
    ]
    for reason in priority:
        if reason in reasons:
            return reason
    return reasons[0] if reasons else "needs_manual_review"


def _decision_bucket_for_reasons(reasons: list[str]) -> str:
    if any(
        reason in reasons
        for reason in {
            "legal_name_search_ambiguous",
            "multiple_compatible_expanded_lei",
            "direct_isin_prefix_mismatch_name_unconfirmed",
        }
    ):
        return "needs_manual_review"
    if any(
        reason in reasons
        for reason in {
            "legal_name_match_country_incompatible",
            "missing_listing_country_for_isin_gate",
            "multiple_legal_name_anchor_candidates",
            "needs_manual_review",
        }
    ):
        return "needs_manual_review"
    if any(
        reason in reasons
        for reason in {
            "valid_isin_no_gleif_lei",
            "gleif_lei_found_but_no_compatible_isin",
            "no_compatible_expanded_isin_for_listing_country",
        }
    ):
        return "requires_provider_or_curated_identity"
    return "fixable_free"


def _decision_bucket_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {bucket: 0 for bucket in TAIL_DECISION_BUCKETS}
    for row in rows:
        bucket = str(row.get("decision_bucket") or "")
        if bucket in counts:
            counts[bucket] += 1
    return {key: value for key, value in sorted(counts.items()) if value}


def _name_anchor_precision_audit(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("entity_attach_method") != "name_anchor_confirmed":
            continue
        out.append(
            {
                "symbol": row.get("symbol") or "",
                "provider_symbol": row.get("provider_symbol") or "",
                "input_name": row.get("internal_candidate_name") or "",
                "internal_candidate_name": row.get("internal_candidate_name") or "",
                "openfigi_name": row.get("openfigi_name") or "",
                "listing_name_used_for_legal_name_search": row.get("listing_name_used_for_legal_name_search") or "",
                "matched_gleif_legal_name": row.get("candidate_legal_name") or row.get("legal_name") or "",
                "lei": row.get("entity_lei") or row.get("lei") or "",
                "legal_country": row.get("candidate_legal_country") or "",
                "headquarters_country": row.get("candidate_headquarters_country") or "",
                "matched_compatible_isins": list(row.get("matched_compatible_isins") or []),
                "compatible_expanded_isin_count": int(row.get("compatible_expanded_isin_count") or 0),
                "lei_expanded_isin_count": int(row.get("lei_expanded_isin_count") or 0),
                "compatible_isin_candidate_sample": list(row.get("compatible_isin_candidate_sample") or []),
                "compatible_isin_candidate_count": int(row.get("compatible_isin_candidate_count") or 0),
                "expected_listing_country_prefix_present": bool(
                    row.get("expected_listing_country_prefix_present")
                ),
                "derived_listing_country": row.get("derived_listing_country") or "",
                "allowed_isin_prefixes": list(row.get("allowed_isin_prefixes") or []),
                "listing_name_search_queries": list(row.get("listing_name_search_queries") or []),
                "compatible_isin_gate_status": row.get("compatible_isin_gate_status") or "",
                "compatible_isin_gate_reject_reason": row.get("compatible_isin_gate_reject_reason") or "",
                "confidence": row.get("attachment_confidence") or "",
                "provenance": row.get("attachment_provenance") or "",
            }
        )
    return out


def _compatible_isins_for_listing(*, candidate: ListingCandidate, isins: list[str]) -> list[str]:
    allowed_prefixes = allowed_isin_prefixes_for_listing(candidate)
    if not allowed_prefixes:
        return []
    return sorted({_symbol(isin) for isin in isins if _symbol(isin)[:2] in allowed_prefixes})


def _sample_isins(isins: list[str] | set[str] | tuple[str, ...], *, limit: int = MAX_ISIN_OUTPUT_SAMPLE) -> list[str]:
    return sorted(_symbol(isin) for isin in isins if _symbol(isin))[:limit]


def _sample_text(values: list[str] | set[str] | tuple[str, ...], *, limit: int = MAX_ISIN_OUTPUT_SAMPLE) -> list[str]:
    return sorted(_symbol(value) for value in values if _symbol(value))[:limit]


def _record_status_counts(records: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        status = str(getattr(record, "status", "") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _unique_ordered(values: list[str]) -> list[str]:
    out = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _names_compatible(listing_name: str, legal_name: str) -> bool:
    normalized_listing = normalize_legal_name_conservative(listing_name)
    return bool(normalized_listing and normalized_listing == normalize_legal_name_conservative(legal_name))


def _tail_without_anchor_examples(rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, str]]:
    examples = []
    for row in rows:
        if _symbol(row.get("listing_country")) == "US" or _symbol(row.get("openfigi_exchange")) == "US":
            continue
        examples.append(
            {
                "symbol": str(row.get("symbol") or ""),
                "provider_symbol": str(row.get("provider_symbol") or ""),
                "listing_country": str(row.get("listing_country") or ""),
                "openfigi_exchange": str(row.get("openfigi_exchange") or ""),
                "isin_status": str(row.get("isin_status") or ""),
                "reason": str(row.get("entity_attach_reason") or ""),
                "decision_bucket": str(row.get("decision_bucket") or ""),
            }
        )
        if len(examples) >= limit:
            break
    return examples


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def _reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("isin_error_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _field_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(field) or "")
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _multi_reason_counts(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        values = row.get(field) or []
        if isinstance(values, str):
            values = [values]
        for value in values:
            reason = str(value or "")
            if not reason:
                continue
            counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _symbol(value: Any) -> str:
    return str(value or "").strip().upper()
