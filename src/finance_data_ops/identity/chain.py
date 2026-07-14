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
from finance_data_ops.identity.isin import IsinRecord, allowed_isin_prefixes_for_listing, isin_cache_rows
from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping
from finance_data_ops.identity.names import legal_name_query_from_listing, normalize_legal_name_conservative
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
        lei_expansions=lei_expansions,
        legal_name_records=legal_name_records,
    )
    rows_by_symbol = {row["symbol"]: row for row in symbol_rows}
    pair_rows = [_pair_row(left, right, pair_type, rows_by_symbol) for left, right, pair_type in selected_pairs]
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
    listing_name_query = legal_name_query_from_listing(listing_name)
    return {
        "symbol": symbol,
        "provider_symbol": candidate.provider_symbol or symbol,
        "listing_country": candidate.country or "",
        "listing_currency": candidate.currency or "",
        "openfigi_ticker": request_payload.get("idValue") or "",
        "openfigi_exchange": request_payload.get("micCode") or request_payload.get("exchCode") or "",
        "openfigi_status": openfigi.status if openfigi else "missing",
        "openfigi_name": openfigi.name if openfigi else "",
        "internal_candidate_name": candidate.name or "",
        "listing_name_used_for_legal_name_search": listing_name_query,
        "figi": openfigi.figi if openfigi else "",
        "compositeFIGI": openfigi.composite_figi if openfigi else "",
        "shareClassFIGI": openfigi.share_class_figi if openfigi else "",
        "security_type": openfigi.security_type if openfigi else "",
        "isin_source": isin_record.source if isin_record else "",
        "isin_status": isin_status,
        "isin_error_reason": isin_record.error_message if isin_record else "",
        "raw_isin": raw_isin,
        "isin": isin,
        "lei_source": gleif.source if gleif and gleif.lei else "",
        "lei_status": gleif.status if gleif else ("not_requested" if not isin else "missing"),
        "direct_lei": direct_lei,
        "lei": direct_lei,
        "legal_name": direct_legal_name,
        "lei_role": _lei_role(gleif),
        "entity_lei": direct_lei,
        "entity_legal_name": direct_legal_name,
        "entity_attach_method": "direct_isin" if direct_lei else "",
        "entity_attach_reason": "direct_isin_to_lei" if direct_lei else "",
        "entity_attach_reasons": ["direct_isin_to_lei"] if direct_lei else [],
        "decision_bucket": "attached" if direct_lei else "",
        "expanded_candidate_isins": [],
        "lei_expanded_isins": [],
        "lei_expanded_isin_count": 0,
        "compatible_expanded_isin_count": 0,
        "matched_compatible_isins": [],
        "legal_name_anchor_status": "not_requested_direct_isin" if direct_lei else "not_evaluated",
        "legal_name_anchor_reject_reason": "",
        "candidate_lei": "",
        "candidate_legal_name": "",
        "candidate_legal_country": "",
        "candidate_headquarters_country": "",
        "candidate_entity_status": "",
        "candidate_registration_status": "",
    }


def _attach_rows_via_lei_expansion(
    *,
    base_rows: list[dict[str, Any]],
    candidates_by_symbol: dict[str, ListingCandidate],
    openfigi_by_symbol: dict[str, OpenFigiMapping],
    lei_expansions: dict[str, GleifLeiIsinRecord],
    legal_name_records: dict[str, GleifLegalNameRecord],
) -> list[dict[str, Any]]:
    direct_lei_names: dict[str, str] = {}
    for row in base_rows:
        lei = _symbol(row.get("direct_lei"))
        if lei:
            direct_lei_names.setdefault(lei, str(row.get("legal_name") or ""))

    out: list[dict[str, Any]] = []
    for row in base_rows:
        attached = dict(row)
        if attached.get("entity_lei"):
            expansion = lei_expansions.get(_symbol(attached.get("entity_lei")))
            if expansion and expansion.status == "success":
                attached["lei_expanded_isins"] = _sample_isins(expansion.isin_list)
                attached["lei_expanded_isin_count"] = len(expansion.isin_list)
                direct_isin = _symbol(attached.get("isin"))
                if direct_isin and direct_isin in set(expansion.isin_list):
                    attached["compatible_expanded_isin_count"] = 1
                    attached["matched_compatible_isins"] = [direct_isin]
            attached["attachment_provenance"] = "isin_direct"
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
                    "attachment_provenance": "needs_review",
                    "attachment_confidence": "review",
                }
            )
        else:
            name_evaluation = _evaluate_name_anchor(
                candidate=candidate,
                openfigi=openfigi,
                lei_expansions=lei_expansions,
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
        out.append(attached)
    return out


def _lei_expansion_matches(
    *,
    candidate: ListingCandidate,
    openfigi: OpenFigiMapping | None,
    lei_expansions: dict[str, GleifLeiIsinRecord],
    direct_lei_names: dict[str, str],
) -> list[tuple[GleifLeiIsinRecord, list[str]]]:
    matches = []
    allowed_prefixes = allowed_isin_prefixes_for_listing(candidate)
    listing_name = _best_listing_name(candidate, openfigi)
    for lei, record in sorted(lei_expansions.items()):
        if lei not in direct_lei_names:
            continue
        if record.status != "success":
            continue
        candidate_isins = [
            isin
            for isin in record.isin_list
            if not allowed_prefixes or isin[:2] in allowed_prefixes
        ]
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
    legal_name_records: dict[str, GleifLegalNameRecord],
) -> dict[str, Any]:
    listing_name = _best_listing_name(candidate, openfigi)
    normalized_listing_name = normalize_legal_name_conservative(listing_name)
    query_name = legal_name_query_from_listing(listing_name)
    base: dict[str, Any] = {
        "listing_name_used_for_legal_name_search": query_name,
        "status": "not_requested" if not normalized_listing_name else "not_found",
        "reject_reason": "",
        "matches": [],
        "candidate_lei": "",
        "candidate_legal_name": "",
        "candidate_legal_country": "",
        "candidate_headquarters_country": "",
        "candidate_entity_status": "",
        "candidate_registration_status": "",
        "compatible_expanded_isin_count": 0,
        "matched_compatible_isins": [],
    }
    if not normalized_listing_name:
        base["reject_reason"] = "no_listing_name_for_legal_name_search"
        return base

    record = legal_name_records.get(_symbol(normalized_listing_name))
    if not record or record.status != "success":
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

    allowed_prefixes = allowed_isin_prefixes_for_listing(candidate)
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
        return base

    matches: list[tuple[dict[str, Any], GleifLeiIsinRecord, list[str]]] = []
    for name_candidate in country_candidates:
        lei = _symbol(name_candidate.get("lei"))
        expansion = lei_expansions.get(lei)
        if not expansion or expansion.status != "success":
            continue
        candidate_isins = [
            isin
            for isin in expansion.isin_list
            if not allowed_prefixes or isin[:2] in allowed_prefixes
        ]
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
            }
        )
        return base

    _copy_name_candidate_diagnostics(base, country_candidates[0])
    base["status"] = "rejected"
    base["reject_reason"] = "gleif_lei_found_but_no_compatible_isin"
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
        "compatible_expanded_isin_count",
        "matched_compatible_isins",
    ):
        if evaluation.get(key):
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
    direct_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "direct_isin"]
    expansion_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "lei_expansion"]
    name_anchor_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "name_anchor_confirmed"]
    ambiguous_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_ambiguous"]
    no_anchor_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_no_anchor"]
    decision_bucket_counts = _decision_bucket_counts(symbol_rows)
    return {
        "candidate_count": candidate_count,
        "isin_found_count": isin_found,
        "isin_found_rate": _rate(isin_found, candidate_count),
        "isin_suspect_count": len(isin_suspect_rows),
        "isin_suspect_reasons": _reason_counts(isin_suspect_rows),
        "lei_found_count": lei_found,
        "lei_found_rate_among_isins": _rate(lei_found, isin_found),
        "anchor_isin_count": isin_found,
        "anchor_lei_count": len({_symbol(row.get("direct_lei")) for row in direct_attached if row.get("direct_lei")}),
        "lei_expanded_isin_count": int(lei_expanded_isin_count),
        "listings_attached_direct_isin": len(direct_attached),
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
        "decision_bucket_counts": decision_bucket_counts,
        "fixable_free": decision_bucket_counts.get("fixable_free", 0),
        "requires_provider_or_curated_identity": decision_bucket_counts.get("requires_provider_or_curated_identity", 0),
        "needs_manual_review": decision_bucket_counts.get("needs_manual_review", 0),
        "name_anchor_precision_audit_count": len(name_anchor_precision_audit),
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
    if methods == {"direct_isin"}:
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
        "gleif_lei_found_but_no_compatible_isin",
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
    if any(reason in reasons for reason in {"legal_name_search_ambiguous", "multiple_compatible_expanded_lei"}):
        return "needs_manual_review"
    if any(
        reason in reasons
        for reason in {
            "legal_name_match_country_incompatible",
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
                "confidence": row.get("attachment_confidence") or "",
                "provenance": row.get("attachment_provenance") or "",
            }
        )
    return out


def _sample_isins(isins: list[str] | set[str] | tuple[str, ...], *, limit: int = MAX_ISIN_OUTPUT_SAMPLE) -> list[str]:
    return sorted(_symbol(isin) for isin in isins if _symbol(isin))[:limit]


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
