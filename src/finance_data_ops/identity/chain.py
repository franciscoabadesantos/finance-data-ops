"""Measure listing -> ISIN -> LEI -> canonical entity identity chain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from finance_data_ops.identity.gleif import (
    GleifIsinLeiRecord,
    GleifLeiIsinRecord,
    gleif_cache_rows,
    gleif_lei_isin_cache_rows,
)
from finance_data_ops.identity.isin import IsinRecord, allowed_isin_prefixes_for_listing, isin_cache_rows
from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping
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


@dataclass(frozen=True, slots=True)
class EntityChainMeasurement:
    symbol_rows: list[dict[str, Any]]
    pair_rows: list[dict[str, Any]]
    summary: dict[str, Any]
    openfigi_cache_rows: list[dict[str, Any]]
    isin_cache_rows: list[dict[str, Any]]
    gleif_cache_rows: list[dict[str, Any]]
    gleif_lei_isin_cache_rows: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary,
            "symbols": self.symbol_rows,
            "pairs": self.pair_rows,
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
    pairs: list[tuple[str, str, str]] | None = None,
    batch_split_retries: int = 0,
) -> EntityChainMeasurement:
    candidates_by_symbol = {_symbol(candidate.symbol): candidate for candidate in candidates}
    openfigi_by_symbol = {_symbol(mapping.symbol): mapping for mapping in openfigi_mappings}
    isin_by_symbol = {_symbol(record.symbol): record for record in isin_records}
    gleif_by_isin = {_symbol(record.isin): record for record in gleif_records}
    lei_expansions = {_symbol(record.lei): record for record in (gleif_lei_isin_records or [])}
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
    )
    rows_by_symbol = {row["symbol"]: row for row in symbol_rows}
    pair_rows = [_pair_row(left, right, pair_type, rows_by_symbol) for left, right, pair_type in selected_pairs]
    summary = _summary(symbol_rows=symbol_rows, pair_rows=pair_rows, batch_split_retries=batch_split_retries)
    return EntityChainMeasurement(
        symbol_rows=symbol_rows,
        pair_rows=pair_rows,
        summary=summary,
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
        "HSBA.L": {"isin": "GB0005405286", "source": "fixture_yfinance"},
        "GOOG": {"isin": "US02079K1079", "source": "fixture_yfinance"},
        "GOOGL": {"isin": "-", "source": "fixture_yfinance"},
        "LEN": {"isin": "US5260571048", "source": "fixture_yfinance"},
        "LENB": {"isin": "-", "source": "fixture_yfinance"},
    }


def acceptance_gleif_fixtures() -> dict[str, dict[str, Any]]:
    return {
        "US8030542042": {"lei": "529900D6BF99LW9R2E68", "legal_name": "SAP SE", "source": "fixture_gleif"},
        "USN070592100": {"lei": "724500Y6DUVHQD8W8H93", "legal_name": "ASML HOLDING N.V.", "source": "fixture_gleif"},
        "US6701002056": {"lei": "549300DAQ1CVT6CXN342", "legal_name": "NOVO NORDISK A/S", "source": "fixture_gleif"},
        "US7672041008": {"lei": "213800YOEO5OQ72G2R82", "legal_name": "RIO TINTO PLC", "source": "fixture_gleif"},
        "GB0005405286": {"lei": "MP6I5ZYZBEU3UXPYFY54", "legal_name": "HSBC HOLDINGS PLC", "source": "fixture_gleif"},
        "US02079K1079": {"lei": "5493006MHB84DD0ZWV18", "legal_name": "ALPHABET INC.", "source": "fixture_gleif"},
        "US5260571048": {"lei": "549300T8O5DJR4R6H745", "legal_name": "LENNAR CORPORATION", "source": "fixture_gleif"},
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
        "LEI:549300T8O5DJR4R6H745": {
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
    return {
        "symbol": symbol,
        "provider_symbol": candidate.provider_symbol or symbol,
        "listing_country": candidate.country or "",
        "listing_currency": candidate.currency or "",
        "openfigi_ticker": request_payload.get("idValue") or "",
        "openfigi_exchange": request_payload.get("micCode") or request_payload.get("exchCode") or "",
        "openfigi_status": openfigi.status if openfigi else "missing",
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
        "expanded_candidate_isins": [],
        "lei_expanded_isins": [],
    }


def _attach_rows_via_lei_expansion(
    *,
    base_rows: list[dict[str, Any]],
    candidates_by_symbol: dict[str, ListingCandidate],
    openfigi_by_symbol: dict[str, OpenFigiMapping],
    lei_expansions: dict[str, GleifLeiIsinRecord],
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
                attached["lei_expanded_isins"] = list(expansion.isin_list)
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
                    "expanded_candidate_isins": candidate_isins,
                    "lei_expanded_isins": list(record.isin_list),
                    "lei_source": record.source,
                    "lei_status": record.status,
                }
            )
        elif len(matches) > 1:
            attached.update(
                {
                    "entity_attach_method": "unattached_ambiguous",
                    "entity_attach_reason": "multiple_compatible_expanded_lei",
                    "expanded_candidate_isins": sorted({isin for _, isins in matches for isin in isins}),
                    "lei_expanded_isins": sorted({isin for record, _ in matches for isin in record.isin_list}),
                }
            )
        else:
            attached.update(
                {
                    "entity_attach_method": "unattached_no_anchor",
                    "entity_attach_reason": "no_compatible_expanded_lei_anchor",
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
    return {
        "pair": [left, right],
        "pair_type": pair_type,
        "grouped": grouped,
        "reason": "shared_lei" if grouped else _not_grouped_reason(left_row, right_row),
        "left_lei": left_lei,
        "right_lei": right_lei,
        "left_isin": left_row.get("isin") or "",
        "right_isin": right_row.get("isin") or "",
        "left_attach_method": left_row.get("entity_attach_method") or "",
        "right_attach_method": right_row.get("entity_attach_method") or "",
    }


def _summary(*, symbol_rows: list[dict[str, Any]], pair_rows: list[dict[str, Any]], batch_split_retries: int) -> dict[str, Any]:
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
    expanded_isins = sorted(
        {
            isin
            for row in symbol_rows
            for isin in row.get("lei_expanded_isins", [])
            if isin
        }
    )
    direct_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "direct_isin"]
    expansion_attached = [row for row in symbol_rows if row.get("entity_attach_method") == "lei_expansion"]
    ambiguous_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_ambiguous"]
    no_anchor_unattached = [row for row in symbol_rows if row.get("entity_attach_method") == "unattached_no_anchor"]
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
        "lei_expanded_isin_count": len(expanded_isins),
        "listings_attached_direct_isin": len(direct_attached),
        "listings_attached_via_lei_expansion": len(expansion_attached),
        "listings_unattached_no_anchor": len(no_anchor_unattached),
        "listings_unattached_ambiguous": len(ambiguous_unattached),
        "acceptance_pairs_grouped": len(grouped_pairs),
        "tail_without_anchor_count": len(no_anchor_unattached),
        "tail_without_anchor_examples": _tail_without_anchor_examples(no_anchor_unattached),
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


def _names_compatible(listing_name: str, legal_name: str) -> bool:
    listing_tokens = _name_tokens(listing_name)
    legal_tokens = _name_tokens(legal_name)
    if not listing_tokens or not legal_tokens:
        return False
    overlap = listing_tokens & legal_tokens
    if len(overlap) >= 2:
        return True
    return bool(overlap and next(iter(legal_tokens)) in overlap)


def _name_tokens(value: str) -> set[str]:
    text = str(value or "").upper()
    replacements = {
        "-": " ",
        "/": " ",
        ".": " ",
        ",": " ",
        "(": " ",
        ")": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    raw_tokens = [token.strip() for token in text.split() if token.strip()]
    aliases = {
        "CORP": "CORPORATION",
        "CO": "COMPANY",
        "NV": "N",
        "N.V": "N",
        "A/S": "AS",
    }
    stop = {
        "A",
        "B",
        "C",
        "CL",
        "CLASS",
        "COMMON",
        "STOCK",
        "ADR",
        "ADS",
        "SPON",
        "SPONS",
        "SPONSORED",
        "DEPOSITARY",
        "RECEIPT",
        "REG",
        "SHS",
        "INC",
        "PLC",
        "LTD",
        "LIMITED",
        "SE",
        "SA",
        "AG",
        "AS",
        "N",
        "V",
        "THE",
    }
    tokens = set()
    for token in raw_tokens:
        normalized = aliases.get(token, token)
        if len(normalized) < 3 or normalized in stop:
            continue
        tokens.add(normalized)
    return tokens


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


def _symbol(value: Any) -> str:
    return str(value or "").strip().upper()
