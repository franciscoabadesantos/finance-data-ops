"""Canonical entity/listing resolver for Entity Layer V0."""

from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from typing import Any

from finance_data_ops.identity.audit import audit
from finance_data_ops.identity.models import (
    EntityListingRecord,
    EntityRecord,
    IdentityAuditRecord,
    IdentityBuildResult,
    ListingCandidate,
    OpenFigiMapping,
)
from finance_data_ops.identity.openfigi import openfigi_cache_rows
from finance_data_ops.identity.primary_listing import apply_primary_listing_policy


def build_entity_identity(
    *,
    candidates: list[ListingCandidate],
    mappings: list[OpenFigiMapping],
) -> IdentityBuildResult:
    candidates_by_symbol = {_symbol(candidate.symbol): candidate for candidate in candidates if _symbol(candidate.symbol)}
    mappings_by_symbol = {_symbol(mapping.symbol): mapping for mapping in mappings if _symbol(mapping.symbol)}
    audits: list[IdentityAuditRecord] = []
    unresolved_symbols: list[str] = []
    ambiguous_symbols: list[str] = []
    grouped: dict[tuple[str, str], list[OpenFigiMapping]] = {}
    security_only_mappings: list[OpenFigiMapping] = []

    for symbol in sorted(candidates_by_symbol):
        mapping = mappings_by_symbol.get(symbol)
        if mapping is None:
            unresolved_symbols.append(symbol)
            audits.append(audit("openfigi_missing_mapping", symbol=symbol, severity="warning"))
            continue
        if mapping.status == "ambiguous":
            ambiguous_symbols.append(symbol)
            audits.append(
                audit(
                    "openfigi_ambiguous_mapping",
                    symbol=symbol,
                    severity="warning",
                    request_hash=mapping.request_hash,
                    error_message=mapping.error_message,
                    response_payload=mapping.response_payload,
                )
            )
            continue
        if mapping.status != "success":
            unresolved_symbols.append(symbol)
            audits.append(
                audit(
                    "openfigi_unresolved_mapping",
                    symbol=symbol,
                    severity="warning",
                    status=mapping.status,
                    error_message=mapping.error_message,
                    request_hash=mapping.request_hash,
                )
            )
            continue

        normalization_audit = _provider_symbol_normalization_audit(mapping, candidates_by_symbol.get(symbol))
        if normalization_audit:
            audits.append(normalization_audit)

        group_key = _strong_group_key(mapping)
        if group_key is None:
            unresolved_symbols.append(symbol)
            security_only_mappings.append(mapping)
            continue
        grouped.setdefault(group_key, []).append(mapping)

    audits.extend(_security_only_audits(security_only_mappings, candidates_by_symbol))
    audits.extend(_fuzzy_suggestion_audits(candidates_by_symbol, mappings_by_symbol, unresolved_symbols))

    entities: list[EntityRecord] = []
    listings: list[EntityListingRecord] = []
    for group_key, group_mappings in sorted(grouped.items(), key=lambda item: item[0]):
        entity, entity_listings, group_audits = _records_for_group(group_key, group_mappings, candidates_by_symbol)
        entity, entity_listings, primary_audits = apply_primary_listing_policy(
            entity=entity,
            listings=entity_listings,
            candidates_by_symbol=candidates_by_symbol,
        )
        entities.append(entity)
        listings.extend(entity_listings)
        audits.extend(group_audits)
        audits.extend(primary_audits)

    return IdentityBuildResult(
        entities=entities,
        listings=listings,
        audits=audits,
        openfigi_cache_rows=openfigi_cache_rows(mappings),
        unresolved_symbols=sorted(set(unresolved_symbols)),
        ambiguous_symbols=sorted(set(ambiguous_symbols)),
    )


def _records_for_group(
    group_key: tuple[str, str],
    mappings: list[OpenFigiMapping],
    candidates_by_symbol: dict[str, ListingCandidate],
) -> tuple[EntityRecord, list[EntityListingRecord], list[IdentityAuditRecord]]:
    key_kind, key_value = group_key
    entity_id = _entity_id(group_key)
    audits: list[IdentityAuditRecord] = []
    sorted_mappings = sorted(mappings, key=lambda mapping: mapping.symbol)
    home_country = _choose_home_country(sorted_mappings, candidates_by_symbol)
    legal_name = _first_text([mapping.name for mapping in sorted_mappings])
    lei = _first_text([mapping.lei for mapping in sorted_mappings])
    if _conflicting_values(mapping.home_country for mapping in sorted_mappings):
        audits.append(
            audit(
                "conflicting_home_country",
                entity_id=entity_id,
                severity="warning",
                values=sorted({mapping.home_country for mapping in sorted_mappings if mapping.home_country}),
                symbols=[mapping.symbol for mapping in sorted_mappings],
            )
        )

    entity = EntityRecord(
        entity_id=entity_id,
        legal_name=legal_name,
        display_name=legal_name or _first_text([candidate.name for candidate in candidates_by_symbol.values()]),
        home_country=home_country,
        lei=lei,
        entity_source="openfigi",
        resolution_confidence=_confidence_for_key_kind(key_kind),
        resolution_status="resolved",
        metadata={
            "identity_key_kind": key_kind,
            "identity_key_value": key_value,
            "source": "entity_layer_v0",
        },
    )
    listings = [_listing_record(entity_id, mapping, candidates_by_symbol.get(_symbol(mapping.symbol))) for mapping in sorted_mappings]
    return entity, listings, audits


def _listing_record(
    entity_id: str,
    mapping: OpenFigiMapping,
    candidate: ListingCandidate | None,
) -> EntityListingRecord:
    candidate_metadata = dict((candidate.metadata if candidate else {}) or {})
    metadata = {
        "openfigi": mapping.metadata,
        "candidate": candidate_metadata,
    }
    return EntityListingRecord(
        symbol=_symbol(mapping.symbol),
        entity_id=entity_id,
        provider_symbol=(candidate.provider_symbol if candidate and candidate.provider_symbol else _symbol(mapping.symbol)),
        exchange=mapping.exchange or (candidate.exchange if candidate else ""),
        exchange_mic=mapping.exchange_mic or (candidate.exchange_mic if candidate else ""),
        country=mapping.country or (candidate.country if candidate else ""),
        currency=mapping.currency or (candidate.currency if candidate else ""),
        figi=mapping.figi,
        composite_figi=mapping.composite_figi,
        share_class_figi=mapping.share_class_figi,
        isin=mapping.isin,
        lei=mapping.lei,
        listing_type=mapping.security_type,
        resolution_source="openfigi",
        resolution_confidence=0.98 if mapping.legal_entity_id or mapping.lei or mapping.isin else 0.0,
        resolution_status="resolved",
        metadata=metadata,
    )


def _strong_group_key(mapping: OpenFigiMapping) -> tuple[str, str] | None:
    for key_kind, value in (
        ("legal_entity_id", mapping.legal_entity_id),
        ("lei", mapping.lei),
        ("isin", mapping.isin),
    ):
        cleaned = str(value or "").strip().upper()
        if cleaned:
            return key_kind, cleaned
    return None


def _entity_id(group_key: tuple[str, str]) -> str:
    raw = f"{group_key[0]}:{group_key[1]}"
    digest = hashlib.sha1(raw.encode()).hexdigest()[:16]
    return f"ofg_{digest}"


def _confidence_for_key_kind(kind: str) -> float:
    if kind in {"legal_entity_id", "lei"}:
        return 0.98
    if kind == "isin":
        return 0.9
    return 0.75


def _choose_home_country(
    mappings: list[OpenFigiMapping],
    candidates_by_symbol: dict[str, ListingCandidate],
) -> str:
    home = _first_text([mapping.home_country for mapping in mappings])
    if home:
        return home
    countries = [mapping.country for mapping in mappings if mapping.country]
    non_us = sorted({country for country in countries if country != "US"})
    if non_us:
        return non_us[0]
    candidate_countries = [candidates_by_symbol.get(_symbol(mapping.symbol)).country for mapping in mappings if candidates_by_symbol.get(_symbol(mapping.symbol))]
    return _first_text(candidate_countries)


def _provider_symbol_normalization_audit(
    mapping: OpenFigiMapping,
    candidate: ListingCandidate | None,
) -> IdentityAuditRecord | None:
    requested = _symbol(mapping.payload.get("idValue"))
    provider = _symbol((candidate.provider_symbol if candidate else "") or mapping.symbol)
    if requested and provider and requested != provider:
        return audit(
            "provider_symbol_normalized",
            symbol=mapping.symbol,
            severity="info",
            provider_symbol=provider,
            openfigi_ticker=requested,
            request_payload=mapping.payload,
        )
    return None


def _security_only_audits(
    mappings: list[OpenFigiMapping],
    candidates_by_symbol: dict[str, ListingCandidate],
) -> list[IdentityAuditRecord]:
    audits: list[IdentityAuditRecord] = []
    for mapping in mappings:
        audits.append(
            audit(
                "security_only_group_not_resolved",
                symbol=mapping.symbol,
                severity="warning",
                figi=mapping.figi,
                composite_figi=mapping.composite_figi,
                share_class_figi=mapping.share_class_figi,
                reason="missing_legal_entity_id_lei_or_isin",
            )
        )

    by_name: dict[str, list[OpenFigiMapping]] = {}
    for mapping in mappings:
        candidate = candidates_by_symbol.get(_symbol(mapping.symbol))
        name = _normalized_name(mapping.name or (candidate.name if candidate else ""))
        if name:
            by_name.setdefault(name, []).append(mapping)

    for name, name_mappings in sorted(by_name.items()):
        unique = sorted(name_mappings, key=lambda mapping: mapping.symbol)
        if len(unique) < 2:
            continue
        security_keys = {_security_key(mapping) for mapping in unique if _security_key(mapping)}
        if len(security_keys) > 1:
            symbols = [mapping.symbol for mapping in unique]
            countries = sorted({mapping.country for mapping in unique if mapping.country})
            audits.append(
                audit(
                    "same_name_different_security_identifier",
                    severity="info",
                    normalized_name=name,
                    symbols=symbols,
                    security_identifiers=sorted(security_keys),
                )
            )
            audits.append(
                audit(
                    "possible_same_company_not_resolved",
                    severity="info",
                    normalized_name=name,
                    symbols=symbols,
                    reason="no_common_legal_entity_id_lei_or_isin",
                )
            )
            if "US" in countries and any(country != "US" for country in countries):
                audits.append(
                    audit(
                        "adr_home_candidate_missing_entity_identifier",
                        severity="info",
                        normalized_name=name,
                        symbols=symbols,
                        countries=countries,
                    )
                )
    return audits


def _fuzzy_suggestion_audits(
    candidates_by_symbol: dict[str, ListingCandidate],
    mappings_by_symbol: dict[str, OpenFigiMapping],
    unresolved_symbols: list[str],
) -> list[IdentityAuditRecord]:
    unresolved = sorted(set(unresolved_symbols))
    buckets: dict[tuple[str, str], list[str]] = {}
    for symbol in unresolved:
        candidate = candidates_by_symbol.get(symbol)
        mapping = mappings_by_symbol.get(symbol)
        bare = _bare_symbol(symbol)
        name = _normalized_name((mapping.name if mapping else "") or (candidate.name if candidate else ""))
        if bare:
            buckets.setdefault(("bare_symbol", bare), []).append(symbol)
        if name:
            buckets.setdefault(("name", name), []).append(symbol)

    audits: list[IdentityAuditRecord] = []
    seen: set[tuple[str, ...]] = set()
    for (kind, value), symbols in buckets.items():
        unique = sorted(set(symbols))
        if len(unique) < 2:
            continue
        key = tuple(unique)
        if key in seen:
            continue
        seen.add(key)
        audits.append(
            audit(
                "fuzzy_match_suggestion_not_resolved",
                severity="info",
                issue_basis=kind,
                basis_value=value,
                symbols=unique,
            )
        )
    return audits


def _symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _bare_symbol(symbol: str) -> str:
    text = _symbol(symbol)
    return text.split(".", maxsplit=1)[0].replace("-", ".")


def _security_key(mapping: OpenFigiMapping) -> str:
    for key_kind, value in (
        ("share_class_figi", mapping.share_class_figi),
        ("composite_figi", mapping.composite_figi),
        ("figi", mapping.figi),
    ):
        cleaned = _symbol(value)
        if cleaned:
            return f"{key_kind}:{cleaned}"
    return ""


def _normalized_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"\b(inc|corp|corporation|ltd|limited|plc|sa|se|ag|nv|asa|adr|sponsored)\b", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _first_text(values: list[Any]) -> str:
    for value in values:
        text = str(value or "").strip().upper()
        if text:
            return text
    return ""


def _conflicting_values(values: Any) -> bool:
    cleaned = {str(value or "").strip().upper() for value in values if str(value or "").strip()}
    return len(cleaned) > 1
