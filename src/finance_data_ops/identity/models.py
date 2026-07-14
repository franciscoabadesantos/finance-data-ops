"""Entity identity V0 data structures.

The entity layer is side-by-side with current ticker-keyed product tables. These
models keep listing identity, entity identity, and audit output explicit so the
resolver never has to mutate product/read-path data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ListingCandidate:
    symbol: str
    provider_symbol: str = ""
    exchange: str = ""
    exchange_mic: str = ""
    country: str = ""
    currency: str = ""
    name: str = ""
    source: str = ""
    has_prices: bool = False
    has_technicals: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return bool(self.has_prices and self.has_technicals)


@dataclass(frozen=True, slots=True)
class OpenFigiRequest:
    symbol: str
    payload: dict[str, Any]
    request_hash: str


@dataclass(frozen=True, slots=True)
class OpenFigiMapping:
    symbol: str
    request_hash: str
    status: str
    payload: dict[str, Any]
    response_payload: dict[str, Any] | None = None
    error_message: str = ""
    figi: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    isin: str = ""
    lei: str = ""
    legal_entity_id: str = ""
    ticker: str = ""
    name: str = ""
    exchange: str = ""
    exchange_mic: str = ""
    country: str = ""
    currency: str = ""
    home_country: str = ""
    security_type: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_strong_identity(self) -> bool:
        return bool(self.legal_entity_id or self.lei or self.isin)

    @property
    def has_security_identity(self) -> bool:
        return bool(self.figi or self.composite_figi or self.share_class_figi)


@dataclass(frozen=True, slots=True)
class EntityRecord:
    entity_id: str
    legal_name: str = ""
    display_name: str = ""
    home_country: str = ""
    lei: str = ""
    entity_source: str = "openfigi"
    resolution_confidence: float = 0.0
    resolution_status: str = "unresolved"
    primary_listing_symbol: str = ""
    primary_listing_reason: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EntityListingRecord:
    symbol: str
    entity_id: str
    provider_symbol: str = ""
    exchange: str = ""
    exchange_mic: str = ""
    country: str = ""
    currency: str = ""
    figi: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    isin: str = ""
    lei: str = ""
    listing_type: str = ""
    is_primary_listing: bool = False
    primary_listing_reason: str = ""
    resolution_source: str = "openfigi"
    resolution_confidence: float = 0.0
    resolution_status: str = "unresolved"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IdentityAuditRecord:
    symbol: str = ""
    entity_id: str = ""
    issue_type: str = ""
    issue_severity: str = "warning"
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IdentityBuildResult:
    entities: list[EntityRecord]
    listings: list[EntityListingRecord]
    audits: list[IdentityAuditRecord]
    openfigi_cache_rows: list[dict[str, Any]]
    unresolved_symbols: list[str]
    ambiguous_symbols: list[str]
    batch_split_retries: int = 0

    def summary(self) -> dict[str, Any]:
        issue_counts: dict[str, int] = {}
        primary_decisions = []
        for audit in self.audits:
            issue_counts[audit.issue_type] = issue_counts.get(audit.issue_type, 0) + 1
        for entity in self.entities:
            if entity.primary_listing_symbol:
                primary_decisions.append(
                    {
                        "entity_id": entity.entity_id,
                        "symbol": entity.primary_listing_symbol,
                        "reason": entity.primary_listing_reason,
                    }
                )
        return {
            "resolved_entities": len([e for e in self.entities if e.resolution_status == "resolved"]),
            "resolved_listings": len([listing for listing in self.listings if listing.resolution_status == "resolved"]),
            "security_resolved_listings": len(
                [
                    row
                    for row in self.openfigi_cache_rows
                    if str(row.get("status") or "").strip().lower() == "success"
                    and _has_security_identifier(row.get("response_payload"))
                ]
            ),
            "entity_resolved_listings": len([listing for listing in self.listings if listing.resolution_status == "resolved"]),
            "entity_unresolved_security_only": issue_counts.get("security_only_group_not_resolved", 0),
            "entity_unresolved_no_openfigi_match": len(
                [
                    row
                    for row in self.openfigi_cache_rows
                    if str(row.get("status") or "").strip().lower() == "not_found"
                ]
            ),
            "entity_unresolved_openfigi_error": len(
                [
                    row
                    for row in self.openfigi_cache_rows
                    if str(row.get("status") or "").strip().lower() == "error"
                ]
            ),
            "strong_company_identifier_groups": len(
                [
                    entity
                    for entity in self.entities
                    if str(entity.metadata.get("identity_key_kind") or "") in {"legal_entity_id", "lei", "isin"}
                ]
            ),
            "security_identifier_only_groups": issue_counts.get("security_only_group_not_resolved", 0),
            "batch_split_retries": int(self.batch_split_retries),
            "entities": len(self.entities),
            "listings_mapped": len(self.listings),
            "unresolved_symbols": len(self.unresolved_symbols),
            "unresolved_symbol_list": list(self.unresolved_symbols),
            "ambiguous_symbols": len(self.ambiguous_symbols),
            "ambiguous_symbol_list": list(self.ambiguous_symbols),
            "audit_suggestions": sum(
                issue_counts.get(issue_type, 0)
                for issue_type in (
                    "possible_same_company_not_resolved",
                    "same_name_different_security_identifier",
                    "adr_home_candidate_missing_entity_identifier",
                    "fuzzy_match_suggestion_not_resolved",
                )
            ),
            "provider_symbol_normalized": issue_counts.get("provider_symbol_normalized", 0),
            "openfigi_errors": len(
                [
                    row
                    for row in self.openfigi_cache_rows
                    if str(row.get("status") or "").strip().lower() == "error"
                ]
            ),
            "openfigi_not_found": len(
                [
                    row
                    for row in self.openfigi_cache_rows
                    if str(row.get("status") or "").strip().lower() == "not_found"
                ]
            ),
            "strong_entity_groups": len(
                [
                    entity
                    for entity in self.entities
                    if str(entity.metadata.get("identity_key_kind") or "") in {"legal_entity_id", "lei", "isin"}
                ]
            ),
            "security_only_groups": issue_counts.get("security_only_group_not_resolved", 0),
            "primary_listing_decisions": primary_decisions,
            "audit_issue_counts": dict(sorted(issue_counts.items())),
        }


def _has_security_identifier(response_payload: Any) -> bool:
    if not isinstance(response_payload, dict):
        return False
    data = response_payload.get("data")
    if not isinstance(data, list) or not data:
        return False
    first = data[0]
    if not isinstance(first, dict):
        return False
    return bool(first.get("figi") or first.get("compositeFIGI") or first.get("compositeFigi") or first.get("shareClassFIGI") or first.get("shareClassFigi"))
