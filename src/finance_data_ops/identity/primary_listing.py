"""Deterministic primary-listing policy for entity identity V0."""

from __future__ import annotations

from dataclasses import replace

from finance_data_ops.identity.audit import audit
from finance_data_ops.identity.models import EntityListingRecord, EntityRecord, IdentityAuditRecord, ListingCandidate


def apply_primary_listing_policy(
    *,
    entity: EntityRecord,
    listings: list[EntityListingRecord],
    candidates_by_symbol: dict[str, ListingCandidate],
) -> tuple[EntityRecord, list[EntityListingRecord], list[IdentityAuditRecord]]:
    audits: list[IdentityAuditRecord] = []
    complete = [listing for listing in listings if _is_complete(listing.symbol, candidates_by_symbol)]
    if not complete:
        audits.append(
            audit(
                "primary_listing_not_selected",
                entity_id=entity.entity_id,
                reason="no_listing_has_prices_and_technicals",
                symbols=[listing.symbol for listing in listings],
            )
        )
        return entity, listings, audits

    if len(complete) == 1:
        chosen = complete[0]
        reason = "only_complete_listing"
    else:
        home = str(entity.home_country or "").strip().upper()
        home_complete = [listing for listing in complete if home and str(listing.country or "").strip().upper() == home]
        if home_complete:
            chosen = _sort_listings(home_complete)[0]
            reason = "complete_home_listing"
        else:
            chosen = _sort_listings(complete)[0]
            reason = "deterministic_complete_listing"
            audits.append(
                audit(
                    "primary_listing_home_listing_unavailable",
                    entity_id=entity.entity_id,
                    reason="no_complete_listing_matches_home_country",
                    home_country=home,
                    selected_symbol=chosen.symbol,
                    complete_symbols=[listing.symbol for listing in complete],
                )
            )

    updated_entity = replace(
        entity,
        primary_listing_symbol=chosen.symbol,
        primary_listing_reason=reason,
    )
    updated_listings = [
        replace(
            listing,
            is_primary_listing=listing.symbol == chosen.symbol,
            primary_listing_reason=reason if listing.symbol == chosen.symbol else "",
        )
        for listing in listings
    ]
    return updated_entity, updated_listings, audits


def _is_complete(symbol: str, candidates_by_symbol: dict[str, ListingCandidate]) -> bool:
    candidate = candidates_by_symbol.get(str(symbol or "").strip().upper())
    return bool(candidate and candidate.is_complete)


def _sort_listings(listings: list[EntityListingRecord]) -> list[EntityListingRecord]:
    return sorted(
        listings,
        key=lambda listing: (
            0 if listing.exchange_mic else 1,
            0 if listing.currency else 1,
            listing.symbol,
        ),
    )
