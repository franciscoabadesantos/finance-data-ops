"""Publisher for side-by-side entity identity tables."""

from __future__ import annotations

from typing import Any

from finance_data_ops.identity.audit import audit_rows
from finance_data_ops.identity.models import EntityListingRecord, EntityRecord, IdentityAuditRecord, IdentityBuildResult
from finance_data_ops.publish.client import Publisher


def publish_entity_identity(
    *,
    publisher: Publisher,
    result: IdentityBuildResult,
) -> dict[str, Any]:
    outputs: dict[str, Any] = {}
    outputs["source_cache.openfigi_mapping_raw"] = publisher.upsert(
        "source_cache.openfigi_mapping_raw",
        result.openfigi_cache_rows,
        on_conflict="request_hash",
    )
    outputs["feature_store.entity_master"] = publisher.upsert(
        "feature_store.entity_master",
        entity_master_rows(result.entities),
        on_conflict="entity_id",
    )
    outputs["feature_store.entity_listing"] = publisher.upsert(
        "feature_store.entity_listing",
        entity_listing_rows(result.listings),
        on_conflict="symbol",
    )
    outputs["feature_store.entity_identity_audit"] = publisher.insert(
        "feature_store.entity_identity_audit",
        audit_rows(result.audits),
    )
    return outputs


def entity_master_rows(records: list[EntityRecord]) -> list[dict[str, Any]]:
    return [
        {
            "entity_id": record.entity_id,
            "legal_name": record.legal_name or None,
            "display_name": record.display_name or None,
            "home_country": record.home_country or None,
            "lei": record.lei or None,
            "entity_source": record.entity_source,
            "resolution_confidence": record.resolution_confidence,
            "resolution_status": record.resolution_status,
            "primary_listing_symbol": record.primary_listing_symbol or None,
            "primary_listing_reason": record.primary_listing_reason or None,
            "metadata": record.metadata,
        }
        for record in records
    ]


def entity_listing_rows(records: list[EntityListingRecord]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": record.symbol,
            "entity_id": record.entity_id,
            "provider_symbol": record.provider_symbol or None,
            "exchange": record.exchange or None,
            "exchange_mic": record.exchange_mic or None,
            "country": record.country or None,
            "currency": record.currency or None,
            "figi": record.figi or None,
            "composite_figi": record.composite_figi or None,
            "share_class_figi": record.share_class_figi or None,
            "isin": record.isin or None,
            "lei": record.lei or None,
            "listing_type": record.listing_type or None,
            "is_primary_listing": record.is_primary_listing,
            "primary_listing_reason": record.primary_listing_reason or None,
            "resolution_source": record.resolution_source,
            "resolution_confidence": record.resolution_confidence,
            "resolution_status": record.resolution_status,
            "metadata": record.metadata,
        }
        for record in records
    ]


def identity_audit_rows(records: list[IdentityAuditRecord]) -> list[dict[str, Any]]:
    return audit_rows(records)
