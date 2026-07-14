"""Publisher for side-by-side entity identity tables."""

from __future__ import annotations

from typing import Any

from finance_data_ops.identity.audit import audit_rows
from finance_data_ops.identity.chain import EntityChainMeasurement
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


def publish_entity_identity_raw_caches(
    *,
    publisher: Publisher,
    measurement: EntityChainMeasurement,
) -> dict[str, Any]:
    return {
        "source_cache.openfigi_mapping_raw": publisher.upsert(
            "source_cache.openfigi_mapping_raw",
            measurement.openfigi_cache_rows,
            on_conflict="request_hash",
        ),
        "source_cache.listing_isin_raw": publisher.upsert(
            "source_cache.listing_isin_raw",
            measurement.isin_cache_rows,
            on_conflict="symbol,provider",
        ),
        "source_cache.gleif_isin_lei_raw": publisher.upsert(
            "source_cache.gleif_isin_lei_raw",
            measurement.gleif_cache_rows,
            on_conflict="isin",
        ),
        "source_cache.gleif_lei_isin_raw": publisher.upsert(
            "source_cache.gleif_lei_isin_raw",
            measurement.gleif_lei_isin_cache_rows,
            on_conflict="lei",
        ),
    }


def build_side_by_side_entity_publication_plan(measurement: EntityChainMeasurement) -> dict[str, Any]:
    """Build entity table rows from a measured run without writing them.

    Confirmed attaches are modeled under LEI-based entity ids. Single-listing
    provisional candidates are kept under symbol-scoped provisional ids so they
    cannot accidentally merge companies before stronger evidence appears.
    """

    master_by_id: dict[str, dict[str, Any]] = {}
    listing_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for row in measurement.symbol_rows:
        method = str(row.get("entity_attach_method") or "")
        entity_lei = str(row.get("entity_lei") or row.get("lei") or "").strip()
        if entity_lei:
            entity_id = f"lei:{entity_lei}"
            status = "resolved"
            confidence = row.get("attachment_confidence") or _publication_confidence(method)
            legal_name = row.get("candidate_legal_name") or row.get("legal_name") or ""
            master_by_id.setdefault(
                entity_id,
                {
                    "entity_id": entity_id,
                    "legal_name": legal_name or None,
                    "display_name": legal_name or row.get("openfigi_name") or row.get("internal_candidate_name") or None,
                    "home_country": row.get("candidate_legal_country") or row.get("home_country") or None,
                    "lei": entity_lei,
                    "entity_source": "entity_identity_measurement",
                    "resolution_confidence": _confidence_score(str(confidence)),
                    "resolution_status": status,
                    "primary_listing_symbol": None,
                    "primary_listing_reason": None,
                    "metadata": {
                        "resolution_state": status,
                        "source": "entity_identity_measurement",
                    },
                },
            )
            listing_rows.append(_publication_listing_row(row=row, entity_id=entity_id, status=status, confidence=str(confidence)))
            continue

        candidate_lei = str(row.get("candidate_lei") or row.get("foreign_issuer_candidate_lei") or "").strip()
        if row.get("listing_group_kind") != "single_listing" or not candidate_lei:
            if row.get("decision_bucket") == "needs_manual_review":
                review_rows.append(_identity_review_row(row=row, entity_id=None, state="needs_manual_review"))
            continue

        entity_id = f"provisional:{row.get('symbol')}"
        legal_name = row.get("candidate_legal_name") or row.get("foreign_issuer_candidate_legal_name") or ""
        master_by_id.setdefault(
            entity_id,
            {
                "entity_id": entity_id,
                "legal_name": legal_name or None,
                "display_name": legal_name or row.get("openfigi_name") or row.get("internal_candidate_name") or None,
                "home_country": row.get("candidate_legal_country") or None,
                "lei": candidate_lei or None,
                "entity_source": "entity_identity_measurement",
                "resolution_confidence": _confidence_score("provisional"),
                "resolution_status": "provisional",
                "primary_listing_symbol": None,
                "primary_listing_reason": None,
                "metadata": {
                    "resolution_state": "provisional",
                    "candidate_lei": candidate_lei,
                    "candidate_legal_name": legal_name,
                    "promotion_triggers": _reevaluation_triggers(),
                },
            },
        )
        listing_rows.append(
            _publication_listing_row(
                row=row,
                entity_id=entity_id,
                status="provisional",
                confidence="provisional",
                attach_method="provisional_single_listing_candidate",
            )
        )

    for audit_row in measurement.heuristic_attach_audit:
        if audit_row.get("review_status") != "machine_verifiably_safe":
            review_rows.append(
                {
                    "symbol": audit_row.get("symbol") or None,
                    "entity_id": audit_row.get("entity_id") or None,
                    "candidate_lei": audit_row.get("candidate_lei") or audit_row.get("entity_lei") or None,
                    "attach_method": audit_row.get("attach_method") or None,
                    "resolution_state": "needs_manual_review",
                    "review_reason": audit_row.get("review_reason") or "heuristic_attach_requires_review",
                    "evidence_summary": audit_row.get("evidence_payload") or {},
                    "conflicting_candidates": audit_row.get("entity_group_conflict_flags") or {},
                }
            )

    return {
        "publication_gate": measurement.publication_gate,
        "feature_store.entity_master": list(master_by_id.values()),
        "feature_store.entity_listing": listing_rows,
        "feature_store.entity_identity_review": review_rows,
        "reevaluation_triggers": _reevaluation_triggers(),
    }


def publish_entity_identity_side_by_side(
    *,
    publisher: Publisher,
    measurement: EntityChainMeasurement,
    allow_unreviewed_heuristics: bool = False,
) -> dict[str, Any]:
    plan = build_side_by_side_entity_publication_plan(measurement)
    gate = dict(plan["publication_gate"])
    if gate.get("status") != "ready_machine_safe" and not allow_unreviewed_heuristics:
        return {
            "status": "blocked",
            "reason": "publication_gate_blocked",
            "publication_gate": gate,
            "planned_rows": {
                "feature_store.entity_master": len(plan["feature_store.entity_master"]),
                "feature_store.entity_listing": len(plan["feature_store.entity_listing"]),
                "feature_store.entity_identity_review": len(plan["feature_store.entity_identity_review"]),
            },
        }

    outputs: dict[str, Any] = {"status": "published_side_by_side", "publication_gate": gate}
    outputs["feature_store.entity_master"] = publisher.upsert(
        "feature_store.entity_master",
        plan["feature_store.entity_master"],
        on_conflict="entity_id",
    )
    outputs["feature_store.entity_listing"] = publisher.upsert(
        "feature_store.entity_listing",
        plan["feature_store.entity_listing"],
        on_conflict="symbol",
    )
    if plan["feature_store.entity_identity_review"]:
        outputs["feature_store.entity_identity_review"] = publisher.insert(
            "feature_store.entity_identity_review",
            plan["feature_store.entity_identity_review"],
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


def _publication_listing_row(
    *,
    row: dict[str, Any],
    entity_id: str,
    status: str,
    confidence: str,
    attach_method: str | None = None,
) -> dict[str, Any]:
    method = attach_method or str(row.get("entity_attach_method") or "")
    return {
        "symbol": row.get("symbol") or "",
        "entity_id": entity_id,
        "provider_symbol": row.get("provider_symbol") or None,
        "exchange": row.get("exchange") or None,
        "exchange_mic": row.get("exchange_mic") or None,
        "country": row.get("country") or row.get("derived_listing_country") or None,
        "currency": row.get("currency") or None,
        "figi": row.get("figi") or None,
        "composite_figi": row.get("compositeFIGI") or row.get("composite_figi") or None,
        "share_class_figi": row.get("shareClassFIGI") or row.get("share_class_figi") or None,
        "isin": row.get("isin") or row.get("raw_isin") or None,
        "lei": row.get("entity_lei") or row.get("lei") or row.get("candidate_lei") or None,
        "listing_type": row.get("security_type") or None,
        "is_primary_listing": False,
        "primary_listing_reason": None,
        "resolution_source": method or "entity_identity_measurement",
        "resolution_confidence": _confidence_score(confidence),
        "resolution_status": status,
        "attach_method": method,
        "attach_confidence": confidence,
        "review_state": status,
        "evidence_payload": {
            "entity_attach_reason": row.get("entity_attach_reason") or "",
            "entity_attach_reasons": list(row.get("entity_attach_reasons") or []),
            "matched_compatible_isins": list(row.get("matched_compatible_isins") or []),
            "candidate_lei": row.get("candidate_lei") or "",
            "candidate_legal_name": row.get("candidate_legal_name") or "",
            "legal_name_anchor_status": row.get("legal_name_anchor_status") or "",
            "foreign_issuer_final_gate_status": row.get("foreign_issuer_final_gate_status") or "",
        },
        "source_freshness": {
            "openfigi_status": row.get("openfigi_status") or "",
            "isin_status": row.get("isin_status") or "",
            "lei_source": row.get("lei_source") or "",
            "isin_source": row.get("isin_source") or "",
        },
        "metadata": {
            "listing_group_kind": row.get("listing_group_kind") or "",
            "listing_group_symbols_in_measurement": list(row.get("listing_group_symbols_in_measurement") or []),
            "reevaluation_triggers": _reevaluation_triggers(),
        },
    }


def _identity_review_row(*, row: dict[str, Any], entity_id: str | None, state: str) -> dict[str, Any]:
    return {
        "symbol": row.get("symbol") or None,
        "entity_id": entity_id,
        "candidate_lei": row.get("candidate_lei") or row.get("foreign_issuer_candidate_lei") or None,
        "attach_method": row.get("entity_attach_method") or None,
        "resolution_state": state,
        "review_reason": row.get("entity_attach_reason") or row.get("decision_bucket") or None,
        "evidence_summary": {
            "candidate_legal_name": row.get("candidate_legal_name") or "",
            "decision_bucket": row.get("decision_bucket") or "",
            "listing_group_kind": row.get("listing_group_kind") or "",
            "entity_attach_reasons": list(row.get("entity_attach_reasons") or []),
        },
        "conflicting_candidates": {},
    }


def _publication_confidence(method: str) -> str:
    if method in {"direct_isin", "lei_expansion", "isin_direct_prefix_mismatch_name_confirmed"}:
        return "high"
    if method in {"name_anchor_confirmed", "foreign_issuer_name_anchor_confirmed"}:
        return "medium"
    return "low"


def _confidence_score(confidence: str) -> float:
    return {
        "high": 0.95,
        "medium": 0.75,
        "low": 0.35,
        "provisional": 0.2,
    }.get(confidence, 0.0)


def _reevaluation_triggers() -> list[str]:
    return [
        "new_listing_appears",
        "openfigi_raw_cache_refresh_changes_security_metadata",
        "provider_isin_changes_from_missing_or_suspect_to_valid",
        "gleif_isin_lei_begins_resolving",
        "gleif_lei_isin_expansion_adds_confirming_isin",
        "legal_name_candidate_changes",
        "manual_review_decision_recorded",
        "corroborating_provider_evidence_appears",
    ]
