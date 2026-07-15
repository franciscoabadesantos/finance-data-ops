"""Publisher for side-by-side entity identity tables."""

from __future__ import annotations

import hashlib
import json
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
    batch_id = build_entity_publication_batch_id(measurement)
    return build_side_by_side_entity_publication_plan_for_batch(measurement=measurement, batch_id=batch_id)


def build_side_by_side_entity_publication_plan_for_batch(
    *,
    measurement: EntityChainMeasurement,
    batch_id: str,
    scope_key: str = "default",
) -> dict[str, Any]:
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
                    "publication_batch_id": batch_id,
                    "metadata": {
                        "resolution_state": status,
                        "source": "entity_identity_measurement",
                        "publication_batch_id": batch_id,
                    },
                },
            )
            listing_rows.append(
                _publication_listing_row(
                    row=row,
                    entity_id=entity_id,
                    status=status,
                    confidence=str(confidence),
                    publication_batch_id=batch_id,
                )
            )
            continue

        candidate_lei = str(row.get("candidate_lei") or row.get("foreign_issuer_candidate_lei") or "").strip()
        if row.get("listing_group_kind") != "single_listing" or not candidate_lei:
            if row.get("decision_bucket") == "needs_manual_review":
                review_rows.append(
                    _identity_review_row(
                        row=row,
                        entity_id=None,
                        state="needs_manual_review",
                        publication_batch_id=batch_id,
                    )
                )
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
                "publication_batch_id": batch_id,
                "metadata": {
                    "resolution_state": "provisional",
                    "candidate_lei": candidate_lei,
                    "candidate_legal_name": legal_name,
                    "promotion_triggers": _reevaluation_triggers(),
                    "publication_batch_id": batch_id,
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
                publication_batch_id=batch_id,
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
                    "publication_batch_id": batch_id,
                    "review_key": _review_key(batch_id=batch_id, symbol=audit_row.get("symbol"), reason=audit_row.get("review_reason")),
                }
            )

    planned_counts = _planned_counts(
        master_rows=list(master_by_id.values()),
        listing_rows=listing_rows,
        review_rows=review_rows,
        measurement=measurement,
    )
    blockers = _publication_blockers(measurement)
    batch_row = _publication_batch_row(
        measurement=measurement,
        batch_id=batch_id,
        scope_key=scope_key,
        mode="dry_run",
        status="blocked" if blockers else "planned",
        planned_counts=planned_counts,
        actual_counts={},
        blocked_reasons=blockers,
    )
    return {
        "batch_id": batch_id,
        "scope_key": scope_key,
        "publication_gate": measurement.publication_gate,
        "publication_blockers": blockers,
        "planned_counts": planned_counts,
        "verification_summary": _verification_summary(measurement),
        "feature_store.entity_identity_publication_batch": [batch_row],
        "feature_store.entity_master": list(master_by_id.values()),
        "feature_store.entity_listing": listing_rows,
        "feature_store.entity_identity_review": review_rows,
        "feature_store.entity_identity_publication_current": [
            {"scope_key": scope_key, "batch_id": batch_id}
        ]
        if not blockers
        else [],
        "reevaluation_triggers": _reevaluation_triggers(),
    }


def publish_entity_identity_side_by_side(
    *,
    publisher: Publisher,
    measurement: EntityChainMeasurement,
    allow_unreviewed_heuristics: bool = False,
    batch_id: str | None = None,
    scope_key: str = "default",
) -> dict[str, Any]:
    plan = build_side_by_side_entity_publication_plan_for_batch(
        measurement=measurement,
        batch_id=batch_id or build_entity_publication_batch_id(measurement),
        scope_key=scope_key,
    )
    gate = dict(plan["publication_gate"])
    blockers = list(plan.get("publication_blockers") or [])
    if (gate.get("status") != "ready_machine_safe" or blockers) and not allow_unreviewed_heuristics:
        return {
            "status": "blocked",
            "reason": "publication_gate_blocked" if gate.get("status") != "ready_machine_safe" else "publication_blockers",
            "publication_gate": gate,
            "publication_blockers": blockers,
            "planned_rows": {
                "feature_store.entity_master": len(plan["feature_store.entity_master"]),
                "feature_store.entity_listing": len(plan["feature_store.entity_listing"]),
                "feature_store.entity_identity_review": len(plan["feature_store.entity_identity_review"]),
            },
        }

    outputs: dict[str, Any] = {
        "status": "published_side_by_side",
        "batch_id": plan["batch_id"],
        "publication_gate": gate,
        "verification_summary": plan["verification_summary"],
    }
    outputs["feature_store.entity_identity_publication_batch_planned"] = publisher.upsert(
        "feature_store.entity_identity_publication_batch",
        [
            {
                **plan["feature_store.entity_identity_publication_batch"][0],
                "mode": "apply_entities",
                "status": "planned",
                "is_current": False,
                "actual_counts": {},
            }
        ],
        on_conflict="batch_id",
    )
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
        outputs["feature_store.entity_identity_review"] = publisher.upsert(
            "feature_store.entity_identity_review",
            plan["feature_store.entity_identity_review"],
            on_conflict="review_key",
        )
    outputs["feature_store.entity_identity_publication_batch"] = publisher.upsert(
        "feature_store.entity_identity_publication_batch",
        [
            {
                **plan["feature_store.entity_identity_publication_batch"][0],
                "mode": "apply_entities",
                "status": "published_side_by_side",
                "is_current": True,
                "actual_counts": plan["planned_counts"],
            }
        ],
        on_conflict="batch_id",
    )
    if plan["feature_store.entity_identity_publication_current"]:
        outputs["feature_store.entity_identity_publication_current"] = publisher.upsert(
            "feature_store.entity_identity_publication_current",
            plan["feature_store.entity_identity_publication_current"],
            on_conflict="scope_key",
        )
    return outputs


def publish_entity_identity_controlled(
    *,
    publisher: Publisher,
    measurement: EntityChainMeasurement,
    apply_caches: bool = False,
    apply_entities: bool = False,
    batch_id: str | None = None,
    scope_key: str = "default",
) -> dict[str, Any]:
    """Dry-run-first controlled side-by-side publication.

    Cache writes and entity writes are intentionally separate flags. When both
    are enabled, raw fact caches are upserted before any feature_store entity
    rows are touched.
    """

    resolved_batch_id = batch_id or build_entity_publication_batch_id(measurement)
    plan = build_side_by_side_entity_publication_plan_for_batch(
        measurement=measurement,
        batch_id=resolved_batch_id,
        scope_key=scope_key,
    )
    mode = _publication_mode(apply_caches=apply_caches, apply_entities=apply_entities)
    result: dict[str, Any] = {
        "status": "dry_run" if not (apply_caches or apply_entities) else "planned",
        "mode": mode,
        "batch_id": resolved_batch_id,
        "publication_gate": plan["publication_gate"],
        "publication_blockers": list(plan["publication_blockers"]),
        "planned_counts": dict(plan["planned_counts"]),
        "verification_summary": dict(plan["verification_summary"]),
        "planned_tables": {
            "source_cache.openfigi_mapping_raw": len(measurement.openfigi_cache_rows),
            "source_cache.listing_isin_raw": len(measurement.isin_cache_rows),
            "source_cache.gleif_isin_lei_raw": len(measurement.gleif_cache_rows),
            "source_cache.gleif_lei_isin_raw": len(measurement.gleif_lei_isin_cache_rows),
            "feature_store.entity_identity_publication_batch": 1,
            "feature_store.entity_master": len(plan["feature_store.entity_master"]),
            "feature_store.entity_listing": len(plan["feature_store.entity_listing"]),
            "feature_store.entity_identity_review": len(plan["feature_store.entity_identity_review"]),
        },
    }

    if not apply_caches and not apply_entities:
        result["planned_cache_publish"] = {
            "source_cache.openfigi_mapping_raw": {"rows": len(measurement.openfigi_cache_rows), "status": "planned"},
            "source_cache.listing_isin_raw": {"rows": len(measurement.isin_cache_rows), "status": "planned"},
            "source_cache.gleif_isin_lei_raw": {"rows": len(measurement.gleif_cache_rows), "status": "planned"},
            "source_cache.gleif_lei_isin_raw": {"rows": len(measurement.gleif_lei_isin_cache_rows), "status": "planned"},
        }
        result["planned_entity_publish"] = {
            "feature_store.entity_master": {"rows": len(plan["feature_store.entity_master"]), "status": "planned"},
            "feature_store.entity_listing": {"rows": len(plan["feature_store.entity_listing"]), "status": "planned"},
            "feature_store.entity_identity_review": {
                "rows": len(plan["feature_store.entity_identity_review"]),
                "status": "planned",
            },
        }
        return result

    if apply_entities and plan["publication_blockers"]:
        result["status"] = "blocked"
        result["reason"] = "publication_blockers"
        return result

    if apply_caches:
        result["cache_publish"] = publish_entity_identity_raw_caches(publisher=publisher, measurement=measurement)

    if apply_entities:
        entity_outputs = publish_entity_identity_side_by_side(
            publisher=publisher,
            measurement=measurement,
            batch_id=resolved_batch_id,
            scope_key=scope_key,
        )
        if entity_outputs.get("status") == "blocked":
            result.update(entity_outputs)
            return result
        result["entity_publish"] = entity_outputs
        result["status"] = "published_side_by_side"
        result["actual_counts"] = _actual_counts_from_outputs(entity_outputs)
    else:
        result["status"] = "cache_published" if apply_caches else result["status"]
    return result


def build_entity_publication_batch_id(measurement: EntityChainMeasurement) -> str:
    payload = {
        "summary": measurement.summary,
        "symbols": [
            {
                "symbol": row.get("symbol"),
                "entity_lei": row.get("entity_lei"),
                "entity_attach_method": row.get("entity_attach_method"),
                "entity_attach_reason": row.get("entity_attach_reason"),
                "matched_compatible_isins": row.get("matched_compatible_isins"),
            }
            for row in measurement.symbol_rows
        ],
        "publication_gate": measurement.publication_gate,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    return f"entity_identity:{digest}"


def _publication_blockers(measurement: EntityChainMeasurement) -> list[str]:
    blockers: list[str] = []
    gate = measurement.publication_gate
    if gate.get("status") != "ready_machine_safe":
        blockers.append("publication_gate_not_ready")
    if int(gate.get("review_required_count") or 0) > 0:
        blockers.append("heuristic_review_required")
    if int(gate.get("group_conflict_count") or 0) > 0:
        blockers.append("group_conflicts_present")
    if int(measurement.summary.get("unresolved_multi_listing_entities_count") or 0) > 0:
        blockers.append("unresolved_multi_listing_entities_present")
    if not bool(measurement.summary.get("guardrail_pairs_unmerged", True)):
        blockers.append("guardrail_pair_merged")
    return blockers


def _planned_counts(
    *,
    master_rows: list[dict[str, Any]],
    listing_rows: list[dict[str, Any]],
    review_rows: list[dict[str, Any]],
    measurement: EntityChainMeasurement,
) -> dict[str, Any]:
    return {
        "source_cache.openfigi_mapping_raw": len(measurement.openfigi_cache_rows),
        "source_cache.listing_isin_raw": len(measurement.isin_cache_rows),
        "source_cache.gleif_isin_lei_raw": len(measurement.gleif_cache_rows),
        "source_cache.gleif_lei_isin_raw": len(measurement.gleif_lei_isin_cache_rows),
        "feature_store.entity_master": len(master_rows),
        "feature_store.entity_listing": len(listing_rows),
        "feature_store.entity_identity_review": len(review_rows),
        "feature_store.entity_identity_publication_batch": 1,
    }


def _verification_summary(measurement: EntityChainMeasurement) -> dict[str, Any]:
    heuristic_rows = [
        row for row in measurement.heuristic_attach_audit if row.get("attach_audit_kind") == "heuristic"
    ]
    short_or_acronym_rows = [
        row
        for row in heuristic_rows
        if row.get("normalized_name_too_short") or row.get("normalized_name_acronym_only")
    ]
    attached_rows = [row for row in measurement.symbol_rows if row.get("entity_lei")]
    provisional_rows = [
        row
        for row in measurement.symbol_rows
        if not row.get("entity_lei") and row.get("candidate_lei") and row.get("listing_group_kind") == "single_listing"
    ]
    return {
        "attached_count": len(attached_rows),
        "attached_by_method": _count_by_field(attached_rows, "entity_attach_method"),
        "listing_rows_by_lifecycle_state": {
            "resolved": len(attached_rows),
            "provisional": len(provisional_rows),
        },
        "total_heuristic_attaches": len(heuristic_rows),
        "heuristic_attaches_by_method": _count_by_field(heuristic_rows, "attach_method"),
        "cjk_apac_heuristic_attaches": len(measurement.cjk_apac_heuristic_attach_audit),
        "short_or_acronym_heuristic_attaches": len(short_or_acronym_rows),
        "review_required_count": int(measurement.publication_gate.get("review_required_count") or 0),
        "review_required_symbols": list(measurement.publication_gate.get("review_required_symbols") or []),
        "group_conflict_count": int(measurement.publication_gate.get("group_conflict_count") or 0),
        "conflict_symbols": list(measurement.publication_gate.get("conflict_symbols") or []),
        "unresolved_multi_listing_entities_count": int(
            measurement.summary.get("unresolved_multi_listing_entities_count") or 0
        ),
        "unresolved_single_listing_provisional_candidates_count": int(
            measurement.summary.get("unresolved_single_listing_provisional_candidates_count") or 0
        ),
        "candidate_lei_retained_provisional_count": int(
            measurement.summary.get("candidate_lei_retained_provisional_count") or 0
        ),
        "provider_or_curated_by_listing_group_kind": dict(
            measurement.summary.get("provider_or_curated_by_listing_group_kind") or {}
        ),
        "manual_review_by_listing_group_kind": dict(measurement.summary.get("manual_review_by_listing_group_kind") or {}),
        "publication_gate_status": measurement.publication_gate.get("status") or "",
    }


def _publication_batch_row(
    *,
    measurement: EntityChainMeasurement,
    batch_id: str,
    scope_key: str,
    mode: str,
    status: str,
    planned_counts: dict[str, Any],
    actual_counts: dict[str, Any],
    blocked_reasons: list[str],
) -> dict[str, Any]:
    return {
        "batch_id": batch_id,
        "scope_key": scope_key,
        "source": "entity_identity_measurement",
        "mode": mode,
        "status": status,
        "is_current": status == "published_side_by_side",
        "publication_gate": measurement.publication_gate,
        "summary": measurement.summary,
        "planned_counts": planned_counts,
        "actual_counts": actual_counts,
        "verification_summary": _verification_summary(measurement),
        "blocked_reasons": list(blocked_reasons),
    }


def _actual_counts_from_outputs(outputs: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table, result in outputs.items():
        if str(table).endswith("_planned"):
            continue
        if isinstance(result, dict) and "rows" in result:
            counts[table] = int(result.get("rows") or 0)
    return counts


def _publication_mode(*, apply_caches: bool, apply_entities: bool) -> str:
    if apply_caches and apply_entities:
        return "apply_cache_and_entities"
    if apply_caches:
        return "apply_cache"
    if apply_entities:
        return "apply_entities"
    return "dry_run"


def _count_by_field(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "")
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _review_key(*, batch_id: str, symbol: Any, reason: Any) -> str:
    raw = f"{batch_id}:{symbol or ''}:{reason or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


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
    publication_batch_id: str,
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
        "publication_batch_id": publication_batch_id,
        "metadata": {
            "listing_group_kind": row.get("listing_group_kind") or "",
            "listing_group_symbols_in_measurement": list(row.get("listing_group_symbols_in_measurement") or []),
            "reevaluation_triggers": _reevaluation_triggers(),
            "publication_batch_id": publication_batch_id,
        },
    }


def _identity_review_row(
    *,
    row: dict[str, Any],
    entity_id: str | None,
    state: str,
    publication_batch_id: str,
) -> dict[str, Any]:
    review_reason = row.get("entity_attach_reason") or row.get("decision_bucket") or None
    return {
        "review_key": _review_key(batch_id=publication_batch_id, symbol=row.get("symbol"), reason=review_reason),
        "symbol": row.get("symbol") or None,
        "entity_id": entity_id,
        "candidate_lei": row.get("candidate_lei") or row.get("foreign_issuer_candidate_lei") or None,
        "attach_method": row.get("entity_attach_method") or None,
        "resolution_state": state,
        "review_reason": review_reason,
        "evidence_summary": {
            "candidate_legal_name": row.get("candidate_legal_name") or "",
            "decision_bucket": row.get("decision_bucket") or "",
            "listing_group_kind": row.get("listing_group_kind") or "",
            "entity_attach_reasons": list(row.get("entity_attach_reasons") or []),
        },
        "conflicting_candidates": {},
        "publication_batch_id": publication_batch_id,
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
