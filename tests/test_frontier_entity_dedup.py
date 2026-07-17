from __future__ import annotations

from types import SimpleNamespace

import pytest

from finance_data_ops.identity.chain import EntityChainMeasurement
from finance_data_ops.identity.frontier_dedup import (
    FrontierDedupOptions,
    TrackedEntityListing,
    classify_frontier_candidates,
    run_frontier_entity_dedup_audit,
)
from finance_data_ops.identity.models import ListingCandidate


def test_frontier_candidate_matching_existing_tracked_lei_is_suppressed() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "SAP.DE",
                    "entity_lei": "529900D6BF99LW9R2E68",
                    "entity_attach_method": "lei_expansion",
                    "attachment_confidence": "high",
                    "entity_attach_reason": "single_compatible_expanded_lei",
                    "entity_legal_name": "SAP SE",
                }
            ]
        ),
        candidates=[_candidate("SAP.DE", country="DE", name="SAP SE")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="SAP",
                entity_id="lei:529900D6BF99LW9R2E68",
                entity_lei="529900D6BF99LW9R2E68",
                legal_name="SAP SE",
                resolution_status="resolved",
            )
        ],
    )

    assert rows[0]["entity_dedup_status"] == "already_tracked_entity"
    assert rows[0]["recommended_action"] == "suppress"
    assert rows[0]["matched_entity_id"] == "lei:529900D6BF99LW9R2E68"
    assert rows[0]["matched_tracked_symbols"] == ["SAP"]


def test_frontier_candidate_already_tracked_by_symbol_suppresses_even_when_resolution_misses() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "GOOGL",
                    "openfigi_status": "not_found",
                    "entity_attach_method": "unattached_no_anchor",
                    "entity_attach_reason": "openfigi_not_found",
                }
            ]
        ),
        candidates=[_candidate("GOOGL", country="US", name="ALPHABET INC.")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="GOOG",
                entity_id="lei:5493006MHB84DD0ZWV18",
                entity_lei="5493006MHB84DD0ZWV18",
                legal_name="ALPHABET INC.",
                resolution_status="resolved",
            ),
            TrackedEntityListing(
                symbol="GOOGL",
                entity_id="lei:5493006MHB84DD0ZWV18",
                entity_lei="5493006MHB84DD0ZWV18",
                legal_name="ALPHABET INC.",
                resolution_status="resolved",
            ),
        ],
    )

    assert rows[0]["entity_dedup_status"] == "already_tracked_entity"
    assert rows[0]["recommended_action"] == "suppress"
    assert rows[0]["reason"] == "candidate_symbol_already_tracked"
    assert rows[0]["candidate_entity_id"] == "lei:5493006MHB84DD0ZWV18"
    assert rows[0]["candidate_lei"] == "5493006MHB84DD0ZWV18"
    assert rows[0]["candidate_legal_name"] == "ALPHABET INC."
    assert rows[0]["matched_entity_legal_name"] == "ALPHABET INC."
    assert rows[0]["matched_tracked_symbols"] == ["GOOG", "GOOGL"]


def test_frontier_candidate_matching_existing_tracked_entity_id_without_lei_is_suppressed() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "CURATED.L",
                    "entity_id": "curated:issuer-1",
                    "entity_attach_method": "curated_identity",
                    "entity_attach_reason": "curated_identity_match",
                }
            ]
        ),
        candidates=[_candidate("CURATED.L", country="GB", name="CURATED PLC")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="CURATED",
                entity_id="curated:issuer-1",
                legal_name="CURATED PLC",
                resolution_status="resolved",
            )
        ],
    )

    assert rows[0]["entity_dedup_status"] == "already_tracked_entity"
    assert rows[0]["recommended_action"] == "suppress"
    assert rows[0]["matched_entity_id"] == "curated:issuer-1"


def test_frontier_candidate_without_cache_evidence_is_cache_miss_not_suppressed() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "MISS",
                    "openfigi_status": "not_found",
                    "entity_attach_reasons": ["openfigi_cache_miss", "legal_name_cache_miss"],
                }
            ]
        ),
        candidates=[_candidate("MISS")],
        tracked_entities=[],
    )

    assert rows[0]["entity_dedup_status"] == "cache_miss"
    assert rows[0]["recommended_action"] == "show_with_warning"


def test_frontier_weak_name_only_candidate_routes_to_review() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "NEC.T",
                    "entity_lei": "353800ZI7YB13OFXXR18",
                    "entity_attach_method": "name_anchor_confirmed",
                    "entity_attach_reason": "legal_name_candidate_confirmed_by_lei_isin_expansion",
                    "decision_bucket": "needs_manual_review",
                }
            ],
            heuristic_attach_audit=[
                {
                    "symbol": "NEC.T",
                    "attach_method": "name_anchor_confirmed",
                    "review_status": "needs_review",
                    "review_reason": "cjk_name_collapsed_to_latin_acronym",
                }
            ],
        ),
        candidates=[_candidate("NEC.T", country="JP", name="NEC CORP")],
        tracked_entities=[],
    )

    assert rows[0]["entity_dedup_status"] == "needs_review"
    assert rows[0]["recommended_action"] == "review"


def test_frontier_new_deterministic_lei_not_in_tracked_is_new_entity_candidate() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "NVO",
                    "entity_lei": "549300DAQ1CVT6CXN342",
                    "entity_attach_method": "direct_isin",
                    "attachment_confidence": "high",
                    "entity_attach_reason": "direct_isin_to_lei",
                    "entity_legal_name": "NOVO NORDISK A/S",
                }
            ]
        ),
        candidates=[_candidate("NVO", country="US", name="NOVO NORDISK A/S")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="SAP",
                entity_id="lei:529900D6BF99LW9R2E68",
                entity_lei="529900D6BF99LW9R2E68",
                legal_name="SAP SE",
                resolution_status="resolved",
            )
        ],
    )

    assert rows[0]["entity_dedup_status"] == "new_entity_candidate"
    assert rows[0]["recommended_action"] == "onboard"
    assert rows[0]["matched_tracked_symbols"] == []


def test_frontier_provisional_current_tracked_rows_do_not_suppress() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "ACME.L",
                    "entity_lei": "549300ACME000000001",
                    "entity_attach_method": "direct_isin",
                    "entity_attach_reason": "direct_isin_to_lei",
                }
            ]
        ),
        candidates=[_candidate("ACME.L", country="GB", name="ACME PLC")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="ACME",
                entity_id="provisional:ACME",
                entity_lei="549300ACME000000001",
                legal_name="ACME PLC",
                resolution_status="provisional",
            )
        ],
    )

    assert rows[0]["entity_dedup_status"] == "new_entity_candidate"
    assert rows[0]["recommended_action"] == "onboard"
    assert rows[0]["matched_tracked_symbols"] == []


def test_frontier_candidate_already_tracked_by_symbol_ignores_provisional_tracked_rows() -> None:
    rows = classify_frontier_candidates(
        measurement=_measurement(
            [
                {
                    "symbol": "PROV",
                    "openfigi_status": "not_found",
                    "entity_attach_method": "unattached_no_anchor",
                    "entity_attach_reason": "openfigi_not_found",
                }
            ]
        ),
        candidates=[_candidate("PROV", country="US", name="PROVISIONAL INC.")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="PROV",
                entity_id="provisional:PROV",
                entity_lei="549300PROVISIONAL",
                legal_name="PROVISIONAL INC.",
                resolution_status="provisional",
            )
        ],
    )

    assert rows[0]["entity_dedup_status"] == "provisional_or_unresolved"
    assert rows[0]["recommended_action"] == "show_with_warning"
    assert rows[0]["reason"] == "openfigi_not_found"
    assert rows[0]["matched_tracked_symbols"] == []


def test_frontier_dedup_uses_cache_first_measurement_without_live_refresh(monkeypatch) -> None:
    captured = {}

    def _fake_raw_cache_measurement(*, args, candidates, selected_symbols, settings):
        captured["args"] = args
        captured["candidates"] = candidates
        captured["selected_symbols"] = selected_symbols
        return _measurement(
            [
                {
                    "symbol": "SAP.DE",
                    "entity_lei": "529900D6BF99LW9R2E68",
                    "entity_attach_method": "lei_expansion",
                    "entity_attach_reason": "single_compatible_expanded_lei",
                }
            ],
            summary={"candidate_count": 1, "raw_cache_enabled": True},
        )

    monkeypatch.setattr("finance_data_ops.identity.frontier_dedup._build_measurement_with_raw_cache", _fake_raw_cache_measurement)

    result = run_frontier_entity_dedup_audit(
        options=FrontierDedupOptions(source="postgres", symbols=("SAP.DE",), scope_key="tracked"),
        settings=SimpleNamespace(database_dsn="", cache_root=None),
        candidates=[_candidate("SAP.DE", country="DE", name="SAP SE")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="SAP",
                entity_id="lei:529900D6BF99LW9R2E68",
                entity_lei="529900D6BF99LW9R2E68",
                legal_name="SAP SE",
                resolution_status="resolved",
            )
        ],
    )

    assert captured["args"].use_raw_cache is True
    assert captured["args"].offline is True
    assert captured["args"].refresh_cache_misses is False
    assert captured["selected_symbols"] == ["SAP.DE"]
    assert result["candidates"][0]["entity_dedup_status"] == "already_tracked_entity"


def test_frontier_dedup_summary_splits_symbol_and_resolved_entity_suppression(monkeypatch) -> None:
    def _fake_raw_cache_measurement(*, args, candidates, selected_symbols, settings):
        return _measurement(
            [
                {
                    "symbol": "GOOGL",
                    "openfigi_status": "not_found",
                    "entity_attach_method": "unattached_no_anchor",
                    "entity_attach_reason": "openfigi_not_found",
                },
                {
                    "symbol": "SAP.DE",
                    "entity_lei": "529900D6BF99LW9R2E68",
                    "entity_attach_method": "lei_expansion",
                    "entity_attach_reason": "single_compatible_expanded_lei",
                },
            ],
            summary={"candidate_count": 2, "raw_cache_enabled": True},
        )

    monkeypatch.setattr("finance_data_ops.identity.frontier_dedup._build_measurement_with_raw_cache", _fake_raw_cache_measurement)

    result = run_frontier_entity_dedup_audit(
        options=FrontierDedupOptions(source="postgres", symbols=("GOOGL", "SAP.DE"), scope_key="tracked"),
        settings=SimpleNamespace(database_dsn="", cache_root=None),
        candidates=[_candidate("GOOGL", name="ALPHABET INC."), _candidate("SAP.DE", country="DE", name="SAP SE")],
        tracked_entities=[
            TrackedEntityListing(
                symbol="GOOG",
                entity_id="lei:5493006MHB84DD0ZWV18",
                entity_lei="5493006MHB84DD0ZWV18",
                legal_name="ALPHABET INC.",
                resolution_status="resolved",
            ),
            TrackedEntityListing(
                symbol="GOOGL",
                entity_id="lei:5493006MHB84DD0ZWV18",
                entity_lei="5493006MHB84DD0ZWV18",
                legal_name="ALPHABET INC.",
                resolution_status="resolved",
            ),
            TrackedEntityListing(
                symbol="SAP",
                entity_id="lei:529900D6BF99LW9R2E68",
                entity_lei="529900D6BF99LW9R2E68",
                legal_name="SAP SE",
                resolution_status="resolved",
            ),
        ],
    )

    assert result["summary"]["already_tracked_by_symbol_count"] == 1
    assert result["summary"]["already_tracked_by_resolved_entity_count"] == 1
    assert result["summary"]["recommended_action_counts"]["suppress"] == 2


def test_frontier_dedup_cache_miss_refresh_requires_live_refresh() -> None:
    with pytest.raises(ValueError, match="refresh_cache_misses requires refresh_live"):
        run_frontier_entity_dedup_audit(
            options=FrontierDedupOptions(source="postgres", refresh_live=False, refresh_cache_misses=True),
            settings=SimpleNamespace(database_dsn="", cache_root=None),
            candidates=[_candidate("SAP.DE")],
            tracked_entities=[],
        )


def _candidate(symbol: str, *, country: str = "US", name: str = "") -> ListingCandidate:
    return ListingCandidate(
        symbol=symbol,
        provider_symbol=symbol,
        country=country,
        currency="USD",
        exchange="NMS",
        name=name,
        source="test_frontier",
    )


def _measurement(
    symbol_rows: list[dict],
    *,
    heuristic_attach_audit: list[dict] | None = None,
    summary: dict | None = None,
) -> EntityChainMeasurement:
    return EntityChainMeasurement(
        symbol_rows=symbol_rows,
        pair_rows=[],
        summary=summary or {"candidate_count": len(symbol_rows)},
        name_anchor_precision_audit=[],
        heuristic_attach_audit=heuristic_attach_audit or [],
        cjk_apac_heuristic_attach_audit=[],
        publication_gate={"status": "ready_machine_safe", "review_required_count": 0},
        openfigi_cache_rows=[],
        isin_cache_rows=[],
        gleif_cache_rows=[],
        gleif_lei_isin_cache_rows=[],
        gleif_entity_cache_rows=[],
    )
