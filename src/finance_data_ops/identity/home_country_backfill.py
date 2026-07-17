"""Cache-only home-country backfill planning for published Entity Layer rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class HomeCountryEvidence:
    lei: str
    home_country: str
    source_table: str
    source_field: str
    legal_name: str = ""


def build_home_country_backfill_plan(
    *,
    entity_rows: list[dict[str, Any]],
    gleif_entity_rows: list[dict[str, Any]],
    gleif_isin_lei_rows: list[dict[str, Any]] | None = None,
    batch_id: str,
) -> dict[str, Any]:
    country_by_lei = build_gleif_home_country_index(
        gleif_entity_rows=gleif_entity_rows,
        gleif_isin_lei_rows=gleif_isin_lei_rows or [],
    )
    changes: list[dict[str, Any]] = []
    missing_cache: list[dict[str, Any]] = []
    no_lei: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []

    scoped_rows = [row for row in entity_rows if str(row.get("publication_batch_id") or "") == batch_id]
    resolved_rows = [row for row in scoped_rows if str(row.get("resolution_status") or "") == "resolved"]
    for row in resolved_rows:
        entity_id = str(row.get("entity_id") or "")
        lei = _clean_lei(row.get("lei") or entity_id.removeprefix("lei:"))
        current = _clean_country(row.get("home_country"))
        if not lei:
            no_lei.append({"entity_id": entity_id, "legal_name": row.get("legal_name") or ""})
            continue
        evidence = country_by_lei.get(lei)
        if not evidence:
            missing_cache.append({"entity_id": entity_id, "lei": lei, "legal_name": row.get("legal_name") or ""})
            continue
        if current == evidence.home_country:
            unchanged.append(
                {
                    "entity_id": entity_id,
                    "lei": lei,
                    "home_country": current,
                    "reason": "already_set",
                    "source_table": evidence.source_table,
                }
            )
            continue
        if current and current != evidence.home_country:
            conflicts.append(
                {
                    "entity_id": entity_id,
                    "lei": lei,
                    "current_home_country": current,
                    "gleif_home_country": evidence.home_country,
                    "source_table": evidence.source_table,
                    "source_field": evidence.source_field,
                }
            )
            unchanged.append(
                {
                    "entity_id": entity_id,
                    "lei": lei,
                    "home_country": current,
                    "reason": "non_empty_conflict_not_overwritten",
                    "source_table": evidence.source_table,
                }
            )
            continue
        changes.append(
            {
                "entity_id": entity_id,
                "lei": lei,
                "legal_name": row.get("legal_name") or evidence.legal_name or "",
                "current_home_country": current or None,
                "new_home_country": evidence.home_country,
                "source_table": evidence.source_table,
                "source_field": evidence.source_field,
                "source_legal_name": evidence.legal_name,
                "publication_batch_id": batch_id,
            }
        )

    return {
        "batch_id": batch_id,
        "total_entities": len(scoped_rows),
        "resolved_entities": len(resolved_rows),
        "home_country_null_before": len(
            [row for row in resolved_rows if not _clean_country(row.get("home_country"))]
        ),
        "home_country_null_after": len(
            [row for row in resolved_rows if not _clean_country(row.get("home_country"))]
        )
        - len(changes),
        "backfilled_count": len(changes),
        "unchanged_count": len(unchanged),
        "missing_cache_count": len(missing_cache),
        "unresolved_no_lei_count": len(no_lei),
        "conflict_count": len(conflicts),
        "examples_changed": changes[:10],
        "missing_cache_examples": missing_cache[:10],
        "conflict_examples": conflicts[:10],
        "changes": changes,
        "blockers": [],
    }


def build_gleif_home_country_index(
    *,
    gleif_entity_rows: list[dict[str, Any]],
    gleif_isin_lei_rows: list[dict[str, Any]],
) -> dict[str, HomeCountryEvidence]:
    by_lei: dict[str, HomeCountryEvidence] = {}
    for row in gleif_entity_rows:
        for candidate in _candidate_payloads_from_gleif_entity_row(row):
            _maybe_add_evidence(
                by_lei,
                lei=candidate.get("lei"),
                legal_name=candidate.get("legal_name") or candidate.get("legalName") or candidate.get("name"),
                legal_country=candidate.get("legal_country") or candidate.get("legalCountry"),
                jurisdiction=(
                    candidate.get("jurisdiction_country")
                    or candidate.get("jurisdictionCountry")
                    or candidate.get("jurisdiction")
                ),
                headquarters_country=candidate.get("headquarters_country") or candidate.get("headquartersCountry"),
                source_table="source_cache.gleif_entity_raw",
            )
        for item in _gleif_payload_items(row.get("response_payload")):
            _maybe_add_evidence_from_gleif_item(
                by_lei,
                item=item,
                source_table="source_cache.gleif_entity_raw",
            )

    for row in gleif_isin_lei_rows:
        lei = row.get("lei")
        legal_name = row.get("legal_name")
        for item in _gleif_payload_items(row.get("response_payload")):
            _maybe_add_evidence_from_gleif_item(
                by_lei,
                item=item,
                source_table="source_cache.gleif_isin_lei_raw",
                fallback_lei=lei,
                fallback_legal_name=legal_name,
            )
        _maybe_add_evidence(
            by_lei,
            lei=lei,
            legal_name=legal_name,
            legal_country=row.get("legal_country"),
            jurisdiction=row.get("jurisdiction"),
            headquarters_country=row.get("headquarters_country"),
            source_table="source_cache.gleif_isin_lei_raw",
        )
    return by_lei


def _candidate_payloads_from_gleif_entity_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = row.get("candidates_payload")
    if isinstance(payload, list):
        return [candidate for candidate in payload if isinstance(candidate, dict)]
    candidates = row.get("candidates")
    if isinstance(candidates, list):
        return [candidate for candidate in candidates if isinstance(candidate, dict)]
    return []


def _maybe_add_evidence_from_gleif_item(
    by_lei: dict[str, HomeCountryEvidence],
    *,
    item: dict[str, Any],
    source_table: str,
    fallback_lei: Any = None,
    fallback_legal_name: Any = None,
) -> None:
    attributes = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
    entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
    legal_name = entity.get("legalName")
    if isinstance(legal_name, dict):
        legal_name = legal_name.get("name")
    legal_address = entity.get("legalAddress") if isinstance(entity.get("legalAddress"), dict) else {}
    headquarters_address = entity.get("headquartersAddress") if isinstance(entity.get("headquartersAddress"), dict) else {}
    _maybe_add_evidence(
        by_lei,
        lei=attributes.get("lei") or item.get("id") or fallback_lei,
        legal_name=legal_name or attributes.get("legalName") or fallback_legal_name,
        legal_country=legal_address.get("country"),
        jurisdiction=entity.get("jurisdiction"),
        headquarters_country=headquarters_address.get("country"),
        source_table=source_table,
    )


def _maybe_add_evidence(
    by_lei: dict[str, HomeCountryEvidence],
    *,
    lei: Any,
    legal_name: Any,
    legal_country: Any,
    jurisdiction: Any,
    headquarters_country: Any,
    source_table: str,
) -> None:
    normalized_lei = _clean_lei(lei)
    if not normalized_lei or normalized_lei in by_lei:
        return
    country, source_field = _best_country_with_source(
        legal_country=legal_country,
        jurisdiction=jurisdiction,
        headquarters_country=headquarters_country,
    )
    if not country:
        return
    by_lei[normalized_lei] = HomeCountryEvidence(
        lei=normalized_lei,
        home_country=country,
        source_table=source_table,
        source_field=source_field,
        legal_name=str(legal_name or "").strip(),
    )


def _best_country_with_source(*, legal_country: Any, jurisdiction: Any, headquarters_country: Any) -> tuple[str, str]:
    legal = _clean_country(legal_country)
    if legal:
        return legal, "legal_country"
    jurisdiction_country = _clean_country(str(jurisdiction or "").split("-", 1)[0])
    if jurisdiction_country:
        return jurisdiction_country, "jurisdiction"
    headquarters = _clean_country(headquarters_country)
    if headquarters:
        return headquarters, "headquarters_country"
    return "", ""


def _gleif_payload_items(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    pages = payload.get("pages")
    if isinstance(pages, list):
        items: list[dict[str, Any]] = []
        for page in pages:
            items.extend(_gleif_payload_items(page))
        return items
    data = payload.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _clean_country(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if len(text) == 2 and text.isalpha() else ""


def _clean_lei(value: Any) -> str:
    return str(value or "").strip().upper()
