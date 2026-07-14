"""GLEIF ISIN-to-LEI enrichment for Entity Layer V0.1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import requests

GLEIF_LEI_RECORDS_URL = "https://api.gleif.org/api/v1/lei-records"


@dataclass(frozen=True, slots=True)
class GleifIsinLeiRecord:
    isin: str
    lei: str = ""
    legal_name: str = ""
    response_payload: dict[str, Any] | None = None
    status: str = "not_found"
    error_message: str = ""
    source: str = "gleif"


class GleifIsinLeiClient:
    def __init__(
        self,
        *,
        fixture_mappings: dict[str, Any] | None = None,
        offline: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        self.fixture_mappings = {str(k).strip().upper(): v for k, v in (fixture_mappings or {}).items()}
        self.offline = bool(offline)
        self.session = session or requests.Session()

    def lookup_isins(self, isins: list[str]) -> list[GleifIsinLeiRecord]:
        out = []
        seen: set[str] = set()
        for raw_isin in isins:
            isin = _clean_text(raw_isin, upper=True)
            if not isin or isin in seen:
                continue
            seen.add(isin)
            out.append(self.lookup_isin(isin))
        return out

    def lookup_isin(self, isin: str) -> GleifIsinLeiRecord:
        cleaned_isin = _clean_text(isin, upper=True)
        if self.fixture_mappings:
            return self._from_fixture(cleaned_isin)
        if self.offline:
            return GleifIsinLeiRecord(
                isin=cleaned_isin,
                status="not_found",
                error_message="offline_without_fixture",
            )
        try:
            response = self.session.get(
                GLEIF_LEI_RECORDS_URL,
                params={"filter[isin]": cleaned_isin},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            return _record_from_gleif_payload(cleaned_isin, payload)
        except Exception as exc:
            return GleifIsinLeiRecord(
                isin=cleaned_isin,
                status="error",
                error_message=str(exc),
            )

    def _from_fixture(self, isin: str) -> GleifIsinLeiRecord:
        raw = self.fixture_mappings.get(isin)
        if raw is None:
            return GleifIsinLeiRecord(
                isin=isin,
                status="not_found",
                error_message="fixture_not_found",
            )
        if isinstance(raw, GleifIsinLeiRecord):
            return replace(raw, isin=isin)
        if isinstance(raw, dict):
            status = _clean_text(raw.get("status"), upper=False) or ("success" if _clean_text(raw.get("lei"), upper=True) else "not_found")
            return GleifIsinLeiRecord(
                isin=isin,
                lei=_clean_text(raw.get("lei"), upper=True),
                legal_name=_clean_text(raw.get("legal_name") or raw.get("legalName")),
                response_payload=raw,
                status=status,
                error_message=_clean_text(raw.get("error_message"), upper=False),
                source=_clean_text(raw.get("source"), upper=False) or "gleif",
            )
        lei = _clean_text(raw, upper=True)
        return GleifIsinLeiRecord(
            isin=isin,
            lei=lei,
            response_payload={"lei": raw},
            status="success" if lei else "not_found",
        )


def gleif_cache_rows(records: list[GleifIsinLeiRecord]) -> list[dict[str, Any]]:
    return [
        {
            "isin": record.isin,
            "lei": record.lei or None,
            "legal_name": record.legal_name or None,
            "response_payload": record.response_payload,
            "status": record.status,
            "error_message": record.error_message or None,
        }
        for record in records
    ]


def _record_from_gleif_payload(isin: str, payload: Any) -> GleifIsinLeiRecord:
    if not isinstance(payload, dict):
        return GleifIsinLeiRecord(isin=isin, response_payload={"body": payload}, status="error", error_message="unexpected_gleif_shape")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        return GleifIsinLeiRecord(isin=isin, response_payload=payload, status="not_found", error_message="no_gleif_isin_mapping")
    first = data[0]
    if not isinstance(first, dict):
        return GleifIsinLeiRecord(isin=isin, response_payload=payload, status="error", error_message="unexpected_gleif_item_shape")
    attributes = first.get("attributes") if isinstance(first.get("attributes"), dict) else {}
    lei = _clean_text(attributes.get("lei") or attributes.get("LEI") or first.get("id"), upper=True)
    entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
    legal_name_value = entity.get("legalName")
    if isinstance(legal_name_value, dict):
        legal_name_value = legal_name_value.get("name")
    legal_name = _clean_text(
        attributes.get("legalName")
        or attributes.get("entityLegalName")
        or legal_name_value
        or entity.get("legalName.name")
    )
    return GleifIsinLeiRecord(
        isin=isin,
        lei=lei,
        legal_name=legal_name,
        response_payload=payload,
        status="success" if lei else "not_found",
        error_message="" if lei else "gleif_mapping_without_lei",
    )


def _clean_text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a"}:
        return ""
    return text.upper() if upper else text
