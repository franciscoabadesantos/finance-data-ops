"""GLEIF ISIN/LEI enrichment for Entity Layer V0.1/V0.2."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import requests

from finance_data_ops.identity.isin import ISIN_PATTERN

GLEIF_LEI_RECORDS_URL = "https://api.gleif.org/api/v1/lei-records"
DEFAULT_GLEIF_PAGE_SIZE = 200
MAX_GLEIF_PAGES = 100


@dataclass(frozen=True, slots=True)
class GleifIsinLeiRecord:
    isin: str
    lei: str = ""
    legal_name: str = ""
    response_payload: dict[str, Any] | None = None
    status: str = "not_found"
    error_message: str = ""
    source: str = "gleif"


@dataclass(frozen=True, slots=True)
class GleifLeiIsinRecord:
    lei: str
    isin_list: list[str]
    response_payload: dict[str, Any] | None = None
    status: str = "not_found"
    error_message: str = ""
    source: str = "gleif"
    legal_name: str = ""


class GleifIsinLeiClient:
    def __init__(
        self,
        *,
        fixture_mappings: dict[str, Any] | None = None,
        offline: bool = False,
        session: requests.Session | None = None,
        page_size: int = DEFAULT_GLEIF_PAGE_SIZE,
        max_pages: int = MAX_GLEIF_PAGES,
    ) -> None:
        self.fixture_mappings = {str(k).strip().upper(): v for k, v in (fixture_mappings or {}).items()}
        self.offline = bool(offline)
        self.session = session or requests.Session()
        self.page_size = max(1, min(int(page_size), 200))
        self.max_pages = max(1, int(max_pages))

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

    def lookup_lei_isins(self, leis: list[str]) -> list[GleifLeiIsinRecord]:
        out = []
        seen: set[str] = set()
        for raw_lei in leis:
            lei = _clean_text(raw_lei, upper=True)
            if not lei or lei in seen:
                continue
            seen.add(lei)
            out.append(self.lookup_lei_isin(lei))
        return out

    def lookup_lei_isin(self, lei: str) -> GleifLeiIsinRecord:
        cleaned_lei = _clean_text(lei, upper=True)
        if self.fixture_mappings:
            return self._lei_isins_from_fixture(cleaned_lei)
        if self.offline:
            return GleifLeiIsinRecord(
                lei=cleaned_lei,
                isin_list=[],
                status="not_found",
                error_message="offline_without_fixture",
            )
        pages: list[dict[str, Any]] = []
        page_number = 1
        try:
            while page_number <= self.max_pages:
                response = self.session.get(
                    f"{GLEIF_LEI_RECORDS_URL}/{cleaned_lei}/isins",
                    params={"page[size]": self.page_size, "page[number]": page_number},
                    timeout=30,
                )
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    pages.append(payload)
                else:
                    pages.append({"body": payload})
                if not _has_next_page(payload=payload, page_number=page_number, page_size=self.page_size):
                    break
                page_number += 1
            if page_number > self.max_pages:
                return GleifLeiIsinRecord(
                    lei=cleaned_lei,
                    isin_list=[],
                    response_payload={"pages": pages},
                    status="error",
                    error_message="gleif_lei_isin_pagination_limit_exceeded",
                    source="gleif_lei_record_isins",
                )
            return _lei_isin_record_from_payload(
                cleaned_lei,
                {"pages": pages},
                source="gleif_lei_record_isins",
            )
        except Exception as exc:
            return GleifLeiIsinRecord(
                lei=cleaned_lei,
                isin_list=[],
                response_payload={"pages": pages} if pages else None,
                status="error",
                error_message=str(exc),
                source="gleif_lei_record_isins",
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

    def _lei_isins_from_fixture(self, lei: str) -> GleifLeiIsinRecord:
        raw = self.fixture_mappings.get(f"LEI:{lei}") or self.fixture_mappings.get(lei)
        if raw is None:
            return GleifLeiIsinRecord(
                lei=lei,
                isin_list=[],
                status="not_found",
                error_message="fixture_not_found",
            )
        if isinstance(raw, GleifLeiIsinRecord):
            return replace(raw, lei=lei)
        if isinstance(raw, dict):
            isins = _clean_isin_list(raw.get("isin_list") or raw.get("isins") or raw.get("isinList") or [])
            status = _clean_text(raw.get("status"), upper=False) or ("success" if isins else "not_found")
            return GleifLeiIsinRecord(
                lei=lei,
                isin_list=isins,
                response_payload=raw,
                status=status,
                error_message=_clean_text(raw.get("error_message"), upper=False),
                source=_clean_text(raw.get("source"), upper=False) or "gleif",
                legal_name=_clean_text(raw.get("legal_name") or raw.get("legalName")),
            )
        if isinstance(raw, (list, tuple, set)):
            isins = _clean_isin_list(raw)
            return GleifLeiIsinRecord(
                lei=lei,
                isin_list=isins,
                response_payload={"isin_list": list(raw)},
                status="success" if isins else "not_found",
            )
        return GleifLeiIsinRecord(
            lei=lei,
            isin_list=[],
            response_payload={"body": raw},
            status="error",
            error_message=f"unsupported_fixture_mapping: {type(raw).__name__}",
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


def gleif_lei_isin_cache_rows(records: list[GleifLeiIsinRecord]) -> list[dict[str, Any]]:
    return [
        {
            "lei": record.lei,
            "response_payload": record.response_payload,
            "isin_list": list(record.isin_list),
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


def _lei_isin_record_from_payload(lei: str, payload: Any, *, source: str) -> GleifLeiIsinRecord:
    if not isinstance(payload, dict):
        return GleifLeiIsinRecord(
            lei=lei,
            isin_list=[],
            response_payload={"body": payload},
            status="error",
            error_message="unexpected_gleif_shape",
            source=source,
        )
    isins = _extract_isins(payload)
    legal_name = _extract_legal_name(payload)
    message = "no_gleif_lei_isin_mapping"
    return GleifLeiIsinRecord(
        lei=lei,
        isin_list=isins,
        response_payload=payload,
        status="success" if isins else "not_found",
        error_message="" if isins else message,
        source=source,
        legal_name=legal_name,
    )


def _extract_isins(payload: Any) -> list[str]:
    found: set[str] = set()

    def visit(value: Any, key: str = "") -> None:
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                visit(inner_value, str(inner_key))
            return
        if isinstance(value, list):
            for item in value:
                visit(item, key)
            return
        text = _clean_text(value, upper=True)
        if not text:
            return
        if (key.lower() in {"isin", "isins", "isin_code", "isincode", "id"} or ISIN_PATTERN.fullmatch(text)) and ISIN_PATTERN.fullmatch(text):
            found.add(text)

    visit(payload)
    return sorted(found)


def _extract_legal_name(payload: dict[str, Any]) -> str:
    pages = payload.get("pages")
    if isinstance(pages, list):
        for page in pages:
            if isinstance(page, dict) and (name := _extract_legal_name(page)):
                return name
    data = payload.get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        attributes = data[0].get("attributes") if isinstance(data[0].get("attributes"), dict) else {}
    elif isinstance(data, dict):
        attributes = data.get("attributes") if isinstance(data.get("attributes"), dict) else {}
    else:
        attributes = payload.get("attributes") if isinstance(payload.get("attributes"), dict) else {}
    entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
    legal_name_value = entity.get("legalName")
    if isinstance(legal_name_value, dict):
        legal_name_value = legal_name_value.get("name")
    return _clean_text(attributes.get("legalName") or attributes.get("entityLegalName") or legal_name_value)


def _clean_isin_list(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted(
        {
            isin
            for raw in values
            if (isin := _clean_text(raw, upper=True)) and ISIN_PATTERN.fullmatch(isin)
        }
    )


def _has_next_page(*, payload: Any, page_number: int, page_size: int) -> bool:
    if not isinstance(payload, dict):
        return False
    links = payload.get("links")
    if isinstance(links, dict) and _clean_text(links.get("next")):
        return True
    meta = payload.get("meta")
    if isinstance(meta, dict):
        pagination = meta.get("pagination") if isinstance(meta.get("pagination"), dict) else meta
        current = _int_or_none(pagination.get("currentPage") or pagination.get("current_page") or pagination.get("page"))
        total_pages = _int_or_none(pagination.get("totalPages") or pagination.get("total_pages"))
        if current is not None and total_pages is not None:
            return current < total_pages
        total = _int_or_none(pagination.get("total") or pagination.get("totalRecords") or pagination.get("total_records"))
        if total is not None:
            return page_number * page_size < total
    data = payload.get("data")
    return isinstance(data, list) and len(data) >= page_size


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


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
