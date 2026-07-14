"""GLEIF ISIN/LEI enrichment for Entity Layer V0.1/V0.2."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, replace
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from finance_data_ops.identity.isin import ISIN_PATTERN
from finance_data_ops.identity.names import normalize_legal_name_conservative

GLEIF_LEI_RECORDS_URL = "https://api.gleif.org/api/v1/lei-records"
DEFAULT_GLEIF_PAGE_SIZE = 200
DEFAULT_GLEIF_REQUEST_SLEEP_SECONDS = 0.5
DEFAULT_GLEIF_LEI_ISIN_MAX_RETRIES = 3
DEFAULT_GLEIF_RETRY_BACKOFF_SECONDS = 1.0
DEFAULT_GLEIF_RETRY_JITTER_SECONDS = 0.25
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


@dataclass(frozen=True, slots=True)
class GleifLegalNameRecord:
    query_name: str
    normalized_query_name: str
    candidates: list[dict[str, Any]]
    response_payload: dict[str, Any] | None = None
    status: str = "not_found"
    error_message: str = ""
    source: str = "gleif_legal_name"


class GleifRateLimitError(RuntimeError):
    """GLEIF returned HTTP 429 after configured retries."""


class GleifIsinLeiClient:
    def __init__(
        self,
        *,
        fixture_mappings: dict[str, Any] | None = None,
        offline: bool = False,
        session: requests.Session | None = None,
        page_size: int = DEFAULT_GLEIF_PAGE_SIZE,
        max_pages: int = MAX_GLEIF_PAGES,
        request_sleep_seconds: float = DEFAULT_GLEIF_REQUEST_SLEEP_SECONDS,
        lei_isin_max_retries: int = DEFAULT_GLEIF_LEI_ISIN_MAX_RETRIES,
        retry_backoff_seconds: float = DEFAULT_GLEIF_RETRY_BACKOFF_SECONDS,
        retry_jitter_seconds: float = DEFAULT_GLEIF_RETRY_JITTER_SECONDS,
        sleep_func: Any = time.sleep,
    ) -> None:
        self.fixture_mappings = {str(k).strip().upper(): v for k, v in (fixture_mappings or {}).items()}
        self.offline = bool(offline)
        self.session = session or requests.Session()
        self.page_size = max(1, min(int(page_size), 200))
        self.max_pages = max(1, int(max_pages))
        self.request_sleep_seconds = max(0.0, float(request_sleep_seconds))
        self.lei_isin_max_retries = max(0, int(lei_isin_max_retries))
        self.retry_backoff_seconds = max(0.0, float(retry_backoff_seconds))
        self.retry_jitter_seconds = max(0.0, float(retry_jitter_seconds))
        self.sleep_func = sleep_func

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
        request_count = 0
        for raw_lei in leis:
            lei = _clean_text(raw_lei, upper=True)
            if not lei or lei in seen:
                continue
            seen.add(lei)
            if request_count and self.request_sleep_seconds and not self.fixture_mappings and not self.offline:
                self.sleep_func(self.request_sleep_seconds)
            out.append(self.lookup_lei_isin(lei))
            request_count += 1
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
                response = self._get_lei_isin_page_with_retry(cleaned_lei=cleaned_lei, page_number=page_number)
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
            if _is_rate_limit_exception(exc):
                return GleifLeiIsinRecord(
                    lei=cleaned_lei,
                    isin_list=[],
                    response_payload={"pages": pages} if pages else None,
                    status="rate_limited",
                    error_message=str(exc),
                    source="gleif_lei_record_isins",
                )
            return GleifLeiIsinRecord(
                lei=cleaned_lei,
                isin_list=[],
                response_payload={"pages": pages} if pages else None,
                status="error",
                error_message=str(exc),
                source="gleif_lei_record_isins",
            )

    def _get_lei_isin_page_with_retry(self, *, cleaned_lei: str, page_number: int) -> requests.Response:
        attempt = 0
        while True:
            response = self.session.get(
                f"{GLEIF_LEI_RECORDS_URL}/{cleaned_lei}/isins",
                params={"page[size]": self.page_size, "page[number]": page_number},
                timeout=30,
            )
            if getattr(response, "status_code", None) != 429:
                response.raise_for_status()
                return response
            if attempt >= self.lei_isin_max_retries:
                raise GleifRateLimitError(_rate_limit_message(response=response, attempt=attempt))
            delay = _retry_delay_seconds(
                response=response,
                attempt=attempt,
                backoff_seconds=self.retry_backoff_seconds,
                jitter_seconds=self.retry_jitter_seconds,
            )
            if delay > 0:
                self.sleep_func(delay)
            attempt += 1

    def search_legal_names(self, names: list[str]) -> list[GleifLegalNameRecord]:
        by_normalized: dict[str, GleifLegalNameRecord] = {}
        for raw_name in names:
            query_name = _clean_text(raw_name)
            normalized = normalize_legal_name_conservative(query_name)
            if not query_name or not normalized:
                continue
            existing = by_normalized.get(normalized)
            if existing and existing.status == "success":
                continue
            record = self.search_legal_name(query_name)
            if not existing or record.status == "success":
                by_normalized[normalized] = record
        return list(by_normalized.values())

    def search_legal_name(self, name: str) -> GleifLegalNameRecord:
        query_name = _clean_text(name)
        normalized = normalize_legal_name_conservative(query_name)
        if self.fixture_mappings:
            return self._legal_name_from_fixture(query_name=query_name, normalized=normalized)
        if self.offline:
            return GleifLegalNameRecord(
                query_name=query_name,
                normalized_query_name=normalized,
                candidates=[],
                status="not_found",
                error_message="offline_without_fixture",
            )
        pages: list[dict[str, Any]] = []
        page_number = 1
        try:
            while page_number <= self.max_pages:
                response = self.session.get(
                    GLEIF_LEI_RECORDS_URL,
                    params={
                        "filter[entity.legalName]": query_name,
                        "page[size]": min(self.page_size, 20),
                        "page[number]": page_number,
                    },
                    timeout=30,
                )
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    pages.append(payload)
                else:
                    pages.append({"body": payload})
                if not _has_next_page(payload=payload, page_number=page_number, page_size=min(self.page_size, 20)):
                    break
                page_number += 1
            payload = {"pages": pages}
            candidates = _legal_name_candidates_from_payload(payload, normalized_query_name=normalized)
            return GleifLegalNameRecord(
                query_name=query_name,
                normalized_query_name=normalized,
                candidates=candidates,
                response_payload=payload,
                status="success" if candidates else "not_found",
                error_message="" if candidates else "no_gleif_legal_name_candidates",
            )
        except Exception as exc:
            return GleifLegalNameRecord(
                query_name=query_name,
                normalized_query_name=normalized,
                candidates=[],
                response_payload={"pages": pages} if pages else None,
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

    def _legal_name_from_fixture(self, *, query_name: str, normalized: str) -> GleifLegalNameRecord:
        raw = (
            self.fixture_mappings.get(f"NAME:{normalized}")
            or self.fixture_mappings.get(f"LEGAL_NAME:{normalized}")
            or self.fixture_mappings.get(normalized)
        )
        if raw is None:
            return GleifLegalNameRecord(
                query_name=query_name,
                normalized_query_name=normalized,
                candidates=[],
                status="not_found",
                error_message="fixture_not_found",
            )
        if isinstance(raw, GleifLegalNameRecord):
            return replace(raw, query_name=query_name, normalized_query_name=normalized)
        candidates = []
        if isinstance(raw, dict):
            values = raw.get("candidates") if isinstance(raw.get("candidates"), list) else [raw]
            candidates = [_legal_name_candidate_from_mapping(value) for value in values if isinstance(value, dict)]
        elif isinstance(raw, list):
            candidates = [_legal_name_candidate_from_mapping(value) for value in raw if isinstance(value, dict)]
        candidates = [
            candidate
            for candidate in candidates
            if candidate.get("lei") and candidate.get("normalized_legal_name") == normalized
        ]
        return GleifLegalNameRecord(
            query_name=query_name,
            normalized_query_name=normalized,
            candidates=candidates,
            response_payload=raw if isinstance(raw, dict) else {"candidates": raw},
            status="success" if candidates else "not_found",
            error_message="" if candidates else "fixture_no_candidates",
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


def _is_rate_limit_exception(exc: Exception) -> bool:
    return isinstance(exc, GleifRateLimitError) or "429" in str(exc) or "Too Many Requests" in str(exc)


def _retry_delay_seconds(
    *,
    response: Any,
    attempt: int,
    backoff_seconds: float,
    jitter_seconds: float,
) -> float:
    retry_after = _retry_after_seconds(response)
    if retry_after is not None:
        return retry_after
    return backoff_seconds * (2**attempt) + (random.uniform(0, jitter_seconds) if jitter_seconds else 0.0)


def _retry_after_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", {}) or {}
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(text)
        now = datetime.now(tz=retry_at.tzinfo) if retry_at.tzinfo else datetime.now()
        return max(0.0, (retry_at - now).total_seconds())
    except Exception:
        return None


def _rate_limit_message(*, response: Any, attempt: int) -> str:
    retry_after = getattr(response, "headers", {}).get("Retry-After") if hasattr(getattr(response, "headers", {}), "get") else None
    suffix = f"; retry_after={retry_after}" if retry_after else ""
    return f"429 Client Error: Too Many Requests for GLEIF LEI->ISIN expansion after {attempt + 1} attempts{suffix}"


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


def _legal_name_candidates_from_payload(payload: dict[str, Any], *, normalized_query_name: str) -> list[dict[str, Any]]:
    candidates = []
    for item in _payload_data_items(payload):
        candidate = _legal_name_candidate_from_gleif_item(item)
        if candidate.get("lei") and candidate.get("normalized_legal_name") == normalized_query_name:
            candidates.append(candidate)
    return candidates


def _payload_data_items(payload: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return items
    pages = payload.get("pages")
    if isinstance(pages, list):
        for page in pages:
            items.extend(_payload_data_items(page))
        return items
    data = payload.get("data")
    if isinstance(data, list):
        items.extend(item for item in data if isinstance(item, dict))
    elif isinstance(data, dict):
        items.append(data)
    return items


def _legal_name_candidate_from_gleif_item(item: dict[str, Any]) -> dict[str, Any]:
    attributes = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
    entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
    registration = attributes.get("registration") if isinstance(attributes.get("registration"), dict) else {}
    legal_name_value = entity.get("legalName")
    if isinstance(legal_name_value, dict):
        legal_name_value = legal_name_value.get("name")
    legal_address = entity.get("legalAddress") if isinstance(entity.get("legalAddress"), dict) else {}
    headquarters_address = entity.get("headquartersAddress") if isinstance(entity.get("headquartersAddress"), dict) else {}
    return _legal_name_candidate_from_mapping(
        {
            "lei": attributes.get("lei") or item.get("id"),
            "legal_name": legal_name_value or attributes.get("legalName"),
            "legal_country": legal_address.get("country"),
            "headquarters_country": headquarters_address.get("country"),
            "jurisdiction": entity.get("jurisdiction"),
            "entity_status": entity.get("status"),
            "registration_status": registration.get("status"),
            "conformity_flag": attributes.get("conformityFlag"),
        }
    )


def _legal_name_candidate_from_mapping(raw: dict[str, Any]) -> dict[str, Any]:
    legal_name = _clean_text(raw.get("legal_name") or raw.get("legalName") or raw.get("name"))
    jurisdiction = _clean_text(raw.get("jurisdiction"), upper=True)
    return {
        "lei": _clean_text(raw.get("lei"), upper=True),
        "legal_name": legal_name,
        "normalized_legal_name": normalize_legal_name_conservative(legal_name),
        "legal_country": _clean_text(raw.get("legal_country") or raw.get("legalCountry"), upper=True),
        "headquarters_country": _clean_text(raw.get("headquarters_country") or raw.get("headquartersCountry"), upper=True),
        "jurisdiction": jurisdiction,
        "jurisdiction_country": jurisdiction.split("-", 1)[0] if jurisdiction else "",
        "entity_status": _clean_text(raw.get("entity_status") or raw.get("entityStatus"), upper=True),
        "registration_status": _clean_text(raw.get("registration_status") or raw.get("registrationStatus"), upper=True),
        "conformity_flag": _clean_text(raw.get("conformity_flag") or raw.get("conformityFlag"), upper=True),
    }


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
