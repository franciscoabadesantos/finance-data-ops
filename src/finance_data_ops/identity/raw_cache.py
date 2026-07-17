"""Raw-cache readers for Entity Layer measurement.

These helpers hydrate the same model records produced by live/fixture clients
from the side-by-side source_cache tables. Cache misses are explicit diagnostic
records and must not be treated as raw facts.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from finance_data_ops.identity.gleif import (
    GleifIsinLeiRecord,
    GleifLegalNameRecord,
    GleifLeiIsinRecord,
)
from finance_data_ops.identity.gleif import _legal_name_candidate_from_mapping
from finance_data_ops.identity.isin import IsinRecord
from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping, OpenFigiRequest
from finance_data_ops.identity.names import normalize_legal_name_conservative
from finance_data_ops.identity.openfigi import (
    build_openfigi_request,
    build_openfigi_request_variants,
)
from finance_data_ops.identity.openfigi import _mapping_from_openfigi_item


CACHE_MISS = "cache_miss"


@dataclass(frozen=True, slots=True)
class RawCacheSnapshot:
    openfigi_rows: list[dict[str, Any]]
    listing_isin_rows: list[dict[str, Any]]
    gleif_isin_lei_rows: list[dict[str, Any]]
    gleif_lei_isin_rows: list[dict[str, Any]]
    gleif_entity_rows: list[dict[str, Any]]

    def openfigi_mappings_for_candidates(self, candidates: list[ListingCandidate]) -> list[OpenFigiMapping]:
        rows_by_hash = {_clean_text(row.get("request_hash"), upper=True): row for row in self.openfigi_rows}
        out: list[OpenFigiMapping] = []
        for candidate in candidates:
            variants = build_openfigi_request_variants(candidate)
            cached = next((rows_by_hash.get(request.request_hash.upper()) for request in variants if rows_by_hash.get(request.request_hash.upper())), None)
            request = next((variant for variant in variants if cached and variant.request_hash.upper() == _clean_text(cached.get("request_hash"), upper=True)), variants[0])
            if cached is None:
                out.append(_openfigi_cache_miss(candidate=candidate, request=variants[0]))
                continue
            out.append(_openfigi_mapping_from_cache_row(request=request, row=cached))
        return out

    def isin_records_for_candidates(self, candidates: list[ListingCandidate], *, provider: str = "yfinance") -> list[IsinRecord]:
        rows_by_key = {
            (_clean_text(row.get("symbol"), upper=True), _clean_text(row.get("provider"), upper=False) or provider): row
            for row in self.listing_isin_rows
        }
        out: list[IsinRecord] = []
        for candidate in candidates:
            symbol = _clean_text(candidate.symbol, upper=True)
            provider_symbol = _clean_text(candidate.provider_symbol or candidate.symbol, upper=True)
            row = rows_by_key.get((symbol, provider))
            if row is None:
                out.append(
                    IsinRecord(
                        symbol=symbol,
                        provider=provider,
                        request_payload={"provider_symbol": provider_symbol},
                        response_payload=None,
                        status="not_found",
                        error_message=CACHE_MISS,
                        source="raw_cache",
                    )
                )
                continue
            out.append(
                IsinRecord(
                    symbol=symbol,
                    provider=_clean_text(row.get("provider"), upper=False) or provider,
                    request_payload=_dict_or_empty(row.get("request_payload")) or {"provider_symbol": provider_symbol},
                    response_payload=_dict_or_none(row.get("response_payload")),
                    isin=_clean_text(row.get("isin"), upper=True),
                    status=_clean_text(row.get("status"), upper=False) or "not_found",
                    error_message=_clean_text(row.get("error_message"), upper=False),
                    source="raw_cache",
                )
            )
        return out

    def gleif_records_for_isins(self, isins: list[str]) -> list[GleifIsinLeiRecord]:
        rows_by_isin = {_clean_text(row.get("isin"), upper=True): row for row in self.gleif_isin_lei_rows}
        out: list[GleifIsinLeiRecord] = []
        seen: set[str] = set()
        for raw_isin in isins:
            isin = _clean_text(raw_isin, upper=True)
            if not isin or isin in seen:
                continue
            seen.add(isin)
            row = rows_by_isin.get(isin)
            if row is None:
                out.append(GleifIsinLeiRecord(isin=isin, status="not_found", error_message=CACHE_MISS, source="raw_cache"))
                continue
            out.append(
                GleifIsinLeiRecord(
                    isin=isin,
                    lei=_clean_text(row.get("lei"), upper=True),
                    legal_name=_clean_text(row.get("legal_name")),
                    response_payload=_dict_or_none(row.get("response_payload")),
                    status=_clean_text(row.get("status"), upper=False) or "not_found",
                    error_message=_clean_text(row.get("error_message"), upper=False),
                    source="raw_cache",
                )
            )
        return out

    def gleif_lei_isin_records_for_leis(self, leis: list[str]) -> list[GleifLeiIsinRecord]:
        rows_by_lei = {_clean_text(row.get("lei"), upper=True): row for row in self.gleif_lei_isin_rows}
        out: list[GleifLeiIsinRecord] = []
        seen: set[str] = set()
        for raw_lei in leis:
            lei = _clean_text(raw_lei, upper=True)
            if not lei or lei in seen:
                continue
            seen.add(lei)
            row = rows_by_lei.get(lei)
            if row is None:
                out.append(GleifLeiIsinRecord(lei=lei, isin_list=[], status="not_found", error_message=CACHE_MISS, source="raw_cache"))
                continue
            out.append(
                GleifLeiIsinRecord(
                    lei=lei,
                    isin_list=_clean_isin_list(row.get("isin_list")),
                    response_payload=_dict_or_none(row.get("response_payload")),
                    status=_clean_text(row.get("status"), upper=False) or "not_found",
                    error_message=_clean_text(row.get("error_message"), upper=False),
                    source="raw_cache",
                )
            )
        return out

    def legal_name_records_for_queries(self, queries: list[str]) -> list[GleifLegalNameRecord]:
        rows_by_normalized = {
            _clean_text(row.get("normalized_query_name"), upper=True): row
            for row in self.gleif_entity_rows
            if _clean_text(row.get("normalized_query_name"), upper=True)
        }
        legacy_candidates = _gleif_entity_candidates(self.gleif_entity_rows)
        out: list[GleifLegalNameRecord] = []
        seen: set[str] = set()
        for query in queries:
            query_name = _clean_text(query)
            normalized = normalize_legal_name_conservative(query_name)
            if not query_name or not normalized or normalized in seen:
                continue
            seen.add(normalized)
            row = rows_by_normalized.get(_clean_text(normalized, upper=True))
            if row is not None:
                out.append(_legal_name_record_from_cache_row(query_name=query_name, normalized=normalized, row=row))
                continue
            candidates = [
                candidate
                for candidate in legacy_candidates
                if candidate.get("lei") and candidate.get("normalized_legal_name") == normalized
            ]
            if candidates:
                out.append(
                    GleifLegalNameRecord(
                        query_name=query_name,
                        normalized_query_name=normalized,
                        candidates=candidates,
                        response_payload={"source_cache.gleif_entity_raw_legacy": len(self.gleif_entity_rows)},
                        status="success" if len(candidates) == 1 else "ambiguous",
                        error_message="",
                        source="raw_cache_gleif_entity",
                    )
                )
                continue
            out.append(
                GleifLegalNameRecord(
                    query_name=query_name,
                    normalized_query_name=normalized,
                    candidates=[],
                    response_payload=None,
                    status="not_found",
                    error_message=CACHE_MISS,
                    source="raw_cache_gleif_entity",
                )
            )
        return out

    def coverage_summary(
        self,
        *,
        candidates: list[ListingCandidate],
        openfigi_mappings: list[OpenFigiMapping],
        isin_records: list[IsinRecord],
        gleif_records: list[GleifIsinLeiRecord],
        gleif_lei_isin_records: list[GleifLeiIsinRecord],
        legal_name_records: list[GleifLegalNameRecord],
        tracked_only: bool,
    ) -> dict[str, Any]:
        return {
            "raw_cache_enabled": True,
            "raw_cache_mode": "cache_read",
            "tracked_only": bool(tracked_only),
            "tracked_candidate_count": len(candidates) if tracked_only else 0,
            "candidate_count": len(candidates),
            "openfigi_cached_current_request_hash_count": _non_miss_count(openfigi_mappings),
            "openfigi_cache_miss_count": _miss_count(openfigi_mappings),
            "listing_isin_cached_count": _non_miss_count(isin_records),
            "listing_isin_cache_miss_count": _miss_count(isin_records),
            "gleif_isin_lei_cached_count": _non_miss_count(gleif_records),
            "gleif_isin_lei_cache_miss_count": _miss_count(gleif_records),
            "gleif_lei_isin_cached_count": _non_miss_count(gleif_lei_isin_records),
            "gleif_lei_isin_cache_miss_count": _miss_count(gleif_lei_isin_records),
            "gleif_entity_cached_count": len(self.gleif_entity_rows),
            "legal_name_cached_count": _non_miss_count(legal_name_records),
            "legal_name_cache_miss_count": _miss_count(legal_name_records),
            "legal_name_not_found_cached_count": _status_count(legal_name_records, "not_found", exclude_cache_miss=True),
            "legal_name_cache_gap_count": _miss_count(legal_name_records),
            "gleif_isin_lei_cached_success": _status_count(gleif_records, "success"),
            "gleif_isin_lei_cached_not_found": _status_count(gleif_records, "not_found", exclude_cache_miss=True),
            "gleif_isin_lei_cache_miss": _miss_count(gleif_records),
            "gleif_lei_isin_cached_success": _status_count(gleif_lei_isin_records, "success"),
            "gleif_lei_isin_cached_not_found": _status_count(
                gleif_lei_isin_records,
                "not_found",
                exclude_cache_miss=True,
            ),
            "gleif_lei_isin_cache_miss": _miss_count(gleif_lei_isin_records),
            "cache_missing_samples": {
                "openfigi": _miss_sample(openfigi_mappings, "symbol"),
                "listing_isin": _miss_sample(isin_records, "symbol"),
                "gleif_isin_lei": _miss_sample(gleif_records, "isin"),
                "gleif_lei_isin": _miss_sample(gleif_lei_isin_records, "lei"),
                "legal_name": _miss_sample(legal_name_records, "query_name"),
            },
        }


def read_postgres_raw_cache_snapshot(*, database_dsn: str, candidates: list[ListingCandidate]) -> RawCacheSnapshot:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for raw cache reads.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres raw cache reads.") from exc

    request_hashes = sorted(
        {
            request.request_hash
            for candidate in candidates
            for request in build_openfigi_request_variants(candidate)
            if request.request_hash
        }
    )
    symbols = sorted({_clean_text(candidate.symbol, upper=True) for candidate in candidates if _clean_text(candidate.symbol, upper=True)})
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        return RawCacheSnapshot(
            openfigi_rows=_query_in(conn, "source_cache.openfigi_mapping_raw", "request_hash", request_hashes),
            listing_isin_rows=_query_listing_isin(conn, symbols=symbols, provider="yfinance"),
            gleif_isin_lei_rows=_query_all(conn, "source_cache.gleif_isin_lei_raw"),
            gleif_lei_isin_rows=_query_all(conn, "source_cache.gleif_lei_isin_raw"),
            gleif_entity_rows=_query_all(conn, "source_cache.gleif_entity_raw"),
        )


def merge_records_by_key(existing: list[Any], new_records: list[Any], key_attr: str) -> list[Any]:
    by_key = {_clean_text(getattr(record, key_attr, ""), upper=True): record for record in existing if _clean_text(getattr(record, key_attr, ""), upper=True)}
    for record in new_records:
        key = _clean_text(getattr(record, key_attr, ""), upper=True)
        if key:
            by_key[key] = record
    return list(by_key.values())


def missing_record_keys(records: list[Any], key_attr: str) -> list[str]:
    return [
        _clean_text(getattr(record, key_attr, ""), upper=True)
        for record in records
        if _clean_text(getattr(record, "error_message", ""), upper=False) == CACHE_MISS
        and _clean_text(getattr(record, key_attr, ""), upper=True)
    ]


def missing_candidate_symbols_from_openfigi(mappings: list[OpenFigiMapping]) -> set[str]:
    return {
        _clean_text(mapping.symbol, upper=True)
        for mapping in mappings
        if _clean_text(mapping.error_message, upper=False) == CACHE_MISS
    }


def missing_candidate_symbols_from_isins(records: list[IsinRecord]) -> set[str]:
    return {
        _clean_text(record.symbol, upper=True)
        for record in records
        if _clean_text(record.error_message, upper=False) == CACHE_MISS
    }


def cache_publishable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if _clean_text(row.get("error_message"), upper=False) != CACHE_MISS
    ]


def _legal_name_record_from_cache_row(*, query_name: str, normalized: str, row: dict[str, Any]) -> GleifLegalNameRecord:
    candidates = [
        candidate
        for candidate in (_list_or_empty(row.get("candidates_payload")) or _list_or_empty(row.get("candidates")))
        if isinstance(candidate, dict)
    ]
    status = _clean_text(row.get("status"), upper=False) or ("success" if candidates else "not_found")
    return GleifLegalNameRecord(
        query_name=_clean_text(row.get("query_name")) or query_name,
        normalized_query_name=_clean_text(row.get("normalized_query_name"), upper=True) or normalized,
        candidates=candidates,
        response_payload=_dict_or_none(row.get("response_payload")),
        status=status,
        error_message=_clean_text(row.get("error_message"), upper=False),
        source="raw_cache_gleif_entity",
    )


def _openfigi_mapping_from_cache_row(*, request: OpenFigiRequest, row: dict[str, Any]) -> OpenFigiMapping:
    payload = _dict_or_none(row.get("response_payload"))
    if payload:
        try:
            mapping = _mapping_from_openfigi_item(request, payload)
            return replace(
                mapping,
                status=_clean_text(row.get("status"), upper=False) or mapping.status,
                error_message=_clean_text(row.get("error_message"), upper=False) or mapping.error_message,
            )
        except Exception:
            pass
    return OpenFigiMapping(
        symbol=request.symbol,
        request_hash=request.request_hash,
        status=_clean_text(row.get("status"), upper=False) or "not_found",
        payload=_dict_or_empty(row.get("request_payload")) or request.payload,
        response_payload=payload,
        error_message=_clean_text(row.get("error_message"), upper=False),
    )


def _openfigi_cache_miss(*, candidate: ListingCandidate, request: OpenFigiRequest) -> OpenFigiMapping:
    return OpenFigiMapping(
        symbol=_clean_text(candidate.symbol, upper=True),
        request_hash=request.request_hash,
        status="not_found",
        payload=request.payload,
        response_payload=None,
        error_message=CACHE_MISS,
    )


def _query_all(conn: Any, table_name: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(f"select * from {_qualified_table_sql(table_name)}")
        return [dict(row) for row in cur.fetchall()]


def _query_in(conn: Any, table_name: str, column_name: str, values: list[str]) -> list[dict[str, Any]]:
    if not values:
        return []
    with conn.cursor() as cur:
        cur.execute(
            f"select * from {_qualified_table_sql(table_name)} where {_quote(column_name)} = any(%s)",
            (values,),
        )
        return [dict(row) for row in cur.fetchall()]


def _query_listing_isin(conn: Any, *, symbols: list[str], provider: str) -> list[dict[str, Any]]:
    if not symbols:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from source_cache.listing_isin_raw
            where symbol = any(%s) and provider = %s
            """,
            (symbols, provider),
        )
        return [dict(row) for row in cur.fetchall()]


def _qualified_table_sql(table_name: str) -> str:
    schema, table = table_name.split(".", maxsplit=1)
    return f"{_quote(schema)}.{_quote(table)}"


def _quote(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _gleif_entity_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if _clean_text(row.get("status"), upper=False) not in {"success", ""}:
            continue
        lei = _clean_text(row.get("lei"), upper=True)
        payload = _dict_or_none(row.get("response_payload")) or {}
        raw = _entity_candidate_mapping_from_payload(payload)
        raw["lei"] = raw.get("lei") or lei
        candidate = _legal_name_candidate_from_mapping(raw)
        if candidate.get("lei") and candidate.get("legal_name"):
            candidates.append(candidate)
    return candidates


def _entity_candidate_mapping_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    item = payload
    data = payload.get("data")
    if isinstance(data, list) and data and isinstance(data[0], dict):
        item = data[0]
    elif isinstance(data, dict):
        item = data
    attributes = item.get("attributes") if isinstance(item.get("attributes"), dict) else {}
    entity = attributes.get("entity") if isinstance(attributes.get("entity"), dict) else {}
    registration = attributes.get("registration") if isinstance(attributes.get("registration"), dict) else {}
    legal_name = entity.get("legalName")
    if isinstance(legal_name, dict):
        legal_name = legal_name.get("name")
    legal_address = entity.get("legalAddress") if isinstance(entity.get("legalAddress"), dict) else {}
    headquarters_address = entity.get("headquartersAddress") if isinstance(entity.get("headquartersAddress"), dict) else {}
    return {
        "lei": attributes.get("lei") or item.get("id") or payload.get("lei"),
        "legal_name": legal_name or attributes.get("legalName") or payload.get("legal_name"),
        "legal_country": legal_address.get("country") or payload.get("legal_country"),
        "headquarters_country": headquarters_address.get("country") or payload.get("headquarters_country"),
        "jurisdiction": entity.get("jurisdiction") or payload.get("jurisdiction"),
        "entity_status": entity.get("status") or payload.get("entity_status"),
        "registration_status": registration.get("status") or payload.get("registration_status"),
        "conformity_flag": attributes.get("conformityFlag") or payload.get("conformity_flag"),
    }


def _non_miss_count(records: list[Any]) -> int:
    return len(records) - _miss_count(records)


def _miss_count(records: list[Any]) -> int:
    return sum(1 for record in records if _clean_text(getattr(record, "error_message", ""), upper=False) == CACHE_MISS)


def _status_count(records: list[Any], status: str, *, exclude_cache_miss: bool = False) -> int:
    return sum(
        1
        for record in records
        if _clean_text(getattr(record, "status", ""), upper=False) == status
        and (
            not exclude_cache_miss
            or _clean_text(getattr(record, "error_message", ""), upper=False) != CACHE_MISS
        )
    )


def _miss_sample(records: list[Any], key_attr: str, limit: int = 10) -> list[str]:
    return [
        _clean_text(getattr(record, key_attr, ""), upper=True)
        for record in records
        if _clean_text(getattr(record, "error_message", ""), upper=False) == CACHE_MISS
    ][:limit]


def _clean_isin_list(value: Any) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(value)
    else:
        values = []
    return sorted({_clean_text(item, upper=True) for item in values if _clean_text(item, upper=True)})


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _dict_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _list_or_empty(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


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
