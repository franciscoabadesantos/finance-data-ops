"""Manual SEC EDGAR filings shadow ingestion.

Only provider raw cache and normalized provider observations are written. This
module deliberately does not create canonical filings, product, or scheduling
side effects.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
import time
from typing import Any, Protocol

import requests

from finance_data_ops.publish.client import PostgresPublisher

SEC_EDGAR_PROVIDER = "sec_edgar"
SEC_TICKER_MAPPING_ENDPOINT = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_ENDPOINT = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data"
_TICKER_MAPPING_SYMBOL = "__ticker_cik_mapping__"
_CACHEABLE_STATUSES = {"success", "not_found", "rate_limited"}
_RAW_STATUSES = _CACHEABLE_STATUSES | {"error"}
_MAX_EXAMPLES = 25


class SecFilingsShadowRepository(Protocol):
    def find_cached_raw(
        self,
        *,
        provider: str,
        endpoint: str,
        provider_symbol: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        ...

    def upsert_raw(self, row: dict[str, Any]) -> None:
        ...

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        ...


@dataclass(frozen=True, slots=True)
class SecEdgarFetchResult:
    provider_symbol: str
    symbol: str | None
    cik: str | None
    endpoint: str
    request_params: dict[str, str]
    request_hash: str
    status: str
    http_status: int | None
    response_payload: Any | None
    error_reason: str | None
    observed_at: datetime

    def to_raw_row(self) -> dict[str, Any]:
        payload = _json_safe(self.response_payload)
        payload_hash = _hash(payload) if payload is not None else None
        observed_at = _utc(self.observed_at)
        return {
            "raw_id": _hash(
                {
                    "kind": "filing_provider_raw.v1",
                    "provider": SEC_EDGAR_PROVIDER,
                    "endpoint": self.endpoint,
                    "provider_symbol": self.provider_symbol,
                    "request_hash": self.request_hash,
                    "status": self.status,
                    "raw_payload_hash": payload_hash,
                    "known_at": observed_at.date().isoformat(),
                }
            ),
            "provider": SEC_EDGAR_PROVIDER,
            "endpoint": self.endpoint,
            "provider_symbol": self.provider_symbol,
            "symbol": self.symbol,
            "cik": self.cik,
            "request_params": _safe_request_params(self.request_params),
            "request_hash": self.request_hash,
            "status": self.status,
            "http_status": self.http_status,
            "provider_updated_at": None,
            "known_at": observed_at.date(),
            "ingested_at": observed_at,
            "raw_payload": payload,
            "raw_payload_hash": payload_hash,
            "error_reason": _safe_error_reason(self.error_reason),
        }


class SecEdgarClient:
    """Minimal SEC data API client with an explicitly configured User-Agent."""

    def __init__(
        self,
        *,
        user_agent: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._user_agent = str(user_agent).strip()
        if not self._user_agent:
            raise ValueError("SEC_EDGAR_USER_AGENT is required for SEC EDGAR requests.")
        self._session = session or requests.Session()
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def fetch_ticker_mapping(self, *, observed_at: datetime) -> SecEdgarFetchResult:
        return self._fetch(
            provider_symbol=_TICKER_MAPPING_SYMBOL,
            symbol=None,
            cik=None,
            endpoint=SEC_TICKER_MAPPING_ENDPOINT,
            request_params={},
            observed_at=observed_at,
        )

    def fetch_submissions(
        self, *, symbol: str, cik: str, observed_at: datetime
    ) -> SecEdgarFetchResult:
        normalized_symbol = _symbol(symbol)
        normalized_cik = _cik(cik)
        endpoint = SEC_SUBMISSIONS_ENDPOINT.format(cik=normalized_cik)
        return self._fetch(
            provider_symbol=normalized_symbol,
            symbol=normalized_symbol,
            cik=normalized_cik,
            endpoint=endpoint,
            request_params={"cik": normalized_cik},
            observed_at=observed_at,
        )

    def _fetch(
        self,
        *,
        provider_symbol: str,
        symbol: str | None,
        cik: str | None,
        endpoint: str,
        request_params: dict[str, str],
        observed_at: datetime,
    ) -> SecEdgarFetchResult:
        request_hash = _request_hash(
            endpoint=endpoint,
            provider_symbol=provider_symbol,
            request_params=request_params,
        )
        try:
            response = self._session.get(
                endpoint,
                params=request_params or None,
                headers={"User-Agent": self._user_agent, "Accept-Encoding": "gzip, deflate"},
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            return _failure(
                provider_symbol=provider_symbol,
                symbol=symbol,
                cik=cik,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=None,
                error_reason=exc.__class__.__name__,
            )
        if response.status_code == 429:
            return _failure(
                provider_symbol=provider_symbol,
                symbol=symbol,
                cik=cik,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="rate_limited",
                http_status=429,
                error_reason="http_429",
                response_payload=_response_json_or_none(response),
            )
        if response.status_code == 404:
            return _failure(
                provider_symbol=provider_symbol,
                symbol=symbol,
                cik=cik,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="not_found",
                http_status=404,
                error_reason="http_404",
                response_payload=_response_json_or_none(response),
            )
        if not 200 <= response.status_code < 300:
            return _failure(
                provider_symbol=provider_symbol,
                symbol=symbol,
                cik=cik,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=response.status_code,
                error_reason=f"http_{response.status_code}",
                response_payload=_response_json_or_none(response),
            )
        try:
            payload = response.json()
        except ValueError:
            return _failure(
                provider_symbol=provider_symbol,
                symbol=symbol,
                cik=cik,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=response.status_code,
                error_reason="invalid_json_response",
            )
        return SecEdgarFetchResult(
            provider_symbol=provider_symbol,
            symbol=symbol,
            cik=cik,
            endpoint=endpoint,
            request_params=request_params,
            request_hash=request_hash,
            status="success",
            http_status=response.status_code,
            response_payload=payload,
            error_reason=None,
            observed_at=observed_at,
        )


class PostgresSecFilingsShadowRepository:
    """Postgres persistence for SEC filings raw cache and observations."""

    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL is required for filings shadow ingestion.")
        self._publisher = PostgresPublisher(
            database_dsn=self._database_dsn,
            application_name="finance-data-ops-sec-filings-shadow",
        )

    def find_cached_raw(
        self,
        *,
        provider: str,
        endpoint: str,
        provider_symbol: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_id, provider, endpoint, provider_symbol, symbol, cik,
                           request_params, request_hash, status, http_status,
                           provider_updated_at, known_at, ingested_at, raw_payload,
                           raw_payload_hash, error_reason
                    FROM source_cache.filing_provider_raw
                    WHERE provider = %s
                      AND endpoint = %s
                      AND provider_symbol = %s
                      AND request_hash = %s
                      AND status = ANY(%s)
                    ORDER BY known_at DESC, ingested_at DESC, raw_id DESC
                    LIMIT 1
                    """,
                    (provider, endpoint, provider_symbol, request_hash, sorted(_CACHEABLE_STATUSES)),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                columns = [description.name for description in cur.description]
                return dict(zip(columns, row, strict=True))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        _validate_raw_row(row)
        try:
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover - deployment dependency
            raise RuntimeError("psycopg JSON adapters are required for filings shadow ingestion.") from exc
        columns = (
            "raw_id", "provider", "endpoint", "provider_symbol", "symbol", "cik",
            "request_params", "request_hash", "status", "http_status", "provider_updated_at",
            "known_at", "ingested_at", "raw_payload", "raw_payload_hash", "error_reason",
        )
        values = [
            Jsonb(row[column]) if column in {"request_params", "raw_payload"} and row.get(column) is not None else row.get(column)
            for column in columns
        ]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO source_cache.filing_provider_raw (
                        raw_id, provider, endpoint, provider_symbol, symbol, cik,
                        request_params, request_hash, status, http_status, provider_updated_at,
                        known_at, ingested_at, raw_payload, raw_payload_hash, error_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_id) DO UPDATE SET
                        ingested_at = EXCLUDED.ingested_at,
                        provider_updated_at = EXCLUDED.provider_updated_at,
                        error_reason = EXCLUDED.error_reason,
                        updated_at = NOW()
                    """,
                    values,
                )

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert(
            "source_cache.filing_provider_observations",
            rows,
            on_conflict="provider_observation_id",
        )


def run_sec_filings_shadow(
    *,
    symbols: Iterable[str],
    repository: SecFilingsShadowRepository | None = None,
    client: SecEdgarClient | Any | None = None,
    env: Mapping[str, str] | None = None,
    dry_run: bool = False,
    refresh: bool = False,
    request_sleep_seconds: float = 0.2,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run SEC-only manual filings shadow ingestion without canonical writes."""
    requested_symbols = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    report = _new_report(requested_symbols)
    if SEC_EDGAR_PROVIDER not in _provider_allowlist(env_map):
        return _skipped(report, "no_filing_providers_enabled")
    user_agent = str(env_map.get("SEC_EDGAR_USER_AGENT") or "").strip()
    if not user_agent:
        report["providers"][SEC_EDGAR_PROVIDER]["skip_reason"] = "sec_user_agent_missing"
        return _skipped(report, "sec_user_agent_missing")
    report["providers"][SEC_EDGAR_PROVIDER]["enabled"] = True
    report["providers"][SEC_EDGAR_PROVIDER]["user_agent_configured"] = True
    if dry_run:
        report["status"] = "dry_run"
        report["provider_observations"]["planned"] = len(requested_symbols)
        return report
    if repository is None:
        raise ValueError("repository is required when SEC filings shadow ingestion is enabled.")
    sec_client = client or SecEdgarClient(user_agent=user_agent)
    now_fn = now or (lambda: datetime.now(UTC))
    mapping_raw, mapping_cached = _load_raw(
        repository=repository,
        client=sec_client,
        endpoint=SEC_TICKER_MAPPING_ENDPOINT,
        provider_symbol=_TICKER_MAPPING_SYMBOL,
        symbol=None,
        cik=None,
        refresh=refresh,
        observed_at=_utc(now_fn()),
        fetch=lambda observed_at: sec_client.fetch_ticker_mapping(observed_at=observed_at),
    )
    _record_raw(report, raw_row=mapping_raw, cached=mapping_cached)
    if str(mapping_raw.get("status")) != "success":
        report["status"] = "completed"
        for symbol in requested_symbols:
            report["symbol_statuses"][symbol] = _symbol_status(
                status=str(mapping_raw.get("status") or "error"),
                reason="ticker_mapping_unavailable",
                raw_row=mapping_raw,
                cache_hit=mapping_cached,
            )
            _append_example(report, "errors", {"symbol": symbol, "reason": "ticker_mapping_unavailable"})
        return report
    mappings = _ticker_mappings(mapping_raw.get("raw_payload"))
    report["provider_observations"]["planned"] = sum(symbol in mappings for symbol in requested_symbols)
    observations: list[dict[str, Any]] = []
    report["status"] = "completed"
    if not mapping_cached and requested_symbols and request_sleep_seconds > 0:
        time.sleep(float(request_sleep_seconds))
    for index, symbol in enumerate(requested_symbols):
        mapping = mappings.get(symbol)
        if mapping is None:
            report["symbol_statuses"][symbol] = {
                "status": "unresolved",
                "reason": "ticker_cik_unresolved",
                "resolved_cik": None,
                "cache_hit": mapping_cached,
            }
            _append_example(report, "unresolved", {"symbol": symbol, "reason": "ticker_cik_unresolved"})
            continue
        cik = mapping["cik"]
        endpoint = SEC_SUBMISSIONS_ENDPOINT.format(cik=cik)
        raw_row, cached = _load_raw(
            repository=repository,
            client=sec_client,
            endpoint=endpoint,
            provider_symbol=symbol,
            symbol=symbol,
            cik=cik,
            refresh=refresh,
            observed_at=_utc(now_fn()),
            fetch=lambda observed_at, symbol=symbol, cik=cik: sec_client.fetch_submissions(
                symbol=symbol, cik=cik, observed_at=observed_at
            ),
        )
        _record_raw(report, raw_row=raw_row, cached=cached)
        status = str(raw_row.get("status") or "error")
        report["symbol_statuses"][symbol] = _symbol_status(
            status=status,
            reason=_safe_error_reason(raw_row.get("error_reason")),
            raw_row=raw_row,
            cache_hit=cached,
            cik=cik,
        )
        if status == "success":
            normalized = normalize_sec_filing_provider_observations(raw_row=raw_row)
            if normalized:
                repository.upsert_provider_observations(normalized)
                observations.extend(normalized)
                report["provider_observations"]["written"] += len(normalized)
        else:
            _append_example(report, "errors", {"symbol": symbol, "reason": _safe_error_reason(raw_row.get("error_reason"))})
        if not cached and index < len(requested_symbols) - 1 and request_sleep_seconds > 0:
            time.sleep(float(request_sleep_seconds))
    report["coverage"] = _coverage(observations)
    return report


def normalize_sec_filing_provider_observations(*, raw_row: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Normalize current SEC submission rows without fetching historical files."""
    if str(raw_row.get("status") or "") != "success":
        return []
    symbol = _symbol(raw_row.get("symbol") or raw_row.get("provider_symbol"))
    cik = _cik(raw_row.get("cik"))
    known_at = _date(raw_row.get("known_at"))
    ingested_at = _datetime(raw_row.get("ingested_at"))
    raw_id = _text(raw_row.get("raw_id"))
    raw_payload_hash = _text(raw_row.get("raw_payload_hash"))
    payload = raw_row.get("raw_payload")
    if not symbol or not cik or known_at is None or ingested_at is None or not raw_id or not isinstance(payload, Mapping):
        return []
    company_name = _text(payload.get("name"))
    historical_files = _historical_file_references(payload)
    records: list[dict[str, Any]] = []
    for entry in _recent_filing_rows(payload):
        accession_number = _text(entry.get("accessionNumber"))
        form_type = _text(entry.get("form"))
        filing_date = _date(entry.get("filingDate"))
        if not accession_number or not form_type or filing_date is None:
            continue
        report_date = _date(entry.get("reportDate"))
        acceptance_datetime = _datetime(entry.get("acceptanceDateTime"))
        primary_document = _text(entry.get("primaryDocument"))
        primary_doc_description = _text(entry.get("primaryDocDescription"))
        filing_url = _filing_url(cik=cik, accession_number=accession_number, primary_document=primary_document)
        observation_body = {
            "provider": SEC_EDGAR_PROVIDER,
            "symbol": symbol,
            "cik": cik,
            "accession_number": accession_number,
            "form_type": form_type,
            "filing_date": filing_date.isoformat(),
            "report_date": report_date.isoformat() if report_date else None,
            "acceptance_datetime": acceptance_datetime.isoformat() if acceptance_datetime else None,
            "primary_document": primary_document,
            "primary_doc_description": primary_doc_description,
            "filing_url": filing_url,
        }
        observation_hash = _hash(observation_body)
        flags: dict[str, Any] = {"sec_edgar_shadow": True}
        if historical_files:
            flags["historical_submission_files_not_fetched"] = True
        records.append(
            {
                "provider_observation_id": _hash(
                    {
                        "kind": "filing_provider_observation.v1",
                        "observation_hash": observation_hash,
                        "known_at": known_at.isoformat(),
                    }
                ),
                "provider": SEC_EDGAR_PROVIDER,
                "provider_event_id": accession_number,
                "provider_symbol": symbol,
                "symbol": symbol,
                "cik": cik,
                "company_name": company_name,
                "accession_number": accession_number,
                "form_type": form_type,
                "filing_date": filing_date,
                "report_date": report_date,
                "acceptance_datetime": acceptance_datetime,
                "primary_document": primary_document,
                "primary_doc_description": primary_doc_description,
                "filing_url": filing_url,
                "provider_updated_at": acceptance_datetime,
                "known_at": known_at,
                "ingested_at": ingested_at,
                "raw_payload_ref": raw_id,
                "raw_payload_hash": raw_payload_hash,
                "observation_hash": observation_hash,
                "source_metadata": {
                    "rawPayloadRef": raw_id,
                    "historicalSubmissionFiles": historical_files,
                },
                "data_quality_flags": flags,
            }
        )
    return records


def _load_raw(
    *,
    repository: SecFilingsShadowRepository,
    client: Any,
    endpoint: str,
    provider_symbol: str,
    symbol: str | None,
    cik: str | None,
    refresh: bool,
    observed_at: datetime,
    fetch: Callable[[datetime], SecEdgarFetchResult],
) -> tuple[dict[str, Any], bool]:
    request_hash = _request_hash(endpoint=endpoint, provider_symbol=provider_symbol, request_params={"cik": cik} if cik else {})
    cached = None if refresh else repository.find_cached_raw(
        provider=SEC_EDGAR_PROVIDER,
        endpoint=endpoint,
        provider_symbol=provider_symbol,
        request_hash=request_hash,
    )
    if cached is not None:
        return cached, True
    result = fetch(observed_at)
    raw_row = result.to_raw_row()
    repository.upsert_raw(raw_row)
    return raw_row, False


def _new_report(symbols: list[str]) -> dict[str, Any]:
    return {
        "status": "skipped",
        "mode": "shadow",
        "symbols_requested": symbols,
        "providers": {
            SEC_EDGAR_PROVIDER: {"enabled": False, "skip_reason": None, "user_agent_configured": False}
        },
        "raw_cache_hits": 0,
        "live_calls": 0,
        "status_counts": {},
        "symbol_statuses": {},
        "provider_observations": {"planned": 0, "written": 0},
        "coverage": {"by_form_type": {}, "filing_date_range_by_symbol": {}},
        "examples": {"unresolved": [], "errors": []},
    }


def _skipped(report: dict[str, Any], reason: str) -> dict[str, Any]:
    report["status"] = "skipped"
    report["reason"] = reason
    return report


def _record_raw(report: dict[str, Any], *, raw_row: Mapping[str, Any], cached: bool) -> None:
    if cached:
        report["raw_cache_hits"] += 1
    else:
        report["live_calls"] += 1
    status = str(raw_row.get("status") or "error")
    report["status_counts"][status] = int(report["status_counts"].get(status, 0)) + 1


def _append_example(report: dict[str, Any], category: str, value: dict[str, Any]) -> None:
    examples = report["examples"][category]
    if len(examples) < _MAX_EXAMPLES:
        examples.append(value)


def _symbol_status(
    *,
    status: str,
    reason: str | None,
    raw_row: Mapping[str, Any],
    cache_hit: bool,
    cik: str | None = None,
) -> dict[str, Any]:
    report_status = "resolved" if status == "success" else status
    return {
        "status": report_status,
        "reason": reason,
        "resolved_cik": cik,
        "http_status": raw_row.get("http_status"),
        "cache_hit": cache_hit,
    }


def _coverage(observations: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(observations)
    counts = Counter(str(row["form_type"]) for row in rows)
    by_symbol: dict[str, list[date]] = defaultdict(list)
    for row in rows:
        filing_date = row.get("filing_date")
        if isinstance(filing_date, date):
            by_symbol[str(row["symbol"])].append(filing_date)
    return {
        "by_form_type": dict(sorted(counts.items())),
        "filing_date_range_by_symbol": {
            symbol: {"min": min(dates).isoformat(), "max": max(dates).isoformat()}
            for symbol, dates in sorted(by_symbol.items())
        },
    }


def _ticker_mappings(payload: Any) -> dict[str, dict[str, str]]:
    rows = payload.values() if isinstance(payload, Mapping) else payload
    if not isinstance(rows, Iterable) or isinstance(rows, (str, bytes)):
        return {}
    mappings: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        ticker = _symbol(row.get("ticker"))
        cik = _cik(row.get("cik_str") if row.get("cik_str") is not None else row.get("cik"))
        if ticker and cik:
            mappings[ticker] = {"cik": cik, "company_name": _text(row.get("title")) or ""}
    return mappings


def _recent_filing_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    filings = payload.get("filings")
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if isinstance(recent, list):
        return [dict(row) for row in recent if isinstance(row, Mapping)]
    if not isinstance(recent, Mapping):
        return []
    columns = {key: value for key, value in recent.items() if isinstance(value, list)}
    count = max((len(values) for values in columns.values()), default=0)
    return [{key: values[index] if index < len(values) else None for key, values in columns.items()} for index in range(count)]


def _historical_file_references(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    filings = payload.get("filings")
    files = filings.get("files") if isinstance(filings, Mapping) else None
    if not isinstance(files, list):
        return []
    return [
        {key: _json_safe(value) for key, value in item.items() if key in {"name", "filingCount", "filingFrom", "filingTo"}}
        for item in files
        if isinstance(item, Mapping)
    ]


def _filing_url(*, cik: str, accession_number: str, primary_document: str | None) -> str | None:
    if not primary_document:
        return None
    accession = accession_number.replace("-", "")
    if not accession or not accession.isalnum():
        return None
    return f"{SEC_ARCHIVES_BASE_URL}/{int(cik)}/{accession}/{primary_document}"


def _failure(
    *,
    provider_symbol: str,
    symbol: str | None,
    cik: str | None,
    endpoint: str,
    request_params: dict[str, str],
    request_hash: str,
    observed_at: datetime,
    status: str,
    http_status: int | None,
    error_reason: str,
    response_payload: Any | None = None,
) -> SecEdgarFetchResult:
    return SecEdgarFetchResult(
        provider_symbol=provider_symbol,
        symbol=symbol,
        cik=cik,
        endpoint=endpoint,
        request_params=request_params,
        request_hash=request_hash,
        status=status,
        http_status=http_status,
        response_payload=response_payload,
        error_reason=error_reason,
        observed_at=observed_at,
    )


def _response_json_or_none(response: requests.Response) -> Any | None:
    try:
        return response.json()
    except ValueError:
        return None


def _provider_allowlist(env: Mapping[str, str]) -> set[str]:
    return {
        provider.strip().lower()
        for provider in str(env.get("DATA_OPS_FILING_PROVIDERS") or "").split(",")
        if provider.strip()
    }


def _request_hash(*, endpoint: str, provider_symbol: str, request_params: Mapping[str, str]) -> str:
    return _hash(
        {
            "provider": SEC_EDGAR_PROVIDER,
            "endpoint": endpoint,
            "provider_symbol": provider_symbol,
            "request_params": _safe_request_params(request_params),
        }
    )


def _validate_raw_row(row: Mapping[str, Any]) -> None:
    if str(row.get("provider") or "") != SEC_EDGAR_PROVIDER:
        raise ValueError("filings raw row provider must be sec_edgar")
    if str(row.get("status") or "") not in _RAW_STATUSES:
        raise ValueError("filings raw row has an invalid status")
    if not _text(row.get("raw_id")) or not _text(row.get("request_hash")):
        raise ValueError("filings raw row requires raw_id and request_hash")


def _safe_request_params(params: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in params.items() if str(key).lower() not in {"authorization", "api_key", "token", "user-agent"}}


def _safe_error_reason(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    if text.startswith("http_") and text[5:].isdigit():
        return text
    return text if text in {"invalid_json_response", "RequestException", "ConnectionError", "Timeout"} else "provider_error"


def _hash(value: Any) -> str:
    encoded = json.dumps(_json_safe(value), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [_json_safe(item) for item in value]
    return value


def _symbols(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(symbol for value in values if (symbol := _symbol(value))))


def _symbol(value: Any) -> str:
    return _text(value).upper()


def _cik(value: Any) -> str:
    text = _text(value)
    if not text or not text.isdigit():
        return ""
    return text.zfill(10)


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return _utc(value).date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    text = _text(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _to_psycopg_dsn(database_dsn: str) -> str:
    dsn = str(database_dsn or "").strip()
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn.removeprefix("postgresql+psycopg://")
    return dsn


def _connect(database_dsn: str):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("psycopg is required for filings shadow ingestion.") from exc
    return psycopg.connect(
        database_dsn,
        autocommit=True,
        application_name="finance-data-ops-sec-filings-shadow",
    )
