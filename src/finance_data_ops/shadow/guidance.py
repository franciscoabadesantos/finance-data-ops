"""Manual raw-first shadow ingestion for company-guidance evidence.

This module never writes a canonical table, calls paid providers, follows
documents, or turns analyst estimates into company guidance.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

from finance_data_ops.publish.client import PostgresPublisher

SEC_EDGAR_GUIDANCE_CANDIDATE = "sec_edgar_guidance_candidate"
IR_PUBLIC_PRESS_RELEASE = "ir_public_press_release"
_SUPPORTED_PROVIDERS = {SEC_EDGAR_GUIDANCE_CANDIDATE, IR_PUBLIC_PRESS_RELEASE}
_RAW_STATUSES = {"success", "not_found", "rate_limited", "error"}
_CACHEABLE_RAW_STATUSES = {"success", "not_found", "rate_limited"}
_CONFIG_PATH = Path(__file__).resolve().parents[3] / "data" / "guidance_sources.json"
_MAX_EXAMPLES = 25
_MAX_SNIPPET_CHARS = 280
_SEC_FORMS = {"8-K", "6-K"}
_SEC_SIGNALS = (
    "item 2.02",
    "item 7.01",
    "item 8.01",
    "ex-99.",
    "press release",
    "earnings release",
    "guidance",
    "outlook",
    "results",
)
_IR_GUIDANCE_PATTERN = re.compile(
    r"\b(guidance|reaffirm(?:s|ed|ing)? guidance|rais(?:e|es|ed|ing) guidance|lower(?:s|ed|ing) guidance|narrow(?:s|ed|ing) guidance)\b",
    re.IGNORECASE,
)


class GuidanceRepository(Protocol):
    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None: ...
    def upsert_raw(self, row: dict[str, Any]) -> None: ...
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: ...
    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class GuidanceFetchResult:
    provider_symbol: str
    symbol: str
    endpoint: str
    request_hash: str
    status: str
    http_status: int | None
    content_type: str | None
    response_payload: str | None
    error_reason: str | None
    observed_at: datetime

    def to_raw_row(self) -> dict[str, Any]:
        observed_at = _utc(self.observed_at)
        payload_hash = _hash(self.response_payload) if self.response_payload is not None else None
        return {
            "raw_id": _hash({"kind": "guidance_provider_raw.v1", "provider": IR_PUBLIC_PRESS_RELEASE, "endpoint": self.endpoint, "provider_symbol": self.provider_symbol, "request_hash": self.request_hash, "status": self.status, "payload_hash": payload_hash, "known_at": observed_at.date().isoformat()}),
            "provider": IR_PUBLIC_PRESS_RELEASE,
            "endpoint": self.endpoint,
            "provider_symbol": self.provider_symbol,
            "symbol": self.symbol,
            "source_document_url": self.endpoint,
            "request_params": {},
            "request_hash": self.request_hash,
            "status": self.status,
            "http_status": self.http_status,
            "content_type": self.content_type,
            "source_published_at": None,
            "source_updated_at": None,
            "known_at": observed_at.date(),
            "fetched_at": observed_at,
            "raw_payload": self.response_payload,
            "raw_payload_hash": payload_hash,
            "error_reason": _safe_reason(self.error_reason),
        }


class IrPublicPressReleaseClient:
    def __init__(self, *, user_agent: str, session: requests.Session | None = None, timeout_seconds: float = 20.0) -> None:
        self._user_agent = str(user_agent).strip()
        if not self._user_agent:
            raise ValueError("DATA_OPS_GUIDANCE_IR_PUBLIC_PAGE_USER_AGENT is required for IR public-page requests.")
        self._session = session or requests.Session()
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def fetch(self, *, symbol: str, source_url: str, observed_at: datetime) -> GuidanceFetchResult:
        request_hash = _request_hash(provider=IR_PUBLIC_PRESS_RELEASE, endpoint=source_url, provider_symbol=symbol)
        try:
            response = self._session.get(source_url, headers={"User-Agent": self._user_agent, "Accept": "text/html,application/xhtml+xml"}, timeout=self._timeout_seconds, allow_redirects=False)
        except requests.RequestException as exc:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="error", http_status=None, reason=exc.__class__.__name__)
        content_type = response.headers.get("Content-Type")
        if response.status_code == 429:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="rate_limited", http_status=429, reason="http_429", content_type=content_type, payload=response.text)
        if response.status_code == 404:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="not_found", http_status=404, reason="http_404", content_type=content_type, payload=response.text)
        if not 200 <= response.status_code < 300:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="error", http_status=response.status_code, reason=f"http_{response.status_code}", content_type=content_type, payload=response.text)
        return GuidanceFetchResult(provider_symbol=symbol, symbol=symbol, endpoint=source_url, request_hash=request_hash, status="success", http_status=response.status_code, content_type=content_type, response_payload=response.text, error_reason=None, observed_at=observed_at)


class PostgresGuidanceRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL or DATABASE_URL is required for guidance shadow ingestion.")
        self._publisher = PostgresPublisher(database_dsn=self._database_dsn, application_name="finance-data-ops-guidance-shadow")

    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_id, provider, endpoint, provider_symbol, symbol,
                           source_document_url, request_params, request_hash, status,
                           http_status, content_type, source_published_at,
                           source_updated_at, known_at, fetched_at, raw_payload,
                           raw_payload_hash, error_reason
                    FROM source_cache.guidance_provider_raw
                    WHERE provider = %s AND endpoint = %s AND provider_symbol = %s
                      AND request_hash = %s AND status = ANY(%s)
                    ORDER BY known_at DESC, fetched_at DESC, raw_id DESC
                    LIMIT 1
                    """,
                    (provider, endpoint, provider_symbol, request_hash, sorted(_CACHEABLE_RAW_STATUSES)),
                )
                row = cur.fetchone()
                return None if row is None else dict(zip([column.name for column in cur.description], row, strict=True))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        _validate_raw(row)
        from psycopg.types.json import Jsonb

        columns = (
            "raw_id", "provider", "endpoint", "provider_symbol", "symbol", "source_document_url",
            "request_params", "request_hash", "status", "http_status", "content_type",
            "source_published_at", "source_updated_at", "known_at", "fetched_at", "raw_payload",
            "raw_payload_hash", "error_reason",
        )
        values = [Jsonb(row[column]) if column in {"request_params", "raw_payload"} and row.get(column) is not None else row.get(column) for column in columns]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO source_cache.guidance_provider_raw (
                        raw_id, provider, endpoint, provider_symbol, symbol, source_document_url,
                        request_params, request_hash, status, http_status, content_type,
                        source_published_at, source_updated_at, known_at, fetched_at, raw_payload,
                        raw_payload_hash, error_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_id) DO UPDATE SET fetched_at = EXCLUDED.fetched_at,
                        source_updated_at = EXCLUDED.source_updated_at,
                        error_reason = EXCLUDED.error_reason, updated_at = NOW()
                    """,
                    values,
                )

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert("source_cache.guidance_provider_observations", rows, on_conflict="provider_observation_id")

    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                params: tuple[Any, ...] = ()
                where = "WHERE provider = 'sec_edgar'"
                if symbols:
                    where += " AND UPPER(symbol) = ANY(%s)"
                    params = (symbols,)
                cur.execute(
                    f"""
                    SELECT provider_observation_id, symbol, company_name, accession_number,
                           form_type, filing_date, acceptance_datetime, primary_document,
                           primary_doc_description, filing_url, known_at, ingested_at,
                           raw_payload_ref, raw_payload_hash, source_metadata, data_quality_flags
                    FROM source_cache.filing_provider_observations
                    {where}
                    """,
                    params,
                )
                return [dict(zip([column.name for column in cur.description], row, strict=True)) for row in cur.fetchall()]


def run_guidance_shadow(
    *, symbols: Iterable[str], repository: GuidanceRepository | None = None,
    ir_client: IrPublicPressReleaseClient | Any | None = None, env: Mapping[str, str] | None = None,
    source_config_path: Path | None = None, dry_run: bool = False, refresh: bool = False,
    request_sleep_seconds: float = 0.25, now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    requested = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    enabled = _provider_allowlist(env_map)
    report = _new_report(requested)
    if not enabled:
        return _skipped(report, "no_guidance_providers_enabled")
    sec_enabled = SEC_EDGAR_GUIDANCE_CANDIDATE in enabled
    ir_requested = IR_PUBLIC_PRESS_RELEASE in enabled
    ir_enabled = ir_requested
    report["providers"][SEC_EDGAR_GUIDANCE_CANDIDATE]["enabled"] = sec_enabled
    user_agent = str(env_map.get("DATA_OPS_GUIDANCE_IR_PUBLIC_PAGE_USER_AGENT") or "").strip()
    if ir_enabled and not user_agent:
        report["providers"][IR_PUBLIC_PRESS_RELEASE]["skip_reason"] = "ir_public_page_user_agent_missing"
        _warning(report, "ir_public_page_user_agent_missing")
        ir_enabled = False
    report["providers"][IR_PUBLIC_PRESS_RELEASE]["enabled"] = ir_enabled
    if not sec_enabled and not ir_enabled:
        return _skipped(report, "ir_public_page_user_agent_missing" if ir_requested else "no_guidance_providers_enabled")
    if dry_run:
        report["status"] = "dry_run"
        report["provider_observations"]["planned"] = len(requested) * int(sec_enabled or ir_enabled)
        return report
    if repository is None:
        raise ValueError("repository is required when guidance shadow ingestion is enabled.")
    now_fn = now or (lambda: datetime.now(UTC))
    observations: list[dict[str, Any]] = []
    if sec_enabled:
        rows = derive_sec_edgar_guidance_candidates(repository.load_filing_observations(symbols=requested), observed_at=_utc(now_fn()))
        report["provider_observations"]["planned"] += len(rows)
        candidate_symbols = {row["symbol"] for row in rows}
        for symbol in requested:
            _symbol_status(report, symbol, SEC_EDGAR_GUIDANCE_CANDIDATE, "success" if symbol in candidate_symbols else "not_found", None if symbol in candidate_symbols else "no_guidance_candidates_from_observed_filings")
        if rows:
            repository.upsert_provider_observations(rows)
            observations.extend(rows)
            report["provider_observations"]["written"] += len(rows)
        report["providers"][SEC_EDGAR_GUIDANCE_CANDIDATE]["status"] = "completed"
    if ir_enabled:
        configs = load_guidance_source_config(source_config_path or _CONFIG_PATH)
        client = ir_client or IrPublicPressReleaseClient(user_agent=user_agent)
        for index, symbol in enumerate(requested):
            config = configs.get(symbol)
            if config is None:
                _symbol_status(report, symbol, IR_PUBLIC_PRESS_RELEASE, "skipped", "no_ir_source_config")
                _warning(report, "no_ir_source_config")
                continue
            source_url = str(config["source_url"])
            if not _host_allowed(source_url, str(config["allowed_host"])):
                _symbol_status(report, symbol, IR_PUBLIC_PRESS_RELEASE, "error", "host_not_allowlisted")
                _warning(report, "permission_unverified_public_page")
                continue
            _warning(report, "permission_unverified_public_page")
            request_hash = _request_hash(provider=IR_PUBLIC_PRESS_RELEASE, endpoint=source_url, provider_symbol=symbol)
            raw = None if refresh else repository.find_cached_raw(provider=IR_PUBLIC_PRESS_RELEASE, endpoint=source_url, provider_symbol=symbol, request_hash=request_hash)
            cached = raw is not None
            if raw is None:
                raw = client.fetch(symbol=symbol, source_url=source_url, observed_at=_utc(now_fn())).to_raw_row()
                repository.upsert_raw(raw)
                report["live_calls"] += 1
            else:
                report["raw_cache_hits"] += 1
            status = str(raw.get("status") or "error")
            _symbol_status(report, symbol, IR_PUBLIC_PRESS_RELEASE, status, _safe_reason(raw.get("error_reason")), cache_hit=cached)
            if status == "success":
                normalized = normalize_ir_public_press_release(raw_row=raw, config=config)
                report["provider_observations"]["planned"] += len(normalized)
                if normalized:
                    repository.upsert_provider_observations(normalized)
                    observations.extend(normalized)
                    report["provider_observations"]["written"] += len(normalized)
                else:
                    _warning(report, "no_deterministic_guidance_candidate")
            if not cached and index < len(requested) - 1 and request_sleep_seconds > 0:
                time.sleep(float(request_sleep_seconds))
    report["status"] = "completed"
    report.update(_summaries(observations, requested))
    report.update(_symbol_outcomes(report))
    return report


def derive_sec_edgar_guidance_candidates(rows: Iterable[Mapping[str, Any]], *, observed_at: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        form = _text(row.get("form_type"))
        if form not in _SEC_FORMS:
            continue
        evidence = " ".join(filter(None, [_text(row.get("form_type")), _text(row.get("primary_document")), _text(row.get("primary_doc_description")), json.dumps(row.get("source_metadata") or {}, default=str)]))
        signal = next((item for item in _SEC_SIGNALS if item in evidence.lower()), None)
        if signal is None:
            continue
        records.append(_observation(provider=SEC_EDGAR_GUIDANCE_CANDIDATE, symbol=_symbol(row.get("symbol")), observed_at=observed_at, source_document_url=_text(row.get("filing_url")), source_document_type="sec_filing_candidate", source_title=_snippet(evidence), related=row, guidance_quote=_snippet(evidence), confidence="medium" if signal.startswith("item") or signal.startswith("ex-99") else "low", parser_version="sec_filing_metadata.v1", source_metadata={"matchedSignal": signal, "sourceFilingProviderObservationId": _text(row.get("provider_observation_id")), "relatedFilingRawPayloadRef": _text(row.get("raw_payload_ref"))}))
    return _dedupe(records)


def normalize_ir_public_press_release(*, raw_row: Mapping[str, Any], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    if str(raw_row.get("status") or "") != "success" or not isinstance(raw_row.get("raw_payload"), str):
        return []
    html = str(raw_row["raw_payload"])
    text_value, title = _html_text_and_title(html)
    snippet = _guidance_snippet(text_value)
    observed_at = _datetime(raw_row.get("fetched_at"))
    known_at = _date(raw_row.get("known_at"))
    if snippet is None or observed_at is None or known_at is None:
        return []
    metadata = {"sourceConfigVersion": _text(config.get("_config_version")), "permissionUnverifiedPublicPage": True}
    return _dedupe([_observation(provider=IR_PUBLIC_PRESS_RELEASE, symbol=_symbol(config.get("symbol")), observed_at=observed_at, known_at=known_at, source_document_url=str(config.get("source_url")), source_document_type="ir_public_press_release", source_title=title, related=None, guidance_quote=snippet, confidence="low", parser_version=f"{config.get('parser')}.v1", raw_payload_ref=_text(raw_row.get("raw_id")), payload_hash=_text(raw_row.get("raw_payload_hash")), source_metadata=metadata)])


def load_guidance_source_config(path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    sources = payload.get("sources", []) if isinstance(payload, Mapping) else payload
    if not isinstance(sources, list):
        return {}
    version = _text(payload.get("version")) if isinstance(payload, Mapping) else None
    return {
        _symbol(item.get("symbol")): {**dict(item), "_config_version": version}
        for item in sources
        if isinstance(item, Mapping)
        and item.get("enabled") is True
        and item.get("provider") == IR_PUBLIC_PRESS_RELEASE
        and _symbol(item.get("symbol"))
        and _is_allowed_ir_source(item)
        and item.get("parser") in {"press_release_html_v1", "generic_press_release_html"}
    }


def _observation(*, provider: str, symbol: str, observed_at: datetime, source_document_url: str | None, source_document_type: str | None, source_title: str | None, related: Mapping[str, Any] | None, guidance_quote: str | None, confidence: str, parser_version: str, known_at: date | None = None, raw_payload_ref: str | None = None, payload_hash: str | None = None, source_metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    known = known_at or observed_at.date()
    related_accession = _text(related.get("accession_number")) if related else None
    related_form = _text(related.get("form_type")) if related else None
    related_url = _text(related.get("filing_url")) if related else None
    identity = {"provider": provider, "symbol": symbol, "relatedAccession": related_accession, "sourceDocumentUrl": source_document_url, "quote": guidance_quote}
    observation_hash = _hash(identity)
    flags: dict[str, Any] = {"guidanceCandidate": True}
    if provider == SEC_EDGAR_GUIDANCE_CANDIDATE:
        flags["secFilingMetadataHeuristic"] = True
    if provider == IR_PUBLIC_PRESS_RELEASE:
        flags["permissionUnverifiedPublicPage"] = True
    if confidence == "low":
        flags["parserLowConfidence"] = True
    return {
        "provider_observation_id": _hash({"kind": "guidance_provider_observation.v1", "observation_hash": observation_hash, "known_at": known.isoformat()}),
        "provider": provider, "provider_guidance_id": None, "provider_revision": None,
        "symbol": symbol, "provider_symbol": symbol, "company_name": _text(related.get("company_name")) if related else None,
        "guidance_type": None, "metric_label_raw": None, "period_label_raw": None,
        "fiscal_year": None, "fiscal_quarter": None, "period_start": None, "period_end": None,
        "value_type": None, "value_point": None, "value_low": None, "value_high": None,
        "unit_raw": None, "currency": None, "basis_raw": None, "qualitative_direction": None,
        "guidance_quote": _snippet(guidance_quote), "quote_locator": None,
        "source_document_type": source_document_type, "source_title": _snippet(source_title),
        "source_document_url": source_document_url, "source_published_at": _datetime(related.get("acceptance_datetime")) if related else None,
        "source_updated_at": _datetime(related.get("acceptance_datetime")) if related else None,
        "related_filing_accession": related_accession, "related_filing_form": related_form, "related_filing_url": related_url,
        "observed_at": observed_at, "known_at": known, "fetched_at": observed_at,
        "raw_payload_ref": raw_payload_ref, "payload_hash": payload_hash or (_text(related.get("raw_payload_hash")) if related else None),
        "observation_hash": observation_hash, "parser_version": parser_version,
        "extraction_confidence": confidence, "data_quality_flags": flags,
        "source_metadata": source_metadata or {},
    }


def _summaries(rows: list[dict[str, Any]], requested: list[str]) -> dict[str, Any]:
    by_provider = Counter(str(row["provider"]) for row in rows)
    by_symbol = Counter(str(row["symbol"]) for row in rows)
    by_document_type = Counter(str(row.get("source_document_type") or "unknown") for row in rows)
    confidence = Counter(str(row["extraction_confidence"]) for row in rows)
    return {"by_provider": dict(sorted(by_provider.items())), "by_symbol": {symbol: by_symbol[symbol] for symbol in requested}, "by_source_document_type": dict(sorted(by_document_type.items())), "extraction_confidence_counts": dict(sorted(confidence.items())), "examples": [_example(row) for row in rows[:_MAX_EXAMPLES]]}


def _new_report(symbols: list[str]) -> dict[str, Any]:
    return {"status": "skipped", "mode": "shadow", "symbols_requested": symbols, "providers": {SEC_EDGAR_GUIDANCE_CANDIDATE: {"enabled": False, "skip_reason": None}, IR_PUBLIC_PRESS_RELEASE: {"enabled": False, "skip_reason": None}}, "live_calls": 0, "raw_cache_hits": 0, "provider_observations": {"planned": 0, "written": 0}, "symbol_statuses": {}, "unresolved_symbols": [], "skipped_symbols": [], "warnings": []}


def _provider_allowlist(env: Mapping[str, str]) -> set[str]:
    return {item.strip().lower() for item in str(env.get("DATA_OPS_GUIDANCE_PROVIDERS") or "").split(",") if item.strip()} & _SUPPORTED_PROVIDERS


def _symbol_outcomes(report: Mapping[str, Any]) -> dict[str, list[str]]:
    skipped, unresolved = [], []
    for symbol, statuses in report["symbol_statuses"].items():
        values = {str(item.get("status")) for item in statuses.values()}
        if "skipped" in values:
            skipped.append(str(symbol))
        if values and values <= {"not_found", "error"}:
            unresolved.append(str(symbol))
    return {"skipped_symbols": sorted(skipped), "unresolved_symbols": sorted(unresolved)}


def _html_text_and_title(html: str) -> tuple[str, str | None]:
    parser = _TextParser(); parser.feed(html)
    return " ".join(parser.parts), parser.title


class _TextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(); self.parts: list[str] = []; self.title: str | None = None; self._in_title = False
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._in_title = tag.lower() == "title"
    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title": self._in_title = False
    def handle_data(self, data: str) -> None:
        clean = " ".join(data.split())
        if clean:
            self.parts.append(clean)
            if self._in_title and self.title is None: self.title = clean


def _guidance_snippet(text_value: str) -> str | None:
    match = _IR_GUIDANCE_PATTERN.search(text_value)
    if match is None:
        return None
    start, end = max(0, match.start() - 110), min(len(text_value), match.end() + 150)
    return _snippet(text_value[start:end])


def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]: return list({str(row["provider_observation_id"]): row for row in rows}.values())
def _example(row: Mapping[str, Any]) -> dict[str, Any]: return {key: row.get(key) for key in ("symbol", "source_document_url", "related_filing_accession", "guidance_type", "extraction_confidence", "guidance_quote")}
def _skipped(report: dict[str, Any], reason: str) -> dict[str, Any]: report.update(status="skipped", reason=reason); return report
def _warning(report: dict[str, Any], warning: str) -> None:
    if warning not in report["warnings"]: report["warnings"].append(warning)
def _symbol_status(report: dict[str, Any], symbol: str, provider: str, status: str, reason: str | None, cache_hit: bool = False) -> None: report["symbol_statuses"].setdefault(symbol, {})[provider] = {"status": status, "reason": reason, "cache_hit": cache_hit}
def _request_hash(*, provider: str, endpoint: str, provider_symbol: str) -> str: return _hash({"provider": provider, "endpoint": endpoint, "provider_symbol": provider_symbol})
def _host_allowed(url: str, allowed_host: str) -> bool: return (urlparse(url).hostname or "").lower() == allowed_host.lower()
def _is_allowed_ir_source(item: Mapping[str, Any]) -> bool:
    source_url, allowed_host = _text(item.get("source_url")), _text(item.get("allowed_host"))
    if not source_url or not allowed_host:
        return False
    parsed = urlparse(source_url)
    return parsed.scheme == "https" and not parsed.path.lower().endswith(".pdf")
def _validate_raw(row: Mapping[str, Any]) -> None:
    if str(row.get("provider") or "") != IR_PUBLIC_PRESS_RELEASE or str(row.get("status") or "") not in _RAW_STATUSES: raise ValueError("invalid guidance raw row")
def _safe_reason(value: Any) -> str | None:
    reason = _text(value)
    return reason if reason and (reason.startswith("http_") or reason in {"ConnectionError", "Timeout"}) else ("provider_error" if reason else None)
def _fetch_failure(*, symbol: str, source_url: str, request_hash: str, observed_at: datetime, status: str, http_status: int | None, reason: str, content_type: str | None = None, payload: str | None = None) -> GuidanceFetchResult: return GuidanceFetchResult(provider_symbol=symbol, symbol=symbol, endpoint=source_url, request_hash=request_hash, status=status, http_status=http_status, content_type=content_type, response_payload=payload, error_reason=reason, observed_at=observed_at)
def _snippet(value: Any) -> str | None:
    text_value = _text(value)
    return text_value[:_MAX_SNIPPET_CHARS] if text_value else None
def _hash(value: Any) -> str: return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
def _symbols(values: Iterable[str]) -> list[str]: return list(dict.fromkeys(symbol for value in values if (symbol := _symbol(value))))
def _symbol(value: Any) -> str: return (_text(value) or "").upper()
def _text(value: Any) -> str | None: return (str(value).strip() if value is not None else "") or None
def _date(value: Any) -> date | None:
    if isinstance(value, datetime): return _utc(value).date()
    if isinstance(value, date): return value
    try: return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError: return None
def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime): return _utc(value)
    try: return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00"))) if value else None
    except ValueError: return None
def _utc(value: datetime) -> datetime: return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
def _to_psycopg_dsn(dsn: str) -> str: return "postgresql://" + str(dsn).strip().removeprefix("postgresql+psycopg://") if str(dsn).strip().startswith("postgresql+psycopg://") else str(dsn).strip()
def _connect(database_dsn: str):
    import psycopg
    return psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-guidance-shadow")
