"""Manual shadow ingestion for SEC-derived and allowlisted IR investor events.

There is no canonical write, provider document fetch, product call, or schedule
in this module. Public IR pages are fail-closed by a versioned configuration.
"""

from __future__ import annotations

from collections import Counter, defaultdict
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

SEC_EDGAR_EVENT_CANDIDATES = "sec_edgar_event_candidates"
IR_PUBLIC_PAGE = "ir_public_page"
_RAW_STATUSES = {"success", "not_found", "rate_limited", "error"}
_CACHEABLE_RAW_STATUSES = {"success", "not_found", "rate_limited"}
_MAX_EXAMPLES = 25
_CONFIG_PATH = Path(__file__).resolve().parents[3] / "data" / "investor_event_sources.json"
_USEFUL_PROXY_FORMS = {"DEF 14A", "DEFA14A", "PRE 14A"}
_MATERIAL_FORMS = {"8-K", "6-K"}
_MATERIAL_SIGNALS = ("investor presentation", "earnings call", "conference call", "webcast", "presentation", "results")


class InvestorEventsRepository(Protocol):
    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None: ...
    def upsert_raw(self, row: dict[str, Any]) -> None: ...
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: ...
    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class InvestorEventFetchResult:
    provider_symbol: str
    symbol: str
    endpoint: str
    request_hash: str
    status: str
    http_status: int | None
    response_payload: str | None
    error_reason: str | None
    observed_at: datetime

    def to_raw_row(self) -> dict[str, Any]:
        payload_hash = _hash(self.response_payload) if self.response_payload is not None else None
        observed_at = _utc(self.observed_at)
        return {
            "raw_id": _hash({"kind": "investor_event_provider_raw.v1", "provider": IR_PUBLIC_PAGE, "endpoint": self.endpoint, "provider_symbol": self.provider_symbol, "request_hash": self.request_hash, "status": self.status, "payload_hash": payload_hash, "known_at": observed_at.date().isoformat()}),
            "provider": IR_PUBLIC_PAGE,
            "endpoint": self.endpoint,
            "provider_symbol": self.provider_symbol,
            "symbol": self.symbol,
            "request_params": {},
            "request_hash": self.request_hash,
            "status": self.status,
            "http_status": self.http_status,
            "provider_updated_at": None,
            "known_at": observed_at.date(),
            "ingested_at": observed_at,
            "raw_payload": self.response_payload,
            "raw_payload_hash": payload_hash,
            "error_reason": _safe_reason(self.error_reason),
        }


class IrPublicPageClient:
    def __init__(self, *, user_agent: str, session: requests.Session | None = None, timeout_seconds: float = 20.0) -> None:
        self._user_agent = str(user_agent).strip()
        if not self._user_agent:
            raise ValueError("DATA_OPS_IR_PUBLIC_PAGE_USER_AGENT is required for public IR page requests.")
        self._session = session or requests.Session()
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def fetch(self, *, symbol: str, source_url: str, observed_at: datetime) -> InvestorEventFetchResult:
        request_hash = _request_hash(provider=IR_PUBLIC_PAGE, endpoint=source_url, provider_symbol=symbol)
        try:
            response = self._session.get(source_url, headers={"User-Agent": self._user_agent, "Accept": "text/html,application/xhtml+xml"}, timeout=self._timeout_seconds, allow_redirects=False)
        except requests.RequestException as exc:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="error", http_status=None, reason=exc.__class__.__name__)
        if response.status_code == 429:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="rate_limited", http_status=429, reason="http_429", payload=response.text)
        if response.status_code == 404:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="not_found", http_status=404, reason="http_404", payload=response.text)
        if not 200 <= response.status_code < 300:
            return _fetch_failure(symbol=symbol, source_url=source_url, request_hash=request_hash, observed_at=observed_at, status="error", http_status=response.status_code, reason=f"http_{response.status_code}", payload=response.text)
        return InvestorEventFetchResult(provider_symbol=symbol, symbol=symbol, endpoint=source_url, request_hash=request_hash, status="success", http_status=response.status_code, response_payload=response.text, error_reason=None, observed_at=observed_at)


class PostgresInvestorEventsRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL is required for investor-events shadow ingestion.")
        self._publisher = PostgresPublisher(database_dsn=self._database_dsn, application_name="finance-data-ops-investor-events-shadow")

    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT raw_id, provider, endpoint, provider_symbol, symbol, request_params,
                           request_hash, status, http_status, provider_updated_at, known_at,
                           ingested_at, raw_payload, raw_payload_hash, error_reason
                    FROM source_cache.investor_event_provider_raw
                    WHERE provider = %s AND endpoint = %s AND provider_symbol = %s
                      AND request_hash = %s
                      AND (
                          status = ANY(%s)
                          OR (status = 'error' AND http_status >= 400 AND http_status < 500)
                      )
                    ORDER BY known_at DESC, ingested_at DESC, raw_id DESC LIMIT 1
                """, (provider, endpoint, provider_symbol, request_hash, sorted(_CACHEABLE_RAW_STATUSES)))
                row = cur.fetchone()
                if row is None:
                    return None
                return dict(zip([column.name for column in cur.description], row, strict=True))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        _validate_raw(row)
        try:
            from psycopg.types.json import Jsonb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg JSON adapters are required for investor-events shadow ingestion.") from exc
        columns = ("raw_id", "provider", "endpoint", "provider_symbol", "symbol", "request_params", "request_hash", "status", "http_status", "provider_updated_at", "known_at", "ingested_at", "raw_payload", "raw_payload_hash", "error_reason")
        values = [Jsonb(row[column]) if column in {"request_params", "raw_payload"} and row.get(column) is not None else row.get(column) for column in columns]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO source_cache.investor_event_provider_raw (
                        raw_id, provider, endpoint, provider_symbol, symbol, request_params,
                        request_hash, status, http_status, provider_updated_at, known_at,
                        ingested_at, raw_payload, raw_payload_hash, error_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_id) DO UPDATE SET ingested_at = EXCLUDED.ingested_at,
                        provider_updated_at = EXCLUDED.provider_updated_at, error_reason = EXCLUDED.error_reason,
                        updated_at = NOW()
                """, values)

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert("source_cache.investor_event_provider_observations", rows, on_conflict="provider_observation_id")

    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                if symbols:
                    cur.execute("""
                        SELECT provider_observation_id, symbol, company_name, accession_number, form_type,
                               filing_date, acceptance_datetime, primary_document, primary_doc_description,
                               filing_url, known_at, ingested_at, raw_payload_ref, raw_payload_hash,
                               source_metadata, data_quality_flags
                        FROM source_cache.filing_provider_observations
                        WHERE provider = 'sec_edgar' AND UPPER(symbol) = ANY(%s)
                    """, (symbols,))
                else:
                    cur.execute("""
                        SELECT provider_observation_id, symbol, company_name, accession_number, form_type,
                               filing_date, acceptance_datetime, primary_document, primary_doc_description,
                               filing_url, known_at, ingested_at, raw_payload_ref, raw_payload_hash,
                               source_metadata, data_quality_flags
                        FROM source_cache.filing_provider_observations WHERE provider = 'sec_edgar'
                    """)
                return [dict(zip([column.name for column in cur.description], row, strict=True)) for row in cur.fetchall()]


def run_investor_events_shadow(
    *, symbols: Iterable[str], repository: InvestorEventsRepository | None = None,
    ir_client: IrPublicPageClient | Any | None = None, env: Mapping[str, str] | None = None,
    source_config_path: Path | None = None, dry_run: bool = False, refresh: bool = False,
    request_sleep_seconds: float = 0.25, now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    requested = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    enabled = _provider_allowlist(env_map)
    report = _new_report(requested)
    if not enabled:
        return _skipped(report, "no_investor_event_providers_enabled")
    sec_enabled = SEC_EDGAR_EVENT_CANDIDATES in enabled
    ir_enabled = IR_PUBLIC_PAGE in enabled
    report["providers"][SEC_EDGAR_EVENT_CANDIDATES]["enabled"] = sec_enabled
    ir_user_agent = str(env_map.get("DATA_OPS_IR_PUBLIC_PAGE_USER_AGENT") or "").strip()
    if ir_enabled and not ir_user_agent:
        report["providers"][IR_PUBLIC_PAGE]["skip_reason"] = "ir_public_page_user_agent_missing"
        report["warnings"].append("ir_public_page_user_agent_missing")
        ir_enabled = False
    report["providers"][IR_PUBLIC_PAGE]["enabled"] = ir_enabled
    if not sec_enabled and not ir_enabled:
        return _skipped(report, "ir_public_page_user_agent_missing")
    if dry_run:
        report["status"] = "dry_run"
        report["provider_observations"]["planned"] = len(requested) * int(sec_enabled or ir_enabled)
        return report
    if repository is None:
        raise ValueError("repository is required when investor-events shadow ingestion is enabled.")
    now_fn = now or (lambda: datetime.now(UTC))
    observations: list[dict[str, Any]] = []
    if sec_enabled:
        sec_rows = derive_sec_edgar_event_candidates(repository.load_filing_observations(symbols=requested), observed_at=_utc(now_fn()))
        report["provider_observations"]["planned"] += len(sec_rows)
        sec_symbols = {str(row["symbol"]) for row in sec_rows}
        for symbol in requested:
            _symbol_provider_status(
                report,
                symbol,
                SEC_EDGAR_EVENT_CANDIDATES,
                "success" if symbol in sec_symbols else "not_found",
                None if symbol in sec_symbols else "no_event_candidates_from_observed_filings",
            )
        if sec_rows:
            repository.upsert_provider_observations(sec_rows)
            observations.extend(sec_rows)
            report["provider_observations"]["written"] += len(sec_rows)
        report["providers"][SEC_EDGAR_EVENT_CANDIDATES]["status"] = "completed"
    if ir_enabled:
        configs = load_investor_event_source_config(source_config_path or _CONFIG_PATH)
        client = ir_client or IrPublicPageClient(user_agent=ir_user_agent)
        for index, symbol in enumerate(requested):
            config = configs.get(symbol)
            if config is None:
                _symbol_provider_status(report, symbol, IR_PUBLIC_PAGE, "skipped", "no_ir_source_config")
                _warning(report, "no_ir_source_config")
                continue
            source_url = str(config["source_url"])
            if not _host_allowed(source_url, str(config["allowed_host"])):
                _symbol_provider_status(report, symbol, IR_PUBLIC_PAGE, "error", "host_not_allowlisted")
                _warning(report, "permission_unverified_public_page")
                continue
            _warning(report, "permission_unverified_public_page")
            request_hash = _request_hash(provider=IR_PUBLIC_PAGE, endpoint=source_url, provider_symbol=symbol)
            raw = None if refresh else repository.find_cached_raw(provider=IR_PUBLIC_PAGE, endpoint=source_url, provider_symbol=symbol, request_hash=request_hash)
            cached = raw is not None
            if raw is None:
                raw = client.fetch(symbol=symbol, source_url=source_url, observed_at=_utc(now_fn())).to_raw_row()
                repository.upsert_raw(raw)
                report["live_calls"] += 1
            else:
                report["raw_cache_hits"] += 1
            status = str(raw.get("status") or "error")
            _symbol_provider_status(report, symbol, IR_PUBLIC_PAGE, status, _safe_reason(raw.get("error_reason")), cache_hit=cached)
            if status == "success":
                normalized = normalize_ir_public_page_observations(raw_row=raw, config=config)
                report["provider_observations"]["planned"] += len(normalized)
                if normalized:
                    repository.upsert_provider_observations(normalized)
                    observations.extend(normalized)
                    report["provider_observations"]["written"] += len(normalized)
                else:
                    _warning(report, "parser_low_confidence")
            if not cached and index < len(requested) - 1 and request_sleep_seconds > 0:
                time.sleep(float(request_sleep_seconds))
    report["status"] = "completed"
    report.update(_summaries(observations, requested))
    report.update(_symbol_outcomes(report))
    return report


def derive_sec_edgar_event_candidates(rows: Iterable[Mapping[str, Any]], *, observed_at: datetime) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for row in rows:
        form = _text(row.get("form_type"))
        if form in _USEFUL_PROXY_FORMS:
            records.append(_observation(provider=SEC_EDGAR_EVENT_CANDIDATES, symbol=_symbol(row.get("symbol")), title="Shareholder / annual meeting candidate", event_type="shareholder_meeting_candidate", description=f"Derived from {form} filing metadata.", related=row, observed_at=observed_at, confidence="medium", parser_version="sec_filing_metadata.v1"))
        elif form in _MATERIAL_FORMS:
            metadata_text = " ".join(filter(None, [_text(row.get("primary_doc_description")), _text(row.get("primary_document")), json.dumps(row.get("source_metadata") or {}, default=str)]))
            signal = next((item for item in _MATERIAL_SIGNALS if item in metadata_text.lower()), None)
            if signal:
                records.append(_observation(provider=SEC_EDGAR_EVENT_CANDIDATES, symbol=_symbol(row.get("symbol")), title=f"{signal.title()} material candidate", event_type="investor_material_candidate", description=f"Heuristic metadata signal '{signal}' from {form}.", related=row, observed_at=observed_at, confidence="low", parser_version="sec_filing_metadata.v1"))
    return _dedupe(records)


def normalize_ir_public_page_observations(*, raw_row: Mapping[str, Any], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    if str(raw_row.get("status") or "") != "success":
        return []
    html = raw_row.get("raw_payload")
    if not isinstance(html, str):
        return []
    observed_at = _datetime(raw_row.get("ingested_at"))
    known_at = _date(raw_row.get("known_at"))
    if observed_at is None or known_at is None:
        return []
    symbol = _symbol(config.get("symbol"))
    parser = str(config.get("parser") or "generic_ir_events_html")
    events = _q4_events(html) if parser == "q4_public_html" else []
    if not events:
        events = _generic_events(html)
    records = []
    for event in events:
        title = _text(event.get("title"))
        if not title:
            continue
        record = _observation(provider=IR_PUBLIC_PAGE, symbol=symbol, title=title, event_type=_text(event.get("event_type_raw")), description=_text(event.get("description_raw")), related=None, observed_at=observed_at, confidence=str(event.get("extraction_confidence") or "low"), parser_version=f"{parser}.v1", provider_event_id=_text(event.get("provider_event_id")), provider_revision=_text(event.get("provider_revision")), starts_at=_datetime(event.get("starts_at_raw")), ends_at=_datetime(event.get("ends_at_raw")), timezone_raw=_text(event.get("timezone_raw")), webcast_url=_same_host_url(event.get("webcast_url_raw"), config), presentation_urls=_same_host_urls(event.get("presentation_urls_raw"), config), attachment_urls=_same_host_urls(event.get("attachment_urls_raw"), config), source_url=str(config.get("source_url")), raw_payload_ref=_text(raw_row.get("raw_id")), payload_hash=_text(raw_row.get("raw_payload_hash")), known_at=known_at)
        config_version = _text(config.get("_config_version"))
        if config_version:
            record["source_metadata"]["sourceConfigVersion"] = config_version
        records.append(record)
    return _dedupe(records)


def load_investor_event_source_config(path: Path) -> dict[str, dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    sources = payload.get("sources", []) if isinstance(payload, Mapping) else payload
    if not isinstance(sources, list):
        return {}
    config_version = _text(payload.get("version")) if isinstance(payload, Mapping) else None
    return {
        _symbol(item.get("symbol")): {**dict(item), "_config_version": config_version}
        for item in sources
        if isinstance(item, Mapping) and item.get("enabled") is True and item.get("provider") == IR_PUBLIC_PAGE
        and _symbol(item.get("symbol")) and _text(item.get("source_url")) and _text(item.get("allowed_host"))
        and item.get("parser") in {"q4_public_html", "generic_ir_events_html"}
    }


def _observation(*, provider: str, symbol: str, title: str, event_type: str | None, description: str | None, related: Mapping[str, Any] | None, observed_at: datetime, confidence: str, parser_version: str, provider_event_id: str | None = None, provider_revision: str | None = None, starts_at: datetime | None = None, ends_at: datetime | None = None, timezone_raw: str | None = None, webcast_url: str | None = None, presentation_urls: list[str] | None = None, attachment_urls: list[str] | None = None, source_url: str | None = None, raw_payload_ref: str | None = None, payload_hash: str | None = None, known_at: date | None = None) -> dict[str, Any]:
    known = known_at or observed_at.date()
    related_accession = _text(related.get("accession_number")) if related else None
    related_form = _text(related.get("form_type")) if related else None
    related_url = _text(related.get("filing_url")) if related else None
    body = {"provider": provider, "symbol": symbol, "provider_event_id": provider_event_id, "title": title, "event_type": event_type, "starts_at": starts_at.isoformat() if starts_at else None, "related_accession": related_accession, "source_url": source_url}
    observation_hash = _hash(body)
    flags: dict[str, Any] = {"permission_unverified_public_page": provider == IR_PUBLIC_PAGE} if provider == IR_PUBLIC_PAGE else {"sec_filing_metadata_heuristic": True}
    if confidence == "low":
        flags["parser_low_confidence"] = True
    return {
        "provider_observation_id": _hash({"kind": "investor_event_provider_observation.v1", "observation_hash": observation_hash, "known_at": known.isoformat()}),
        "provider": provider, "provider_event_id": provider_event_id, "provider_revision": provider_revision,
        "symbol": symbol, "provider_symbol": symbol, "company_name": _text(related.get("company_name")) if related else None,
        "event_type_raw": event_type, "title_raw": title, "description_raw": description,
        "starts_at_raw": starts_at, "ends_at_raw": ends_at, "timezone_raw": timezone_raw, "status_raw": None,
        "webcast_url_raw": webcast_url, "presentation_urls_raw": presentation_urls or None,
        "attachment_urls_raw": attachment_urls or None, "related_press_release_urls_raw": None,
        "related_filing_accession": related_accession, "related_filing_form": related_form, "related_filing_url": related_url,
        "source_published_at": _datetime(related.get("acceptance_datetime")) if related else None,
        "source_updated_at": _datetime(related.get("acceptance_datetime")) if related else None,
        "observed_at": observed_at, "known_at": known, "fetched_at": observed_at, "source_url": source_url or related_url,
        "raw_payload_ref": raw_payload_ref, "payload_hash": payload_hash, "observation_hash": observation_hash,
        "parser_version": parser_version, "extraction_confidence": confidence,
        "data_quality_flags": flags, "source_metadata": {"relatedFilingProviderObservationId": _text(related.get("provider_observation_id")) if related else None},
    }


class _Markup(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.headings: list[str] = []
        self.anchors: list[str] = []
        self.scripts: list[str] = []
        self._tag: str | None = None
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._tag = tag.lower()
        if self._tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.anchors.append(href)
        self._chunks = []

    def handle_data(self, data: str) -> None:
        self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        text_value = " ".join(self._chunks).strip()
        if tag.lower() in {"h1", "h2", "h3", "h4"} and text_value:
            self.headings.append(text_value)
        if tag.lower() == "script" and text_value:
            self.scripts.append(text_value)
        self._chunks = []


def _q4_events(html: str) -> list[dict[str, Any]]:
    parser = _Markup(); parser.feed(html)
    events: list[dict[str, Any]] = []
    for script in parser.scripts:
        for item in _json_objects(script):
            if _text(item.get("EventId")) or _text(item.get("StartDate")):
                attachments = item.get("Attachments") or item.get("AttachmentLinks") or []
                attachment_urls = [_text(value.get("Url") if isinstance(value, Mapping) else value) for value in attachments] if isinstance(attachments, list) else []
                events.append({"provider_event_id": _text(item.get("EventId")), "provider_revision": _text(item.get("RevisionNumber")), "title": _text(item.get("Title")) or _text(item.get("EventTitle")), "description_raw": _text(item.get("Description")), "event_type_raw": _text(item.get("EventType")), "starts_at_raw": item.get("StartDate"), "ends_at_raw": item.get("EndDate"), "timezone_raw": _text(item.get("TimeZone")), "webcast_url_raw": _text(item.get("WebCastLink")), "presentation_urls_raw": [_text(item.get("PresentationLink"))] if _text(item.get("PresentationLink")) else [], "attachment_urls_raw": [url for url in attachment_urls if url], "extraction_confidence": "high"})
    return events


def _generic_events(html: str) -> list[dict[str, Any]]:
    parser = _Markup(); parser.feed(html)
    if not parser.headings:
        return []
    date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", html)
    return [{"title": parser.headings[0], "starts_at_raw": date_match.group(0) if date_match else None, "webcast_url_raw": None, "presentation_urls_raw": [], "attachment_urls_raw": [], "extraction_confidence": "low"}]


def _json_objects(script: str) -> list[Mapping[str, Any]]:
    decoder = json.JSONDecoder()
    out: list[Mapping[str, Any]] = []
    for match in re.finditer(r"[\[{]", script):
        try:
            parsed, _ = decoder.raw_decode(script[match.start():])
        except ValueError:
            continue
        out.extend(_walk_json(parsed))
    return out


def _walk_json(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value, *[item for child in value.values() for item in _walk_json(child)]]
    if isinstance(value, list):
        return [item for child in value for item in _walk_json(child)]
    return []


def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return list({str(row["provider_observation_id"]): row for row in rows}.values())


def _summaries(rows: list[dict[str, Any]], requested: list[str]) -> dict[str, Any]:
    by_provider = Counter(str(row["provider"]) for row in rows)
    by_symbol = Counter(str(row["symbol"]) for row in rows)
    by_type = Counter(str(row.get("event_type_raw") or "unknown") for row in rows)
    confidence = Counter(str(row["extraction_confidence"]) for row in rows)
    return {"by_provider": dict(sorted(by_provider.items())), "by_symbol": {symbol: by_symbol[symbol] for symbol in requested}, "by_event_type_raw": dict(sorted(by_type.items())), "extraction_confidence_counts": dict(sorted(confidence.items())), "examples": {"sec_derived": [_example(row) for row in rows if row["provider"] == SEC_EDGAR_EVENT_CANDIDATES][: _MAX_EXAMPLES], "ir_derived": [_example(row) for row in rows if row["provider"] == IR_PUBLIC_PAGE][: _MAX_EXAMPLES]}}


def _symbol_outcomes(report: Mapping[str, Any]) -> dict[str, list[str]]:
    skipped: list[str] = []
    unresolved: list[str] = []
    for symbol, provider_statuses in report["symbol_statuses"].items():
        statuses = {str(item.get("status")) for item in provider_statuses.values()}
        if "skipped" in statuses:
            skipped.append(str(symbol))
        if statuses and statuses <= {"not_found", "error"}:
            unresolved.append(str(symbol))
    return {"skipped_symbols": sorted(skipped), "unresolved_symbols": sorted(unresolved)}


def _new_report(symbols: list[str]) -> dict[str, Any]:
    return {"status": "skipped", "mode": "shadow", "symbols_requested": symbols, "providers": {SEC_EDGAR_EVENT_CANDIDATES: {"enabled": False, "skip_reason": None}, IR_PUBLIC_PAGE: {"enabled": False, "skip_reason": None}}, "raw_cache_hits": 0, "live_calls": 0, "provider_observations": {"planned": 0, "written": 0}, "symbol_statuses": {}, "skipped_symbols": [], "unresolved_symbols": [], "warnings": []}


def _skipped(report: dict[str, Any], reason: str) -> dict[str, Any]: report.update(status="skipped", reason=reason); return report
def _warning(report: dict[str, Any], warning: str) -> None:
    if warning not in report["warnings"]: report["warnings"].append(warning)
def _symbol_provider_status(report: dict[str, Any], symbol: str, provider: str, status: str, reason: str | None, cache_hit: bool = False) -> None: report["symbol_statuses"].setdefault(symbol, {})[provider] = {"status": status, "reason": reason, "cache_hit": cache_hit}
def _example(row: Mapping[str, Any]) -> dict[str, Any]: return {key: row.get(key) for key in ("symbol", "title_raw", "event_type_raw", "related_filing_accession", "source_url", "extraction_confidence")}
def _provider_allowlist(env: Mapping[str, str]) -> set[str]: return {item.strip().lower() for item in str(env.get("DATA_OPS_INVESTOR_EVENT_PROVIDERS") or "").split(",") if item.strip()}
def _host_allowed(url: str, allowed_host: str) -> bool: return (urlparse(url).hostname or "").lower() == allowed_host.lower()
def _same_host_url(value: Any, config: Mapping[str, Any]) -> str | None:
    url = _text(value)
    return url if url and _host_allowed(url, str(config.get("allowed_host") or "")) else None
def _same_host_urls(values: Any, config: Mapping[str, Any]) -> list[str]: return [url for value in values or [] if (url := _same_host_url(value, config))]
def _fetch_failure(*, symbol: str, source_url: str, request_hash: str, observed_at: datetime, status: str, http_status: int | None, reason: str, payload: str | None = None) -> InvestorEventFetchResult: return InvestorEventFetchResult(provider_symbol=symbol, symbol=symbol, endpoint=source_url, request_hash=request_hash, status=status, http_status=http_status, response_payload=payload, error_reason=reason, observed_at=observed_at)
def _request_hash(*, provider: str, endpoint: str, provider_symbol: str) -> str: return _hash({"provider": provider, "endpoint": endpoint, "provider_symbol": provider_symbol})
def _validate_raw(row: Mapping[str, Any]) -> None:
    if str(row.get("provider") or "") != IR_PUBLIC_PAGE or str(row.get("status") or "") not in _RAW_STATUSES: raise ValueError("invalid investor-event raw row")
def _safe_reason(value: Any) -> str | None:
    reason = _text(value)
    return reason if reason and (reason.startswith("http_") or reason in {"invalid_json_response", "ConnectionError", "Timeout"}) else ("provider_error" if reason else None)
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
    return psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-investor-events-shadow")
