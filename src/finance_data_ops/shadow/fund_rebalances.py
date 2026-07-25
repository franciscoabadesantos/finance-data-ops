"""Fail-closed public index-announcement rebalance shadow ingestion.

Holdings snapshots are intentionally never parsed or differenced here. A
provider observation requires explicit announcement and effective dates plus
explicit add/remove constituents in the configured document payload.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

from finance_data_ops.publish.client import PostgresPublisher

PROVIDERS = {"nasdaq_index_public", "sp_dow_jones_index_public", "msci_index_public", "ftse_russell_index_public"}
_CACHEABLE_STATUSES = {"success", "not_found", "rate_limited"}
_CONFIG_PATH = Path(__file__).resolve().parents[3] / "data" / "fund_rebalance_sources.json"
_MAX_EXAMPLES = 25

class FundRebalanceRepository(Protocol):
    def find_cached_raw(self, *, provider: str, provider_url: str, source_cache_key: str) -> dict[str, Any] | None: ...
    def upsert_raw(self, row: dict[str, Any]) -> None: ...
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: ...

@dataclass(frozen=True, slots=True)
class RebalanceFetchResult:
    provider: str; provider_url: str; source_cache_key: str; status: str; http_status: int | None; payload: Any | None; error_reason: str | None; fetched_at: datetime; response_headers: dict[str, str]
    def to_raw_row(self, *, config: Mapping[str, Any]) -> dict[str, Any]:
        payload_hash = _hash(self.payload) if self.payload is not None else None
        return {"raw_id": _hash({"kind": "fund_rebalance_provider_raw.v1", "provider": self.provider, "url": self.provider_url, "cacheKey": self.source_cache_key, "status": self.status, "payloadHash": payload_hash}), "provider": self.provider, "provider_url": self.provider_url, "provider_endpoint_or_dataset": _text(config.get("provider_endpoint_or_dataset")), "fetched_at": _utc(self.fetched_at), "source_published_at": None, "payload_format": "json", "raw_payload": self.payload, "raw_payload_sha256": payload_hash, "http_status": self.http_status, "response_headers_json": self.response_headers, "entitlement_scope": "public_web_unknown", "ingest_version": "fund_rebalance_announcement.v1", "query_context_json": {"indexIdentifier": _text(config.get("index_identifier")), "parser": _text(config.get("parser"))}, "source_cache_key": self.source_cache_key, "status": self.status, "error_reason": _safe_reason(self.error_reason)}

class PublicIndexAnnouncementClient:
    def __init__(self, *, user_agent: str, session: requests.Session | None = None, timeout_seconds: float = 20.0) -> None:
        self._user_agent = str(user_agent).strip()
        if not self._user_agent: raise ValueError("DATA_OPS_FUND_REBALANCE_USER_AGENT is required for public index-provider requests.")
        self._session = session or requests.Session(); self._timeout_seconds = max(1.0, float(timeout_seconds))
    def fetch(self, *, provider: str, source_url: str, fetched_at: datetime) -> RebalanceFetchResult:
        cache_key = _cache_key(provider, source_url)
        try: response = self._session.get(source_url, headers={"User-Agent": self._user_agent, "Accept": "application/json,text/html"}, timeout=self._timeout_seconds, allow_redirects=False)
        except requests.RequestException as exc: return _failure(provider, source_url, cache_key, fetched_at, "error", None, exc.__class__.__name__)
        headers = _safe_headers(response.headers)
        if response.status_code == 429: return _failure(provider, source_url, cache_key, fetched_at, "rate_limited", 429, "http_429", _json_or_text(response), headers)
        if response.status_code == 404: return _failure(provider, source_url, cache_key, fetched_at, "not_found", 404, "http_404", _json_or_text(response), headers)
        if not 200 <= response.status_code < 300: return _failure(provider, source_url, cache_key, fetched_at, "error", response.status_code, f"http_{response.status_code}", _json_or_text(response), headers)
        return RebalanceFetchResult(provider, source_url, cache_key, "success", response.status_code, _json_or_text(response), None, _utc(fetched_at), headers)

class PostgresFundRebalanceRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn: raise ValueError("DATA_OPS_DATABASE_URL or DATABASE_URL is required for fund-rebalance shadow ingestion.")
        self._publisher = PostgresPublisher(database_dsn=self._database_dsn, application_name="finance-data-ops-fund-rebalances-shadow")
    def find_cached_raw(self, *, provider: str, provider_url: str, source_cache_key: str) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT raw_id, provider, provider_url, fetched_at, source_published_at, payload_format, raw_payload, raw_payload_sha256, http_status, response_headers_json, entitlement_scope, ingest_version, query_context_json, source_cache_key, status, error_reason FROM source_cache.fund_rebalance_provider_raw WHERE provider = %s AND provider_url = %s AND source_cache_key = %s AND status = ANY(%s) ORDER BY fetched_at DESC, raw_id DESC LIMIT 1", (provider, provider_url, source_cache_key, sorted(_CACHEABLE_STATUSES)))
                row = cur.fetchone(); return None if row is None else dict(zip([column.name for column in cur.description], row, strict=True))
    def upsert_raw(self, row: dict[str, Any]) -> None:
        from psycopg.types.json import Jsonb
        columns = ("raw_id", "provider", "provider_url", "provider_endpoint_or_dataset", "fetched_at", "source_published_at", "payload_format", "raw_payload", "raw_payload_sha256", "http_status", "response_headers_json", "entitlement_scope", "ingest_version", "query_context_json", "source_cache_key", "status", "error_reason")
        values = [Jsonb(row[column]) if column in {"raw_payload", "response_headers_json", "query_context_json"} and row.get(column) is not None else row.get(column) for column in columns]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur: cur.execute(f"INSERT INTO source_cache.fund_rebalance_provider_raw ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) ON CONFLICT (raw_id) DO UPDATE SET fetched_at = EXCLUDED.fetched_at, http_status = EXCLUDED.http_status, error_reason = EXCLUDED.error_reason, updated_at = NOW()", values)
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: self._publisher.upsert("source_cache.fund_rebalance_provider_observations", rows, on_conflict="provider_observation_id")

def run_fund_rebalances_shadow(*, repository: FundRebalanceRepository | None = None, client: Any | None = None, env: Mapping[str, str] | None = None, source_config_path: Path | None = None, dry_run: bool = False, refresh: bool = False, request_sleep_seconds: float = 0.25, now: Callable[[], datetime] | None = None) -> dict[str, Any]:
    env_map = dict(os.environ) if env is None else dict(env); enabled = _allowlist(env_map); configs = load_fund_rebalance_source_config(source_config_path or _CONFIG_PATH, enabled)
    report = _report()
    if not enabled or not configs:
        return _skipped(report, "no_fund_rebalance_providers_enabled" if not enabled else "no_fund_rebalance_sources_enabled")
    user_agent = str(env_map.get("DATA_OPS_FUND_REBALANCE_USER_AGENT") or "").strip()
    if not user_agent: return _skipped(report, "fund_rebalance_user_agent_missing")
    for provider in enabled: report["providers"].setdefault(provider, {"enabled": provider in {str(c["provider"]) for c in configs}, "skip_reason": None})
    if dry_run: report["status"] = "dry_run"; return report
    if repository is None: raise ValueError("repository is required when fund-rebalance shadow ingestion is enabled.")
    client = client or PublicIndexAnnouncementClient(user_agent=user_agent); now_fn = now or (lambda: datetime.now(UTC)); observations: list[dict[str, Any]] = []
    for index, config in enumerate(configs):
        provider, source_url = str(config["provider"]), str(config["source_url"]); cache_key = _cache_key(provider, source_url)
        raw = None if refresh else repository.find_cached_raw(provider=provider, provider_url=source_url, source_cache_key=cache_key); hit = raw is not None
        if raw is None: raw = client.fetch(provider=provider, source_url=source_url, fetched_at=_utc(now_fn())).to_raw_row(config=config); repository.upsert_raw(raw); report["live_calls"] += 1
        else: report["raw_cache_hits"] += 1
        status = str(raw.get("status") or "error"); normalized = parse_explicit_rebalance_announcement(raw_row=raw, config=config) if status == "success" else []
        report["source_statuses"].append({"provider": provider, "source_url": source_url, "status": status, "reason": _safe_reason(raw.get("error_reason")) if status != "success" else (None if normalized else "explicit_rebalance_data_unavailable"), "cache_hit": hit, "http_status": raw.get("http_status")})
        report["provider_observations"]["planned"] += len(normalized)
        if normalized: repository.upsert_provider_observations(normalized); observations.extend(normalized); report["provider_observations"]["written"] += len(normalized)
        if not hit and index < len(configs) - 1 and request_sleep_seconds > 0: time.sleep(float(request_sleep_seconds))
    report.update(status="completed", by_provider=dict(Counter(row["provider"] for row in observations)), by_rebalance_type=dict(Counter(row["rebalance_type"] for row in observations)), change_side_counts=dict(Counter(row.get("change_side") or "unknown" for row in observations)), examples=[{key: row.get(key) for key in ("provider", "index_identifier_raw", "announcement_date", "effective_date", "change_side", "constituent_symbol", "source_url")} for row in observations[:_MAX_EXAMPLES]])
    return report

def load_fund_rebalance_source_config(path: Path, enabled: set[str]) -> list[dict[str, Any]]:
    try: payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError: return []
    sources = payload.get("sources", []) if isinstance(payload, Mapping) else []
    return [{**dict(item), "_config_version": payload.get("version")} for item in sources if isinstance(item, Mapping) and item.get("enabled") is True and str(item.get("provider")) in enabled and str(item.get("provider")) in PROVIDERS and _host_allowed(str(item.get("source_url") or ""), str(item.get("allowed_host") or "")) and str(item.get("parser")) == "explicit_rebalance_json_v1"]

def parse_explicit_rebalance_announcement(*, raw_row: Mapping[str, Any], config: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Only accepts a deliberately structured announcement fixture/API payload.

    HTML, CSV, XLSX and holdings-only payloads remain raw-only in Phase 1.
    """
    payload = raw_row.get("raw_payload")
    if raw_row.get("status") != "success" or not isinstance(payload, Mapping) or payload.get("holdings") is not None: return []
    announcement, effective = _date(payload.get("announcement_date")), _date(payload.get("effective_date")); changes = payload.get("changes")
    if announcement is None or effective is None or not isinstance(changes, list): return []
    fetched_at = _datetime(raw_row.get("fetched_at"))
    if fetched_at is None: return []
    provider, source_url = str(config["provider"]), str(config["source_url"]); group_id = _hash({"provider": provider, "sourceUrl": source_url, "announcement": announcement.isoformat(), "effective": effective.isoformat(), "index": config.get("index_identifier")})
    rows: list[dict[str, Any]] = []
    for change in changes:
        if not isinstance(change, Mapping) or str(change.get("side") or "").lower() not in {"add", "remove"} or not _text(change.get("symbol")): continue
        side, constituent_symbol = str(change["side"]).lower(), _symbol(change["symbol"]); observation_hash = _hash({"group": group_id, "side": side, "symbol": constituent_symbol})
        rows.append({"provider_observation_id": _hash({"kind": "fund_rebalance_provider_observation.v1", "observationHash": observation_hash, "knownAt": fetched_at.date().isoformat()}), "provider": provider, "provider_event_group_id": group_id, "provider_event_id": _text(payload.get("event_id")), "rebalance_type": "index_rebalance", "fund_symbol": None, "fund_name": None, "index_identifier_raw": _text(config.get("index_identifier")), "announcement_date": announcement, "effective_date": effective, "change_side": side, "constituent_symbol": constituent_symbol, "constituent_name": _text(change.get("name")), "constituent_identifier_json": None, "weight": None, "shares": None, "source_url": source_url, "source_document_accession": None, "source_published_at": None, "known_at": fetched_at.date(), "fetched_at": fetched_at, "raw_payload_ref": _text(raw_row.get("raw_id")), "payload_hash": _text(raw_row.get("raw_payload_sha256")), "observation_hash": observation_hash, "parser_version": "explicit_rebalance_json.v1", "observation_confidence": "high", "data_quality_flags": {"explicitAnnouncementAndEffectiveDate": True, "holdingsSnapshotInference": False}, "source_metadata": {"sourceConfigVersion": _text(config.get("_config_version")), "parser": "explicit_rebalance_json_v1"}})
    return rows

def _report() -> dict[str, Any]: return {"status": "skipped", "mode": "shadow", "providers": {}, "live_calls": 0, "raw_cache_hits": 0, "provider_observations": {"planned": 0, "written": 0}, "source_statuses": [], "warnings": ["holdings_snapshot_inference_disabled"]}
def _skipped(report: dict[str, Any], reason: str) -> dict[str, Any]: report.update(status="skipped", reason=reason); return report
def _allowlist(env: Mapping[str, str]) -> set[str]: return {value.strip().lower() for value in str(env.get("DATA_OPS_FUND_REBALANCE_PROVIDERS") or "").split(",") if value.strip()} & PROVIDERS
def _host_allowed(source_url: str, allowed_host: str) -> bool: return bool(source_url and allowed_host and urlparse(source_url).hostname and urlparse(source_url).hostname.lower() == allowed_host.lower())
def _cache_key(provider: str, source_url: str) -> str: return _hash({"provider": provider, "sourceUrl": source_url})
def _failure(provider: str, url: str, key: str, fetched_at: datetime, status: str, code: int | None, reason: str, payload: Any | None = None, headers: dict[str, str] | None = None) -> RebalanceFetchResult: return RebalanceFetchResult(provider, url, key, status, code, payload, reason, _utc(fetched_at), headers or {})
def _json_or_text(response: Any) -> Any:
    try: return response.json()
    except ValueError: return {"content": str(response.text)[:1000]}
def _safe_headers(headers: Mapping[str, Any]) -> dict[str, str]: return {key: str(value) for key, value in headers.items() if key.lower() in {"content-type", "retry-after", "etag"}}
def _safe_reason(value: Any) -> str | None: return _text(value)[:160] if _text(value) else None
def _hash(value: Any) -> str: return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
def _text(value: Any) -> str | None: return str(value).strip() if value is not None and str(value).strip() else None
def _symbol(value: Any) -> str: return (_text(value) or "").upper()
def _date(value: Any) -> date | None:
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
    return psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-fund-rebalances-shadow")
