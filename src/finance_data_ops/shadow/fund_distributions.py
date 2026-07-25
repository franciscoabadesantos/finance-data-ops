"""Manual Tiingo fund-distribution shadow ingestion.

This module persists provider raw payloads and normalized provider observations
only. It makes no canonical, display, or tax-character claim.
"""

from __future__ import annotations

from collections import Counter
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

TIINGO_FUND_DISTRIBUTIONS = "tiingo_fund_distributions"
TIINGO_DISTRIBUTIONS_ENDPOINT = "https://api.tiingo.com/tiingo/corporate-actions/{symbol}/distributions"
_CACHEABLE_STATUSES = {"success", "not_found", "rate_limited"}
_MAX_EXAMPLES = 25


class FundDistributionRepository(Protocol):
    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, source_cache_key: str) -> dict[str, Any] | None: ...
    def upsert_raw(self, row: dict[str, Any]) -> None: ...
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: ...


@dataclass(frozen=True, slots=True)
class TiingoDistributionFetchResult:
    symbol: str
    endpoint: str
    source_cache_key: str
    status: str
    http_status: int | None
    payload: Any | None
    error_reason: str | None
    fetched_at: datetime
    response_headers: dict[str, str]

    def to_raw_row(self) -> dict[str, Any]:
        payload_hash = _hash(self.payload) if self.payload is not None else None
        fetched_at = _utc(self.fetched_at)
        return {
            "raw_id": _hash({"kind": "fund_distribution_provider_raw.v1", "provider": TIINGO_FUND_DISTRIBUTIONS, "endpoint": self.endpoint, "symbol": self.symbol, "cacheKey": self.source_cache_key, "status": self.status, "payloadHash": payload_hash}),
            "provider": TIINGO_FUND_DISTRIBUTIONS,
            "provider_endpoint_or_page": self.endpoint,
            "provider_request_params_json": {},
            "provider_symbol": self.symbol,
            "symbol": self.symbol,
            "instrument_type": None,
            "fetched_at": fetched_at,
            "http_status": self.http_status,
            "response_headers_json": self.response_headers,
            "source_published_at": None,
            "source_updated_at": None,
            "payload_format": "json",
            "raw_payload": self.payload,
            "raw_payload_sha256": payload_hash,
            "entitlement_scope": "internal_use_only",
            "ingest_version": "tiingo_fund_distributions.v1",
            "source_cache_key": self.source_cache_key,
            "status": self.status,
            "error_reason": _safe_reason(self.error_reason),
        }


class TiingoFundDistributionsClient:
    def __init__(self, *, api_key: str, session: requests.Session | None = None, timeout_seconds: float = 20.0) -> None:
        self._api_key = str(api_key).strip()
        if not self._api_key:
            raise ValueError("TIINGO_API_KEY is required for live Tiingo distribution requests.")
        self._session = session or requests.Session()
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def fetch(self, *, symbol: str, fetched_at: datetime) -> TiingoDistributionFetchResult:
        symbol = _symbol(symbol)
        endpoint = TIINGO_DISTRIBUTIONS_ENDPOINT.format(symbol=symbol)
        cache_key = _cache_key(endpoint, symbol)
        try:
            response = self._session.get(endpoint, params={"token": self._api_key}, timeout=self._timeout_seconds)
        except requests.RequestException as exc:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", None, exc.__class__.__name__)
        headers = _safe_headers(response.headers)
        if response.status_code == 429:
            return _failure(symbol, endpoint, cache_key, fetched_at, "rate_limited", 429, "http_429", _json_or_none(response), headers)
        if response.status_code == 404:
            return _failure(symbol, endpoint, cache_key, fetched_at, "not_found", 404, "http_404", _json_or_none(response), headers)
        if not 200 <= response.status_code < 300:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", response.status_code, f"http_{response.status_code}", _json_or_none(response), headers)
        try:
            payload = response.json()
        except ValueError:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", response.status_code, "invalid_json_response", None, headers)
        rows = payload if isinstance(payload, list) else []
        return TiingoDistributionFetchResult(symbol, endpoint, cache_key, "success" if rows else "not_found", response.status_code, payload, None, _utc(fetched_at), headers)


class PostgresFundDistributionRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL or DATABASE_URL is required for fund-distribution shadow ingestion.")
        self._publisher = PostgresPublisher(database_dsn=self._database_dsn, application_name="finance-data-ops-fund-distributions-shadow")

    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, source_cache_key: str) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT raw_id, provider, provider_endpoint_or_page, provider_symbol, symbol, instrument_type, fetched_at, http_status, response_headers_json, source_published_at, source_updated_at, payload_format, raw_payload, raw_payload_sha256, entitlement_scope, ingest_version, source_cache_key, status, error_reason FROM source_cache.fund_distribution_provider_raw WHERE provider = %s AND provider_endpoint_or_page = %s AND provider_symbol = %s AND source_cache_key = %s AND status = ANY(%s) ORDER BY fetched_at DESC, raw_id DESC LIMIT 1""", (provider, endpoint, provider_symbol, source_cache_key, sorted(_CACHEABLE_STATUSES)))
                row = cur.fetchone()
                return None if row is None else dict(zip([column.name for column in cur.description], row, strict=True))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        from psycopg.types.json import Jsonb
        columns = ("raw_id", "provider", "provider_endpoint_or_page", "provider_request_params_json", "provider_symbol", "symbol", "instrument_type", "fetched_at", "http_status", "response_headers_json", "source_published_at", "source_updated_at", "payload_format", "raw_payload", "raw_payload_sha256", "entitlement_scope", "ingest_version", "source_cache_key", "status", "error_reason")
        values = [Jsonb(row[column]) if column in {"provider_request_params_json", "response_headers_json", "raw_payload"} and row.get(column) is not None else row.get(column) for column in columns]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO source_cache.fund_distribution_provider_raw ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))}) ON CONFLICT (raw_id) DO UPDATE SET fetched_at = EXCLUDED.fetched_at, http_status = EXCLUDED.http_status, error_reason = EXCLUDED.error_reason, updated_at = NOW()", values)

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert("source_cache.fund_distribution_provider_observations", rows, on_conflict="provider_observation_id")


def run_fund_distributions_shadow(*, symbols: Iterable[str], repository: FundDistributionRepository | None = None, client: Any | None = None, env: Mapping[str, str] | None = None, dry_run: bool = False, refresh: bool = False, request_sleep_seconds: float = 0.25, now: Callable[[], datetime] | None = None) -> dict[str, Any]:
    requested = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    enabled = _allowlist(env_map)
    report = _report(requested)
    if TIINGO_FUND_DISTRIBUTIONS not in enabled or not str(env_map.get("TIINGO_API_KEY") or "").strip():
        report.update(status="skipped", reason="no_fund_distribution_providers_enabled")
        report["providers"][TIINGO_FUND_DISTRIBUTIONS]["skip_reason"] = "tiingo_api_key_missing" if TIINGO_FUND_DISTRIBUTIONS in enabled else "not_allowlisted"
        return report
    report["providers"][TIINGO_FUND_DISTRIBUTIONS]["enabled"] = True
    if dry_run:
        report.update(status="dry_run")
        return report
    if repository is None:
        raise ValueError("repository is required when fund-distribution shadow ingestion is enabled.")
    client = client or TiingoFundDistributionsClient(api_key=str(env_map["TIINGO_API_KEY"]))
    now_fn = now or (lambda: datetime.now(UTC))
    observations: list[dict[str, Any]] = []
    for index, symbol in enumerate(requested):
        endpoint = TIINGO_DISTRIBUTIONS_ENDPOINT.format(symbol=symbol)
        cache_key = _cache_key(endpoint, symbol)
        raw = None if refresh else repository.find_cached_raw(provider=TIINGO_FUND_DISTRIBUTIONS, endpoint=endpoint, provider_symbol=symbol, source_cache_key=cache_key)
        cache_hit = raw is not None
        if raw is None:
            raw = client.fetch(symbol=symbol, fetched_at=_utc(now_fn())).to_raw_row()
            repository.upsert_raw(raw)
            report["live_calls"] += 1
        else:
            report["raw_cache_hits"] += 1
        status = str(raw.get("status") or "error")
        report["symbol_statuses"][symbol] = {"status": status, "reason": _safe_reason(raw.get("error_reason")), "cache_hit": cache_hit, "http_status": raw.get("http_status")}
        normalized = normalize_tiingo_fund_distribution_observations(raw_row=raw) if status == "success" else []
        report["provider_observations"]["planned"] += len(normalized)
        if normalized:
            repository.upsert_provider_observations(normalized)
            observations.extend(normalized)
            report["provider_observations"]["written"] += len(normalized)
        if not cache_hit and index < len(requested) - 1 and request_sleep_seconds > 0:
            time.sleep(float(request_sleep_seconds))
    report.update(status="completed", by_symbol={symbol: sum(row["symbol"] == symbol for row in observations) for symbol in requested}, by_provider=dict(Counter(row["provider"] for row in observations)), observation_confidence_counts=dict(Counter(row["observation_confidence"] for row in observations)), unresolved_symbols=[symbol for symbol in requested if not any(row["symbol"] == symbol for row in observations)], examples=[{key: row.get(key) for key in ("symbol", "ex_date", "distribution_amount", "source_url", "observation_confidence")} for row in observations[:_MAX_EXAMPLES]])
    return report


def normalize_tiingo_fund_distribution_observations(*, raw_row: Mapping[str, Any]) -> list[dict[str, Any]]:
    if raw_row.get("status") != "success" or not isinstance(raw_row.get("raw_payload"), list):
        return []
    fetched_at = _datetime(raw_row.get("fetched_at"))
    if fetched_at is None:
        return []
    rows: list[dict[str, Any]] = []
    for item in raw_row["raw_payload"]:
        if not isinstance(item, Mapping) or (ex_date := _date(item.get("exDate"))) is None:
            continue
        symbol = _symbol(item.get("ticker") or raw_row.get("symbol"))
        if not symbol:
            continue
        observation_hash = _hash({"provider": TIINGO_FUND_DISTRIBUTIONS, "symbol": symbol, "permaTicker": item.get("permaTicker"), "exDate": ex_date.isoformat(), "distribution": item.get("distribution"), "declarationDate": item.get("declarationDate"), "paymentDate": item.get("paymentDate")})
        rows.append({"provider_observation_id": _hash({"kind": "fund_distribution_provider_observation.v1", "observationHash": observation_hash, "knownAt": fetched_at.date().isoformat()}), "provider": TIINGO_FUND_DISTRIBUTIONS, "provider_event_id": None, "provider_symbol": symbol, "provider_permaticker_or_cusip": _text(item.get("permaTicker")), "symbol": symbol, "fund_name": _text(item.get("name")), "instrument_type": None, "distribution_type_raw": _text(item.get("distributionType")), "distribution_type_canonical": None, "declaration_date": _date(item.get("declarationDate")), "ex_date": ex_date, "record_date": _date(item.get("recordDate")), "payable_date": _date(item.get("paymentDate")), "distribution_amount": _number(item.get("distribution")), "currency": None, "distribution_frequency_raw": _text(item.get("distributionFrequency")), "source_url": _text(raw_row.get("provider_endpoint_or_page")), "source_document_accession": None, "source_published_at": None, "source_updated_at": None, "known_at": fetched_at.date(), "fetched_at": fetched_at, "raw_payload_ref": _text(raw_row.get("raw_id")), "payload_hash": _text(raw_row.get("raw_payload_sha256")), "observation_hash": observation_hash, "parser_version": "tiingo_fund_distributions.v1", "observation_confidence": "medium", "data_quality_flags": {"tiingoDistribution": True, "currencyUnavailable": True, "distributionTypeUnverified": item.get("distributionType") is None}, "source_metadata": {"entitlementScope": _text(raw_row.get("entitlement_scope")), "providerFields": {"permaTicker": _text(item.get("permaTicker")), "distributionFrequency": _text(item.get("distributionFrequency"))}}})
    return list({row["provider_observation_id"]: row for row in rows}.values())


def _failure(symbol: str, endpoint: str, cache_key: str, fetched_at: datetime, status: str, http_status: int | None, reason: str, payload: Any | None = None, headers: dict[str, str] | None = None) -> TiingoDistributionFetchResult: return TiingoDistributionFetchResult(symbol, endpoint, cache_key, status, http_status, payload, reason, _utc(fetched_at), headers or {})
def _report(symbols: list[str]) -> dict[str, Any]: return {"status": "skipped", "mode": "shadow", "symbols_requested": symbols, "providers": {TIINGO_FUND_DISTRIBUTIONS: {"enabled": False, "skip_reason": None}}, "live_calls": 0, "raw_cache_hits": 0, "provider_observations": {"planned": 0, "written": 0}, "symbol_statuses": {}, "unresolved_symbols": [], "warnings": []}
def _allowlist(env: Mapping[str, str]) -> set[str]: return {value.strip().lower() for value in str(env.get("DATA_OPS_FUND_DISTRIBUTION_PROVIDERS") or "").split(",") if value.strip()}
def _cache_key(endpoint: str, symbol: str) -> str: return _hash({"provider": TIINGO_FUND_DISTRIBUTIONS, "endpoint": endpoint, "symbol": symbol})
def _safe_headers(headers: Mapping[str, Any]) -> dict[str, str]: return {key: str(value) for key, value in headers.items() if key.lower() in {"content-type", "retry-after", "etag"}}
def _json_or_none(response: Any) -> Any | None:
    try: return response.json()
    except ValueError: return None
def _safe_reason(value: Any) -> str | None: return _text(value)[:160] if _text(value) else None
def _hash(value: Any) -> str: return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
def _symbols(values: Iterable[str]) -> list[str]: return list(dict.fromkeys(value for item in values if (value := _symbol(item))))
def _symbol(value: Any) -> str: return (_text(value) or "").upper()
def _text(value: Any) -> str | None: return str(value).strip() if value is not None and str(value).strip() else None
def _date(value: Any) -> date | None:
    try: return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError: return None
def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime): return _utc(value)
    try: return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00"))) if value else None
    except ValueError: return None
def _number(value: Any) -> float | None:
    try: return float(value) if value is not None else None
    except (TypeError, ValueError): return None
def _utc(value: datetime) -> datetime: return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
def _to_psycopg_dsn(dsn: str) -> str: return "postgresql://" + str(dsn).strip().removeprefix("postgresql+psycopg://") if str(dsn).strip().startswith("postgresql+psycopg://") else str(dsn).strip()
def _connect(database_dsn: str):
    import psycopg
    return psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-fund-distributions-shadow")
