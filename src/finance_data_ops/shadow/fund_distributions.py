"""Manual Tiingo fund-distribution shadow ingestion.

This module persists provider raw payloads and normalized provider observations
only. It makes no canonical, display, or tax-character claim.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
import time
from typing import Any, Protocol

import requests

from finance_data_ops.publish.client import PostgresPublisher

TIINGO_FUND_DISTRIBUTIONS = "tiingo_fund_distributions"
TIINGO_DISTRIBUTIONS_ENDPOINT = "https://api.tiingo.com/tiingo/corporate-actions/{symbol}/distributions"
TIINGO_EOD_PRICES_ENDPOINT = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"
_CORPORATE_ACTIONS_ENDPOINT_FAMILY = "tiingo_corporate_actions_distributions"
_EOD_ENDPOINT_FAMILY = "tiingo_eod_prices"
_CACHEABLE_STATUSES = {"success", "not_found", "rate_limited"}
_MAX_EXAMPLES = 25


class FundDistributionRepository(Protocol):
    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, source_cache_key: str, cache_forbidden: bool = False) -> dict[str, Any] | None: ...
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
    request_params: dict[str, str] = field(default_factory=dict)
    source_endpoint_family: str = _CORPORATE_ACTIONS_ENDPOINT_FAMILY

    def to_raw_row(self) -> dict[str, Any]:
        payload_hash = _hash(self.payload) if self.payload is not None else None
        fetched_at = _utc(self.fetched_at)
        return {
            "raw_id": _hash({"kind": "fund_distribution_provider_raw.v1", "provider": TIINGO_FUND_DISTRIBUTIONS, "endpoint": self.endpoint, "symbol": self.symbol, "cacheKey": self.source_cache_key, "status": self.status, "payloadHash": payload_hash}),
            "provider": TIINGO_FUND_DISTRIBUTIONS,
            "provider_endpoint_or_page": self.endpoint,
            "provider_request_params_json": self.request_params,
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
            "ingest_version": f"tiingo_fund_distributions.{self.source_endpoint_family}.v1",
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

    def fetch_eod_divcash(self, *, symbol: str, start_date: date, end_date: date, fetched_at: datetime) -> TiingoDistributionFetchResult:
        symbol = _symbol(symbol)
        endpoint = TIINGO_EOD_PRICES_ENDPOINT.format(symbol=symbol)
        request_params = {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}
        cache_key = _cache_key(endpoint, symbol, request_params)
        try:
            response = self._session.get(endpoint, params={**request_params, "token": self._api_key}, timeout=self._timeout_seconds)
        except requests.RequestException as exc:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", None, exc.__class__.__name__, request_params=request_params, source_endpoint_family=_EOD_ENDPOINT_FAMILY)
        headers = _safe_headers(response.headers)
        if response.status_code == 429:
            return _failure(symbol, endpoint, cache_key, fetched_at, "rate_limited", 429, "http_429", _json_or_none(response), headers, request_params, _EOD_ENDPOINT_FAMILY)
        if response.status_code == 404:
            return _failure(symbol, endpoint, cache_key, fetched_at, "not_found", 404, "http_404", _json_or_none(response), headers, request_params, _EOD_ENDPOINT_FAMILY)
        if not 200 <= response.status_code < 300:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", response.status_code, f"http_{response.status_code}", _json_or_none(response), headers, request_params, _EOD_ENDPOINT_FAMILY)
        try:
            payload = response.json()
        except ValueError:
            return _failure(symbol, endpoint, cache_key, fetched_at, "error", response.status_code, "invalid_json_response", None, headers, request_params, _EOD_ENDPOINT_FAMILY)
        rows = payload if isinstance(payload, list) else []
        return TiingoDistributionFetchResult(symbol, endpoint, cache_key, "success" if rows else "not_found", response.status_code, payload, None, _utc(fetched_at), headers, request_params, _EOD_ENDPOINT_FAMILY)


class PostgresFundDistributionRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL or DATABASE_URL is required for fund-distribution shadow ingestion.")
        self._publisher = PostgresPublisher(database_dsn=self._database_dsn, application_name="finance-data-ops-fund-distributions-shadow")

    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, source_cache_key: str, cache_forbidden: bool = False) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT raw_id, provider, provider_endpoint_or_page, provider_symbol, symbol, instrument_type, fetched_at, http_status, response_headers_json, source_published_at, source_updated_at, payload_format, raw_payload, raw_payload_sha256, entitlement_scope, ingest_version, source_cache_key, status, error_reason FROM source_cache.fund_distribution_provider_raw WHERE provider = %s AND provider_endpoint_or_page = %s AND provider_symbol = %s AND source_cache_key = %s AND (status = ANY(%s) OR (%s AND status = 'error' AND http_status = 403)) ORDER BY fetched_at DESC, raw_id DESC LIMIT 1""", (provider, endpoint, provider_symbol, source_cache_key, sorted(_CACHEABLE_STATUSES), cache_forbidden))
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
    eod_fallback_enabled = _enabled(env_map.get("DATA_OPS_FUND_DISTRIBUTION_TIINGO_EOD_FALLBACK"))
    eod_lookback_days = _lookback_days(env_map.get("DATA_OPS_FUND_DISTRIBUTION_EOD_LOOKBACK_DAYS"))
    observations: list[dict[str, Any]] = []
    for index, symbol in enumerate(requested):
        endpoint = TIINGO_DISTRIBUTIONS_ENDPOINT.format(symbol=symbol)
        cache_key = _cache_key(endpoint, symbol)
        observed_at = _utc(now_fn())
        main_raw = None if refresh else repository.find_cached_raw(provider=TIINGO_FUND_DISTRIBUTIONS, endpoint=endpoint, provider_symbol=symbol, source_cache_key=cache_key, cache_forbidden=eod_fallback_enabled)
        main_cache_hit = main_raw is not None
        if main_raw is None:
            main_raw = client.fetch(symbol=symbol, fetched_at=observed_at).to_raw_row()
            raw = main_raw
            repository.upsert_raw(raw)
            report["live_calls"] += 1
        else:
            report["raw_cache_hits"] += 1
        raw = main_raw
        fallback: dict[str, Any] | None = None
        fallback_cache_hit = False
        if _is_forbidden(main_raw) and eod_fallback_enabled:
            eod_endpoint = TIINGO_EOD_PRICES_ENDPOINT.format(symbol=symbol)
            end_date = observed_at.date()
            start_date = end_date - timedelta(days=eod_lookback_days)
            eod_params = {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}
            eod_cache_key = _cache_key(eod_endpoint, symbol, eod_params)
            fallback = None if refresh else repository.find_cached_raw(provider=TIINGO_FUND_DISTRIBUTIONS, endpoint=eod_endpoint, provider_symbol=symbol, source_cache_key=eod_cache_key)
            fallback_cache_hit = fallback is not None
            if fallback is None:
                fallback = client.fetch_eod_divcash(symbol=symbol, start_date=start_date, end_date=end_date, fetched_at=observed_at).to_raw_row()
                repository.upsert_raw(fallback)
                report["live_calls"] += 1
            else:
                report["raw_cache_hits"] += 1
            raw = fallback
            report["warnings"].append({"symbol": symbol, "reason": "tiingo_eod_divcash_fallback_only"})
        status = str(raw.get("status") or "error")
        report["symbol_statuses"][symbol] = _symbol_status(raw=raw, cache_hit=main_cache_hit and fallback is None, main_raw=main_raw, main_cache_hit=main_cache_hit, fallback_raw=fallback, fallback_cache_hit=fallback_cache_hit)
        normalized = _normalize_raw(raw) if status == "success" else []
        report["provider_observations"]["planned"] += len(normalized)
        if normalized:
            repository.upsert_provider_observations(normalized)
            observations.extend(normalized)
            report["provider_observations"]["written"] += len(normalized)
        if (not main_cache_hit or (fallback is not None and not fallback_cache_hit)) and index < len(requested) - 1 and request_sleep_seconds > 0:
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
        frequency = _text(item.get("distributionFrequency") or item.get("distributionFreqency"))
        rows.append({"provider_observation_id": _hash({"kind": "fund_distribution_provider_observation.v1", "observationHash": observation_hash, "knownAt": fetched_at.date().isoformat()}), "provider": TIINGO_FUND_DISTRIBUTIONS, "provider_event_id": None, "provider_symbol": symbol, "provider_permaticker_or_cusip": _text(item.get("permaTicker")), "symbol": symbol, "fund_name": _text(item.get("name")), "instrument_type": None, "distribution_type_raw": _text(item.get("distributionType")), "distribution_type_canonical": None, "declaration_date": _date(item.get("declarationDate")), "ex_date": ex_date, "record_date": _date(item.get("recordDate")), "payable_date": _date(item.get("paymentDate")), "distribution_amount": _number(item.get("distribution")), "currency": None, "distribution_frequency_raw": frequency, "source_url": _text(raw_row.get("provider_endpoint_or_page")), "source_document_accession": None, "source_published_at": None, "source_updated_at": None, "known_at": fetched_at.date(), "fetched_at": fetched_at, "raw_payload_ref": _text(raw_row.get("raw_id")), "payload_hash": _text(raw_row.get("raw_payload_sha256")), "observation_hash": observation_hash, "parser_version": "tiingo_fund_distributions.v1", "observation_confidence": "medium", "data_quality_flags": {"tiingoDistribution": True, "currencyUnavailable": True, "distributionTypeUnverified": item.get("distributionType") is None}, "source_metadata": {"entitlementScope": _text(raw_row.get("entitlement_scope")), "sourceEndpointFamily": _CORPORATE_ACTIONS_ENDPOINT_FAMILY, "providerFields": {"permaTicker": _text(item.get("permaTicker")), "distributionFrequency": frequency}}})
    return list({row["provider_observation_id"]: row for row in rows}.values())


def normalize_tiingo_eod_divcash_observations(*, raw_row: Mapping[str, Any]) -> list[dict[str, Any]]:
    if raw_row.get("status") != "success" or not isinstance(raw_row.get("raw_payload"), list):
        return []
    fetched_at = _datetime(raw_row.get("fetched_at"))
    if fetched_at is None:
        return []
    rows: list[dict[str, Any]] = []
    for item in raw_row["raw_payload"]:
        if not isinstance(item, Mapping) or (ex_date := _date(item.get("date"))) is None or (amount := _number(item.get("divCash"))) is None or amount <= 0:
            continue
        symbol = _symbol(item.get("ticker") or raw_row.get("symbol"))
        if not symbol:
            continue
        observation_hash = _hash({"provider": TIINGO_FUND_DISTRIBUTIONS, "sourceEndpointFamily": _EOD_ENDPOINT_FAMILY, "symbol": symbol, "exDate": ex_date.isoformat(), "divCash": amount})
        rows.append({"provider_observation_id": _hash({"kind": "fund_distribution_provider_observation.v1", "observationHash": observation_hash, "knownAt": fetched_at.date().isoformat()}), "provider": TIINGO_FUND_DISTRIBUTIONS, "provider_event_id": None, "provider_symbol": symbol, "provider_permaticker_or_cusip": None, "symbol": symbol, "fund_name": None, "instrument_type": None, "distribution_type_raw": None, "distribution_type_canonical": None, "declaration_date": None, "ex_date": ex_date, "record_date": None, "payable_date": None, "distribution_amount": amount, "currency": None, "distribution_frequency_raw": None, "source_url": _text(raw_row.get("provider_endpoint_or_page")), "source_document_accession": None, "source_published_at": None, "source_updated_at": None, "known_at": fetched_at.date(), "fetched_at": fetched_at, "raw_payload_ref": _text(raw_row.get("raw_id")), "payload_hash": _text(raw_row.get("raw_payload_sha256")), "observation_hash": observation_hash, "parser_version": "tiingo_eod_divcash_fallback.v1", "observation_confidence": "low", "data_quality_flags": {"tiingoEodDivCashFallback": True, "corporateActionsDistributionEndpointForbidden": True, "distributionDatesUnavailable": True, "currencyUnavailable": True, "distributionTypeUnverified": True}, "source_metadata": {"entitlementScope": _text(raw_row.get("entitlement_scope")), "sourceEndpointFamily": _EOD_ENDPOINT_FAMILY, "providerFields": {"date": ex_date.isoformat(), "divCash": amount}}})
    return list({row["provider_observation_id"]: row for row in rows}.values())


def _failure(
    symbol: str,
    endpoint: str,
    cache_key: str,
    fetched_at: datetime,
    status: str,
    http_status: int | None,
    reason: str,
    payload: Any | None = None,
    headers: dict[str, str] | None = None,
    request_params: dict[str, str] | None = None,
    source_endpoint_family: str = _CORPORATE_ACTIONS_ENDPOINT_FAMILY,
) -> TiingoDistributionFetchResult:
    return TiingoDistributionFetchResult(
        symbol,
        endpoint,
        cache_key,
        status,
        http_status,
        payload,
        reason,
        _utc(fetched_at),
        headers or {},
        request_params or {},
        source_endpoint_family,
    )


def _normalize_raw(raw_row: Mapping[str, Any]) -> list[dict[str, Any]]:
    endpoint = _text(raw_row.get("provider_endpoint_or_page")) or ""
    if endpoint == TIINGO_EOD_PRICES_ENDPOINT.format(symbol=_symbol(raw_row.get("symbol"))):
        return normalize_tiingo_eod_divcash_observations(raw_row=raw_row)
    return normalize_tiingo_fund_distribution_observations(raw_row=raw_row)


def _is_forbidden(raw_row: Mapping[str, Any]) -> bool:
    return str(raw_row.get("status") or "") == "error" and raw_row.get("http_status") == 403


def _symbol_status(
    *,
    raw: Mapping[str, Any],
    cache_hit: bool,
    main_raw: Mapping[str, Any],
    main_cache_hit: bool,
    fallback_raw: Mapping[str, Any] | None,
    fallback_cache_hit: bool,
) -> dict[str, Any]:
    result = {
        "status": str(raw.get("status") or "error"),
        "reason": _safe_reason(raw.get("error_reason")),
        "cache_hit": cache_hit,
        "http_status": raw.get("http_status"),
        "main_endpoint": {
            "status": str(main_raw.get("status") or "error"),
            "reason": _safe_reason(main_raw.get("error_reason")),
            "cache_hit": main_cache_hit,
            "http_status": main_raw.get("http_status"),
        },
        "fallback": {"used": False},
    }
    if fallback_raw is not None:
        result["cache_hit"] = fallback_cache_hit
        result["fallback"] = {
            "used": True,
            "source_endpoint_family": _EOD_ENDPOINT_FAMILY,
            "status": str(fallback_raw.get("status") or "error"),
            "reason": _safe_reason(fallback_raw.get("error_reason")),
            "cache_hit": fallback_cache_hit,
            "http_status": fallback_raw.get("http_status"),
        }
    return result


def _enabled(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _lookback_days(value: Any) -> int:
    try:
        return max(1, int(value)) if value is not None and str(value).strip() else 730
    except (TypeError, ValueError):
        return 730


def _report(symbols: list[str]) -> dict[str, Any]: return {"status": "skipped", "mode": "shadow", "symbols_requested": symbols, "providers": {TIINGO_FUND_DISTRIBUTIONS: {"enabled": False, "skip_reason": None}}, "live_calls": 0, "raw_cache_hits": 0, "provider_observations": {"planned": 0, "written": 0}, "symbol_statuses": {}, "unresolved_symbols": [], "warnings": []}
def _allowlist(env: Mapping[str, str]) -> set[str]: return {value.strip().lower() for value in str(env.get("DATA_OPS_FUND_DISTRIBUTION_PROVIDERS") or "").split(",") if value.strip()}
def _cache_key(endpoint: str, symbol: str, request_params: Mapping[str, str] | None = None) -> str: return _hash({"provider": TIINGO_FUND_DISTRIBUTIONS, "endpoint": endpoint, "symbol": symbol, "requestParams": dict(request_params or {})})
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
