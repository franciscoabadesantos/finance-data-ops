"""Manual corporate-actions shadow ingestion.

The module retains provider payloads and writes provider-specific observations
only. It deliberately has no canonical Feature Store, product, scheduling, or
price-adjustment side effects.
"""

from __future__ import annotations

from collections import defaultdict
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

FMP_PROVIDER = "fmp"
YAHOO_FINANCE_PROVIDER = "yahoo_finance"
DIVIDEND = "dividend"
SPLIT = "split"
ACTION_TYPES = (DIVIDEND, SPLIT)
FMP_DIVIDENDS_ENDPOINT = "https://financialmodelingprep.com/stable/dividends"
FMP_SPLITS_ENDPOINT = "https://financialmodelingprep.com/stable/splits"
YAHOO_DIVIDENDS_ENDPOINT = "yfinance:Ticker.dividends"
YAHOO_SPLITS_ENDPOINT = "yfinance:Ticker.splits"
_CACHEABLE_RAW_STATUSES = {"success", "not_found"}
_RAW_STATUSES = {"success", "not_found", "rate_limited", "error"}
_MAX_EXAMPLES = 25


class CorporateActionsShadowRepository(Protocol):
    def find_cached_raw(
        self,
        *,
        provider: str,
        endpoint: str,
        provider_symbol: str,
        action_type: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        ...

    def upsert_raw(self, row: dict[str, Any]) -> None:
        ...

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        ...


class CorporateActionsClient(Protocol):
    def fetch(
        self, *, symbol: str, action_type: str, observed_at: datetime
    ) -> "CorporateActionsFetchResult":
        ...


@dataclass(frozen=True, slots=True)
class CorporateActionsFetchResult:
    provider: str
    provider_symbol: str
    action_type: str
    endpoint: str
    request_params: dict[str, str]
    request_hash: str
    status: str
    http_status: int | None
    response_payload: Any | None
    error_reason: str | None
    provider_updated_at: datetime | None
    observed_at: datetime

    def to_raw_row(self) -> dict[str, Any]:
        payload = _sanitize_payload(self.response_payload)
        payload_hash = _payload_hash(payload) if payload is not None else None
        observed_at = _utc(self.observed_at)
        return {
            "raw_id": _raw_id(
                provider=self.provider,
                endpoint=self.endpoint,
                provider_symbol=self.provider_symbol,
                action_type=self.action_type,
                request_hash=self.request_hash,
                status=self.status,
                raw_payload_hash=payload_hash,
            ),
            "provider": self.provider,
            "endpoint": self.endpoint,
            "provider_symbol": self.provider_symbol,
            "symbol": self.provider_symbol,
            "action_type": self.action_type,
            "request_params": _sanitize_request_params(self.request_params),
            "request_hash": self.request_hash,
            "status": self.status,
            "http_status": self.http_status,
            "provider_updated_at": self.provider_updated_at,
            "known_at": observed_at.date(),
            "ingested_at": observed_at,
            "raw_payload": payload,
            "raw_payload_hash": payload_hash,
            "error_reason": _safe_error_reason(self.error_reason),
        }


class FinancialModelingPrepCorporateActionsClient:
    """FMP client with API-key header authentication only."""

    def __init__(
        self,
        *,
        api_key: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._api_key = str(api_key).strip()
        if not self._api_key:
            raise ValueError("FMP API key is required for live FMP requests.")
        self._session = session or requests.Session()
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def fetch(
        self, *, symbol: str, action_type: str, observed_at: datetime
    ) -> CorporateActionsFetchResult:
        provider_symbol = _symbol(symbol)
        endpoint = _fmp_endpoint(action_type)
        request_params = {"symbol": provider_symbol}
        request_hash = _request_hash(
            provider=FMP_PROVIDER,
            endpoint=endpoint,
            provider_symbol=provider_symbol,
            action_type=action_type,
            request_params=request_params,
        )
        try:
            response = self._session.get(
                endpoint,
                params=request_params,
                headers={"apikey": self._api_key},
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=None,
                error_reason=exc.__class__.__name__,
            )
        if response.status_code == 429:
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="rate_limited",
                http_status=response.status_code,
                error_reason="http_429",
                response_payload=_response_json_or_none(response),
            )
        if response.status_code == 404:
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="not_found",
                http_status=response.status_code,
                error_reason="http_404",
                response_payload=_response_json_or_none(response),
            )
        if not 200 <= response.status_code < 300:
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
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
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=response.status_code,
                error_reason="invalid_json_response",
            )
        if _provider_error_payload(payload):
            return _fetch_failure(
                provider=FMP_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=response.status_code,
                error_reason="provider_error_payload",
                response_payload=payload,
            )
        rows = _payload_rows(payload)
        return CorporateActionsFetchResult(
            provider=FMP_PROVIDER,
            provider_symbol=provider_symbol,
            action_type=action_type,
            endpoint=endpoint,
            request_params=request_params,
            request_hash=request_hash,
            status="success" if rows else "not_found",
            http_status=response.status_code,
            response_payload=payload,
            error_reason=None,
            provider_updated_at=_latest_provider_updated_at(rows),
            observed_at=observed_at,
        )


class YahooFinanceCorporateActionsClient:
    """yfinance dividends/splits adapter with explicit index-date semantics."""

    def __init__(self, *, ticker_factory: Callable[[str], Any] | None = None) -> None:
        self._ticker_factory = ticker_factory or _yfinance_ticker

    def fetch(
        self, *, symbol: str, action_type: str, observed_at: datetime
    ) -> CorporateActionsFetchResult:
        provider_symbol = _symbol(symbol)
        endpoint = _yahoo_endpoint(action_type)
        request_params = {"symbol": provider_symbol}
        request_hash = _request_hash(
            provider=YAHOO_FINANCE_PROVIDER,
            endpoint=endpoint,
            provider_symbol=provider_symbol,
            action_type=action_type,
            request_params=request_params,
        )
        try:
            ticker = self._ticker_factory(provider_symbol)
            series = ticker.dividends if action_type == DIVIDEND else ticker.splits
            payload = _yahoo_series_payload(series, action_type=action_type)
        except Exception as exc:  # yfinance exposes provider/network errors through varied exception types.
            return _fetch_failure(
                provider=YAHOO_FINANCE_PROVIDER,
                provider_symbol=provider_symbol,
                action_type=action_type,
                endpoint=endpoint,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                status="error",
                http_status=None,
                error_reason=exc.__class__.__name__,
            )
        return CorporateActionsFetchResult(
            provider=YAHOO_FINANCE_PROVIDER,
            provider_symbol=provider_symbol,
            action_type=action_type,
            endpoint=endpoint,
            request_params=request_params,
            request_hash=request_hash,
            status="success" if payload else "not_found",
            http_status=None,
            response_payload=payload,
            error_reason=None,
            provider_updated_at=None,
            observed_at=observed_at,
        )


class PostgresCorporateActionsShadowRepository:
    """Persistence for corporate-actions raw cache and normalized observations."""

    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL is required for corporate-actions shadow ingestion.")
        self._publisher = PostgresPublisher(
            database_dsn=self._database_dsn,
            application_name="finance-data-ops-corporate-actions-shadow",
        )

    def find_cached_raw(
        self,
        *,
        provider: str,
        endpoint: str,
        provider_symbol: str,
        action_type: str,
        request_hash: str,
    ) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_id, provider, endpoint, provider_symbol, symbol, action_type,
                           request_params, request_hash, status, http_status,
                           provider_updated_at, known_at, ingested_at, raw_payload,
                           raw_payload_hash, error_reason
                    FROM source_cache.corporate_action_provider_raw
                    WHERE provider = %s
                      AND endpoint = %s
                      AND provider_symbol = %s
                      AND action_type = %s
                      AND request_hash = %s
                      AND status = ANY(%s)
                    ORDER BY known_at DESC, ingested_at DESC, raw_id DESC
                    LIMIT 1
                    """,
                    (
                        provider,
                        endpoint,
                        provider_symbol,
                        action_type,
                        request_hash,
                        sorted(_CACHEABLE_RAW_STATUSES),
                    ),
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
            raise RuntimeError("psycopg JSON adapters are required for corporate-actions shadow ingestion.") from exc
        columns = (
            "raw_id", "provider", "endpoint", "provider_symbol", "symbol", "action_type",
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
                    INSERT INTO source_cache.corporate_action_provider_raw (
                        raw_id, provider, endpoint, provider_symbol, symbol, action_type,
                        request_params, request_hash, status, http_status, provider_updated_at,
                        known_at, ingested_at, raw_payload, raw_payload_hash, error_reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_id) DO UPDATE SET
                        ingested_at = EXCLUDED.ingested_at,
                        provider_updated_at = EXCLUDED.provider_updated_at,
                        error_reason = EXCLUDED.error_reason
                    """,
                    values,
                )

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert(
            "source_cache.corporate_action_provider_observations",
            rows,
            on_conflict="provider_observation_id",
        )


def run_corporate_actions_shadow(
    *,
    symbols: Iterable[str],
    repository: CorporateActionsShadowRepository | None = None,
    clients: Mapping[str, CorporateActionsClient] | None = None,
    env: Mapping[str, str] | None = None,
    dry_run: bool = False,
    refresh: bool = False,
    request_sleep_seconds: float = 0.25,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run manual raw/observation-only corporate-actions shadow ingestion."""
    requested_symbols = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    report = _new_report(symbols=requested_symbols)
    enabled = _enabled_providers(report=report, env=env_map)
    if not enabled:
        if FMP_PROVIDER in _provider_allowlist(env_map) and not _fmp_api_key(env_map):
            return _skipped_report(report, "fmp_api_key_missing")
        return _skipped_report(report, "no_corporate_action_providers_enabled")
    if dry_run:
        report["status"] = "dry_run"
        report["provider_observations"]["planned"] = len(requested_symbols) * len(ACTION_TYPES) * len(enabled)
        return report
    if repository is None:
        raise ValueError("repository is required when corporate-actions shadow ingestion is enabled.")
    client_map = dict(clients or {})
    if FMP_PROVIDER in enabled:
        client_map.setdefault(FMP_PROVIDER, FinancialModelingPrepCorporateActionsClient(api_key=_fmp_api_key(env_map)))
    if YAHOO_FINANCE_PROVIDER in enabled:
        client_map.setdefault(YAHOO_FINANCE_PROVIDER, YahooFinanceCorporateActionsClient())
    now_fn = now or (lambda: datetime.now(UTC))
    observations: list[dict[str, Any]] = []

    requests_to_run = [
        (provider, symbol, action_type)
        for provider in enabled
        for symbol in requested_symbols
        for action_type in ACTION_TYPES
    ]
    report["status"] = "completed"
    report["provider_observations"]["planned"] = len(requests_to_run)
    for index, (provider, symbol, action_type) in enumerate(requests_to_run):
        client = client_map[provider]
        observed_at = _utc(now_fn())
        endpoint = _provider_endpoint(provider=provider, action_type=action_type)
        request_params = {"symbol": symbol}
        request_hash = _request_hash(
            provider=provider,
            endpoint=endpoint,
            provider_symbol=symbol,
            action_type=action_type,
            request_params=request_params,
        )
        raw_row = None if refresh else repository.find_cached_raw(
            provider=provider,
            endpoint=endpoint,
            provider_symbol=symbol,
            action_type=action_type,
            request_hash=request_hash,
        )
        if raw_row is not None:
            report["raw_cache_hits"] += 1
            cached = True
        else:
            result = client.fetch(symbol=symbol, action_type=action_type, observed_at=observed_at)
            raw_row = result.to_raw_row()
            repository.upsert_raw(raw_row)
            report["live_calls"] += 1
            cached = False
            if index < len(requests_to_run) - 1 and request_sleep_seconds > 0:
                time.sleep(float(request_sleep_seconds))
        status = str(raw_row.get("status") or "error")
        _increment_status(report, provider=provider, status=status)
        report["symbol_statuses"].setdefault(symbol, {}).setdefault(provider, {})[action_type] = {
            "status": status,
            "http_status": raw_row.get("http_status"),
            "reason": _safe_error_reason(raw_row.get("error_reason")),
            "cache_hit": cached,
        }
        if status != "success":
            continue
        normalized = normalize_corporate_action_provider_observations(raw_row=raw_row)
        if normalized:
            repository.upsert_provider_observations(normalized)
            observations.extend(normalized)
            report["provider_observations"]["written"] += len(normalized)

    report["coverage"] = _coverage(observations)
    report["overlap_fmp_vs_yahoo"] = _overlap_report(observations)
    report["conflicts"] = _conflict_report(observations)
    return report


def normalize_corporate_action_provider_observations(*, raw_row: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize one successful provider raw row without inferring missing fields."""
    if str(raw_row.get("status") or "") != "success":
        return []
    provider = _text(raw_row.get("provider"))
    action_type = _text(raw_row.get("action_type"))
    provider_symbol = _symbol(raw_row.get("provider_symbol"))
    known_at = _coerce_date(raw_row.get("known_at"))
    ingested_at = _coerce_datetime(raw_row.get("ingested_at"))
    if provider not in {FMP_PROVIDER, YAHOO_FINANCE_PROVIDER} or action_type not in ACTION_TYPES:
        return []
    if not provider_symbol or known_at is None or ingested_at is None:
        return []
    records: list[dict[str, Any]] = []
    for payload in _payload_rows(raw_row.get("raw_payload")):
        record = (
            _normalize_dividend(
                provider=provider,
                provider_symbol=provider_symbol,
                payload=payload,
                raw_row=raw_row,
                known_at=known_at,
                ingested_at=ingested_at,
            )
            if action_type == DIVIDEND
            else _normalize_split(
                provider=provider,
                provider_symbol=provider_symbol,
                payload=payload,
                raw_row=raw_row,
                known_at=known_at,
                ingested_at=ingested_at,
            )
        )
        if record is not None:
            records.append(record)
    unique = {record["provider_observation_id"]: record for record in records}
    return sorted(unique.values(), key=lambda row: (row["symbol"], row["action_type"], row["ex_date"], row["provider_observation_id"]))


def _normalize_dividend(
    *,
    provider: str,
    provider_symbol: str,
    payload: dict[str, Any],
    raw_row: dict[str, Any],
    known_at: date,
    ingested_at: datetime,
) -> dict[str, Any] | None:
    ex_date = _coerce_date(_first(payload, "exDate", "ex_date", "date", "provider_date"))
    if ex_date is None:
        return None
    fields = _base_observation_fields(
        provider=provider,
        provider_symbol=provider_symbol,
        symbol=_symbol(_first(payload, "symbol", "ticker") or provider_symbol),
        action_type=DIVIDEND,
        ex_date=ex_date,
        provider_event_id=_text(_first(payload, "id", "eventId", "event_id")),
        provider_updated_at=_coerce_datetime(_first(payload, "lastUpdated", "updatedAt", "updated_at")) or raw_row.get("provider_updated_at"),
        known_at=known_at,
        ingested_at=ingested_at,
        raw_row=raw_row,
        data_quality_flags=_provider_flags(provider=provider, action_type=DIVIDEND),
    )
    fields.update(
        {
            "payment_date": _coerce_date(_first(payload, "paymentDate", "payment_date")),
            "record_date": _coerce_date(_first(payload, "recordDate", "record_date")),
            "declaration_date": _coerce_date(_first(payload, "declarationDate", "declaration_date")),
            "cash_amount": _coerce_number(_first(payload, "dividend", "cashAmount", "cash_amount", "amount", "value")),
            "adjusted_cash_amount": _coerce_number(_first(payload, "adjDividend", "adjustedDividend", "adjusted_cash_amount")),
            "dividend_yield": _coerce_number(_first(payload, "yield", "dividendYield", "dividend_yield")),
            "frequency": _text(_first(payload, "frequency")),
            "currency": _text(_first(payload, "currency")),
            "split_ratio_text": None,
            "split_numerator": None,
            "split_denominator": None,
            "split_factor": None,
        }
    )
    return _finalize_observation(fields)


def _normalize_split(
    *,
    provider: str,
    provider_symbol: str,
    payload: dict[str, Any],
    raw_row: dict[str, Any],
    known_at: date,
    ingested_at: datetime,
) -> dict[str, Any] | None:
    ex_date = _coerce_date(_first(payload, "date", "exDate", "ex_date", "provider_date"))
    if ex_date is None:
        return None
    numerator = _coerce_number(_first(payload, "numerator", "splitNumerator", "split_numerator"))
    denominator = _coerce_number(_first(payload, "denominator", "splitDenominator", "split_denominator"))
    ratio_text = _text(_first(payload, "splitRatio", "split_ratio", "ratio"))
    if (numerator is None or denominator is None) and ratio_text:
        numerator, denominator = _ratio_components(ratio_text, numerator=numerator, denominator=denominator)
    factor = _coerce_number(_first(payload, "splitFactor", "split_factor", "factor"))
    if factor is None and numerator is not None and denominator not in {None, 0.0}:
        factor = numerator / denominator
    fields = _base_observation_fields(
        provider=provider,
        provider_symbol=provider_symbol,
        symbol=_symbol(_first(payload, "symbol", "ticker") or provider_symbol),
        action_type=SPLIT,
        ex_date=ex_date,
        provider_event_id=_text(_first(payload, "id", "eventId", "event_id")),
        provider_updated_at=_coerce_datetime(_first(payload, "lastUpdated", "updatedAt", "updated_at")) or raw_row.get("provider_updated_at"),
        known_at=known_at,
        ingested_at=ingested_at,
        raw_row=raw_row,
        data_quality_flags=_provider_flags(provider=provider, action_type=SPLIT),
    )
    fields.update(
        {
            "payment_date": None,
            "record_date": None,
            "declaration_date": None,
            "cash_amount": None,
            "adjusted_cash_amount": None,
            "dividend_yield": None,
            "frequency": None,
            "currency": None,
            "split_ratio_text": ratio_text,
            "split_numerator": numerator,
            "split_denominator": denominator,
            "split_factor": factor,
        }
    )
    return _finalize_observation(fields)


def _base_observation_fields(
    *,
    provider: str,
    provider_symbol: str,
    symbol: str,
    action_type: str,
    ex_date: date,
    provider_event_id: str | None,
    provider_updated_at: object | None,
    known_at: date,
    ingested_at: datetime,
    raw_row: dict[str, Any],
    data_quality_flags: dict[str, Any],
) -> dict[str, Any]:
    return {
        "provider": provider,
        "provider_event_id": provider_event_id,
        "provider_symbol": provider_symbol,
        "symbol": symbol,
        "action_type": action_type,
        "ex_date": ex_date,
        "provider_date": ex_date,
        "provider_date_semantics": "provider_index_date" if provider == YAHOO_FINANCE_PROVIDER else "ex_date",
        "provider_updated_at": provider_updated_at,
        "known_at": known_at,
        "ingested_at": ingested_at,
        "raw_payload_ref": raw_row["raw_id"],
        "raw_payload_hash": raw_row.get("raw_payload_hash"),
        "data_quality_flags": data_quality_flags,
    }


def _finalize_observation(fields: dict[str, Any]) -> dict[str, Any]:
    identity = {
        "kind": "corporate_action_provider_observation.v1",
        "provider": fields["provider"],
        "provider_event_id": fields["provider_event_id"],
        "symbol": fields["symbol"],
        "action_type": fields["action_type"],
        "ex_date": fields["ex_date"],
        "known_at": fields["known_at"],
    }
    fields["provider_observation_id"] = _payload_hash(identity)
    fields["observation_hash"] = _payload_hash(
        {key: value for key, value in fields.items() if key not in {"provider_observation_id", "ingested_at", "observation_hash"}}
    )
    return fields


def _provider_flags(*, provider: str, action_type: str) -> dict[str, Any]:
    if provider == YAHOO_FINANCE_PROVIDER:
        flags: dict[str, Any] = {
            "providerDateFromIndex": True,
            "providerDateSemantics": "provider_index_date",
            "currencyNotProvidedBySource": True,
        }
        if action_type == DIVIDEND:
            flags.update(
                {
                    "paymentDateNotProvidedBySource": True,
                    "recordDateNotProvidedBySource": True,
                    "declarationDateNotProvidedBySource": True,
                    "adjustedCashAmountNotProvidedBySource": True,
                    "dividendYieldNotProvidedBySource": True,
                    "frequencyNotProvidedBySource": True,
                }
            )
        return flags
    return {}


def _coverage(rows: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {
        FMP_PROVIDER: {DIVIDEND: 0, SPLIT: 0},
        YAHOO_FINANCE_PROVIDER: {DIVIDEND: 0, SPLIT: 0},
    }
    for row in rows:
        provider = _text(row.get("provider"))
        action_type = _text(row.get("action_type"))
        if provider in result and action_type in ACTION_TYPES:
            result[provider][action_type] += 1
    return result


def _overlap_report(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_key: dict[tuple[str, str], dict[str, set[date]]] = defaultdict(lambda: defaultdict(set))
    for row in rows:
        provider = _text(row.get("provider"))
        action_type = _text(row.get("action_type"))
        ex_date = _coerce_date(row.get("ex_date"))
        symbol = _text(row.get("symbol"))
        if provider in {FMP_PROVIDER, YAHOO_FINANCE_PROVIDER} and action_type in ACTION_TYPES and ex_date and symbol:
            by_key[(symbol.upper(), action_type)][provider].add(ex_date)
    entries = []
    for (symbol, action_type), dates in sorted(by_key.items()):
        shared = dates[FMP_PROVIDER] & dates[YAHOO_FINANCE_PROVIDER]
        entries.append(
            {
                "symbol": symbol,
                "action_type": action_type,
                "fmp_events": len(dates[FMP_PROVIDER]),
                "yahoo_finance_events": len(dates[YAHOO_FINANCE_PROVIDER]),
                "exact_ex_date_matches": len(shared),
            }
        )
    return {
        "by_symbol_action_type": entries,
        "total_exact_ex_date_matches": sum(item["exact_ex_date_matches"] for item in entries),
    }


def _conflict_report(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {
        "dividend_ex_date_mismatch": [],
        "dividend_amount_mismatch": [],
        "split_date_mismatch": [],
        "split_factor_mismatch": [],
    }
    grouped: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        provider = _text(row.get("provider"))
        action_type = _text(row.get("action_type"))
        symbol = _text(row.get("symbol"))
        if provider in {FMP_PROVIDER, YAHOO_FINANCE_PROVIDER} and action_type in ACTION_TYPES and symbol:
            grouped[(symbol.upper(), action_type)][provider].append(row)
    for (symbol, action_type), providers in grouped.items():
        fmp = providers[FMP_PROVIDER]
        yahoo = providers[YAHOO_FINANCE_PROVIDER]
        if not fmp or not yahoo:
            continue
        fmp_by_date = {row["ex_date"]: row for row in fmp if _coerce_date(row.get("ex_date"))}
        yahoo_by_date = {row["ex_date"]: row for row in yahoo if _coerce_date(row.get("ex_date"))}
        unmatched_fmp = sorted(set(fmp_by_date) - set(yahoo_by_date))
        unmatched_yahoo = sorted(set(yahoo_by_date) - set(fmp_by_date))
        date_bucket = "dividend_ex_date_mismatch" if action_type == DIVIDEND else "split_date_mismatch"
        for ex_date in unmatched_fmp:
            buckets[date_bucket].append({"symbol": symbol, "fmp_ex_date": ex_date, "yahoo_ex_date": None})
        for ex_date in unmatched_yahoo:
            buckets[date_bucket].append({"symbol": symbol, "fmp_ex_date": None, "yahoo_ex_date": ex_date})
        for ex_date in sorted(set(fmp_by_date) & set(yahoo_by_date)):
            fmp_row, yahoo_row = fmp_by_date[ex_date], yahoo_by_date[ex_date]
            if action_type == DIVIDEND:
                fmp_amount = _coerce_number(fmp_row.get("cash_amount"))
                yahoo_amount = _coerce_number(yahoo_row.get("cash_amount"))
                if fmp_amount is not None and yahoo_amount is not None and not _numbers_equal(fmp_amount, yahoo_amount):
                    buckets["dividend_amount_mismatch"].append(
                        {"symbol": symbol, "ex_date": ex_date, "fmp_cash_amount": fmp_amount, "yahoo_cash_amount": yahoo_amount}
                    )
            else:
                fmp_factor = _coerce_number(fmp_row.get("split_factor"))
                yahoo_factor = _coerce_number(yahoo_row.get("split_factor"))
                if fmp_factor is not None and yahoo_factor is not None and not _numbers_equal(fmp_factor, yahoo_factor):
                    buckets["split_factor_mismatch"].append(
                        {"symbol": symbol, "ex_date": ex_date, "fmp_split_factor": fmp_factor, "yahoo_split_factor": yahoo_factor}
                    )
    return {name: {"count": len(examples), "examples": examples[:_MAX_EXAMPLES]} for name, examples in buckets.items()}


def _new_report(*, symbols: list[str]) -> dict[str, Any]:
    return {
        "status": "skipped",
        "mode": "shadow",
        "reason": None,
        "symbols": symbols,
        "providers": {
            FMP_PROVIDER: {"enabled": False, "skip_reason": "not_allowlisted"},
            YAHOO_FINANCE_PROVIDER: {"enabled": False, "skip_reason": "not_allowlisted"},
        },
        "live_calls": 0,
        "raw_cache_hits": 0,
        "status_counts": {status: 0 for status in sorted(_RAW_STATUSES)},
        "status_counts_by_provider": {},
        "symbol_statuses": {},
        "provider_observations": {"planned": 0, "written": 0},
        "coverage": {},
        "overlap_fmp_vs_yahoo": {},
        "conflicts": {},
    }


def _enabled_providers(*, report: dict[str, Any], env: Mapping[str, str]) -> list[str]:
    allowlisted = _provider_allowlist(env)
    enabled: list[str] = []
    if FMP_PROVIDER in allowlisted:
        if _fmp_api_key(env):
            report["providers"][FMP_PROVIDER] = {"enabled": True, "skip_reason": None}
            enabled.append(FMP_PROVIDER)
        else:
            report["providers"][FMP_PROVIDER] = {"enabled": False, "skip_reason": "fmp_api_key_missing"}
    if YAHOO_FINANCE_PROVIDER in allowlisted:
        report["providers"][YAHOO_FINANCE_PROVIDER] = {"enabled": True, "skip_reason": None}
        enabled.append(YAHOO_FINANCE_PROVIDER)
    return enabled


def _skipped_report(report: dict[str, Any], reason: str) -> dict[str, Any]:
    report["status"] = "skipped"
    report["reason"] = reason
    return report


def _increment_status(report: dict[str, Any], *, provider: str, status: str) -> None:
    normalized = status if status in _RAW_STATUSES else "error"
    report["status_counts"][normalized] += 1
    by_provider = report["status_counts_by_provider"].setdefault(
        provider, {known_status: 0 for known_status in sorted(_RAW_STATUSES)}
    )
    by_provider[normalized] += 1


def _fmp_endpoint(action_type: str) -> str:
    if action_type == DIVIDEND:
        return FMP_DIVIDENDS_ENDPOINT
    if action_type == SPLIT:
        return FMP_SPLITS_ENDPOINT
    raise ValueError(f"Unsupported corporate action type: {action_type}")


def _yahoo_endpoint(action_type: str) -> str:
    if action_type == DIVIDEND:
        return YAHOO_DIVIDENDS_ENDPOINT
    if action_type == SPLIT:
        return YAHOO_SPLITS_ENDPOINT
    raise ValueError(f"Unsupported corporate action type: {action_type}")


def _provider_endpoint(*, provider: str, action_type: str) -> str:
    if provider == FMP_PROVIDER:
        return _fmp_endpoint(action_type)
    if provider == YAHOO_FINANCE_PROVIDER:
        return _yahoo_endpoint(action_type)
    raise ValueError(f"Unsupported corporate-actions provider: {provider}")


def _fetch_failure(
    *,
    provider: str,
    provider_symbol: str,
    action_type: str,
    endpoint: str,
    request_params: dict[str, str],
    request_hash: str,
    observed_at: datetime,
    status: str,
    http_status: int | None,
    error_reason: str,
    response_payload: Any | None = None,
) -> CorporateActionsFetchResult:
    return CorporateActionsFetchResult(
        provider=provider,
        provider_symbol=provider_symbol,
        action_type=action_type,
        endpoint=endpoint,
        request_params=request_params,
        request_hash=request_hash,
        status=status,
        http_status=http_status,
        response_payload=response_payload,
        error_reason=error_reason,
        provider_updated_at=None,
        observed_at=observed_at,
    )


def _symbols(symbols: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(_symbol(symbol) for symbol in symbols if _symbol(symbol)))


def _symbol(value: object | None) -> str:
    return str(value or "").strip().upper()


def _text(value: object | None) -> str | None:
    token = str(value).strip() if value is not None else ""
    return token or None


def _coerce_date(value: object | None) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    try:
        return date.fromisoformat(token[:10])
    except ValueError:
        return None


def _coerce_datetime(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if value is None:
        return None
    token = str(value).strip().replace("Z", "+00:00")
    if not token:
        return None
    try:
        return _utc(datetime.fromisoformat(token))
    except ValueError:
        return None


def _coerce_number(value: object | None) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number and number not in {float("inf"), float("-inf")} else None


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if payload.get(key) is not None:
            return payload[key]
    return None


def _payload_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "historical", "dividends", "splits"):
            if isinstance(payload.get(key), list):
                return [dict(row) for row in payload[key] if isinstance(row, dict)]
        return [dict(payload)] if payload else []
    return []


def _provider_error_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(key.lower() in {"error", "error message", "message"} for key in payload)


def _response_json_or_none(response: Any) -> Any | None:
    try:
        return response.json()
    except (TypeError, ValueError):
        return None


def _latest_provider_updated_at(rows: list[dict[str, Any]]) -> datetime | None:
    values = [_coerce_datetime(_first(row, "lastUpdated", "updatedAt", "updated_at")) for row in rows]
    return max((value for value in values if value is not None), default=None)


def _ratio_components(
    ratio_text: str, *, numerator: float | None, denominator: float | None
) -> tuple[float | None, float | None]:
    for separator in (":", "/"):
        if separator in ratio_text:
            left, right = ratio_text.split(separator, 1)
            return numerator or _coerce_number(left), denominator or _coerce_number(right)
    return numerator, denominator


def _numbers_equal(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-9, max(abs(left), abs(right)) * 1e-9)


def _yahoo_series_payload(series: Any, *, action_type: str) -> list[dict[str, Any]]:
    items = series.items() if hasattr(series, "items") else []
    rows = []
    for provider_date, value in items:
        event_date = _coerce_date(provider_date)
        number = _coerce_number(value)
        if event_date is None or number is None:
            continue
        row: dict[str, Any] = {"date": event_date.isoformat(), "symbol": None}
        if action_type == DIVIDEND:
            row["cashAmount"] = number
        else:
            row["splitFactor"] = number
        rows.append(row)
    return rows


def _yfinance_ticker(symbol: str) -> Any:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise RuntimeError("yfinance is required for Yahoo corporate-actions shadow ingestion.") from exc
    return yf.Ticker(symbol)


def _request_hash(
    *, provider: str, endpoint: str, provider_symbol: str, action_type: str, request_params: dict[str, str]
) -> str:
    return _payload_hash(
        {
            "kind": "corporate_action_provider_request.v1",
            "provider": provider,
            "endpoint": endpoint,
            "provider_symbol": provider_symbol,
            "action_type": action_type,
            "request_params": _sanitize_request_params(request_params),
        }
    )


def _raw_id(
    *, provider: str, endpoint: str, provider_symbol: str, action_type: str,
    request_hash: str, status: str, raw_payload_hash: str | None,
) -> str:
    return _payload_hash(
        {
            "kind": "corporate_action_provider_raw.v1",
            "provider": provider,
            "endpoint": endpoint,
            "provider_symbol": provider_symbol,
            "action_type": action_type,
            "request_hash": request_hash,
            "status": status,
            "raw_payload_hash": raw_payload_hash,
        }
    )


def _payload_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=_json_default, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _sanitize_request_params(params: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(key): "[redacted]" if _sensitive_key(str(key)) else str(value)
        for key, value in params.items()
        if value is not None
    }


def _sanitize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[redacted]" if _sensitive_key(str(key)) else _sanitize_payload(inner)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_payload(item) for item in value]
    return value


def _sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(token in lowered for token in ("apikey", "api_key", "token", "secret", "password"))


def _safe_error_reason(value: object | None) -> str | None:
    token = _text(value)
    if token is None:
        return None
    return "provider_error" if any(word in token.lower() for word in ("apikey", "api_key", "token", "secret", "password")) else token[:160]


def _provider_allowlist(env: Mapping[str, str]) -> set[str]:
    return {
        part.strip().lower()
        for part in str(env.get("DATA_OPS_CORPORATE_ACTION_PROVIDERS") or "").split(",")
        if part.strip()
    }


def _fmp_api_key(env: Mapping[str, str]) -> str:
    return str(env.get("FMP_API_KEY") or env.get("DATA_OPS_FMP_API_KEY") or "").strip()


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _to_psycopg_dsn(dsn: str) -> str:
    token = str(dsn or "").strip()
    prefix = "postgresql+psycopg://"
    return "postgresql://" + token.removeprefix(prefix) if token.startswith(prefix) else token


def _connect(database_dsn: str):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("psycopg[binary] is required for corporate-actions shadow ingestion.") from exc
    return psycopg.connect(
        database_dsn,
        autocommit=True,
        application_name="finance-data-ops-corporate-actions-shadow",
    )


def _validate_raw_row(row: Mapping[str, Any]) -> None:
    if row.get("status") not in _RAW_STATUSES:
        raise ValueError("Corporate-actions raw status is invalid.")
    if row.get("action_type") not in ACTION_TYPES:
        raise ValueError("Corporate-actions action type is invalid.")
    if not row.get("raw_id") or not row.get("request_hash"):
        raise ValueError("Corporate-actions raw row requires raw_id and request_hash.")
