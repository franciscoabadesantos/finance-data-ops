"""FMP earnings shadow ingestion.

This module owns external FMP I/O, raw-response retention, and normalized FMP
provider observations. It deliberately never writes ``source_cache.earnings``
or the Feature Store canonical table; arbitration is a later phase.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
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
FMP_EARNINGS_ENDPOINT = "https://financialmodelingprep.com/stable/earnings"
REVENUE_SANITY_RECENT_PERIOD_LIMIT = 8
_CACHEABLE_RAW_STATUSES = {"success", "not_found"}
_RAW_STATUSES = {"success", "not_found", "rate_limited", "error"}


class FmpEarningsShadowRepository(Protocol):
    def find_cached_raw(self, *, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None:
        ...

    def upsert_raw(self, row: dict[str, Any]) -> None:
        ...

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        ...

    def load_yahoo_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        ...

    def load_quarterly_revenue_statement_items(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        ...


class FmpHttpClient(Protocol):
    def fetch(self, *, symbol: str, observed_at: datetime) -> "FmpFetchResult":
        ...


@dataclass(frozen=True, slots=True)
class FmpFetchResult:
    provider_symbol: str
    endpoint: str
    request_params: dict[str, str]
    request_hash: str
    status: str
    http_status: int | None
    response_payload: Any | None
    error_payload: dict[str, Any] | None
    observed_at: datetime

    def to_raw_row(self) -> dict[str, Any]:
        response_hash = _payload_hash(self.response_payload) if self.response_payload is not None else None
        raw_payload_ref = _raw_payload_ref(
            endpoint=self.endpoint,
            provider_symbol=self.provider_symbol,
            request_hash=self.request_hash,
            status=self.status,
            response_hash=response_hash,
        )
        observed_at = _utc(self.observed_at)
        return {
            "raw_payload_ref": raw_payload_ref,
            "provider": FMP_PROVIDER,
            "endpoint": self.endpoint,
            "provider_symbol": self.provider_symbol,
            "symbol": self.provider_symbol,
            "request_params": dict(self.request_params),
            "request_hash": self.request_hash,
            "http_status": self.http_status,
            "status": self.status,
            "provider_updated_at": None,
            "first_seen_at": observed_at,
            "last_seen_at": observed_at,
            "ingested_at": observed_at,
            "response_hash": response_hash,
            "response_payload": self.response_payload,
            "error_payload": self.error_payload,
            "data_quality_flags": {},
        }


class FinancialModelingPrepEarningsClient:
    """Small FMP client with the API key isolated to an HTTP header."""

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

    def fetch(self, *, symbol: str, observed_at: datetime) -> FmpFetchResult:
        provider_symbol = _symbol(symbol)
        request_params = {"symbol": provider_symbol}
        request_hash = _request_hash(endpoint=FMP_EARNINGS_ENDPOINT, provider_symbol=provider_symbol)
        try:
            response = self._session.get(
                FMP_EARNINGS_ENDPOINT,
                params=request_params,
                headers={"apikey": self._api_key},
                timeout=self._timeout_seconds,
            )
        except requests.RequestException as exc:
            return FmpFetchResult(
                provider_symbol=provider_symbol,
                endpoint=FMP_EARNINGS_ENDPOINT,
                request_params=request_params,
                request_hash=request_hash,
                status="error",
                http_status=None,
                response_payload=None,
                error_payload={"error": exc.__class__.__name__, "message": str(exc)},
                observed_at=observed_at,
            )

        if response.status_code == 429:
            return _http_failure_result(
                provider_symbol=provider_symbol,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                http_status=response.status_code,
                status="rate_limited",
                response=response,
            )
        if response.status_code == 404:
            return _http_failure_result(
                provider_symbol=provider_symbol,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                http_status=response.status_code,
                status="not_found",
                response=response,
            )
        if response.status_code < 200 or response.status_code >= 300:
            return _http_failure_result(
                provider_symbol=provider_symbol,
                request_params=request_params,
                request_hash=request_hash,
                observed_at=observed_at,
                http_status=response.status_code,
                status="error",
                response=response,
            )
        try:
            payload = response.json()
        except ValueError:
            return FmpFetchResult(
                provider_symbol=provider_symbol,
                endpoint=FMP_EARNINGS_ENDPOINT,
                request_params=request_params,
                request_hash=request_hash,
                status="error",
                http_status=response.status_code,
                response_payload=None,
                error_payload={"error": "invalid_json_response"},
                observed_at=observed_at,
            )
        provider_error = _provider_error_payload(payload)
        if provider_error is not None:
            return FmpFetchResult(
                provider_symbol=provider_symbol,
                endpoint=FMP_EARNINGS_ENDPOINT,
                request_params=request_params,
                request_hash=request_hash,
                status="error",
                http_status=response.status_code,
                response_payload=payload,
                error_payload=provider_error,
                observed_at=observed_at,
            )
        status = "success" if _extract_event_rows(payload) else "not_found"
        return FmpFetchResult(
            provider_symbol=provider_symbol,
            endpoint=FMP_EARNINGS_ENDPOINT,
            request_params=request_params,
            request_hash=request_hash,
            status=status,
            http_status=response.status_code,
            response_payload=payload,
            error_payload=None,
            observed_at=observed_at,
        )


class PostgresFmpEarningsShadowRepository:
    """Postgres implementation for the Phase 0/1 multi-source tables."""

    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL is required for FMP shadow ingestion.")
        self._publisher = PostgresPublisher(
            database_dsn=self._database_dsn,
            application_name="finance-data-ops-fmp-earnings-shadow",
        )

    def find_cached_raw(self, *, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_payload_ref, provider, endpoint, provider_symbol, symbol,
                           request_params, request_hash, http_status, status,
                           provider_updated_at, first_seen_at, last_seen_at, ingested_at,
                           response_hash, response_payload, error_payload, data_quality_flags
                    FROM source_cache.earnings_provider_raw
                    WHERE provider = %s
                      AND endpoint = %s
                      AND provider_symbol = %s
                      AND request_hash = %s
                      AND status = ANY(%s)
                    ORDER BY last_seen_at DESC
                    LIMIT 1
                    """,
                    (FMP_PROVIDER, endpoint, provider_symbol, request_hash, sorted(_CACHEABLE_RAW_STATUSES)),
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
            raise RuntimeError("psycopg JSON adapters are required for FMP shadow ingestion.") from exc
        columns = (
            "raw_payload_ref", "provider", "endpoint", "provider_symbol", "symbol",
            "request_params", "request_hash", "http_status", "status", "provider_updated_at",
            "first_seen_at", "last_seen_at", "ingested_at", "response_hash", "response_payload",
            "error_payload", "data_quality_flags",
        )
        values = [
            Jsonb(row[column]) if column in {"request_params", "response_payload", "error_payload", "data_quality_flags"} and row.get(column) is not None else row.get(column)
            for column in columns
        ]
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO source_cache.earnings_provider_raw (
                        raw_payload_ref, provider, endpoint, provider_symbol, symbol,
                        request_params, request_hash, http_status, status, provider_updated_at,
                        first_seen_at, last_seen_at, ingested_at, response_hash, response_payload,
                        error_payload, data_quality_flags
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (raw_payload_ref) DO UPDATE SET
                        last_seen_at = EXCLUDED.last_seen_at,
                        ingested_at = EXCLUDED.ingested_at,
                        http_status = EXCLUDED.http_status,
                        error_payload = EXCLUDED.error_payload,
                        data_quality_flags = EXCLUDED.data_quality_flags
                    """,
                    values,
                )

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert(
            "source_cache.earnings_event_provider_observations",
            rows,
            on_conflict="provider_observation_id",
        )

    def load_yahoo_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        if not symbols:
            return []
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT provider, symbol, fiscal_period, report_date, eps_actual,
                           eps_estimate, revenue_actual, revenue_estimate
                    FROM source_cache.earnings_event_provider_observations
                    WHERE provider = %s AND UPPER(symbol) = ANY(%s)
                    """,
                    ("yahoo_finance", symbols),
                )
                columns = [description.name for description in cur.description]
                return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    def load_quarterly_revenue_statement_items(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        if not symbols:
            return []
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, value, currency, fiscal_year, fiscal_quarter,
                           period_end, known_at, source
                    FROM feature_store.financial_statement_line_items
                    WHERE statement_type = 'income_statement'
                      AND line_item_id = 'revenue'
                      AND period_type = 'quarterly'
                      AND UPPER(symbol) = ANY(%s)
                    ORDER BY symbol, known_at DESC, period_end DESC
                    """,
                    (symbols,),
                )
                columns = [description.name for description in cur.description]
                return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def run_fmp_earnings_shadow(
    *,
    symbols: Iterable[str],
    repository: FmpEarningsShadowRepository | None = None,
    client: FmpHttpClient | None = None,
    env: dict[str, str] | None = None,
    dry_run: bool = False,
    refresh: bool = False,
    request_sleep_seconds: float = 0.25,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Run manual FMP shadow ingestion without touching canonical earnings."""
    requested_symbols = _symbols(symbols)
    env_map = dict(os.environ) if env is None else dict(env)
    api_key = _fmp_api_key(env_map)
    report = _new_shadow_report(symbols=requested_symbols)

    if not api_key:
        return _skipped_report(report, "fmp_api_key_missing")
    if FMP_PROVIDER not in _provider_allowlist(env_map):
        return _skipped_report(report, "fmp_not_allowlisted")
    report["fmp"] = {"enabled": True, "skip_reason": None}
    if dry_run:
        report["status"] = "dry_run"
        report["provider_observations"]["planned"] = len(requested_symbols)
        return report
    if repository is None:
        raise ValueError("repository is required when FMP shadow ingestion is enabled.")
    client = client or FinancialModelingPrepEarningsClient(api_key=api_key)
    now_fn = now or (lambda: datetime.now(UTC))
    fmp_observations: list[dict[str, Any]] = []

    for index, symbol in enumerate(requested_symbols):
        observed_at = _utc(now_fn())
        request_hash = _request_hash(endpoint=FMP_EARNINGS_ENDPOINT, provider_symbol=symbol)
        cached = None if refresh else repository.find_cached_raw(
            endpoint=FMP_EARNINGS_ENDPOINT,
            provider_symbol=symbol,
            request_hash=request_hash,
        )
        if cached is not None:
            report["raw_cache_hits"] += 1
            raw_row = dict(cached)
            status = str(raw_row.get("status") or "error")
            repository.upsert_raw(raw_row)
        else:
            result = client.fetch(symbol=symbol, observed_at=observed_at)
            raw_row = result.to_raw_row()
            status = result.status
            report["live_calls"] += 1
            repository.upsert_raw(raw_row)
            if index < len(requested_symbols) - 1 and request_sleep_seconds > 0:
                time.sleep(float(request_sleep_seconds))

        _increment_status(report, status)
        report["symbol_statuses"][symbol] = _symbol_status(raw_row=raw_row, status=status)
        if status != "success":
            continue
        observations = normalize_fmp_provider_observations(
            raw_row=raw_row,
            known_at=observed_at.date(),
            ingested_at=observed_at,
        )
        if observations:
            repository.upsert_provider_observations(observations)
            report["provider_observations"]["written"] += len(observations)
            fmp_observations.extend(observations)

    report["coverage"] = _coverage(fmp_observations)
    yahoo_observations = repository.load_yahoo_observations(symbols=requested_symbols)
    report["overlap_with_yahoo"] = _overlap_count(fmp_observations, yahoo_observations)
    report["conflicts"] = build_yahoo_conflict_report(fmp_observations, yahoo_observations)
    try:
        statement_revenue_rows = repository.load_quarterly_revenue_statement_items(symbols=requested_symbols)
    except Exception:  # Shadow quality must not block FMP raw/provider persistence.
        report["revenue_sanity"] = _unavailable_revenue_sanity_report()
    else:
        report["revenue_sanity"] = build_revenue_sanity_report(fmp_observations, statement_revenue_rows)
        if fmp_observations:
            repository.upsert_provider_observations(
                _with_revenue_sanity_flags(fmp_observations, revenue_sanity=report["revenue_sanity"])
            )
    return report


def normalize_fmp_provider_observations(
    *,
    raw_row: dict[str, Any],
    known_at: date,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    """Normalize FMP rows without deriving timing, confirmation, or revenue."""
    if str(raw_row.get("status") or "") != "success":
        return []
    provider_symbol = _symbol(raw_row.get("provider_symbol"))
    raw_payload_ref = _text(raw_row.get("raw_payload_ref"))
    raw_payload_hash = _text(raw_row.get("response_hash"))
    records: list[dict[str, Any]] = []
    for payload in _extract_event_rows(raw_row.get("response_payload")):
        symbol = _symbol(payload.get("symbol") or provider_symbol)
        report_date = _coerce_date(_first(payload, "date", "reportDate", "earningsDate", "earnings_date"))
        if report_date is None:
            continue
        fiscal_period, fiscal_flags = _fiscal_period(payload)
        provider_event_id = _text(_first(payload, "id", "eventId", "event_id"))
        eps_actual = _coerce_number(_first(payload, "epsActual", "actualEPS", "actualEps", "eps"))
        eps_estimate = _coerce_number(_first(payload, "epsEstimated", "epsEstimate", "estimatedEPS", "estimatedEps"))
        revenue_actual = _coerce_number(_first(payload, "revenueActual", "actualRevenue", "revenue"))
        revenue_estimate = _coerce_number(_first(payload, "revenueEstimated", "revenueEstimate", "estimatedRevenue"))
        flags: dict[str, Any] = {
            "timezoneUnavailable": True,
            "confirmationNotProvidedBySource": True,
            "beforeAfterMarketNotProvidedBySource": True,
            "epsSurpriseUnavailable": True,
            "revenueSurpriseUnavailable": True,
            **fiscal_flags,
        }
        observation_fields = {
            "provider": FMP_PROVIDER,
            "provider_event_id": provider_event_id,
            "provider_symbol": provider_symbol,
            "symbol": symbol,
            "fiscal_period": fiscal_period,
            "report_date": report_date,
            "report_time": None,
            "timezone": None,
            "before_after_market": None,
            "event_status": _event_status(
                eps_actual=eps_actual,
                eps_estimate=eps_estimate,
                revenue_actual=revenue_actual,
                revenue_estimate=revenue_estimate,
            ),
            "is_confirmed": None,
            "eps_actual": eps_actual,
            "eps_estimate": eps_estimate,
            "eps_surprise": None,
            "revenue_actual": revenue_actual,
            "revenue_estimate": revenue_estimate,
            "revenue_surprise": None,
            "currency": _text(_first(payload, "currency", "reportedCurrency")),
            "provider_updated_at": _coerce_datetime(_first(payload, "lastUpdated", "updatedAt", "updated_from_date")),
            "known_at": known_at,
            "ingested_at": _utc(ingested_at),
            "raw_payload_ref": raw_payload_ref,
            "raw_payload_hash": raw_payload_hash,
            "data_quality_flags": flags,
        }
        observation_fields["provider_observation_id"] = _provider_observation_id(observation_fields)
        observation_fields["observation_hash"] = _payload_hash(
            {key: value for key, value in observation_fields.items() if key not in {"provider_observation_id", "ingested_at", "observation_hash"}}
        )
        records.append(observation_fields)
    return sorted(records, key=lambda row: (row["symbol"], row["report_date"], row["fiscal_period"], row["provider_observation_id"]))


def build_yahoo_conflict_report(
    fmp_observations: list[dict[str, Any]],
    yahoo_observations: list[dict[str, Any]],
) -> dict[str, Any]:
    conflicts = {
        "report_date_mismatch": [],
        "fiscal_period_mismatch": [],
        "fmp_fiscal_period_unavailable": [],
        "eps_actual_mismatch": [],
        "eps_estimate_mismatch": [],
        "revenue_available_only_in_fmp": [],
    }
    eps_buckets = {
        "small_rounding_delta": [],
        "small_consensus_delta": [],
        "large_eps_conflict": [],
    }
    for fmp in fmp_observations:
        if _fiscal_period_unavailable(fmp):
            conflicts["fmp_fiscal_period_unavailable"].append(_quality_example(fmp=fmp, yahoo=None))
        matches = _yahoo_matches(fmp, yahoo_observations)
        if not matches:
            continue
        yahoo = matches[0]
        symbol = str(fmp["symbol"])
        if fmp.get("report_date") != yahoo.get("report_date"):
            conflicts["report_date_mismatch"].append(symbol)
        if not _fiscal_period_unavailable(fmp) and fmp.get("fiscal_period") != yahoo.get("fiscal_period"):
            conflicts["fiscal_period_mismatch"].append(symbol)
        if _different_numbers(fmp.get("eps_actual"), yahoo.get("eps_actual")):
            conflicts["eps_actual_mismatch"].append(symbol)
        if _different_numbers(fmp.get("eps_estimate"), yahoo.get("eps_estimate")):
            conflicts["eps_estimate_mismatch"].append(symbol)
        if _fmp_only_revenue(fmp, yahoo):
            conflicts["revenue_available_only_in_fmp"].append(symbol)
        eps_bucket = _eps_conflict_bucket(fmp=fmp, yahoo=yahoo)
        if eps_bucket is not None:
            eps_buckets[eps_bucket].append(_quality_example(fmp=fmp, yahoo=yahoo))
    report = {
        name: _conflict_summary(values)
        for name, values in conflicts.items()
    }
    report["eps_conflicts"] = {
        "thresholds": {
            "small_rounding_delta_max": 0.02,
            "small_consensus_delta_max": 0.05,
        },
        **{
            name: _examples_summary(values, sort_by_delta=name == "large_eps_conflict")
            for name, values in eps_buckets.items()
        },
    }
    return report


def build_revenue_sanity_report(
    fmp_observations: list[dict[str, Any]],
    statement_revenue_rows: list[dict[str, Any]],
    *,
    recent_period_limit: int = REVENUE_SANITY_RECENT_PERIOD_LIMIT,
) -> dict[str, Any]:
    """Compare recent revenue sequences without treating statement known_at as an earnings date."""
    if recent_period_limit < 1:
        raise ValueError("recent_period_limit must be positive.")
    exact_matches: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    missing_comparators: list[dict[str, Any]] = []
    scale_anomalies: list[dict[str, Any]] = []
    fmp_rows_by_symbol = _recent_fmp_revenue_rows_by_symbol(
        fmp_observations,
        recent_period_limit=recent_period_limit,
    )
    statement_rows_by_symbol = _recent_statement_revenue_rows_by_symbol(
        statement_revenue_rows,
        recent_period_limit=recent_period_limit,
    )
    sequence_rows_considered = 0
    symbol_summaries: dict[str, dict[str, int]] = {
        symbol: {
            "exact_matches": 0,
            "mismatches": 0,
            "missing_statement_comparators": 0,
            "scale_anomalies": 0,
        }
        for symbol in fmp_rows_by_symbol
    }
    for symbol, fmp_rows in fmp_rows_by_symbol.items():
        statement_rows = statement_rows_by_symbol.get(symbol, [])
        for rank, fmp in enumerate(fmp_rows, start=1):
            sequence_rows_considered += 1
            fmp_revenue = _coerce_number(fmp.get("revenue_actual"))
            if rank > len(statement_rows):
                symbol_summaries[symbol]["missing_statement_comparators"] += 1
                missing_comparators.append(
                    {
                        **_quality_example(fmp=fmp, yahoo=None),
                        "sequence_rank": rank,
                        "reason": "no_recent_statement_sequence_comparator",
                    }
                )
                continue
            statement = statement_rows[rank - 1]
            statement_revenue = _coerce_number(statement.get("value"))
            if statement_revenue is None:
                symbol_summaries[symbol]["missing_statement_comparators"] += 1
                missing_comparators.append(
                    {
                        **_quality_example(fmp=fmp, yahoo=None),
                        "sequence_rank": rank,
                        "statement_period_end": _coerce_date(statement.get("period_end")),
                        "statement_known_at": _coerce_date(statement.get("known_at")),
                        "reason": "statement_revenue_value_missing",
                    }
                )
                continue
            example = {
                **_quality_example(fmp=fmp, yahoo=None),
                "sequence_rank": rank,
                "statement_period_end": _coerce_date(statement.get("period_end")),
                "statement_known_at": _coerce_date(statement.get("known_at")),
                "statement_revenue_actual": statement_revenue,
                "revenue_delta": fmp_revenue - statement_revenue,
            }
            if _same_number(fmp_revenue, statement_revenue):
                symbol_summaries[symbol]["exact_matches"] += 1
                exact_matches.append(example)
                continue
            symbol_summaries[symbol]["mismatches"] += 1
            mismatches.append(example)
            scale_factor = _scale_anomaly_factor(fmp_revenue, statement_revenue)
            if scale_factor is not None:
                symbol_summaries[symbol]["scale_anomalies"] += 1
                scale_anomalies.append({**example, "scale_factor": scale_factor})
    return {
        "status": "completed",
        "mapping_policy": "recent_report_date_to_recent_statement_period_sequence",
        "recent_period_limit_per_symbol": recent_period_limit,
        "purpose": "sanity_check_only_not_pit_validation_or_arbitration",
        "fmp_revenue_actual_rows": sum(
            _coerce_number(row.get("revenue_actual")) is not None for row in fmp_observations
        ),
        "sequence_rows_considered": sequence_rows_considered,
        "exact_matches": _examples_summary(exact_matches),
        "mismatches": _examples_summary(mismatches, sort_by_delta=True),
        "missing_statement_comparators": _examples_summary(missing_comparators),
        "scale_anomalies": _examples_summary(scale_anomalies, sort_by_delta=True),
        "by_symbol": {
            symbol: {
                **summary,
                "status": _revenue_sanity_status(summary),
            }
            for symbol, summary in sorted(symbol_summaries.items())
        },
    }


def _recent_fmp_revenue_rows_by_symbol(
    fmp_observations: list[dict[str, Any]],
    *,
    recent_period_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in fmp_observations:
        symbol = _symbol(row.get("symbol"))
        if not symbol or _coerce_date(row.get("report_date")) is None:
            continue
        if _coerce_number(row.get("revenue_actual")) is None:
            continue
        rows_by_symbol.setdefault(symbol, []).append(row)
    return {
        symbol: sorted(
            rows,
            key=lambda row: (
                _coerce_date(row.get("report_date")) or date.min,
                _coerce_date(row.get("known_at")) or date.min,
                str(row.get("provider_observation_id") or ""),
            ),
            reverse=True,
        )[:recent_period_limit]
        for symbol, rows in rows_by_symbol.items()
    }


def _recent_statement_revenue_rows_by_symbol(
    statement_revenue_rows: list[dict[str, Any]],
    *,
    recent_period_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    latest_rows: dict[tuple[str, date], dict[str, Any]] = {}
    for row in statement_revenue_rows:
        symbol = _symbol(row.get("symbol"))
        period_end = _coerce_date(row.get("period_end"))
        if not symbol or period_end is None:
            continue
        key = (symbol, period_end)
        current = latest_rows.get(key)
        if current is None or _statement_observation_sort_key(row) > _statement_observation_sort_key(current):
            latest_rows[key] = row
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for (symbol, _), row in latest_rows.items():
        rows_by_symbol.setdefault(symbol, []).append(row)
    return {
        symbol: sorted(
            rows,
            key=lambda row: _coerce_date(row.get("period_end")) or date.min,
            reverse=True,
        )[:recent_period_limit]
        for symbol, rows in rows_by_symbol.items()
    }


def _statement_observation_sort_key(row: dict[str, Any]) -> tuple[date, str, float]:
    value = _coerce_number(row.get("value"))
    return (
        _coerce_date(row.get("known_at")) or date.min,
        str(row.get("source") or ""),
        float("-inf") if value is None else value,
    )


def _revenue_sanity_status(summary: dict[str, int]) -> str:
    if summary["mismatches"] or summary["scale_anomalies"]:
        return "blocked"
    if summary["exact_matches"]:
        return "passed"
    return "unverified"


def _with_revenue_sanity_flags(
    observations: list[dict[str, Any]],
    *,
    revenue_sanity: dict[str, Any],
) -> list[dict[str, Any]]:
    """Attach per-symbol shadow-quality state without changing source values."""
    mapping_policy = _text(revenue_sanity.get("mapping_policy"))
    summaries = revenue_sanity.get("by_symbol")
    by_symbol = summaries if isinstance(summaries, dict) else {}
    flagged: list[dict[str, Any]] = []
    for observation in observations:
        symbol = _symbol(observation.get("symbol"))
        summary = by_symbol.get(symbol)
        quality = summary if isinstance(summary, dict) else {}
        flags = dict(observation.get("data_quality_flags") or {})
        flags["revenueSanity"] = {
            "status": _text(quality.get("status")) or "unverified",
            "mappingPolicy": mapping_policy,
            "exactMatches": int(quality.get("exact_matches") or 0),
            "mismatches": int(quality.get("mismatches") or 0),
            "scaleAnomalies": int(quality.get("scale_anomalies") or 0),
        }
        row = {**observation, "data_quality_flags": flags}
        row["observation_hash"] = _payload_hash(
            {
                key: value
                for key, value in row.items()
                if key not in {"provider_observation_id", "ingested_at", "observation_hash"}
            }
        )
        flagged.append(row)
    return flagged


def _symbol_status(*, raw_row: dict[str, Any], status: str) -> dict[str, Any]:
    error_payload = raw_row.get("error_payload")
    error_code = error_payload.get("error") if isinstance(error_payload, dict) else None
    return {
        "status": status if status in _RAW_STATUSES else "error",
        "http_status": raw_row.get("http_status"),
        "reason": _text(error_code),
    }


def _fiscal_period_unavailable(row: dict[str, Any]) -> bool:
    return _text(row.get("fiscal_period")) in {None, "unknown"}


def _eps_conflict_bucket(*, fmp: dict[str, Any], yahoo: dict[str, Any]) -> str | None:
    actual_delta = _absolute_delta(fmp.get("eps_actual"), yahoo.get("eps_actual"))
    estimate_delta = _absolute_delta(fmp.get("eps_estimate"), yahoo.get("eps_estimate"))
    if actual_delta is None and estimate_delta is None:
        return None
    if (actual_delta is not None and actual_delta > 0.02) or (estimate_delta is not None and estimate_delta > 0.05):
        return "large_eps_conflict"
    if actual_delta is not None:
        return "small_rounding_delta"
    return "small_consensus_delta"


def _quality_example(*, fmp: dict[str, Any], yahoo: dict[str, Any] | None) -> dict[str, Any]:
    example = {
        "symbol": _symbol(fmp.get("symbol")),
        "report_date": _coerce_date(fmp.get("report_date")),
        "fmp_fiscal_period": _text(fmp.get("fiscal_period")),
        "fmp_eps_actual": _coerce_number(fmp.get("eps_actual")),
        "fmp_eps_estimate": _coerce_number(fmp.get("eps_estimate")),
        "fmp_revenue_actual": _coerce_number(fmp.get("revenue_actual")),
    }
    if yahoo is not None:
        example.update(
            {
                "yahoo_fiscal_period": _text(yahoo.get("fiscal_period")),
                "yahoo_eps_actual": _coerce_number(yahoo.get("eps_actual")),
                "yahoo_eps_estimate": _coerce_number(yahoo.get("eps_estimate")),
                "eps_actual_delta": _absolute_delta(fmp.get("eps_actual"), yahoo.get("eps_actual")),
                "eps_estimate_delta": _absolute_delta(fmp.get("eps_estimate"), yahoo.get("eps_estimate")),
            }
        )
    return example


def _conflict_summary(values: list[Any]) -> dict[str, Any]:
    if values and isinstance(values[0], dict):
        return _examples_summary(values)
    symbols = sorted(set(str(value) for value in values))
    return {"count": len(values), "symbols": symbols}


def _examples_summary(values: list[dict[str, Any]], *, sort_by_delta: bool = False) -> dict[str, Any]:
    if sort_by_delta:
        ordered = sorted(
            values,
            key=lambda row: max(
                abs(_coerce_number(row.get("eps_actual_delta")) or 0.0),
                abs(_coerce_number(row.get("eps_estimate_delta")) or 0.0),
                abs(_coerce_number(row.get("revenue_delta")) or 0.0),
            ),
            reverse=True,
        )
    else:
        ordered = sorted(values, key=lambda row: (str(row.get("symbol") or ""), str(row.get("report_date") or "")), reverse=True)
    return {"count": len(values), "examples": ordered[:5]}


def _absolute_delta(left: Any, right: Any) -> float | None:
    left_number, right_number = _coerce_number(left), _coerce_number(right)
    if left_number is None or right_number is None:
        return None
    return round(abs(left_number - right_number), 12)


def _same_number(left: float, right: float) -> bool:
    return abs(left - right) <= max(1e-6, abs(right) * 1e-9)


def _scale_anomaly_factor(left: float, right: float) -> int | None:
    if left == 0.0 or right == 0.0:
        return None
    ratio = abs(left / right)
    for factor in (1_000, 1_000_000, 1_000_000_000):
        if abs(ratio - factor) / factor <= 0.01 or abs(ratio - (1 / factor)) * factor <= 0.01:
            return factor
    return None


def _unavailable_revenue_sanity_report() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": "statement_comparator_unavailable",
        "mapping_policy": "recent_report_date_to_recent_statement_period_sequence",
    }


def _http_failure_result(
    *,
    provider_symbol: str,
    request_params: dict[str, str],
    request_hash: str,
    observed_at: datetime,
    http_status: int,
    status: str,
    response: Any,
) -> FmpFetchResult:
    payload = _response_json_or_none(response)
    error_payload: dict[str, Any] = {"error": f"http_{http_status}"}
    if payload is not None:
        error_payload["response"] = payload
    return FmpFetchResult(
        provider_symbol=provider_symbol,
        endpoint=FMP_EARNINGS_ENDPOINT,
        request_params=request_params,
        request_hash=request_hash,
        status=status,
        http_status=http_status,
        response_payload=payload,
        error_payload=error_payload,
        observed_at=observed_at,
    )


def _new_shadow_report(*, symbols: list[str]) -> dict[str, Any]:
    return {
        "status": "completed",
        "mode": "shadow",
        "provider": FMP_PROVIDER,
        "symbols_requested": symbols,
        "fmp": {"enabled": False, "skip_reason": None},
        "raw_cache_hits": 0,
        "live_calls": 0,
        "status_counts": {status: 0 for status in sorted(_RAW_STATUSES)},
        "symbol_statuses": {},
        "provider_observations": {"written": 0, "planned": 0},
        "coverage": {"revenue": 0, "eps": 0},
        "overlap_with_yahoo": 0,
        "conflicts": {},
        "revenue_sanity": {"status": "not_run"},
    }


def _skipped_report(report: dict[str, Any], reason: str) -> dict[str, Any]:
    report["status"] = "skipped"
    report["reason"] = reason
    report["fmp"] = {"enabled": False, "skip_reason": reason}
    return report


def _increment_status(report: dict[str, Any], status: str) -> None:
    normalized = status if status in _RAW_STATUSES else "error"
    report["status_counts"][normalized] += 1


def _coverage(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "revenue": sum(bool(row.get("revenue_actual") is not None or row.get("revenue_estimate") is not None) for row in rows),
        "eps": sum(bool(row.get("eps_actual") is not None or row.get("eps_estimate") is not None) for row in rows),
    }


def _overlap_count(fmp_rows: list[dict[str, Any]], yahoo_rows: list[dict[str, Any]]) -> int:
    return sum(bool(_yahoo_matches(row, yahoo_rows)) for row in fmp_rows)


def _yahoo_matches(fmp: dict[str, Any], yahoo_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    same_symbol = [row for row in yahoo_rows if _symbol(row.get("symbol")) == _symbol(fmp.get("symbol"))]
    fiscal_period = _text(fmp.get("fiscal_period"))
    if fiscal_period and fiscal_period != "unknown":
        matches = [row for row in same_symbol if _text(row.get("fiscal_period")) == fiscal_period]
        if matches:
            return matches
    report_date = _coerce_date(fmp.get("report_date"))
    return [row for row in same_symbol if _coerce_date(row.get("report_date")) == report_date]


def _fmp_only_revenue(fmp: dict[str, Any], yahoo: dict[str, Any]) -> bool:
    return any(
        fmp.get(field) is not None and yahoo.get(field) is None
        for field in ("revenue_actual", "revenue_estimate")
    )


def _different_numbers(left: Any, right: Any) -> bool:
    left_number, right_number = _coerce_number(left), _coerce_number(right)
    return left_number is not None and right_number is not None and abs(left_number - right_number) > 1e-12


def _fmp_api_key(env: dict[str, str]) -> str:
    return str(env.get("FMP_API_KEY") or env.get("DATA_OPS_FMP_API_KEY") or "").strip()


def _provider_allowlist(env: dict[str, str]) -> set[str]:
    return {part.strip().lower() for part in str(env.get("DATA_OPS_EARNINGS_PROVIDERS") or "").split(",") if part.strip()}


def _request_hash(*, endpoint: str, provider_symbol: str) -> str:
    return _payload_hash({"provider": FMP_PROVIDER, "endpoint": endpoint, "provider_symbol": provider_symbol})


def _raw_payload_ref(*, endpoint: str, provider_symbol: str, request_hash: str, status: str, response_hash: str | None) -> str:
    return _payload_hash({"kind": "fmp_earnings_raw.v1", "endpoint": endpoint, "provider_symbol": provider_symbol, "request_hash": request_hash, "status": status, "response_hash": response_hash})


def _provider_observation_id(row: dict[str, Any]) -> str:
    provider_event_id = _text(row.get("provider_event_id"))
    identity = {
        "kind": "fmp_earnings_provider_observation.v1",
        "provider": FMP_PROVIDER,
        "provider_event_id": provider_event_id,
        "symbol": row["symbol"],
        "fiscal_period": row["fiscal_period"],
        "report_date": row["report_date"],
        "known_at": row["known_at"],
    }
    return _payload_hash(identity)


def _payload_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=_json_default, separators=(",", ":")).encode("utf-8")).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _extract_event_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "earnings"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [dict(row) for row in candidate if isinstance(row, dict)]
        return [dict(payload)] if any(key in payload for key in ("date", "reportDate", "earningsDate")) else []
    return []


def _fiscal_period(payload: dict[str, Any]) -> tuple[str, dict[str, bool]]:
    explicit = _text(_first(payload, "fiscalPeriod", "fiscal_period", "period"))
    if explicit:
        return explicit, {}
    fiscal_date = _coerce_date(_first(payload, "fiscalDateEnding", "fiscal_date_ending"))
    if fiscal_date is None:
        return "unknown", {"fiscalPeriodUnavailable": True}
    return f"{fiscal_date.year}Q{((fiscal_date.month - 1) // 3) + 1}", {"fiscalPeriodDerivedFromFiscalDateEnding": True}


def _event_status(*, eps_actual: float | None, eps_estimate: float | None, revenue_actual: float | None, revenue_estimate: float | None) -> str:
    if eps_actual is not None or revenue_actual is not None:
        return "reported"
    if eps_estimate is not None or revenue_estimate is not None:
        return "estimated"
    return "scheduled"


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


def _response_json_or_none(response: Any) -> Any | None:
    try:
        return response.json()
    except (TypeError, ValueError):
        return None


def _provider_error_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    for key in ("Error Message", "error", "error_message"):
        message = _text(payload.get(key))
        if message:
            return {"error": "provider_error", "message": message}
    return None


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if value is None:
        return None
    normalized = str(value).strip().replace("Z", "+00:00")
    try:
        return _utc(datetime.fromisoformat(normalized))
    except ValueError:
        return None


def _coerce_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("FMP provider symbol is required.")
    return normalized


def _symbols(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = _symbol(value)
        if symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out


def _text(value: Any) -> str | None:
    normalized = str(value).strip() if value is not None else ""
    return normalized or None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _validate_raw_row(row: dict[str, Any]) -> None:
    if row.get("status") not in _RAW_STATUSES:
        raise ValueError("Invalid FMP raw status.")
    if row.get("provider") != FMP_PROVIDER:
        raise ValueError("FMP raw rows must use provider='fmp'.")


def _connect(database_dsn: str):
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("psycopg[binary] is required for FMP shadow ingestion.") from exc
    try:
        return psycopg.connect(
            _to_psycopg_dsn(database_dsn),
            autocommit=True,
            application_name="finance-data-ops-fmp-earnings-shadow",
        )
    except psycopg.Error:
        # Do not propagate driver conninfo errors because they can contain the
        # full DSN, including credentials.
        raise RuntimeError("Unable to connect to Postgres for FMP earnings shadow ingestion.") from None


def _to_psycopg_dsn(database_dsn: str) -> str:
    """Translate SQLAlchemy's psycopg URL dialect to psycopg conninfo syntax."""
    dsn = str(database_dsn or "").strip()
    prefix = "postgresql+psycopg://"
    if dsn.startswith(prefix):
        return "postgresql://" + dsn.removeprefix(prefix)
    return dsn
