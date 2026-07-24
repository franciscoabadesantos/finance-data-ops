from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from finance_data_ops.shadow.fmp_earnings import (
    FMP_EARNINGS_ENDPOINT,
    FinancialModelingPrepEarningsClient,
    FmpFetchResult,
    PostgresFmpEarningsShadowRepository,
    _to_psycopg_dsn,
    build_revenue_sanity_report,
    normalize_fmp_provider_observations,
    run_fmp_earnings_shadow,
)


@dataclass
class _Repository:
    cached_raw: dict[str, Any] | None = None
    yahoo_rows: list[dict[str, Any]] = field(default_factory=list)
    statement_revenue_rows: list[dict[str, Any]] = field(default_factory=list)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observation_rows: list[dict[str, Any]] = field(default_factory=list)

    def find_cached_raw(self, **_: Any) -> dict[str, Any] | None:
        return self.cached_raw

    def upsert_raw(self, row: dict[str, Any]) -> None:
        self.raw_rows.append(dict(row))

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self.observation_rows.extend(dict(row) for row in rows)

    def load_yahoo_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        assert symbols == ["AAPL"]
        return list(self.yahoo_rows)

    def load_quarterly_revenue_statement_items(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        assert symbols == ["AAPL"]
        return list(self.statement_revenue_rows)


@dataclass
class _Client:
    result: FmpFetchResult
    calls: list[str] = field(default_factory=list)

    def fetch(self, *, symbol: str, observed_at: datetime) -> FmpFetchResult:
        self.calls.append(symbol)
        return self.result


def _result(*, payload: Any, status: str = "success", http_status: int | None = 200) -> FmpFetchResult:
    return FmpFetchResult(
        provider_symbol="AAPL",
        endpoint=FMP_EARNINGS_ENDPOINT,
        request_params={"symbol": "AAPL"},
        request_hash="request-hash",
        status=status,
        http_status=http_status,
        response_payload=payload,
        error_payload=None,
        observed_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )


def _fmp_payload(**overrides: Any) -> list[dict[str, Any]]:
    row = {
        "symbol": "AAPL",
        "date": "2026-07-30",
        "fiscalDateEnding": "2026-06-30",
        "epsActual": 1.5,
        "epsEstimated": 1.4,
        "revenueActual": 100.0,
        "revenueEstimated": 99.0,
        "currency": "USD",
        "lastUpdated": "2026-07-24T08:00:00Z",
    }
    row.update(overrides)
    return [row]


def _enabled_env() -> dict[str, str]:
    return {"FMP_API_KEY": "test-key", "DATA_OPS_EARNINGS_PROVIDERS": "yahoo_finance,fmp"}


def test_sqlalchemy_psycopg_dsn_is_normalized_for_repository_reads_and_writes() -> None:
    sqlalchemy_dsn = "postgresql+psycopg://worker:password@example.invalid:5432/finance"

    repository = PostgresFmpEarningsShadowRepository(database_dsn=sqlalchemy_dsn)

    assert _to_psycopg_dsn(sqlalchemy_dsn) == "postgresql://worker:password@example.invalid:5432/finance"
    assert repository._database_dsn == "postgresql://worker:password@example.invalid:5432/finance"
    assert repository._publisher.database_dsn == "postgresql://worker:password@example.invalid:5432/finance"


def test_psycopg_dsn_is_left_unchanged() -> None:
    dsn = "postgresql://worker:password@example.invalid:5432/finance"

    assert _to_psycopg_dsn(dsn) == dsn


def test_fmp_without_key_is_cleanly_skipped_without_repository_or_http() -> None:
    report = run_fmp_earnings_shadow(symbols=["AAPL"], env={})

    assert report["status"] == "skipped"
    assert report["reason"] == "fmp_api_key_missing"
    assert report["live_calls"] == 0
    assert report["provider_observations"]["written"] == 0


def test_fmp_requires_explicit_provider_allowlist() -> None:
    report = run_fmp_earnings_shadow(symbols=["AAPL"], env={"FMP_API_KEY": "test-key"})

    assert report["status"] == "skipped"
    assert report["reason"] == "fmp_not_allowlisted"


def test_success_raw_writes_normalized_fmp_observation_and_preserves_revenue() -> None:
    repository = _Repository()
    client = _Client(_result(payload=_fmp_payload()))

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
        now=lambda: datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )

    assert report["status"] == "completed"
    assert report["live_calls"] == 1
    assert report["status_counts"]["success"] == 1
    assert report["provider_observations"]["written"] == 1
    assert repository.raw_rows[0]["provider"] == "fmp"
    assert repository.raw_rows[0]["request_params"] == {"symbol": "AAPL"}
    assert "apikey" not in repository.raw_rows[0]["request_params"]
    observation = repository.observation_rows[0]
    assert observation["provider"] == "fmp"
    assert observation["revenue_actual"] == 100.0
    assert observation["revenue_estimate"] == 99.0
    assert observation["raw_payload_ref"] == repository.raw_rows[0]["raw_payload_ref"]
    assert report["coverage"] == {"revenue": 1, "eps": 1}


def test_fmp_observation_does_not_infer_timezone_confirmation_or_market_session() -> None:
    raw_row = _result(payload=_fmp_payload()).to_raw_row()

    observation = normalize_fmp_provider_observations(
        raw_row=raw_row,
        known_at=date(2026, 7, 24),
        ingested_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )[0]

    assert observation["timezone"] is None
    assert observation["is_confirmed"] is None
    assert observation["before_after_market"] is None
    assert observation["data_quality_flags"]["timezoneUnavailable"] is True
    assert observation["data_quality_flags"]["confirmationNotProvidedBySource"] is True
    assert observation["data_quality_flags"]["fiscalPeriodDerivedFromFiscalDateEnding"] is True


def test_cached_success_is_reused_without_live_http_call() -> None:
    cached = _result(payload=_fmp_payload()).to_raw_row()
    repository = _Repository(cached_raw=cached)
    client = _Client(_result(payload=_fmp_payload()))

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    assert report["raw_cache_hits"] == 1
    assert report["live_calls"] == 0
    assert client.calls == []
    assert len(repository.observation_rows) == 1


def test_rate_limited_response_is_not_not_found_and_writes_no_observation() -> None:
    repository = _Repository()
    client = _Client(_result(payload={"error": "too many requests"}, status="rate_limited", http_status=429))

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    assert report["status_counts"]["rate_limited"] == 1
    assert report["status_counts"]["not_found"] == 0
    assert repository.raw_rows[0]["status"] == "rate_limited"
    assert repository.observation_rows == []


def test_http_client_keeps_api_key_out_of_request_params_and_classifies_200_error_payload() -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict[str, str]:
            return {"Error Message": "subscription required"}

    class _Session:
        calls: list[dict[str, Any]] = []

        def get(self, url: str, **kwargs: Any) -> _Response:
            self.calls.append({"url": url, **kwargs})
            return _Response()

    session = _Session()
    result = FinancialModelingPrepEarningsClient(api_key="secret-key", session=session).fetch(
        symbol="AAPL",
        observed_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )

    assert result.status == "error"
    assert result.request_params == {"symbol": "AAPL"}
    assert session.calls[0]["headers"] == {"apikey": "secret-key"}
    assert session.calls[0]["params"] == {"symbol": "AAPL"}


def test_conflicts_against_yahoo_are_reported_without_canonical_write() -> None:
    repository = _Repository(
        yahoo_rows=[
            {
                "provider": "yahoo_finance",
                "symbol": "AAPL",
                "fiscal_period": "2026Q2",
                "report_date": date(2026, 7, 29),
                "eps_actual": 1.4,
                "eps_estimate": 1.3,
                "revenue_actual": None,
                "revenue_estimate": None,
            }
        ]
    )
    client = _Client(_result(payload=_fmp_payload()))

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    assert report["overlap_with_yahoo"] == 1
    assert report["conflicts"]["report_date_mismatch"]["count"] == 1
    assert report["conflicts"]["eps_actual_mismatch"]["count"] == 1
    assert report["conflicts"]["eps_estimate_mismatch"]["count"] == 1
    assert report["conflicts"]["revenue_available_only_in_fmp"]["count"] == 1
    assert repository.raw_rows and repository.observation_rows
    assert not hasattr(repository, "canonical_rows")


def test_unknown_fmp_fiscal_period_is_unavailable_not_a_provider_conflict() -> None:
    repository = _Repository(
        yahoo_rows=[
            {
                "provider": "yahoo_finance",
                "symbol": "AAPL",
                "fiscal_period": "2026Q2",
                "report_date": date(2026, 7, 30),
                "eps_actual": 1.5,
                "eps_estimate": 1.4,
                "revenue_actual": None,
                "revenue_estimate": None,
            }
        ]
    )
    client = _Client(_result(payload=_fmp_payload(fiscalDateEnding=None)))

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    assert report["conflicts"]["fmp_fiscal_period_unavailable"]["count"] == 1
    assert report["conflicts"]["fiscal_period_mismatch"]["count"] == 0


def test_eps_quality_buckets_large_conflict_with_representative_example() -> None:
    repository = _Repository(
        yahoo_rows=[
            {
                "provider": "yahoo_finance",
                "symbol": "AAPL",
                "fiscal_period": "2024Q4",
                "report_date": date(2024, 10, 31),
                "eps_actual": 0.97,
                "eps_estimate": 0.95,
                "revenue_actual": None,
                "revenue_estimate": None,
            }
        ]
    )
    client = _Client(
        _result(
            payload=_fmp_payload(
                date="2024-10-31",
                fiscalDateEnding="2024-12-31",
                epsActual=1.64,
                epsEstimated=1.60,
            )
        )
    )

    report = run_fmp_earnings_shadow(
        symbols=["AAPL"],
        repository=repository,
        client=client,
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    large = report["conflicts"]["eps_conflicts"]["large_eps_conflict"]
    assert large["count"] == 1
    assert large["examples"][0]["report_date"] == date(2024, 10, 31)
    assert large["examples"][0]["eps_actual_delta"] == 0.67


def test_revenue_sanity_reports_exact_mismatch_missing_and_scale_anomaly() -> None:
    fmp_rows = [
        {"symbol": "AAPL", "report_date": date(2026, 1, 30), "revenue_actual": 100.0, "fiscal_period": "unknown"},
        {"symbol": "AAPL", "report_date": date(2026, 4, 30), "revenue_actual": 100.0, "fiscal_period": "unknown"},
        {"symbol": "AAPL", "report_date": date(2026, 7, 30), "revenue_actual": 100.0, "fiscal_period": "unknown"},
    ]
    statement_rows = [
        {"symbol": "AAPL", "known_at": date(2026, 1, 30), "period_end": date(2025, 12, 31), "value": 100.0},
        {"symbol": "AAPL", "known_at": date(2026, 4, 30), "period_end": date(2026, 3, 31), "value": 0.1},
    ]

    report = build_revenue_sanity_report(fmp_rows, statement_rows)

    assert report["exact_matches"]["count"] == 1
    assert report["mismatches"]["count"] == 1
    assert report["missing_statement_comparators"]["count"] == 1
    assert report["scale_anomalies"]["count"] == 1
    assert report["scale_anomalies"]["examples"][0]["scale_factor"] == 1_000


def test_observation_ids_and_hashes_are_deterministic() -> None:
    raw_row = _result(payload=_fmp_payload()).to_raw_row()
    kwargs = {
        "raw_row": raw_row,
        "known_at": date(2026, 7, 24),
        "ingested_at": datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    }

    first = normalize_fmp_provider_observations(**kwargs)[0]
    second = normalize_fmp_provider_observations(**kwargs)[0]

    assert first["provider_observation_id"] == second["provider_observation_id"]
    assert first["observation_hash"] == second["observation_hash"]


def test_dry_run_is_planned_without_http_or_database_writes() -> None:
    report = run_fmp_earnings_shadow(symbols=["AAPL"], env=_enabled_env(), dry_run=True)

    assert report["status"] == "dry_run"
    assert report["provider_observations"] == {"written": 0, "planned": 1}
    assert report["live_calls"] == 0
