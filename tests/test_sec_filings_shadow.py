from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from finance_data_ops.shadow.sec_filings import (
    SEC_EDGAR_PROVIDER,
    SEC_SUBMISSIONS_ENDPOINT,
    SEC_TICKER_MAPPING_ENDPOINT,
    PostgresSecFilingsShadowRepository,
    SecEdgarFetchResult,
    _to_psycopg_dsn,
    normalize_sec_filing_provider_observations,
    run_sec_filings_shadow,
)


NOW = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)


@dataclass
class _Repository:
    cached: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observation_rows: list[dict[str, Any]] = field(default_factory=list)

    def find_cached_raw(self, *, endpoint: str, provider_symbol: str, request_hash: str, **_: Any) -> dict[str, Any] | None:
        return self.cached.get((endpoint, provider_symbol, request_hash))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        self.raw_rows.append(dict(row))
        self.cached[(str(row["endpoint"]), str(row["provider_symbol"]), str(row["request_hash"]))] = dict(row)

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        by_id = {str(row["provider_observation_id"]): row for row in self.observation_rows}
        for row in rows:
            by_id[str(row["provider_observation_id"])] = dict(row)
        self.observation_rows = list(by_id.values())


@dataclass
class _Client:
    mapping: Any
    submissions: dict[str, Any]
    submission_statuses: dict[str, tuple[str, int | None, str | None]] = field(default_factory=dict)
    mapping_calls: int = 0
    submission_calls: list[tuple[str, str]] = field(default_factory=list)

    def fetch_ticker_mapping(self, *, observed_at: datetime) -> SecEdgarFetchResult:
        self.mapping_calls += 1
        return _result(
            provider_symbol="__ticker_cik_mapping__",
            endpoint=SEC_TICKER_MAPPING_ENDPOINT,
            observed_at=observed_at,
            payload=self.mapping,
        )

    def fetch_submissions(self, *, symbol: str, cik: str, observed_at: datetime) -> SecEdgarFetchResult:
        self.submission_calls.append((symbol, cik))
        status, http_status, reason = self.submission_statuses.get(symbol, ("success", 200, None))
        return _result(
            provider_symbol=symbol,
            symbol=symbol,
            cik=cik,
            endpoint=SEC_SUBMISSIONS_ENDPOINT.format(cik=cik),
            observed_at=observed_at,
            payload=self.submissions.get(symbol),
            status=status,
            http_status=http_status,
            reason=reason,
        )


def _result(
    *,
    provider_symbol: str,
    endpoint: str,
    observed_at: datetime,
    payload: Any,
    symbol: str | None = None,
    cik: str | None = None,
    status: str = "success",
    http_status: int | None = 200,
    reason: str | None = None,
) -> SecEdgarFetchResult:
    request_params = {"cik": cik} if cik else {}
    from finance_data_ops.shadow.sec_filings import _request_hash

    return SecEdgarFetchResult(
        provider_symbol=provider_symbol,
        symbol=symbol,
        cik=cik,
        endpoint=endpoint,
        request_params=request_params,
        request_hash=_request_hash(endpoint=endpoint, provider_symbol=provider_symbol, request_params=request_params),
        status=status,
        http_status=http_status,
        response_payload=payload,
        error_reason=reason,
        observed_at=observed_at,
    )


def _mapping() -> dict[str, dict[str, Any]]:
    return {
        "0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."},
        "1": {"ticker": "MSFT", "cik_str": 789019, "title": "Microsoft Corp."},
    }


def _submissions(*, cik: str, accession: str = "0000320193-26-000001") -> dict[str, Any]:
    return {
        "name": "Apple Inc.",
        "filings": {
            "recent": {
                "accessionNumber": [accession],
                "form": ["10-Q"],
                "filingDate": ["2026-07-23"],
                "reportDate": ["2026-06-30"],
                "acceptanceDateTime": ["2026-07-23T16:05:04.000Z"],
                "primaryDocument": ["aapl-20260630.htm"],
                "primaryDocDescription": ["10-Q"],
            },
            "files": [{"name": "CIK0000320193-submissions-001.json", "filingCount": 1000}],
        },
    }


def _env() -> dict[str, str]:
    return {"DATA_OPS_FILING_PROVIDERS": SEC_EDGAR_PROVIDER, "SEC_EDGAR_USER_AGENT": "Example contact@example.com"}


def _raw_row() -> dict[str, Any]:
    return _result(
        provider_symbol="AAPL",
        symbol="AAPL",
        cik="0000320193",
        endpoint=SEC_SUBMISSIONS_ENDPOINT.format(cik="0000320193"),
        observed_at=NOW,
        payload=_submissions(cik="0000320193"),
    ).to_raw_row()


def test_missing_user_agent_skips_without_network_or_database() -> None:
    report = run_sec_filings_shadow(
        symbols=["AAPL"],
        env={"DATA_OPS_FILING_PROVIDERS": SEC_EDGAR_PROVIDER},
    )

    assert report["status"] == "skipped"
    assert report["reason"] == "sec_user_agent_missing"
    assert report["live_calls"] == 0
    assert report["providers"][SEC_EDGAR_PROVIDER]["user_agent_configured"] is False


def test_ticker_mapping_resolves_us_symbols_and_writes_normalized_observations() -> None:
    repository = _Repository()
    client = _Client(mapping=_mapping(), submissions={"AAPL": _submissions(cik="0000320193"), "MSFT": _submissions(cik="0000789019")})

    report = run_sec_filings_shadow(
        symbols=["AAPL", "MSFT"], repository=repository, client=client, env=_env(), request_sleep_seconds=0, now=lambda: NOW
    )

    assert report["status"] == "completed"
    assert report["symbol_statuses"]["AAPL"]["status"] == "resolved"
    assert report["symbol_statuses"]["AAPL"]["resolved_cik"] == "0000320193"
    assert report["symbol_statuses"]["MSFT"]["resolved_cik"] == "0000789019"
    assert report["provider_observations"]["written"] == 2
    assert report["coverage"]["by_form_type"] == {"10-Q": 2}
    assert client.mapping_calls == 1
    assert client.submission_calls == [("AAPL", "0000320193"), ("MSFT", "0000789019")]


def test_unresolved_symbol_is_blocked_without_submission_request() -> None:
    repository = _Repository()
    client = _Client(mapping=_mapping(), submissions={})

    report = run_sec_filings_shadow(
        symbols=["ASML"], repository=repository, client=client, env=_env(), request_sleep_seconds=0, now=lambda: NOW
    )

    assert report["status"] == "completed"
    assert report["symbol_statuses"]["ASML"]["status"] == "unresolved"
    assert report["symbol_statuses"]["ASML"]["reason"] == "ticker_cik_unresolved"
    assert client.submission_calls == []
    assert report["examples"]["unresolved"] == [{"symbol": "ASML", "reason": "ticker_cik_unresolved"}]


def test_normalization_is_deterministic_and_derives_filing_url() -> None:
    first = normalize_sec_filing_provider_observations(raw_row=_raw_row())[0]
    second = normalize_sec_filing_provider_observations(raw_row=_raw_row())[0]

    assert first["provider_observation_id"] == second["provider_observation_id"]
    assert first["observation_hash"] == second["observation_hash"]
    assert first["filing_url"] == "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/aapl-20260630.htm"
    assert first["source_metadata"]["historicalSubmissionFiles"][0]["name"] == "CIK0000320193-submissions-001.json"
    assert first["data_quality_flags"]["historical_submission_files_not_fetched"] is True


def test_raw_row_is_deterministic_and_does_not_retain_user_agent() -> None:
    first = _raw_row()
    second = _raw_row()

    assert first["raw_id"] == second["raw_id"]
    assert first["raw_payload_hash"] == second["raw_payload_hash"]
    assert first["request_params"] == {"cik": "0000320193"}
    assert "user-agent" not in str(first["request_params"]).lower()


def test_cached_mapping_and_submissions_avoid_second_live_call() -> None:
    repository = _Repository()
    client = _Client(mapping=_mapping(), submissions={"AAPL": _submissions(cik="0000320193")})

    first = run_sec_filings_shadow(
        symbols=["AAPL"], repository=repository, client=client, env=_env(), request_sleep_seconds=0, now=lambda: NOW
    )
    second = run_sec_filings_shadow(
        symbols=["AAPL"], repository=repository, client=client, env=_env(), request_sleep_seconds=0, now=lambda: NOW
    )

    assert first["live_calls"] == 2
    assert second["live_calls"] == 0
    assert second["raw_cache_hits"] == 2
    assert client.mapping_calls == 1
    assert client.submission_calls == [("AAPL", "0000320193")]


def test_rate_limited_submission_is_reported_with_safe_reason() -> None:
    repository = _Repository()
    client = _Client(
        mapping=_mapping(),
        submissions={"AAPL": None},
        submission_statuses={"AAPL": ("rate_limited", 429, "http_429")},
    )

    report = run_sec_filings_shadow(
        symbols=["AAPL"], repository=repository, client=client, env=_env(), request_sleep_seconds=0, now=lambda: NOW
    )

    status = report["symbol_statuses"]["AAPL"]
    assert status["status"] == "rate_limited"
    assert status["http_status"] == 429
    assert status["reason"] == "http_429"
    assert "contact@example.com" not in str(report)


def test_sqlalchemy_dsn_is_normalized_for_psycopg() -> None:
    dsn = "postgresql+psycopg://worker:password@example.invalid:5432/finance"
    repository = PostgresSecFilingsShadowRepository(database_dsn=dsn)

    assert _to_psycopg_dsn(dsn) == "postgresql://worker:password@example.invalid:5432/finance"
    assert repository._database_dsn == "postgresql://worker:password@example.invalid:5432/finance"
