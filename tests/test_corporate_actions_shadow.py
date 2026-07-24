from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from finance_data_ops.shadow.corporate_actions import (
    DIVIDEND,
    FMP_DIVIDENDS_ENDPOINT,
    FMP_PROVIDER,
    FMP_SPLITS_ENDPOINT,
    SPLIT,
    YAHOO_FINANCE_PROVIDER,
    CorporateActionsFetchResult,
    FinancialModelingPrepCorporateActionsClient,
    PostgresCorporateActionsShadowRepository,
    YahooFinanceCorporateActionsClient,
    _to_psycopg_dsn,
    normalize_corporate_action_provider_observations,
    run_corporate_actions_shadow,
)


@dataclass
class _Repository:
    cached_raw: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observation_rows: list[dict[str, Any]] = field(default_factory=list)

    def find_cached_raw(self, *, provider: str, action_type: str, **_: Any) -> dict[str, Any] | None:
        return self.cached_raw.get((provider, action_type))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        self.raw_rows.append(dict(row))

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        by_id = {str(row["provider_observation_id"]): row for row in self.observation_rows}
        for row in rows:
            by_id[str(row["provider_observation_id"])] = dict(row)
        self.observation_rows = list(by_id.values())


@dataclass
class _Client:
    provider: str
    payloads: dict[str, Any]
    calls: list[tuple[str, str]] = field(default_factory=list)
    statuses: dict[str, tuple[str, int | None, str | None]] = field(default_factory=dict)

    def fetch(self, *, symbol: str, action_type: str, observed_at: datetime) -> CorporateActionsFetchResult:
        self.calls.append((symbol, action_type))
        if self.provider == FMP_PROVIDER:
            endpoint = FMP_DIVIDENDS_ENDPOINT if action_type == DIVIDEND else FMP_SPLITS_ENDPOINT
        else:
            endpoint = f"yfinance:Ticker.{action_type}s"
        status, http_status, reason = self.statuses.get(action_type, ("success", 200, None))
        return CorporateActionsFetchResult(
            provider=self.provider,
            provider_symbol=symbol,
            action_type=action_type,
            endpoint=endpoint,
            request_params={"symbol": symbol},
            request_hash=f"{self.provider}-{symbol}-{action_type}",
            status=status,
            http_status=http_status,
            response_payload=self.payloads.get(action_type),
            error_reason=reason,
            provider_updated_at=None,
            observed_at=observed_at,
        )


def _enabled_env() -> dict[str, str]:
    return {
        "FMP_API_KEY": "test-key",
        "DATA_OPS_CORPORATE_ACTION_PROVIDERS": "fmp,yahoo_finance",
    }


def _raw_row(*, provider: str, action_type: str, payload: Any) -> dict[str, Any]:
    return CorporateActionsFetchResult(
        provider=provider,
        provider_symbol="AAPL",
        action_type=action_type,
        endpoint=FMP_DIVIDENDS_ENDPOINT if action_type == DIVIDEND else FMP_SPLITS_ENDPOINT,
        request_params={"symbol": "AAPL"},
        request_hash=f"{provider}-{action_type}",
        status="success",
        http_status=200,
        response_payload=payload,
        error_reason=None,
        provider_updated_at=None,
        observed_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    ).to_raw_row()


def _fmp_dividend_payload(**overrides: Any) -> list[dict[str, Any]]:
    row = {
        "symbol": "AAPL",
        "date": "2026-08-10",
        "paymentDate": "2026-08-15",
        "recordDate": "2026-08-12",
        "declarationDate": "2026-07-20",
        "dividend": 0.25,
        "adjDividend": 0.25,
        "yield": 0.004,
        "frequency": "quarterly",
        "currency": "USD",
        "lastUpdated": "2026-07-24T08:00:00Z",
    }
    row.update(overrides)
    return [row]


def _fmp_split_payload(**overrides: Any) -> list[dict[str, Any]]:
    row = {"symbol": "AAPL", "date": "2020-08-31", "numerator": 4, "denominator": 1}
    row.update(overrides)
    return [row]


def test_corporate_actions_without_allowlist_is_skipped_without_http_or_database() -> None:
    report = run_corporate_actions_shadow(symbols=["AAPL"], env={})

    assert report["status"] == "skipped"
    assert report["reason"] == "no_corporate_action_providers_enabled"
    assert report["live_calls"] == 0
    assert report["provider_observations"]["written"] == 0


def test_fmp_allowlisted_without_key_is_skipped_without_network() -> None:
    report = run_corporate_actions_shadow(
        symbols=["AAPL"], env={"DATA_OPS_CORPORATE_ACTION_PROVIDERS": "fmp"}
    )

    assert report["status"] == "skipped"
    assert report["reason"] == "fmp_api_key_missing"
    assert report["providers"][FMP_PROVIDER]["skip_reason"] == "fmp_api_key_missing"


def test_fmp_dividend_normalization_preserves_dates_amounts_and_currency() -> None:
    observation = normalize_corporate_action_provider_observations(
        raw_row=_raw_row(provider=FMP_PROVIDER, action_type=DIVIDEND, payload=_fmp_dividend_payload())
    )[0]

    assert observation["action_type"] == DIVIDEND
    assert observation["ex_date"] == date(2026, 8, 10)
    assert observation["payment_date"] == date(2026, 8, 15)
    assert observation["record_date"] == date(2026, 8, 12)
    assert observation["declaration_date"] == date(2026, 7, 20)
    assert observation["cash_amount"] == 0.25
    assert observation["adjusted_cash_amount"] == 0.25
    assert observation["currency"] == "USD"
    assert observation["provider_date_semantics"] == "ex_date"


def test_fmp_split_normalization_derives_factor_only_from_explicit_ratio_components() -> None:
    observation = normalize_corporate_action_provider_observations(
        raw_row=_raw_row(provider=FMP_PROVIDER, action_type=SPLIT, payload=_fmp_split_payload())
    )[0]

    assert observation["ex_date"] == date(2020, 8, 31)
    assert observation["split_numerator"] == 4.0
    assert observation["split_denominator"] == 1.0
    assert observation["split_factor"] == 4.0
    assert observation["currency"] is None


def test_yahoo_normalization_uses_index_date_and_does_not_invent_dividend_fields() -> None:
    raw_row = _raw_row(
        provider=YAHOO_FINANCE_PROVIDER,
        action_type=DIVIDEND,
        payload=[{"date": "2026-08-10", "cashAmount": 0.25}],
    )

    observation = normalize_corporate_action_provider_observations(raw_row=raw_row)[0]

    assert observation["ex_date"] == date(2026, 8, 10)
    assert observation["payment_date"] is None
    assert observation["record_date"] is None
    assert observation["declaration_date"] is None
    assert observation["currency"] is None
    assert observation["provider_date_semantics"] == "provider_index_date"
    assert observation["data_quality_flags"]["paymentDateNotProvidedBySource"] is True


def test_yfinance_client_serializes_dividend_and_split_indices_as_provider_dates() -> None:
    class _Series:
        def __init__(self, rows: list[tuple[date, float]]) -> None:
            self._rows = rows

        def items(self):
            return iter(self._rows)

    class _Ticker:
        dividends = _Series([(date(2026, 8, 10), 0.25)])
        splits = _Series([(date(2020, 8, 31), 4.0)])

    client = YahooFinanceCorporateActionsClient(ticker_factory=lambda _: _Ticker())
    observed_at = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)

    dividend = client.fetch(symbol="9684.T", action_type=DIVIDEND, observed_at=observed_at)
    split = client.fetch(symbol="9684.T", action_type=SPLIT, observed_at=observed_at)

    assert dividend.status == "success"
    assert dividend.response_payload == [{"date": "2026-08-10", "symbol": None, "cashAmount": 0.25}]
    assert split.response_payload == [{"date": "2020-08-31", "symbol": None, "splitFactor": 4.0}]


def test_cache_first_avoids_live_calls_and_preserves_cached_observations() -> None:
    cached_dividend = _raw_row(provider=FMP_PROVIDER, action_type=DIVIDEND, payload=_fmp_dividend_payload())
    cached_split = _raw_row(provider=FMP_PROVIDER, action_type=SPLIT, payload=_fmp_split_payload())
    repository = _Repository(cached_raw={(FMP_PROVIDER, DIVIDEND): cached_dividend, (FMP_PROVIDER, SPLIT): cached_split})
    client = _Client(FMP_PROVIDER, {DIVIDEND: _fmp_dividend_payload(), SPLIT: _fmp_split_payload()})

    report = run_corporate_actions_shadow(
        symbols=["AAPL"],
        repository=repository,
        clients={FMP_PROVIDER: client},
        env={"FMP_API_KEY": "test-key", "DATA_OPS_CORPORATE_ACTION_PROVIDERS": "fmp"},
        request_sleep_seconds=0,
    )

    assert report["status"] == "completed"
    assert report["raw_cache_hits"] == 2
    assert report["live_calls"] == 0
    assert client.calls == []
    assert report["provider_observations"]["written"] == 2


def test_shadow_report_writes_only_raw_and_provider_observations_with_safe_statuses() -> None:
    repository = _Repository()
    fmp = _Client(
        FMP_PROVIDER,
        {DIVIDEND: _fmp_dividend_payload(), SPLIT: {"error": "plan limited", "apiKey": "should-not-persist"}},
        statuses={SPLIT: ("error", 402, "http_402")},
    )
    yahoo = _Client(
        YAHOO_FINANCE_PROVIDER,
        {DIVIDEND: [{"date": "2026-08-10", "cashAmount": 0.25}], SPLIT: [{"date": "2020-08-31", "splitFactor": 4.0}]},
    )

    report = run_corporate_actions_shadow(
        symbols=["AAPL"],
        repository=repository,
        clients={FMP_PROVIDER: fmp, YAHOO_FINANCE_PROVIDER: yahoo},
        env=_enabled_env(),
        request_sleep_seconds=0,
        now=lambda: datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    )

    assert report["status"] == "completed"
    assert report["status_counts"] == {"error": 1, "not_found": 0, "rate_limited": 0, "success": 3}
    assert report["symbol_statuses"]["AAPL"][FMP_PROVIDER][SPLIT] == {
        "status": "error", "http_status": 402, "reason": "http_402", "cache_hit": False
    }
    assert report["coverage"][FMP_PROVIDER] == {DIVIDEND: 1, SPLIT: 0}
    assert report["coverage"][YAHOO_FINANCE_PROVIDER] == {DIVIDEND: 1, SPLIT: 1}
    assert len(repository.raw_rows) == 4
    assert len(repository.observation_rows) == 3
    assert all("feature_store" not in str(row) for row in repository.raw_rows)


def test_fmp_not_found_and_rate_limited_raw_statuses_are_preserved_without_observations() -> None:
    repository = _Repository()
    client = _Client(
        FMP_PROVIDER,
        {DIVIDEND: [], SPLIT: {"error": "rate limited"}},
        statuses={DIVIDEND: ("not_found", 404, "http_404"), SPLIT: ("rate_limited", 429, "http_429")},
    )

    report = run_corporate_actions_shadow(
        symbols=["ASML"],
        repository=repository,
        clients={FMP_PROVIDER: client},
        env={"FMP_API_KEY": "test-key", "DATA_OPS_CORPORATE_ACTION_PROVIDERS": "fmp"},
        request_sleep_seconds=0,
    )

    assert report["status_counts"] == {"error": 0, "not_found": 1, "rate_limited": 1, "success": 0}
    assert [row["status"] for row in repository.raw_rows] == ["not_found", "rate_limited"]
    assert repository.observation_rows == []


def test_cached_fmp_http_402_avoids_repeated_calls_and_reports_cache_hits() -> None:
    first_repository = _Repository()
    first_client = _Client(
        FMP_PROVIDER,
        {DIVIDEND: {"error": "plan limited"}, SPLIT: {"error": "plan limited"}},
        statuses={DIVIDEND: ("error", 402, "http_402"), SPLIT: ("error", 402, "http_402")},
    )
    env = {"FMP_API_KEY": "test-key", "DATA_OPS_CORPORATE_ACTION_PROVIDERS": "fmp"}

    first = run_corporate_actions_shadow(
        symbols=["ASML"],
        repository=first_repository,
        clients={FMP_PROVIDER: first_client},
        env=env,
        request_sleep_seconds=0,
    )
    cached_repository = _Repository(
        cached_raw={(FMP_PROVIDER, row["action_type"]): row for row in first_repository.raw_rows}
    )
    second_client = _Client(FMP_PROVIDER, {DIVIDEND: [], SPLIT: []})

    second = run_corporate_actions_shadow(
        symbols=["ASML"],
        repository=cached_repository,
        clients={FMP_PROVIDER: second_client},
        env=env,
        request_sleep_seconds=0,
    )

    assert first["status_counts"]["error"] == 2
    assert all(row["http_status"] == 402 and row["error_reason"] == "http_402" for row in first_repository.raw_rows)
    assert all("test-key" not in str(row) for row in first_repository.raw_rows)
    assert second["raw_cache_hits"] == 2
    assert second["live_calls"] == 0
    assert second_client.calls == []
    assert second["symbol_statuses"]["ASML"][FMP_PROVIDER][DIVIDEND]["cache_hit"] is True
    assert second["symbol_statuses"]["ASML"][FMP_PROVIDER][SPLIT]["cache_hit"] is True


def test_overlap_and_conflicts_are_reported_by_action_type() -> None:
    repository = _Repository()
    fmp = _Client(
        FMP_PROVIDER,
        {
            DIVIDEND: _fmp_dividend_payload(dividend=0.30),
            SPLIT: _fmp_split_payload(numerator=4, denominator=1),
        },
    )
    yahoo = _Client(
        YAHOO_FINANCE_PROVIDER,
        {
            DIVIDEND: [{"date": "2026-08-10", "cashAmount": 0.25}],
            SPLIT: [{"date": "2020-09-01", "splitFactor": 2.0}],
        },
    )

    report = run_corporate_actions_shadow(
        symbols=["AAPL"],
        repository=repository,
        clients={FMP_PROVIDER: fmp, YAHOO_FINANCE_PROVIDER: yahoo},
        env=_enabled_env(),
        request_sleep_seconds=0,
    )

    assert report["overlap_fmp_vs_yahoo"]["total_exact_ex_date_matches"] == 1
    assert report["conflicts"]["dividend_amount_mismatch"]["count"] == 1
    assert report["conflicts"]["split_date_mismatch"]["count"] == 2
    assert report["conflicts"]["split_factor_mismatch"]["count"] == 0


def test_observation_ids_hashes_and_request_params_are_deterministic_and_safe() -> None:
    raw = CorporateActionsFetchResult(
        provider=FMP_PROVIDER,
        provider_symbol="AAPL",
        action_type=DIVIDEND,
        endpoint=FMP_DIVIDENDS_ENDPOINT,
        request_params={"symbol": "AAPL", "apikey": "secret-key"},
        request_hash="same-request",
        status="success",
        http_status=200,
        response_payload=_fmp_dividend_payload(apiKey="secret-key"),
        error_reason=None,
        provider_updated_at=None,
        observed_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
    ).to_raw_row()

    first = normalize_corporate_action_provider_observations(raw_row=raw)[0]
    second = normalize_corporate_action_provider_observations(raw_row=raw)[0]

    assert raw["request_params"] == {"symbol": "AAPL", "apikey": "[redacted]"}
    assert raw["raw_payload"][0]["apiKey"] == "[redacted]"
    assert first["provider_observation_id"] == second["provider_observation_id"]
    assert first["observation_hash"] == second["observation_hash"]


def test_sqlalchemy_dsn_is_normalized_for_corporate_actions_repository() -> None:
    sqlalchemy_dsn = "postgresql+psycopg://worker:password@example.invalid:5432/finance"
    repository = PostgresCorporateActionsShadowRepository(database_dsn=sqlalchemy_dsn)

    assert _to_psycopg_dsn(sqlalchemy_dsn) == "postgresql://worker:password@example.invalid:5432/finance"
    assert repository._database_dsn == "postgresql://worker:password@example.invalid:5432/finance"


def test_fmp_client_does_not_put_api_key_in_request_params() -> None:
    class _Response:
        status_code = 200

        @staticmethod
        def json() -> list[dict[str, Any]]:
            return _fmp_dividend_payload()

    class _Session:
        calls: list[dict[str, Any]] = []

        def get(self, url: str, **kwargs: Any) -> _Response:
            self.calls.append({"url": url, **kwargs})
            return _Response()

    session = _Session()
    result = FinancialModelingPrepCorporateActionsClient(api_key="secret-key", session=session).fetch(
        symbol="AAPL", action_type=DIVIDEND, observed_at=datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    )

    assert result.request_params == {"symbol": "AAPL"}
    assert session.calls[0]["headers"] == {"apikey": "secret-key"}
