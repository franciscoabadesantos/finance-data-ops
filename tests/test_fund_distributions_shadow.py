from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from finance_data_ops.shadow.fund_distributions import (
    TIINGO_FUND_DISTRIBUTIONS,
    TIINGO_DISTRIBUTIONS_ENDPOINT,
    TIINGO_EOD_PRICES_ENDPOINT,
    TiingoDistributionFetchResult,
    normalize_tiingo_eod_divcash_observations,
    normalize_tiingo_fund_distribution_observations,
    run_fund_distributions_shadow,
)

NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)

@dataclass
class _Repository:
    cached: dict[str, dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observations: dict[str, dict[str, Any]] = field(default_factory=dict)
    def find_cached_raw(self, *, endpoint: str, provider_symbol: str, source_cache_key: str, **_: Any) -> dict[str, Any] | None:
        return self.cached.get(source_cache_key) or self.cached.get(f"{provider_symbol}:{endpoint}") or self.cached.get(provider_symbol)
    def upsert_raw(self, row: dict[str, Any]) -> None:
        copied = dict(row)
        self.raw_rows.append(copied)
        self.cached[str(copied["source_cache_key"])] = copied
        self.cached[f"{copied['provider_symbol']}:{copied['provider_endpoint_or_page']}"] = copied
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: self.observations.update({str(row["provider_observation_id"]): row for row in rows})

@dataclass
class _Client:
    calls: int = 0
    def fetch(self, *, symbol: str, fetched_at: datetime) -> TiingoDistributionFetchResult:
        self.calls += 1
        return TiingoDistributionFetchResult(symbol, f"https://example.test/{symbol}", f"key-{symbol}", "success", 200, [{"ticker": symbol, "permaTicker": "perm", "exDate": "2026-08-10", "paymentDate": "2026-08-15", "recordDate": "2026-08-12", "declarationDate": "2026-07-20", "distribution": 1.25, "distributionFrequency": "quarterly"}], None, fetched_at, {"Content-Type": "application/json"})


@dataclass
class _ForbiddenThenEodClient:
    main_calls: int = 0
    eod_calls: int = 0

    def fetch(self, *, symbol: str, fetched_at: datetime) -> TiingoDistributionFetchResult:
        self.main_calls += 1
        endpoint = TIINGO_DISTRIBUTIONS_ENDPOINT.format(symbol=symbol)
        return TiingoDistributionFetchResult(symbol, endpoint, f"main-{symbol}", "error", 403, {"detail": "forbidden"}, "http_403", fetched_at, {"Content-Type": "application/json"})

    def fetch_eod_divcash(self, *, symbol: str, start_date: date, end_date: date, fetched_at: datetime) -> TiingoDistributionFetchResult:
        self.eod_calls += 1
        endpoint = TIINGO_EOD_PRICES_ENDPOINT.format(symbol=symbol)
        params = {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}
        return TiingoDistributionFetchResult(symbol, endpoint, f"eod-{symbol}-{start_date.isoformat()}-{end_date.isoformat()}", "success", 200, [{"date": "2026-06-20T00:00:00.000Z", "divCash": 0.75}, {"date": "2026-05-20T00:00:00.000Z", "divCash": 0}, {"date": "2026-04-20T00:00:00.000Z", "divCash": None}], None, fetched_at, {"Content-Type": "application/json"}, params, "tiingo_eod_prices")

def _env() -> dict[str, str]: return {"DATA_OPS_FUND_DISTRIBUTION_PROVIDERS": TIINGO_FUND_DISTRIBUTIONS, "TIINGO_API_KEY": "test-key"}

def test_disabled_or_missing_key_skips_without_repository_or_network() -> None:
    assert run_fund_distributions_shadow(symbols=["SPY"], env={})["reason"] == "no_fund_distribution_providers_enabled"
    report = run_fund_distributions_shadow(symbols=["SPY"], env={"DATA_OPS_FUND_DISTRIBUTION_PROVIDERS": TIINGO_FUND_DISTRIBUTIONS})
    assert report["status"] == "skipped"
    assert report["live_calls"] == 0

def test_tiingo_normalization_preserves_nullable_fields_without_inference() -> None:
    raw = _Client().fetch(symbol="SPY", fetched_at=NOW).to_raw_row()
    observation = normalize_tiingo_fund_distribution_observations(raw_row=raw)[0]
    assert observation["ex_date"].isoformat() == "2026-08-10"
    assert observation["distribution_amount"] == 1.25
    assert observation["payable_date"].isoformat() == "2026-08-15"
    assert observation["currency"] is None
    assert observation["distribution_type_canonical"] is None
    assert observation["distribution_frequency_raw"] == "quarterly"
    assert observation["provider_event_id"] is None

def test_cache_first_avoids_second_live_call_and_is_idempotent() -> None:
    repository, client = _Repository(), _Client()
    first = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env=_env(), now=lambda: NOW)
    repository.cached["SPY"] = repository.raw_rows[0]
    second = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env=_env(), now=lambda: NOW)
    assert first["live_calls"] == 1 and second["raw_cache_hits"] == 1
    assert client.calls == 1
    assert len(repository.observations) == 1
    assert "test-key" not in str(repository.raw_rows) + str(second)


def test_forbidden_main_endpoint_without_opt_in_keeps_current_error_behavior() -> None:
    repository, client = _Repository(), _ForbiddenThenEodClient()
    report = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env=_env(), now=lambda: NOW)
    assert report["status"] == "completed"
    assert report["symbol_statuses"]["SPY"]["status"] == "error"
    assert report["symbol_statuses"]["SPY"]["fallback"]["used"] is False
    assert report["provider_observations"]["written"] == 0
    assert client.main_calls == 1 and client.eod_calls == 0


def test_forbidden_main_endpoint_uses_opt_in_eod_divcash_fallback_and_preserves_nulls() -> None:
    repository, client = _Repository(), _ForbiddenThenEodClient()
    report = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env={**_env(), "DATA_OPS_FUND_DISTRIBUTION_TIINGO_EOD_FALLBACK": "true"}, now=lambda: NOW)
    observation = next(iter(repository.observations.values()))
    assert report["live_calls"] == 2
    assert report["symbol_statuses"]["SPY"]["main_endpoint"]["http_status"] == 403
    assert report["symbol_statuses"]["SPY"]["fallback"]["used"] is True
    assert report["observation_confidence_counts"] == {"low": 1}
    assert client.main_calls == 1 and client.eod_calls == 1
    assert observation["ex_date"].isoformat() == "2026-06-20"
    assert observation["distribution_amount"] == 0.75
    assert observation["declaration_date"] is None
    assert observation["record_date"] is None
    assert observation["payable_date"] is None
    assert observation["currency"] is None
    assert observation["distribution_type_raw"] is None
    assert observation["distribution_frequency_raw"] is None
    assert observation["observation_confidence"] == "low"
    assert observation["data_quality_flags"]["tiingoEodDivCashFallback"] is True
    assert observation["source_metadata"]["sourceEndpointFamily"] == "tiingo_eod_prices"
    assert "test-key" not in str(repository.raw_rows) + str(report)


def test_forbidden_main_and_eod_fallback_are_cache_first_on_second_run() -> None:
    repository, client = _Repository(), _ForbiddenThenEodClient()
    env = {**_env(), "DATA_OPS_FUND_DISTRIBUTION_TIINGO_EOD_FALLBACK": "true"}
    first = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env=env, now=lambda: NOW)
    second = run_fund_distributions_shadow(symbols=["SPY"], repository=repository, client=client, env=env, now=lambda: NOW)
    assert first["live_calls"] == 2
    assert second["live_calls"] == 0
    assert second["raw_cache_hits"] == 2
    assert second["symbol_statuses"]["SPY"]["main_endpoint"]["cache_hit"] is True
    assert second["symbol_statuses"]["SPY"]["fallback"]["cache_hit"] is True
    assert client.main_calls == 1 and client.eod_calls == 1


def test_distribution_frequency_accepts_tiingo_documented_typo_variant() -> None:
    raw = _Client().fetch(symbol="SPY", fetched_at=NOW).to_raw_row()
    raw["raw_payload"][0].pop("distributionFrequency")
    raw["raw_payload"][0]["distributionFreqency"] = "annual"
    assert normalize_tiingo_fund_distribution_observations(raw_row=raw)[0]["distribution_frequency_raw"] == "annual"


def test_eod_normalizer_ignores_zero_and_null_divcash() -> None:
    raw = _ForbiddenThenEodClient().fetch_eod_divcash(symbol="SPY", start_date=date(2024, 7, 25), end_date=date(2026, 7, 25), fetched_at=NOW).to_raw_row()
    observations = normalize_tiingo_eod_divcash_observations(raw_row=raw)
    assert len(observations) == 1
    assert observations[0]["distribution_amount"] == 0.75
