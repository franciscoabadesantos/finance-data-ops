from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from finance_data_ops.shadow.fund_distributions import (
    TIINGO_FUND_DISTRIBUTIONS,
    TiingoDistributionFetchResult,
    normalize_tiingo_fund_distribution_observations,
    run_fund_distributions_shadow,
)

NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)

@dataclass
class _Repository:
    cached: dict[str, dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observations: dict[str, dict[str, Any]] = field(default_factory=dict)
    def find_cached_raw(self, *, provider_symbol: str, **_: Any) -> dict[str, Any] | None: return self.cached.get(provider_symbol)
    def upsert_raw(self, row: dict[str, Any]) -> None: self.raw_rows.append(dict(row))
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: self.observations.update({str(row["provider_observation_id"]): row for row in rows})

@dataclass
class _Client:
    calls: int = 0
    def fetch(self, *, symbol: str, fetched_at: datetime) -> TiingoDistributionFetchResult:
        self.calls += 1
        return TiingoDistributionFetchResult(symbol, f"https://example.test/{symbol}", f"key-{symbol}", "success", 200, [{"ticker": symbol, "permaTicker": "perm", "exDate": "2026-08-10", "paymentDate": "2026-08-15", "recordDate": "2026-08-12", "declarationDate": "2026-07-20", "distribution": 1.25, "distributionFrequency": "quarterly"}], None, fetched_at, {"Content-Type": "application/json"})

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
