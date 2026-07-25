from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from finance_data_ops.shadow.fund_rebalances import RebalanceFetchResult, run_fund_rebalances_shadow

NOW = datetime(2026, 7, 25, 12, tzinfo=UTC)
CONFIG = {"version": "test.v1", "sources": [{"provider": "nasdaq_index_public", "source_url": "https://index.example.test/notice.json", "allowed_host": "index.example.test", "index_identifier": "Test 100", "parser": "explicit_rebalance_json_v1", "enabled": True}]}

@dataclass
class _Repository:
    cached: dict[str, dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observations: dict[str, dict[str, Any]] = field(default_factory=dict)
    def find_cached_raw(self, *, provider_url: str, **_: Any) -> dict[str, Any] | None: return self.cached.get(provider_url)
    def upsert_raw(self, row: dict[str, Any]) -> None: self.raw_rows.append(row)
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: self.observations.update({str(row["provider_observation_id"]): row for row in rows})

@dataclass
class _Client:
    payload: Any; calls: int = 0
    def fetch(self, *, provider: str, source_url: str, fetched_at: datetime) -> RebalanceFetchResult:
        self.calls += 1
        return RebalanceFetchResult(provider, source_url, "test-cache-key", "success", 200, self.payload, None, fetched_at, {})

def _config(tmp_path: Path) -> Path:
    path = tmp_path / "sources.json"; path.write_text(json.dumps(CONFIG)); return path
def _env(**extra: str) -> dict[str, str]: return {"DATA_OPS_FUND_REBALANCE_PROVIDERS": "nasdaq_index_public", "DATA_OPS_FUND_REBALANCE_USER_AGENT": "Company contact@example.com", **extra}

def test_no_config_or_user_agent_is_fail_closed_without_network(tmp_path: Path) -> None:
    assert run_fund_rebalances_shadow(env=_env(), source_config_path=tmp_path / "missing.json")["reason"] == "no_fund_rebalance_sources_enabled"
    report = run_fund_rebalances_shadow(env={"DATA_OPS_FUND_REBALANCE_PROVIDERS": "nasdaq_index_public"}, source_config_path=_config(tmp_path))
    assert report["reason"] == "fund_rebalance_user_agent_missing"

def test_holdings_snapshots_are_raw_only_and_never_inferred(tmp_path: Path) -> None:
    repository, client = _Repository(), _Client({"announcement_date": "2026-07-01", "effective_date": "2026-07-20", "holdings": [{"symbol": "AAPL"}]})
    report = run_fund_rebalances_shadow(repository=repository, client=client, env=_env(), source_config_path=_config(tmp_path), now=lambda: NOW)
    assert report["provider_observations"]["written"] == 0
    assert report["source_statuses"][0]["reason"] == "explicit_rebalance_data_unavailable"

def test_explicit_announcement_and_changes_emit_deterministic_observations(tmp_path: Path) -> None:
    payload = {"announcement_date": "2026-07-01", "effective_date": "2026-07-20", "changes": [{"side": "add", "symbol": "AAPL"}, {"side": "remove", "symbol": "MSFT"}]}
    repository, client = _Repository(), _Client(payload)
    first = run_fund_rebalances_shadow(repository=repository, client=client, env=_env(), source_config_path=_config(tmp_path), now=lambda: NOW)
    repository.cached[CONFIG["sources"][0]["source_url"]] = repository.raw_rows[0]
    second = run_fund_rebalances_shadow(repository=repository, client=client, env=_env(), source_config_path=_config(tmp_path), now=lambda: NOW)
    assert first["provider_observations"]["written"] == 2
    assert second["raw_cache_hits"] == 1 and client.calls == 1
    assert {row["change_side"] for row in repository.observations.values()} == {"add", "remove"}
    assert all(row["weight"] is None and row["shares"] is None for row in repository.observations.values())
