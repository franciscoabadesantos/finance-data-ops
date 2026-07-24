from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

from finance_data_ops.shadow.guidance import (
    IR_PUBLIC_PRESS_RELEASE,
    SEC_EDGAR_GUIDANCE_CANDIDATE,
    GuidanceFetchResult,
    derive_sec_edgar_guidance_candidates,
    run_guidance_shadow,
)


NOW = datetime(2026, 7, 25, 10, 0, tzinfo=UTC)


@dataclass
class _Repository:
    filing_rows: list[dict[str, Any]] = field(default_factory=list)
    cached: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    raw_rows: list[dict[str, Any]] = field(default_factory=list)
    observation_rows: list[dict[str, Any]] = field(default_factory=list)

    def find_cached_raw(self, *, provider: str, endpoint: str, provider_symbol: str, request_hash: str) -> dict[str, Any] | None:
        return self.cached.get((provider, endpoint, provider_symbol))

    def upsert_raw(self, row: dict[str, Any]) -> None:
        self.raw_rows.append(dict(row))
        self.cached[(str(row["provider"]), str(row["endpoint"]), str(row["provider_symbol"]))] = dict(row)

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        by_id = {str(row["provider_observation_id"]): row for row in self.observation_rows}
        for row in rows:
            by_id[str(row["provider_observation_id"])] = dict(row)
        self.observation_rows = list(by_id.values())

    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        return [row for row in self.filing_rows if str(row["symbol"]).upper() in symbols]


@dataclass
class _IrClient:
    html: str
    calls: list[str] = field(default_factory=list)

    def fetch(self, *, symbol: str, source_url: str, observed_at: datetime) -> GuidanceFetchResult:
        from finance_data_ops.shadow.guidance import _request_hash

        self.calls.append(source_url)
        return GuidanceFetchResult(
            provider_symbol=symbol,
            symbol=symbol,
            endpoint=source_url,
            request_hash=_request_hash(
                provider=IR_PUBLIC_PRESS_RELEASE,
                endpoint=source_url,
                provider_symbol=symbol,
            ),
            status="success",
            http_status=200,
            content_type="text/html",
            response_payload=self.html,
            error_reason=None,
            observed_at=observed_at,
        )


def _filing(*, symbol: str = "AAPL", form: str = "8-K", description: str | None = "Item 2.02 earnings release") -> dict[str, Any]:
    return {
        "provider_observation_id": f"sec-{symbol}-{form}",
        "symbol": symbol,
        "company_name": "Example Inc.",
        "accession_number": f"0000000000-26-{symbol[-1:]}00001",
        "form_type": form,
        "filing_date": date(2026, 7, 24),
        "acceptance_datetime": NOW,
        "primary_document": "report.htm",
        "primary_doc_description": description,
        "filing_url": "https://www.sec.gov/Archives/edgar/data/1/report.htm",
        "known_at": date(2026, 7, 25),
        "ingested_at": NOW,
        "raw_payload_ref": "filing-raw",
        "raw_payload_hash": "filing-hash",
        "source_metadata": {},
        "data_quality_flags": {},
    }


def _env(*providers: str) -> dict[str, str]:
    return {
        "DATA_OPS_GUIDANCE_PROVIDERS": ",".join(providers),
        "DATA_OPS_GUIDANCE_IR_PUBLIC_PAGE_USER_AGENT": "Example Company contact@example.com",
    }


def _config(path: Path, *, allowed_host: str = "investors.example.com") -> Path:
    path.write_text(
        json.dumps(
            {
                "version": "guidance_sources.v1",
                "sources": [
                    {
                        "symbol": "AAPL",
                        "provider": IR_PUBLIC_PRESS_RELEASE,
                        "source_url": "https://investors.example.com/news/release",
                        "allowed_host": allowed_host,
                        "parser": "press_release_html_v1",
                        "enabled": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_missing_providers_skips_without_network_or_database() -> None:
    report = run_guidance_shadow(symbols=["AAPL"], env={})
    assert report["status"] == "skipped"
    assert report["reason"] == "no_guidance_providers_enabled"
    assert report["live_calls"] == 0


def test_sec_provider_derives_candidates_only_from_local_useful_filings() -> None:
    repository = _Repository(filing_rows=[_filing(), _filing(symbol="MSFT", form="6-K", description="EX-99.1 guidance update")])
    report = run_guidance_shadow(
        symbols=["AAPL", "MSFT", "9684.T"],
        repository=repository,
        env=_env(SEC_EDGAR_GUIDANCE_CANDIDATE),
        now=lambda: NOW,
    )
    assert report["live_calls"] == 0
    assert report["provider_observations"] == {"planned": 2, "written": 2}
    assert report["symbol_statuses"]["AAPL"][SEC_EDGAR_GUIDANCE_CANDIDATE]["status"] == "success"
    assert report["symbol_statuses"]["9684.T"][SEC_EDGAR_GUIDANCE_CANDIDATE]["status"] == "not_found"
    assert report["unresolved_symbols"] == ["9684.T"]
    assert all(row["guidance_type"] is None for row in repository.observation_rows)
    assert all(row["data_quality_flags"]["guidanceCandidate"] is True for row in repository.observation_rows)


def test_irrelevant_filing_is_not_a_guidance_candidate() -> None:
    rows = derive_sec_edgar_guidance_candidates([_filing(description="Director appointment")], observed_at=NOW)
    assert rows == []


def test_ir_provider_requires_user_agent_without_blocking_sec() -> None:
    repository = _Repository(filing_rows=[_filing()])
    report = run_guidance_shadow(
        symbols=["AAPL"],
        repository=repository,
        env={"DATA_OPS_GUIDANCE_PROVIDERS": f"{SEC_EDGAR_GUIDANCE_CANDIDATE},{IR_PUBLIC_PRESS_RELEASE}"},
        now=lambda: NOW,
    )
    assert report["status"] == "completed"
    assert report["providers"][IR_PUBLIC_PRESS_RELEASE]["skip_reason"] == "ir_public_page_user_agent_missing"
    assert report["providers"][SEC_EDGAR_GUIDANCE_CANDIDATE]["enabled"] is True
    assert report["live_calls"] == 0


def test_ir_host_allowlist_is_exact_and_does_not_fetch(tmp_path: Path) -> None:
    repository = _Repository()
    client = _IrClient("<p>Guidance maintained.</p>")
    report = run_guidance_shadow(
        symbols=["AAPL"],
        repository=repository,
        ir_client=client,
        env=_env(IR_PUBLIC_PRESS_RELEASE),
        source_config_path=_config(tmp_path / "sources.json", allowed_host="other.example.com"),
        request_sleep_seconds=0,
        now=lambda: NOW,
    )
    assert report["symbol_statuses"]["AAPL"][IR_PUBLIC_PRESS_RELEASE]["reason"] == "host_not_allowlisted"
    assert client.calls == []


def test_ir_config_rejects_direct_pdf_sources(tmp_path: Path) -> None:
    path = _config(tmp_path / "sources.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["sources"][0]["source_url"] = "https://investors.example.com/release.pdf"
    path.write_text(json.dumps(payload), encoding="utf-8")
    report = run_guidance_shadow(
        symbols=["AAPL"],
        repository=_Repository(),
        env=_env(IR_PUBLIC_PRESS_RELEASE),
        source_config_path=path,
        request_sleep_seconds=0,
        now=lambda: NOW,
    )
    assert report["symbol_statuses"]["AAPL"][IR_PUBLIC_PRESS_RELEASE]["reason"] == "no_ir_source_config"


def test_ir_cache_hit_avoids_second_fetch_and_examples_are_capped(tmp_path: Path) -> None:
    repository = _Repository()
    secret_tail = "SENSITIVE_FULL_TEXT_SHOULD_NOT_APPEAR"
    client = _IrClient("<title>Q2 release</title><p>Company reaffirmed guidance for the year. " + ("x" * 500) + secret_tail + "</p>")
    kwargs = {
        "symbols": ["AAPL"],
        "repository": repository,
        "ir_client": client,
        "env": _env(IR_PUBLIC_PRESS_RELEASE),
        "source_config_path": _config(tmp_path / "sources.json"),
        "request_sleep_seconds": 0,
        "now": lambda: NOW,
    }
    first = run_guidance_shadow(**kwargs)
    second = run_guidance_shadow(**kwargs)
    assert first["live_calls"] == 1
    assert second["live_calls"] == 0
    assert second["raw_cache_hits"] == 1
    assert len(client.calls) == 1
    example = second["examples"][0]
    assert len(example["guidance_quote"]) <= 280
    assert secret_tail not in json.dumps(example)
