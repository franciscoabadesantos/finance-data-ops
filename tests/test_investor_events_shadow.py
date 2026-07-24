from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import json
from pathlib import Path
from typing import Any

from finance_data_ops.shadow.investor_events import (
    IR_PUBLIC_PAGE,
    SEC_EDGAR_EVENT_CANDIDATES,
    InvestorEventFetchResult,
    derive_sec_edgar_event_candidates,
    load_investor_event_source_config,
    normalize_ir_public_page_observations,
    run_investor_events_shadow,
)


NOW = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)


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
        return [row for row in self.filing_rows if not symbols or str(row["symbol"]).upper() in symbols]


@dataclass
class _IrClient:
    html: str
    calls: list[str] = field(default_factory=list)

    def fetch(self, *, symbol: str, source_url: str, observed_at: datetime) -> InvestorEventFetchResult:
        from finance_data_ops.shadow.investor_events import _request_hash

        self.calls.append(source_url)
        return InvestorEventFetchResult(
            provider_symbol=symbol,
            symbol=symbol,
            endpoint=source_url,
            request_hash=_request_hash(provider=IR_PUBLIC_PAGE, endpoint=source_url, provider_symbol=symbol),
            status="success",
            http_status=200,
            response_payload=self.html,
            error_reason=None,
            observed_at=observed_at,
        )


def _filing(*, form: str, description: str | None = None) -> dict[str, Any]:
    return {
        "provider_observation_id": f"sec-{form}", "symbol": "AAPL", "company_name": "Apple Inc.",
        "accession_number": "0000320193-26-000001", "form_type": form,
        "filing_date": date(2026, 7, 23), "acceptance_datetime": NOW,
        "primary_document": "aapl.htm", "primary_doc_description": description,
        "filing_url": "https://www.sec.gov/Archives/edgar/data/320193/aapl.htm",
        "known_at": date(2026, 7, 24), "ingested_at": NOW,
        "raw_payload_ref": "raw-sec", "raw_payload_hash": "hash", "source_metadata": {}, "data_quality_flags": {},
    }


def _config(path: Path, *, allowed_host: str = "investors.example.com") -> Path:
    path.write_text(json.dumps({"version": "investor_event_sources.v1", "sources": [{"symbol": "AAPL", "provider": "ir_public_page", "source_url": "https://investors.example.com/events", "allowed_host": allowed_host, "parser": "q4_public_html", "enabled": True}]}), encoding="utf-8")
    return path


def _env(*providers: str) -> dict[str, str]:
    return {"DATA_OPS_INVESTOR_EVENT_PROVIDERS": ",".join(providers), "DATA_OPS_IR_PUBLIC_PAGE_USER_AGENT": "Example contact@example.com"}


def _q4_html() -> str:
    return """<script type="application/json">[{"EventId":"evt-1","RevisionNumber":"2","Title":"Q2 Earnings Call","EventType":"Earnings","StartDate":"2026-08-01T13:00:00Z","EndDate":"2026-08-01T14:00:00Z","TimeZone":"America/New_York","WebCastLink":"https://investors.example.com/webcast","PresentationLink":"https://investors.example.com/presentation","Attachments":[{"Url":"https://investors.example.com/deck.pdf"}]}]</script>"""


def test_missing_providers_skips_without_network_or_database() -> None:
    report = run_investor_events_shadow(symbols=["AAPL"], env={})
    assert report["status"] == "skipped"
    assert report["reason"] == "no_investor_event_providers_enabled"
    assert report["live_calls"] == 0


def test_unknown_provider_skips_without_ir_user_agent_warning() -> None:
    report = run_investor_events_shadow(
        symbols=["AAPL"],
        env={"DATA_OPS_INVESTOR_EVENT_PROVIDERS": "unknown_provider"},
    )
    assert report["status"] == "skipped"
    assert report["reason"] == "no_investor_event_providers_enabled"
    assert report["warnings"] == []


def test_missing_ir_user_agent_skips_ir_without_network() -> None:
    report = run_investor_events_shadow(symbols=["AAPL"], env={"DATA_OPS_INVESTOR_EVENT_PROVIDERS": IR_PUBLIC_PAGE})
    assert report["status"] == "skipped"
    assert report["reason"] == "ir_public_page_user_agent_missing"
    assert report["providers"][IR_PUBLIC_PAGE]["skip_reason"] == "ir_public_page_user_agent_missing"
    assert report["skipped_symbols"] == []


def test_ir_source_config_rejects_unknown_parser(tmp_path: Path) -> None:
    path = tmp_path / "sources.json"
    path.write_text(json.dumps({"version": "v1", "sources": [{"symbol": "AAPL", "provider": IR_PUBLIC_PAGE, "source_url": "https://investors.example.com/events", "allowed_host": "investors.example.com", "parser": "unsupported", "enabled": True}]}), encoding="utf-8")
    assert load_investor_event_source_config(path) == {}


def test_host_allowlist_is_enforced(tmp_path: Path) -> None:
    repository = _Repository()
    client = _IrClient(_q4_html())
    report = run_investor_events_shadow(symbols=["AAPL"], repository=repository, ir_client=client, env=_env(IR_PUBLIC_PAGE), source_config_path=_config(tmp_path / "sources.json", allowed_host="other.example.com"), request_sleep_seconds=0, now=lambda: NOW)
    assert report["symbol_statuses"]["AAPL"][IR_PUBLIC_PAGE]["reason"] == "host_not_allowlisted"
    assert report["unresolved_symbols"] == ["AAPL"]
    assert client.calls == []


def test_sec_proxy_filing_derives_shareholder_meeting_candidate() -> None:
    rows = derive_sec_edgar_event_candidates([_filing(form="DEF 14A")], observed_at=NOW)
    assert len(rows) == 1
    assert rows[0]["event_type_raw"] == "shareholder_meeting_candidate"
    assert rows[0]["related_filing_accession"] == "0000320193-26-000001"
    assert rows[0]["extraction_confidence"] == "medium"


def test_sec_material_filing_remains_low_confidence_heuristic() -> None:
    rows = derive_sec_edgar_event_candidates([_filing(form="8-K", description="Investor Presentation")], observed_at=NOW)
    assert len(rows) == 1
    assert rows[0]["event_type_raw"] == "investor_material_candidate"
    assert rows[0]["extraction_confidence"] == "low"
    assert rows[0]["data_quality_flags"]["sec_filing_metadata_heuristic"] is True


def test_sec_runner_uses_only_observed_filings_and_reports_candidates() -> None:
    repository = _Repository(filing_rows=[_filing(form="DEF 14A")])
    report = run_investor_events_shadow(
        symbols=["AAPL", "MSFT"],
        repository=repository,
        env={"DATA_OPS_INVESTOR_EVENT_PROVIDERS": "sec_edgar"},
        now=lambda: NOW,
    )
    assert report["live_calls"] == 0
    assert report["provider_observations"] == {"planned": 1, "written": 1}
    assert report["symbol_statuses"]["AAPL"][SEC_EDGAR_EVENT_CANDIDATES]["status"] == "success"
    assert report["symbol_statuses"]["MSFT"][SEC_EDGAR_EVENT_CANDIDATES]["status"] == "not_found"


def test_q4_style_fixture_normalizes_event_fields_and_urls() -> None:
    raw = _IrClient(_q4_html()).fetch(symbol="AAPL", source_url="https://investors.example.com/events", observed_at=NOW).to_raw_row()
    config = {"symbol": "AAPL", "source_url": "https://investors.example.com/events", "allowed_host": "investors.example.com", "parser": "q4_public_html"}
    first = normalize_ir_public_page_observations(raw_row=raw, config=config)[0]
    second = normalize_ir_public_page_observations(raw_row=raw, config=config)[0]
    assert first["provider_event_id"] == "evt-1"
    assert first["provider_revision"] == "2"
    assert first["starts_at_raw"] == datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
    assert first["ends_at_raw"] == datetime(2026, 8, 1, 14, 0, tzinfo=UTC)
    assert first["timezone_raw"] == "America/New_York"
    assert first["webcast_url_raw"].endswith("/webcast")
    assert first["attachment_urls_raw"] == ["https://investors.example.com/deck.pdf"]
    assert first["provider_observation_id"] == second["provider_observation_id"]
    assert first["observation_hash"] == second["observation_hash"]


def test_q4_config_falls_back_to_generic_markup_and_preserves_config_version(tmp_path: Path) -> None:
    config = load_investor_event_source_config(_config(tmp_path / "sources.json"))
    raw = _IrClient("<h2>Investor Day 2026-08-03</h2>").fetch(
        symbol="AAPL",
        source_url="https://investors.example.com/events",
        observed_at=NOW,
    ).to_raw_row()
    rows = normalize_ir_public_page_observations(raw_row=raw, config=config["AAPL"])
    assert rows[0]["title_raw"] == "Investor Day 2026-08-03"
    assert rows[0]["source_metadata"]["sourceConfigVersion"] == "investor_event_sources.v1"


def test_ir_cache_hit_avoids_second_live_fetch(tmp_path: Path) -> None:
    repository = _Repository()
    client = _IrClient(_q4_html())
    kwargs = {"symbols": ["AAPL"], "repository": repository, "ir_client": client, "env": _env(IR_PUBLIC_PAGE), "source_config_path": _config(tmp_path / "sources.json"), "request_sleep_seconds": 0, "now": lambda: NOW}
    first = run_investor_events_shadow(**kwargs)
    second = run_investor_events_shadow(**kwargs)
    assert first["live_calls"] == 1
    assert second["live_calls"] == 0
    assert second["raw_cache_hits"] == 1
    assert len(client.calls) == 1
    assert "permission_unverified_public_page" in second["warnings"]
