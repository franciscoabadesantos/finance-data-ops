from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

from finance_data_ops.shadow.equity_capital_events import (
    SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE,
    derive_sec_edgar_equity_capital_candidates,
    run_equity_capital_events_shadow,
)


NOW = datetime(2026, 7, 25, 12, 0, tzinfo=UTC)


@dataclass
class _Repository:
    filing_rows: list[dict[str, Any]] = field(default_factory=list)
    observation_rows: list[dict[str, Any]] = field(default_factory=list)

    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        return [row for row in self.filing_rows if str(row["symbol"]).upper() in symbols]

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        records = {str(row["provider_observation_id"]): row for row in self.observation_rows}
        records.update({str(row["provider_observation_id"]): dict(row) for row in rows})
        self.observation_rows = list(records.values())


def _filing(*, symbol: str = "AAPL", form: str = "8-K", description: str | None = "Share repurchase program") -> dict[str, Any]:
    return {
        "provider_observation_id": f"filing-{symbol}-{form}-{description}",
        "symbol": symbol,
        "company_name": f"{symbol} Inc.",
        "accession_number": f"0000000000-26-{symbol[-1:]}00001",
        "form_type": form,
        "filing_date": date(2026, 7, 24),
        "acceptance_datetime": NOW,
        "primary_document": "report.htm",
        "primary_doc_description": description,
        "filing_url": "https://www.sec.gov/Archives/edgar/data/1/report.htm",
        "known_at": date(2026, 7, 24),
        "ingested_at": NOW,
        "raw_payload_ref": "filing-raw",
        "raw_payload_hash": "filing-hash",
        "source_metadata": {},
        "data_quality_flags": {},
    }


def _env() -> dict[str, str]:
    return {"DATA_OPS_EQUITY_CAPITAL_EVENT_PROVIDERS": SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE}


def test_missing_providers_skips_without_database_or_network() -> None:
    report = run_equity_capital_events_shadow(symbols=["AAPL"], env={})
    assert report["status"] == "skipped"
    assert report["reason"] == "no_equity_capital_event_providers_enabled"
    assert report["live_calls"] == 0


def test_sec_provider_uses_local_filings_only_and_creates_buyback_candidates() -> None:
    repository = _Repository(filing_rows=[
        _filing(form="10-Q", description="Item 703 issuer purchases of equity securities"),
        _filing(symbol="MSFT", form="8-K", description="Authorized share repurchase program"),
    ])
    report = run_equity_capital_events_shadow(
        symbols=["AAPL", "MSFT", "9684.T"], repository=repository, env=_env(), now=lambda: NOW
    )

    assert report["live_calls"] == 0
    assert report["provider_observations"] == {"planned": 2, "written": 2}
    assert report["by_event_type"] == {"buyback_actual": 1, "buyback_authorization": 1}
    assert report["unresolved_symbols"] == ["9684.T"]
    actual = next(row for row in repository.observation_rows if row["event_type"] == "buyback_actual")
    assert actual["amount_executed"] is None
    assert actual["data_quality_flags"]["actualRepurchaseCandidate"] is True
    assert actual["data_quality_flags"]["parserLowConfidence"] is True


def test_s3_424b_and_8k_metadata_create_only_document_candidates() -> None:
    rows = derive_sec_edgar_equity_capital_candidates(
        [
            _filing(symbol="ASML", form="S-3", description="Shelf registration statement"),
            _filing(symbol="ASML", form="424B5", description="At-the-market equity distribution agreement"),
            _filing(symbol="ASML", form="8-K", description="Underwritten public offering of common stock"),
        ],
        observed_at=NOW,
    )
    assert {row["event_type"] for row in rows} == {"shelf_registration", "atm_program", "secondary_offering"}
    assert all(row["event_status"] == "candidate" for row in rows)
    assert all(row["currency"] is None and row["share_count_announced"] is None for row in rows)


def test_irrelevant_form4_and_schedule_13d_are_ignored() -> None:
    rows = derive_sec_edgar_equity_capital_candidates(
        [
            _filing(form="4", description="Share repurchase program"),
            _filing(form="Schedule 13D", description="Public offering"),
            _filing(form="8-K", description="Director appointment"),
        ],
        observed_at=NOW,
    )
    assert rows == []


def test_filing_without_document_url_is_not_emitted() -> None:
    filing = _filing()
    filing["filing_url"] = None
    assert derive_sec_edgar_equity_capital_candidates([filing], observed_at=NOW) == []


def test_no_candidate_symbol_is_reported_not_found_and_examples_are_capped() -> None:
    long_description = "Share repurchase program " + ("x" * 500)
    repository = _Repository(filing_rows=[_filing(description=long_description)])
    report = run_equity_capital_events_shadow(
        symbols=["AAPL", "MSFT"], repository=repository, env=_env(), now=lambda: NOW
    )
    assert report["symbol_statuses"]["MSFT"][SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE]["status"] == "not_found"
    assert report["unresolved_symbols"] == ["MSFT"]
    assert len(report["examples"][0]["quote_snippet"]) <= 280


def test_deterministic_observation_ids_make_reruns_idempotent() -> None:
    repository = _Repository(filing_rows=[_filing()])
    first = run_equity_capital_events_shadow(symbols=["AAPL"], repository=repository, env=_env(), now=lambda: NOW)
    second = run_equity_capital_events_shadow(symbols=["AAPL"], repository=repository, env=_env(), now=lambda: NOW)
    assert first["provider_observations"]["written"] == second["provider_observations"]["written"] == 1
    assert len(repository.observation_rows) == 1
    assert repository.observation_rows[0]["provider"] == SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE
