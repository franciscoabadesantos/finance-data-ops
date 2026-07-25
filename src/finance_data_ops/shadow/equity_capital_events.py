"""Manual SEC-filing-only shadow ingestion for equity-capital event evidence."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, date, datetime
import hashlib
import json
import os
from typing import Any, Protocol

from finance_data_ops.publish.client import PostgresPublisher


SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE = "sec_edgar_equity_capital_candidate"
_SUPPORTED_PROVIDERS = {SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE}
_MAX_EXAMPLES = 25
_MAX_SNIPPET_CHARS = 280
_IGNORED_FORMS = {"4", "SCHEDULE 13D", "SC 13D", "13D"}

_RULES: tuple[tuple[str, set[str], tuple[str, ...]], ...] = (
    ("buyback_actual", {"10-Q", "10-K"}, ("issuer purchases of equity securities", "item 703", "repurchase table", "share repurchase", "stock repurchase")),
    ("buyback_authorization", {"8-K"}, ("share repurchase program", "stock repurchase program", "buyback authorization", "authorized repurchase", "accelerated share repurchase", " asr")),
    ("atm_program", {"8-K", "S-3", "S-3ASR", "424B", "424B5"}, ("at-the-market", "atm offering", "equity distribution agreement", "sales agreement")),
    ("secondary_offering", {"8-K", "S-3", "424B", "424B5"}, ("public offering", "secondary offering", "offering of common stock", "underwritten offering")),
    ("shelf_registration", {"S-3", "S-3ASR", "424B", "424B5"}, ("shelf registration", "registration statement", "prospectus supplement")),
)


class EquityCapitalEventsRepository(Protocol):
    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]: ...
    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None: ...


class PostgresEquityCapitalEventsRepository:
    def __init__(self, *, database_dsn: str) -> None:
        self._database_dsn = _to_psycopg_dsn(database_dsn)
        if not self._database_dsn:
            raise ValueError("DATA_OPS_DATABASE_URL or DATABASE_URL is required for equity-capital shadow ingestion.")
        self._publisher = PostgresPublisher(
            database_dsn=self._database_dsn,
            application_name="finance-data-ops-equity-capital-events-shadow",
        )

    def load_filing_observations(self, *, symbols: list[str]) -> list[dict[str, Any]]:
        with _connect(self._database_dsn) as conn:
            with conn.cursor() as cur:
                params: tuple[Any, ...] = ()
                where = "WHERE provider = 'sec_edgar'"
                if symbols:
                    where += " AND UPPER(symbol) = ANY(%s)"
                    params = (symbols,)
                cur.execute(
                    f"""
                    SELECT provider_observation_id, symbol, company_name, accession_number,
                           form_type, filing_date, acceptance_datetime, primary_document,
                           primary_doc_description, filing_url, known_at, ingested_at,
                           raw_payload_ref, raw_payload_hash, source_metadata, data_quality_flags
                    FROM source_cache.filing_provider_observations
                    {where}
                    """,
                    params,
                )
                return [dict(zip([column.name for column in cur.description], row, strict=True)) for row in cur.fetchall()]

    def upsert_provider_observations(self, rows: list[dict[str, Any]]) -> None:
        self._publisher.upsert(
            "source_cache.equity_capital_event_provider_observations",
            rows,
            on_conflict="provider_observation_id",
        )


def run_equity_capital_events_shadow(
    *, symbols: Iterable[str], repository: EquityCapitalEventsRepository | None = None,
    env: Mapping[str, str] | None = None, dry_run: bool = False,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    requested = _symbols(symbols)
    enabled = _provider_allowlist(dict(os.environ) if env is None else env)
    report = _new_report(requested)
    if not enabled:
        report.update(status="skipped", reason="no_equity_capital_event_providers_enabled")
        return report
    report["providers"][SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE]["enabled"] = True
    if dry_run:
        report.update(status="dry_run")
        return report
    if repository is None:
        raise ValueError("repository is required when equity-capital shadow ingestion is enabled.")
    observed_at = _utc((now or (lambda: datetime.now(UTC)))())
    observations = derive_sec_edgar_equity_capital_candidates(
        repository.load_filing_observations(symbols=requested), observed_at=observed_at
    )
    report["provider_observations"]["planned"] = len(observations)
    candidate_symbols = {row["symbol"] for row in observations}
    for symbol in requested:
        _symbol_status(
            report,
            symbol,
            "success" if symbol in candidate_symbols else "not_found",
            None if symbol in candidate_symbols else "no_equity_capital_candidates_from_observed_filings",
        )
    if observations:
        repository.upsert_provider_observations(observations)
        report["provider_observations"]["written"] = len(observations)
    report["providers"][SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE]["status"] = "completed"
    report.update(_summaries(observations, requested))
    report["unresolved_symbols"] = sorted(
        symbol for symbol in requested if symbol not in candidate_symbols
    )
    report["status"] = "completed"
    return report


def derive_sec_edgar_equity_capital_candidates(
    rows: Iterable[Mapping[str, Any]], *, observed_at: datetime
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for row in rows:
        form = (_text(row.get("form_type")) or "").upper()
        if (
            form in _IGNORED_FORMS
            or form.startswith("SCHEDULE 13D")
            or not _symbol(row.get("symbol"))
            or not _text(row.get("filing_url"))
        ):
            continue
        evidence = _evidence(row)
        for event_type, allowed_forms, signals in _RULES:
            if not _form_matches(form, allowed_forms):
                continue
            signal = next((value for value in signals if value in evidence.lower()), None)
            if signal is None:
                continue
            observations.append(_observation(row=row, event_type=event_type, signal=signal, observed_at=observed_at))
    return _dedupe(observations)


def _observation(*, row: Mapping[str, Any], event_type: str, signal: str, observed_at: datetime) -> dict[str, Any]:
    symbol = _symbol(row.get("symbol"))
    accession = _text(row.get("accession_number"))
    filing_url = _text(row.get("filing_url"))
    form = _text(row.get("form_type"))
    evidence = _evidence(row)
    known_at = _utc(observed_at).date()
    observation_hash = _hash({"provider": SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE, "symbol": symbol, "accession": accession, "event_type": event_type, "signal": signal})
    flags: dict[str, Any] = {
        "equityCapitalCandidate": True,
        "secEdgarDerived": True,
        "secFilingMetadataHeuristic": True,
        "parserLowConfidence": True,
    }
    flags[_event_flag(event_type)] = True
    return {
        "provider_observation_id": _hash({"kind": "equity_capital_event_provider_observation.v1", "observation_hash": observation_hash, "known_at": known_at.isoformat()}),
        "provider": SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE,
        "provider_event_id": None,
        "provider_revision": None,
        "symbol": symbol,
        "provider_symbol": symbol,
        "company_name": _text(row.get("company_name")),
        "event_family": "equity_capital",
        "event_type": event_type,
        "event_subtype": "asr" if "asr" in signal.lower() else None,
        "event_status": "candidate",
        "source_document_type": "sec_filing_candidate",
        "source_title": _snippet(_text(row.get("primary_doc_description")) or _text(row.get("primary_document")) or form),
        "source_document_url": filing_url,
        "related_filing_accession": accession,
        "related_filing_form": form,
        "related_filing_url": filing_url,
        "related_exhibit": _exhibit(row),
        "announcement_date": None,
        "filing_date": _date(row.get("filing_date")),
        "accepted_at": _datetime(row.get("acceptance_datetime")),
        "effective_date": None,
        "period_start": None,
        "period_end": None,
        "program_expiration_date": None,
        "amount_authorized": None,
        "amount_announced": None,
        "amount_executed": None,
        "share_count_authorized": None,
        "share_count_announced": None,
        "share_count_executed": None,
        "average_price": None,
        "currency": None,
        "counterparty_or_agent": None,
        "program_name": None,
        "security_type": None,
        "quote_snippet": _snippet(evidence),
        "evidence_locator": {"matchedSignal": signal, "source": "filing_metadata"},
        "observed_at": observed_at,
        "known_at": known_at,
        "fetched_at": observed_at,
        "raw_payload_ref": None,
        "payload_hash": _text(row.get("raw_payload_hash")),
        "observation_hash": observation_hash,
        "parser_version": "sec_filing_metadata_equity_capital.v1",
        "extraction_confidence": "low",
        "data_quality_flags": flags,
        "source_metadata": {
            "matchedSignal": signal,
            "sourceFilingProviderObservationId": _text(row.get("provider_observation_id")),
            "relatedFilingRawPayloadRef": _text(row.get("raw_payload_ref")),
            "sourceFilingDataQualityFlags": _metadata(row.get("data_quality_flags")),
            "sourceFilingMetadata": _metadata(row.get("source_metadata")),
        },
    }


def _new_report(symbols: list[str]) -> dict[str, Any]:
    return {
        "status": "skipped", "mode": "shadow", "symbols_requested": symbols,
        "providers": {SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE: {"enabled": False, "skip_reason": None}},
        "live_calls": 0, "raw_cache_hits": 0,
        "provider_observations": {"planned": 0, "written": 0},
        "symbol_statuses": {}, "unresolved_symbols": [], "skipped_symbols": [], "warnings": [],
    }


def _summaries(rows: list[dict[str, Any]], requested: list[str]) -> dict[str, Any]:
    by_symbol = Counter(str(row["symbol"]) for row in rows)
    return {
        "by_provider": dict(sorted(Counter(str(row["provider"]) for row in rows).items())),
        "by_symbol": {symbol: by_symbol[symbol] for symbol in requested},
        "by_event_type": dict(sorted(Counter(str(row["event_type"]) for row in rows).items())),
        "by_source_document_type": dict(sorted(Counter(str(row.get("source_document_type") or "unknown") for row in rows).items())),
        "extraction_confidence_counts": dict(sorted(Counter(str(row["extraction_confidence"]) for row in rows).items())),
        "examples": [{key: row.get(key) for key in ("symbol", "event_type", "related_filing_accession", "source_document_url", "extraction_confidence", "quote_snippet")} for row in rows[:_MAX_EXAMPLES]],
    }


def _provider_allowlist(env: Mapping[str, str]) -> set[str]:
    return {item.strip().lower() for item in str(env.get("DATA_OPS_EQUITY_CAPITAL_EVENT_PROVIDERS") or "").split(",") if item.strip()} & _SUPPORTED_PROVIDERS
def _symbol_status(report: dict[str, Any], symbol: str, status: str, reason: str | None) -> None: report["symbol_statuses"].setdefault(symbol, {})[SEC_EDGAR_EQUITY_CAPITAL_CANDIDATE] = {"status": status, "reason": reason, "cache_hit": False}
def _evidence(row: Mapping[str, Any]) -> str: return " ".join(filter(None, (_text(row.get("form_type")), _text(row.get("primary_document")), _text(row.get("primary_doc_description")), _text(row.get("filing_url")), json.dumps(row.get("source_metadata") or {}, default=str))))
def _form_matches(form: str, allowed: set[str]) -> bool: return form in allowed or (form.startswith("424B") and "424B" in allowed)
def _event_flag(event_type: str) -> str: return {"buyback_actual": "actualRepurchaseCandidate", "buyback_authorization": "authorizationCandidate", "atm_program": "atmProgramCandidate", "secondary_offering": "secondaryOfferingCandidate", "shelf_registration": "shelfRegistrationCandidate"}[event_type]
def _exhibit(row: Mapping[str, Any]) -> str | None:
    for value in (_text(row.get("primary_document")), _text(row.get("primary_doc_description"))):
        if value and "ex-" in value.lower(): return value
    return None
def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]: return list({str(row["provider_observation_id"]): row for row in rows}.values())
def _hash(value: Any) -> str: return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
def _symbols(values: Iterable[str]) -> list[str]: return list(dict.fromkeys(symbol for value in values if (symbol := _symbol(value))))
def _symbol(value: Any) -> str: return (_text(value) or "").upper()
def _text(value: Any) -> str | None: return str(value).strip() if value is not None and str(value).strip() else None
def _snippet(value: Any) -> str | None: return (_text(value) or "")[:_MAX_SNIPPET_CHARS] or None
def _metadata(value: Any) -> dict[str, Any]: return dict(value) if isinstance(value, Mapping) else {}
def _date(value: Any) -> date | None:
    if isinstance(value, datetime): return _utc(value).date()
    if isinstance(value, date): return value
    try: return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError: return None
def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime): return _utc(value)
    try: return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00"))) if value else None
    except ValueError: return None
def _utc(value: datetime) -> datetime: return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
def _to_psycopg_dsn(dsn: str) -> str: return "postgresql://" + str(dsn).strip().removeprefix("postgresql+psycopg://") if str(dsn).strip().startswith("postgresql+psycopg://") else str(dsn).strip()
def _connect(database_dsn: str):
    import psycopg
    return psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-equity-capital-events-shadow")
