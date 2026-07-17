"""Frontier/onboarding candidate entity dedup audit."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from finance_data_ops.identity.chain import acceptance_fixture_candidates
from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.identity.universe import build_candidate_universe_from_frames
from finance_data_ops.settings import DataOpsSettings, load_settings
from scripts.measure_entity_identity_chain import (
    _build_measurement_with_raw_cache,
    build_entity_identity_measurement,
)


DEFAULT_FRONTIER_DEDUP_SCOPE_KEY = "tracked"


@dataclass(frozen=True, slots=True)
class TrackedEntityListing:
    symbol: str
    entity_id: str
    entity_lei: str = ""
    legal_name: str = ""
    attach_method: str = ""
    review_state: str = ""
    resolution_status: str = ""


@dataclass(frozen=True, slots=True)
class FrontierDedupOptions:
    source: str = "postgres"
    scope_key: str = DEFAULT_FRONTIER_DEDUP_SCOPE_KEY
    symbols: tuple[str, ...] = ()
    candidates_file: str | None = None
    candidate_source: str = "symbols"
    refresh_live: bool = False
    refresh_cache_misses: bool = False
    cache_root: str | None = None
    curated_identity_file: str | None = None
    batch_size: int | None = None
    request_sleep_seconds: float = 6.5
    gleif_page_size: int = 200
    gleif_request_sleep_seconds: float = 0.5
    gleif_lei_isin_max_retries: int = 3
    gleif_retry_backoff_seconds: float = 1.0
    gleif_retry_jitter_seconds: float = 0.25


def run_frontier_entity_dedup_audit(
    *,
    options: FrontierDedupOptions,
    settings: DataOpsSettings | None = None,
    tracked_entities: list[TrackedEntityListing] | None = None,
    candidates: list[ListingCandidate] | None = None,
) -> dict[str, Any]:
    if options.refresh_cache_misses and not options.refresh_live:
        raise ValueError("refresh_cache_misses requires refresh_live.")
    settings = settings or load_settings(cache_root=options.cache_root)
    resolved_candidates = candidates or load_frontier_candidates(options=options, settings=settings)
    measurement_args = _measurement_args_from_options(options)
    if options.source == "fixtures":
        measurement_args.symbols = [",".join(candidate.symbol for candidate in resolved_candidates)]
        measurement = build_entity_identity_measurement(args=measurement_args, settings=settings)
    else:
        measurement = _build_measurement_with_raw_cache(
            args=measurement_args,
            candidates=resolved_candidates,
            selected_symbols=[candidate.symbol for candidate in resolved_candidates],
            settings=settings,
        )
    current_tracked = tracked_entities
    if current_tracked is None:
        current_tracked = (
            fixture_tracked_entity_listings()
            if options.source == "fixtures"
            else read_current_tracked_entity_listings(database_dsn=settings.database_dsn, scope_key=options.scope_key)
        )
    rows = classify_frontier_candidates(
        measurement=measurement,
        candidates=resolved_candidates,
        tracked_entities=current_tracked,
    )
    summary = _summary(rows=rows, measurement_summary=getattr(measurement, "summary", {}) or {}, options=options)
    return {
        "mode": "dry_run",
        "scope_key": options.scope_key,
        "candidate_source": options.candidate_source,
        "summary": summary,
        "candidates": rows,
    }


def classify_frontier_candidates(
    *,
    measurement: Any,
    candidates: list[ListingCandidate],
    tracked_entities: list[TrackedEntityListing],
) -> list[dict[str, Any]]:
    tracked_by_entity_id, tracked_by_lei = _tracked_indexes(tracked_entities)
    audit_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): row for row in getattr(measurement, "heuristic_attach_audit", []) or []
    }
    measured_by_symbol = {
        str(row.get("symbol") or "").strip().upper(): row for row in getattr(measurement, "symbol_rows", []) or []
    }
    out: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = str(candidate.symbol or "").strip().upper()
        row = measured_by_symbol.get(symbol, {})
        audit_row = audit_by_symbol.get(symbol, {})
        entity_lei = str(row.get("entity_lei") or row.get("lei") or "").strip().upper()
        entity_id = str(row.get("entity_id") or (f"lei:{entity_lei}" if entity_lei else "")).strip()
        tracked_matches = []
        if entity_id and entity_id in tracked_by_entity_id:
            tracked_matches = tracked_by_entity_id[entity_id]
        elif entity_lei and entity_lei in tracked_by_lei:
            tracked_matches = tracked_by_lei[entity_lei]

        status, action, reason = _classify_row(
            row=row,
            audit_row=audit_row,
            tracked_matches=tracked_matches,
            has_resolved_entity=bool(entity_id or entity_lei),
        )
        matched_entity = tracked_matches[0] if tracked_matches else None
        out.append(
            {
                "candidate_symbol": symbol,
                "candidate_provider_symbol": candidate.provider_symbol or symbol,
                "candidate_name": candidate.name or row.get("internal_candidate_name") or row.get("openfigi_name") or "",
                "candidate_country": candidate.country or row.get("derived_listing_country") or row.get("listing_country") or "",
                "candidate_exchange": candidate.exchange or row.get("openfigi_exchange") or "",
                "entity_dedup_status": status,
                "candidate_entity_id": entity_id,
                "candidate_lei": entity_lei,
                "candidate_legal_name": row.get("entity_legal_name")
                or row.get("candidate_legal_name")
                or row.get("legal_name")
                or "",
                "matched_entity_id": matched_entity.entity_id if matched_entity else "",
                "matched_entity_lei": matched_entity.entity_lei if matched_entity else "",
                "matched_entity_legal_name": matched_entity.legal_name if matched_entity else "",
                "matched_tracked_symbols": sorted(match.symbol for match in tracked_matches),
                "confidence": row.get("attachment_confidence") or "",
                "method": row.get("entity_attach_method") or "",
                "reason": reason,
                "recommended_action": action,
            }
        )
    return out


def load_frontier_candidates(*, options: FrontierDedupOptions, settings: DataOpsSettings) -> list[ListingCandidate]:
    if options.candidates_file:
        return read_frontier_candidates_file(options.candidates_file)
    if options.source == "fixtures":
        symbols = list(options.symbols) or ["SAP.DE", "ACME", "TLS.AX"]
        return acceptance_fixture_candidates(symbols=symbols) or [
            ListingCandidate(symbol=symbol, provider_symbol=symbol, country="US", source="fixtures")
            for symbol in symbols
        ]
    if options.symbols:
        return [
            ListingCandidate(symbol=_symbol(symbol), provider_symbol=_symbol(symbol), source="frontier_symbols")
            for symbol in options.symbols
            if _symbol(symbol)
        ]
    if options.candidate_source == "etf-holding-identity":
        return read_postgres_etf_holding_frontier_candidates(database_dsn=settings.database_dsn)
    raise ValueError("--symbols, --candidates-file, or --candidate-source etf-holding-identity is required.")


def read_frontier_candidates_file(path: str | Path) -> list[ListingCandidate]:
    candidate_path = Path(path)
    if candidate_path.suffix.lower() == ".json":
        payload = json.loads(candidate_path.read_text())
        rows = payload if isinstance(payload, list) else payload.get("candidates", [])
    else:
        with candidate_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
    out: list[ListingCandidate] = []
    for row in rows:
        if isinstance(row, str):
            symbol = _symbol(row)
            row = {"symbol": symbol}
        symbol = _symbol(row.get("symbol") or row.get("onboard_symbol") or row.get("provider_symbol") or row.get("source_symbol"))
        if not symbol:
            continue
        out.append(
            ListingCandidate(
                symbol=symbol,
                provider_symbol=_symbol(row.get("provider_symbol") or row.get("onboard_symbol") or symbol) or symbol,
                exchange=_text(row.get("exchange") or row.get("source_exchange") or row.get("onboard_exchange"), upper=True),
                exchange_mic=_text(row.get("exchange_mic") or row.get("source_exchange_mic"), upper=True),
                country=_text(row.get("country") or row.get("source_country"), upper=True),
                currency=_text(row.get("currency"), upper=True),
                name=_text(row.get("name") or row.get("source_name") or row.get("company_name")),
                source="frontier_candidates_file",
            )
        )
    return out


def read_postgres_etf_holding_frontier_candidates(*, database_dsn: str | None) -> list[ListingCandidate]:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for frontier candidate reads.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres frontier reads.") from exc
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        if not _table_exists(conn, "public", "etf_holding_onboarding_identity"):
            return []
        columns = _available_columns(conn, "public", "etf_holding_onboarding_identity")
        selected = [
            column
            for column in (
                "source_symbol",
                "source_name",
                "source_country",
                "source_exchange",
                "source_exchange_mic",
                "provider_symbol",
                "onboard_symbol",
                "is_onboardable",
            )
            if column in columns
        ]
        if not selected:
            return []
        where = " where is_onboardable is true" if "is_onboardable" in columns else ""
        with conn.cursor() as cur:
            cur.execute(
                f"select {', '.join(_quote(column) for column in selected)} "
                f"from public.etf_holding_onboarding_identity{where}"
            )
            frame = pd.DataFrame([dict(row) for row in cur.fetchall()])
    return build_candidate_universe_from_frames(etf_holding_identity=frame)


def read_current_tracked_entity_listings(*, database_dsn: str | None, scope_key: str) -> list[TrackedEntityListing]:
    if not database_dsn:
        raise ValueError("DATA_OPS_DATABASE_URL is required for current Entity Layer reads.")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Postgres Entity Layer reads.") from exc
    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select batch_id
                from feature_store.entity_identity_publication_current
                where scope_key = %s
                """,
                (scope_key,),
            )
            batch_row = cur.fetchone()
            if not batch_row:
                return []
            batch_id = str(batch_row["batch_id"])
            cur.execute(
                """
                select
                  l.symbol,
                  l.entity_id,
                  coalesce(m.lei, l.lei, '') as entity_lei,
                  coalesce(m.legal_name, '') as legal_name,
                  coalesce(l.attach_method, '') as attach_method,
                  coalesce(l.review_state, '') as review_state,
                  coalesce(l.resolution_status, '') as resolution_status
                from feature_store.entity_listing l
                join feature_store.entity_master m on m.entity_id = l.entity_id
                where l.publication_batch_id = %s
                  and m.publication_batch_id = %s
                  and coalesce(l.resolution_status, '') = 'resolved'
                  and coalesce(m.resolution_status, '') = 'resolved'
                """,
                (batch_id, batch_id),
            )
            rows = cur.fetchall()
    return [
        TrackedEntityListing(
            symbol=_symbol(row.get("symbol")),
            entity_id=str(row.get("entity_id") or ""),
            entity_lei=_text(row.get("entity_lei"), upper=True),
            legal_name=_text(row.get("legal_name")),
            attach_method=_text(row.get("attach_method"), upper=False),
            review_state=_text(row.get("review_state"), upper=False),
            resolution_status=_text(row.get("resolution_status"), upper=False),
        )
        for row in rows
        if _symbol(row.get("symbol")) and str(row.get("entity_id") or "")
    ]


def fixture_tracked_entity_listings() -> list[TrackedEntityListing]:
    return [
        TrackedEntityListing(
            symbol="SAP",
            entity_id="lei:529900D6BF99LW9R2E68",
            entity_lei="529900D6BF99LW9R2E68",
            legal_name="SAP SE",
            attach_method="direct_isin",
            review_state="resolved",
            resolution_status="resolved",
        )
    ]


def _classify_row(
    *,
    row: dict[str, Any],
    audit_row: dict[str, Any],
    tracked_matches: list[TrackedEntityListing],
    has_resolved_entity: bool,
) -> tuple[str, str, str]:
    if tracked_matches and has_resolved_entity:
        return "already_tracked_entity", "suppress", "resolved_entity_already_tracked"
    if _is_review_required(row=row, audit_row=audit_row):
        return "needs_review", "review", row.get("entity_attach_reason") or audit_row.get("review_reason") or "identity_review_required"
    if _has_cache_miss(row):
        return "cache_miss", "show_with_warning", "raw_identity_cache_miss"
    if row.get("entity_lei"):
        return "new_entity_candidate", "onboard", row.get("entity_attach_reason") or "resolved_entity_not_tracked"
    if row.get("candidate_lei") or row.get("foreign_issuer_candidate_lei"):
        return "provisional_or_unresolved", "show_with_warning", row.get("entity_attach_reason") or "candidate_lei_not_confirmed"
    return "provisional_or_unresolved", "show_with_warning", row.get("entity_attach_reason") or "entity_identity_unresolved"


def _is_review_required(*, row: dict[str, Any], audit_row: dict[str, Any]) -> bool:
    if str(row.get("decision_bucket") or "") == "needs_manual_review":
        return True
    if audit_row and str(audit_row.get("review_status") or "") not in {"", "machine_verifiably_safe"}:
        return True
    return False


def _has_cache_miss(row: dict[str, Any]) -> bool:
    reasons = row.get("entity_attach_reasons") or []
    if any("cache_miss" in str(reason) for reason in reasons):
        return True
    for key in (
        "openfigi_status",
        "isin_error_reason",
        "lei_status",
        "legal_name_anchor_reject_reason",
        "legal_name_candidate_lei_expansion_error",
    ):
        if str(row.get(key) or "") == "cache_miss":
            return True
    return False


def _tracked_indexes(
    tracked_entities: list[TrackedEntityListing],
) -> tuple[dict[str, list[TrackedEntityListing]], dict[str, list[TrackedEntityListing]]]:
    by_entity_id: dict[str, list[TrackedEntityListing]] = {}
    by_lei: dict[str, list[TrackedEntityListing]] = {}
    for row in tracked_entities:
        if row.entity_id and row.resolution_status == "resolved":
            by_entity_id.setdefault(row.entity_id, []).append(row)
        if row.entity_lei and row.resolution_status == "resolved":
            by_lei.setdefault(row.entity_lei, []).append(row)
    return by_entity_id, by_lei


def _summary(*, rows: list[dict[str, Any]], measurement_summary: dict[str, Any], options: FrontierDedupOptions) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    for row in rows:
        status_counts[str(row.get("entity_dedup_status") or "")] = status_counts.get(str(row.get("entity_dedup_status") or ""), 0) + 1
        action_counts[str(row.get("recommended_action") or "")] = action_counts.get(str(row.get("recommended_action") or ""), 0) + 1
    return {
        "scope_key": options.scope_key,
        "candidate_count": len(rows),
        "status_counts": dict(sorted(status_counts.items())),
        "recommended_action_counts": dict(sorted(action_counts.items())),
        "refresh_live": bool(options.refresh_live),
        "refresh_cache_misses": bool(options.refresh_cache_misses),
        "raw_cache_enabled": options.source == "postgres",
        "cache_miss_count": status_counts.get("cache_miss", 0),
        "measurement_cache_miss_counts": {
            "openfigi": int(measurement_summary.get("openfigi_cache_miss_count") or 0),
            "listing_isin": int(measurement_summary.get("listing_isin_cache_miss_count") or 0),
            "gleif_isin_lei": int(measurement_summary.get("gleif_isin_lei_cache_miss_count") or 0),
            "gleif_lei_isin": int(measurement_summary.get("gleif_lei_isin_cache_miss_count") or 0),
            "legal_name": int(measurement_summary.get("legal_name_cache_miss_count") or 0),
        },
    }


def _measurement_args_from_options(options: FrontierDedupOptions) -> SimpleNamespace:
    return SimpleNamespace(
        symbols=list(options.symbols),
        source=options.source,
        offline=not bool(options.refresh_live),
        use_raw_cache=options.source == "postgres",
        refresh_cache_misses=bool(options.refresh_cache_misses),
        tracked_only=False,
        apply_cache=False,
        cache_root=options.cache_root,
        curated_identity_file=options.curated_identity_file,
        batch_size=options.batch_size,
        request_sleep_seconds=options.request_sleep_seconds,
        gleif_page_size=options.gleif_page_size,
        gleif_request_sleep_seconds=options.gleif_request_sleep_seconds,
        gleif_lei_isin_max_retries=options.gleif_lei_isin_max_retries,
        gleif_retry_backoff_seconds=options.gleif_retry_backoff_seconds,
        gleif_retry_jitter_seconds=options.gleif_retry_jitter_seconds,
    )


def _available_columns(conn: Any, schema: str, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s and table_name = %s
            """,
            (schema, table),
        )
        return {str(row["column_name"]) for row in cur.fetchall()}


def _table_exists(conn: Any, schema: str, table: str) -> bool:
    return bool(_available_columns(conn, schema, table))


def _quote(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return ""
    return text.upper() if upper else text
