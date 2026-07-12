"""Audit and reconcile ticker_registry coverage for source refresh scheduling."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import json
import re
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from finance_data_ops.geography import country_from_source_or_symbol, normalize_country, region_for_country
from finance_data_ops.symbology import normalize_symbol_with_country
from finance_data_ops.validation.symbol_resolution import (
    ALLOWED_PROMOTION_STATUSES,
    CANONICAL_REFRESH_REGIONS,
    normalize_refresh_region,
)
from finance_data_ops.validation.ticker_registry import TICKER_REGISTRY_COLUMNS, build_registry_key

RECONCILIATION_NOTE_KEY = "source_universe_reconciliation"
US_SHARE_CLASS_ALIAS_PATTERN = re.compile(r"^[A-Z]{1,5}[.-][A-Z]$")
SCHEDULE_REGION_SET = set(CANONICAL_REFRESH_REGIONS)
APAC_SCHEDULE_COUNTRIES = {"IL"}
US_SCHEDULE_EXCHANGES = {
    "AMEX",
    "ASE",
    "B3",
    "BMF",
    "BVMF",
    "NASDAQ",
    "NMS",
    "NYQ",
    "NYSE",
    "PCX",
    "SAO",
    "TOR",
    "TSX",
    "XASE",
    "XBMF",
    "XNYS",
    "XNAS",
    "XTSE",
}
EU_SCHEDULE_EXCHANGES = {
    "AMS",
    "CPH",
    "EPA",
    "ETR",
    "HEL",
    "LIS",
    "LSE",
    "MIL",
    "OSL",
    "PAR",
    "STO",
    "SWX",
    "XAMS",
    "XCSE",
    "XETR",
    "XLIS",
    "XLON",
    "XMIL",
    "XPAR",
    "XSTO",
    "XSWX",
}
APAC_SCHEDULE_EXCHANGES = {
    "ASX",
    "HKG",
    "HKEX",
    "JPX",
    "KSC",
    "NSE",
    "SES",
    "SHH",
    "SHZ",
    "TASE",
    "TLV",
    "TSE",
    "XASX",
    "XHKG",
    "XJPX",
    "XSES",
    "XTAE",
    "XTKS",
}


@dataclass(frozen=True, slots=True)
class SourceUniverseReconciliationPlan:
    generated_at: str
    candidate_count: int
    required_symbol_count: int
    selected_symbol_count: int
    issues: list[dict[str, Any]]
    issue_counts: dict[str, int]
    materialized_residue: list[dict[str, Any]]
    reviewed_exclusions: list[dict[str, Any]]
    upsert_rows: list[dict[str, Any]]
    supersede_rows: list[dict[str, Any]]
    reject_rows: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_source_universe_reconciliation_plan(
    *,
    registry_frame: pd.DataFrame | None,
    readiness_frame: pd.DataFrame | None,
    prices_frame: pd.DataFrame | None,
    technicals_frame: pd.DataFrame | None,
    ticker_page_summary_frame: pd.DataFrame | None,
    entity_attributes_frame: pd.DataFrame | None = None,
    fundamentals_frame: pd.DataFrame | None = None,
    earnings_frame: pd.DataFrame | None = None,
    reviewed_exclusions: Iterable[str] | None = None,
) -> SourceUniverseReconciliationPlan:
    registry_rows = _records(registry_frame)
    candidates = _build_candidates(
        readiness_frame=readiness_frame,
        prices_frame=prices_frame,
        technicals_frame=technicals_frame,
        ticker_page_summary_frame=ticker_page_summary_frame,
        fundamentals_frame=fundamentals_frame,
        earnings_frame=earnings_frame,
    )
    entity_by_symbol = _entity_attributes_by_symbol(entity_attributes_frame)
    rows_by_symbol = _registry_rows_by_symbol(registry_rows)
    selected_rows_by_symbol = _selected_registry_rows_by_symbol(registry_rows)
    exclusions = {str(value).strip().upper() for value in reviewed_exclusions or [] if str(value).strip()}

    issues: list[dict[str, Any]] = []
    materialized_residue: list[dict[str, Any]] = []
    reviewed: list[dict[str, Any]] = []
    upsert_rows: list[dict[str, Any]] = []
    supersede_rows: list[dict[str, Any]] = []
    reject_rows: list[dict[str, Any]] = []

    for symbol in sorted(candidates):
        candidate = candidates[symbol]
        if not candidate["should_refresh"]:
            materialized_residue.append(
                {
                    "symbol": symbol,
                    "sources": sorted(candidate["sources"]),
                    "reason": "price_only_materialized_not_tracked",
                }
            )
            continue

        if symbol in exclusions:
            reviewed.append({"symbol": symbol, "sources": sorted(candidate["sources"]), "reason": "reviewed_exclusion"})
            continue

        related_rows = rows_by_symbol.get(symbol, [])
        selected_rows = selected_rows_by_symbol.get(symbol, [])
        if len(selected_rows) == 1 and _selected_row_matches_canonical(
            symbol=symbol,
            row=selected_rows[0],
            related_rows=related_rows,
            entity_attributes=entity_by_symbol.get(symbol, {}),
        ):
            continue

        if len(selected_rows) > 1:
            reason = "duplicate/conflicting canonical rows"
            issue_rows = selected_rows
        elif len(selected_rows) == 1:
            reason = "duplicate/conflicting canonical rows"
            issue_rows = selected_rows
        else:
            reason = _missing_reason(related_rows)
            issue_rows = related_rows

        issues.append(_issue(reason, symbol, candidate, issue_rows))
        action_row = _build_reconciled_registry_row(
            symbol=symbol,
            candidate=candidate,
            related_rows=related_rows,
            entity_attributes=entity_by_symbol.get(symbol, {}),
        )
        if action_row is not None:
            upsert_rows.append(action_row)
            supersede_rows.extend(
                _build_supersede_rows(
                    symbol=symbol,
                    related_rows=related_rows,
                    canonical_row=action_row,
                    reason=reason,
                )
            )

    issue_counts = Counter(str(issue["reason"]) for issue in issues)
    return SourceUniverseReconciliationPlan(
        generated_at=datetime.now(UTC).isoformat(),
        candidate_count=len(candidates),
        required_symbol_count=sum(1 for candidate in candidates.values() if candidate["should_refresh"]),
        selected_symbol_count=len(selected_rows_by_symbol),
        issues=issues,
        issue_counts=dict(sorted(issue_counts.items())),
        materialized_residue=materialized_residue,
        reviewed_exclusions=reviewed,
        upsert_rows=upsert_rows,
        supersede_rows=_dedupe_rows_by_registry_key(supersede_rows),
        reject_rows=reject_rows,
    )


def _build_candidates(
    *,
    readiness_frame: pd.DataFrame | None,
    prices_frame: pd.DataFrame | None,
    technicals_frame: pd.DataFrame | None,
    ticker_page_summary_frame: pd.DataFrame | None,
    fundamentals_frame: pd.DataFrame | None,
    earnings_frame: pd.DataFrame | None,
) -> dict[str, dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    def ensure(symbol: str) -> dict[str, Any]:
        normalized = str(symbol).strip().upper()
        row = candidates.setdefault(
            normalized,
            {
                "symbol": normalized,
                "sources": set(),
                "should_refresh": False,
                "has_prices": False,
                "has_technicals": False,
                "has_summary": False,
                "has_fundamentals": False,
                "has_earnings": False,
            },
        )
        return row

    for symbol in _tracked_readiness_symbols(readiness_frame):
        row = ensure(symbol)
        row["sources"].add("feature_store.ticker_readiness")
        row["should_refresh"] = True

    for symbol in _symbols_from_frame(prices_frame):
        row = ensure(symbol)
        row["sources"].add("source_cache.market_price_daily")
        row["has_prices"] = True

    for symbol in _symbols_from_frame(technicals_frame):
        row = ensure(symbol)
        row["sources"].add("feature_store.technical_features_daily")
        row["has_technicals"] = True
        row["should_refresh"] = True

    for symbol in _symbols_from_frame(ticker_page_summary_frame):
        row = ensure(symbol)
        row["sources"].add("feature_store.ticker_page_summary")
        row["has_summary"] = True
        row["should_refresh"] = True

    for symbol in _symbols_from_frame(fundamentals_frame):
        row = ensure(symbol)
        row["has_fundamentals"] = True

    for symbol in _symbols_from_frame(earnings_frame):
        row = ensure(symbol)
        row["has_earnings"] = True

    return {symbol: row for symbol, row in candidates.items() if symbol}


def _tracked_readiness_symbols(frame: pd.DataFrame | None) -> set[str]:
    if frame is None or frame.empty:
        return set()
    symbol_col = _symbol_column(frame)
    if symbol_col is None:
        return set()
    safe = frame.copy()
    if "is_tracked" in safe.columns:
        mask = safe["is_tracked"].map(_coerce_bool)
    elif "tracked_search_ready" in safe.columns:
        mask = safe["tracked_search_ready"].map(_coerce_bool)
    elif "readiness_status" in safe.columns:
        mask = safe["readiness_status"].astype(str).str.strip().str.lower().isin({"ready", "tracked", "fresh"})
    else:
        mask = pd.Series([True] * len(safe.index), index=safe.index)
    return {
        str(value).strip().upper()
        for value in safe.loc[mask, symbol_col].tolist()
        if str(value).strip()
    }


def _symbols_from_frame(frame: pd.DataFrame | None) -> set[str]:
    if frame is None or frame.empty:
        return set()
    symbol_col = _symbol_column(frame)
    if symbol_col is None:
        return set()
    return {str(value).strip().upper() for value in frame[symbol_col].dropna().tolist() if str(value).strip()}


def _entity_attributes_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    key_col = "entity_id" if "entity_id" in frame.columns else _symbol_column(frame)
    if key_col is None:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        symbol = str(row.get(key_col) or "").strip().upper()
        if symbol:
            out[symbol] = dict(row)
    return out


def _registry_rows_by_symbol(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        keys = set()
        keys.update(_symbol_group_keys(row.get("normalized_symbol")))
        keys.update(_symbol_group_keys(row.get("input_symbol")))
        for key in keys:
            if key and key not in {"NONE", "NULL", "NAN"}:
                out.setdefault(key, []).append(row)
    return out


def _selected_registry_rows_by_symbol(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        if not _is_selected_registry_row(row):
            continue
        for symbol in _symbol_group_keys(row.get("normalized_symbol")):
            out.setdefault(symbol, []).append(row)
    return out


def _is_selected_registry_row(row: Mapping[str, Any]) -> bool:
    symbol = str(row.get("normalized_symbol") or "").strip().upper()
    return (
        str(row.get("status") or "").strip().lower() == "active"
        and str(row.get("promotion_status") or "").strip().lower() in ALLOWED_PROMOTION_STATUSES
        and _coerce_bool(row.get("market_supported"))
        and symbol not in {"", "NONE", "NULL", "NAN"}
    )


def _missing_reason(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return "no_registry_row"
    if any(str(row.get("status") or "").strip().lower() == "rejected" for row in rows):
        active_canonical = [row for row in rows if str(row.get("status") or "").strip().lower() == "active"]
        if not any(_is_selected_registry_row(row) for row in active_canonical):
            return "rejected_or_superseded_without_active_canonical"
    if all(str(row.get("normalized_symbol") or "").strip().upper() in {"", "NONE", "NULL", "NAN"} for row in rows):
        return "missing_normalized_symbol"
    if any(not _coerce_bool(row.get("market_supported")) for row in rows):
        return "market_supported_false"
    if any(
        str(row.get("status") or "").strip().lower() == "pending_validation"
        or str(row.get("promotion_status") or "").strip().lower() == "pending_validation"
        for row in rows
    ):
        return "pending_validation"
    return "not_promoted"


def _issue(
    reason: str,
    symbol: str,
    candidate: Mapping[str, Any],
    registry_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "reason": reason,
        "sources": sorted(candidate.get("sources") or []),
        "registry_keys": [str(row.get("registry_key") or "") for row in registry_rows if str(row.get("registry_key") or "")],
        "registry_statuses": sorted(
            {
                str(row.get("status") or "").strip().lower()
                for row in registry_rows
                if str(row.get("status") or "").strip()
            }
        ),
        "promotion_statuses": sorted(
            {
                str(row.get("promotion_status") or "").strip().lower()
                for row in registry_rows
                if str(row.get("promotion_status") or "").strip()
            }
        ),
    }


def _build_reconciled_registry_row(
    *,
    symbol: str,
    candidate: Mapping[str, Any],
    related_rows: Sequence[Mapping[str, Any]],
    entity_attributes: Mapping[str, Any],
) -> dict[str, Any] | None:
    metadata = _registry_metadata(
        symbol=symbol,
        entity_attributes=entity_attributes,
        related_rows=related_rows,
    )
    canonical_key = build_registry_key(input_symbol=symbol, region=metadata["region"], exchange=metadata["exchange"])
    base = _choose_base_registry_row(
        symbol=symbol,
        rows=related_rows,
        canonical_key=canonical_key,
        canonical_region=metadata["region"],
        canonical_exchange=metadata["exchange"],
    )
    now_iso = datetime.now(UTC).isoformat()
    validation_status = "validated_full" if candidate.get("has_fundamentals") and candidate.get("has_earnings") else "validated_market_only"
    if base:
        row = {column: base.get(column) for column in TICKER_REGISTRY_COLUMNS}
    else:
        row = {column: None for column in TICKER_REGISTRY_COLUMNS}

    row.update(
        {
            "registry_key": canonical_key,
            "input_symbol": symbol,
            "normalized_symbol": symbol,
            "region": metadata["region"],
            "exchange": metadata["exchange"],
            "exchange_mic": metadata["exchange_mic"],
            "currency": metadata["currency"],
            "instrument_type": str(row.get("instrument_type") or "unknown").strip().lower() or "unknown",
            "status": "active",
            "market_supported": True,
            "fundamentals_supported": bool(candidate.get("has_fundamentals")),
            "earnings_supported": bool(candidate.get("has_earnings")),
            "validation_status": validation_status,
            "validation_reason": "reconciled_from_materialized_product_universe",
            "promotion_status": validation_status,
            "last_validated_at": now_iso,
            "notes": _merge_notes(
                row.get("notes"),
                {
                    RECONCILIATION_NOTE_KEY: True,
                    "reconciled_at": now_iso,
                    "sources": sorted(candidate.get("sources") or []),
                },
            ),
            "updated_at": now_iso,
        }
    )
    return row


def _choose_base_registry_row(
    *,
    symbol: str,
    rows: Sequence[Mapping[str, Any]],
    canonical_key: str,
    canonical_region: str,
    canonical_exchange: str | None,
) -> dict[str, Any] | None:
    if not rows:
        return None
    normalized_symbol = str(symbol).strip().upper()

    def score(row: Mapping[str, Any]) -> tuple[int, int, int, int, int]:
        status = str(row.get("status") or "").strip().lower()
        normalized = str(row.get("normalized_symbol") or "").strip().upper()
        registry_key = str(row.get("registry_key") or "").strip()
        row_region = normalize_refresh_region(str(row.get("region") or ""))
        row_exchange = _row_exchange_token(row)
        return (
            5 if registry_key == canonical_key else 0,
            3 if row_region == canonical_region else 0,
            2 if row_exchange == (canonical_exchange or "") else 0,
            2 if status != "rejected" else 0,
            1 if normalized == normalized_symbol else 0,
        )

    return dict(sorted(rows, key=score, reverse=True)[0])


def _registry_metadata(
    *,
    symbol: str,
    entity_attributes: Mapping[str, Any],
    related_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    country = normalize_country(
        entity_attributes.get("country")
        or entity_attributes.get("home_country")
        or country_from_source_or_symbol(None, symbol)
    )
    normalized_symbol = normalize_symbol_with_country(symbol, country) or str(symbol).strip().upper()
    product_region = entity_attributes.get("region") or region_for_country(country)
    schedule_region = _canonical_schedule_region(
        product_region=product_region,
        country=country,
        entity_exchange=_nullable_upper(entity_attributes.get("exchange")),
        rows=related_rows,
    )
    return {
        "symbol": normalized_symbol,
        "region": schedule_region,
        "exchange": _canonical_exchange(
            entity_exchange=_nullable_upper(entity_attributes.get("exchange")),
            canonical_region=schedule_region,
            symbol=normalized_symbol,
            rows=related_rows,
        ),
        "exchange_mic": _nullable_upper(entity_attributes.get("exchange_mic")),
        "currency": _nullable_upper(entity_attributes.get("currency")),
    }


def _canonical_schedule_region(
    *,
    product_region: Any,
    country: Any,
    entity_exchange: str | None,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    product_schedule = normalize_refresh_region(str(product_region or ""))
    if product_schedule in SCHEDULE_REGION_SET:
        return product_schedule

    country_code = normalize_country(country)
    if country_code in APAC_SCHEDULE_COUNTRIES:
        return "apac"

    country_schedule = normalize_refresh_region(country_code)
    if country_schedule in SCHEDULE_REGION_SET:
        return country_schedule

    existing_schedule = _existing_safe_schedule_region(rows)
    if existing_schedule:
        return existing_schedule

    exchange_schedule = _schedule_region_for_exchange(entity_exchange)
    if exchange_schedule:
        return exchange_schedule

    row_exchange_schedule = _schedule_region_for_exchange(*[_row_exchange_token(row) for row in rows])
    if row_exchange_schedule:
        return row_exchange_schedule

    return "us"


def _existing_safe_schedule_region(rows: Sequence[Mapping[str, Any]]) -> str | None:
    ranked: list[tuple[int, str]] = []
    for row in rows:
        region = normalize_refresh_region(str(row.get("region") or ""))
        if region not in SCHEDULE_REGION_SET:
            continue
        status = str(row.get("status") or "").strip().lower()
        promotion_status = str(row.get("promotion_status") or "").strip().lower()
        ranked.append(
            (
                (3 if _is_selected_registry_row(row) else 0)
                + (2 if status in {"active", "pending_validation"} else 0)
                + (1 if promotion_status in ALLOWED_PROMOTION_STATUSES or promotion_status == "pending_validation" else 0),
                region,
            )
        )
    if not ranked:
        return None
    return sorted(ranked, reverse=True)[0][1]


def _schedule_region_for_exchange(*exchanges: str | None) -> str | None:
    for exchange in exchanges:
        token = _nullable_upper(exchange)
        if not token:
            continue
        if token in US_SCHEDULE_EXCHANGES:
            return "us"
        if token in EU_SCHEDULE_EXCHANGES:
            return "eu"
        if token in APAC_SCHEDULE_EXCHANGES:
            return "apac"
        normalized = normalize_refresh_region(token)
        if normalized in SCHEDULE_REGION_SET:
            return normalized
    return None


def _selected_row_matches_canonical(
    *,
    symbol: str,
    row: Mapping[str, Any],
    related_rows: Sequence[Mapping[str, Any]],
    entity_attributes: Mapping[str, Any],
) -> bool:
    metadata = _registry_metadata(symbol=symbol, entity_attributes=entity_attributes, related_rows=related_rows)
    canonical_key = build_registry_key(input_symbol=symbol, region=metadata["region"], exchange=metadata["exchange"])
    return (
        str(row.get("registry_key") or "").strip() == canonical_key
        and str(row.get("normalized_symbol") or "").strip().upper() == str(symbol).strip().upper()
        and normalize_refresh_region(str(row.get("region") or "")) == metadata["region"]
    )


def _build_supersede_rows(
    *,
    symbol: str,
    related_rows: Sequence[Mapping[str, Any]],
    canonical_row: Mapping[str, Any],
    reason: str,
) -> list[dict[str, Any]]:
    canonical_key = str(canonical_row.get("registry_key") or "").strip()
    now_iso = datetime.now(UTC).isoformat()
    out: list[dict[str, Any]] = []
    for raw in related_rows:
        row_key = str(raw.get("registry_key") or "").strip()
        if not row_key or row_key == canonical_key:
            continue
        status = str(raw.get("status") or "").strip().lower()
        if status == "rejected" and str(raw.get("promotion_status") or "").strip().lower() == "rejected":
            continue
        row = {column: raw.get(column) for column in TICKER_REGISTRY_COLUMNS}
        row.update(
            {
                "status": "rejected",
                "market_supported": False,
                "fundamentals_supported": False,
                "earnings_supported": False,
                "validation_status": "rejected",
                "validation_reason": f"superseded_by:{canonical_key}",
                "promotion_status": "rejected",
                "notes": _merge_notes(
                    row.get("notes"),
                    {
                        RECONCILIATION_NOTE_KEY: True,
                        "superseded_by": canonical_key,
                        "superseded_at": now_iso,
                        "superseded_reason": reason,
                        "canonical_symbol": str(symbol).strip().upper(),
                    },
                ),
                "updated_at": now_iso,
            }
        )
        out.append(row)
    return out


def _canonical_exchange(
    *,
    entity_exchange: str | None,
    canonical_region: str,
    symbol: str,
    rows: Sequence[Mapping[str, Any]],
) -> str | None:
    if entity_exchange:
        return entity_exchange
    candidates = sorted(
        {_row_exchange_token(row) for row in rows if _row_exchange_token(row)},
        key=lambda exchange: _exchange_score(exchange, canonical_region=canonical_region, symbol=symbol, rows=rows),
        reverse=True,
    )
    return candidates[0] if candidates else None


def _exchange_score(
    exchange: str,
    *,
    canonical_region: str,
    symbol: str,
    rows: Sequence[Mapping[str, Any]],
) -> tuple[int, int, int, str]:
    preferred_rank = {
        "NMS": 100,
        "NYQ": 95,
        "ASE": 90,
        "PCX": 85,
        "CPH": 80,
        "LIS": 80,
        "ASX": 80,
        "HKG": 80,
        "NASDAQ": 10,
    }.get(exchange, 50)
    matching_rows = [row for row in rows if _row_exchange_token(row) == exchange]
    region_match = any(normalize_refresh_region(str(row.get("region") or "")) == canonical_region for row in matching_rows)
    symbol_match = any(str(row.get("normalized_symbol") or "").strip().upper() == symbol for row in matching_rows)
    selected = any(_is_selected_registry_row(row) for row in matching_rows)
    return (preferred_rank, 20 if region_match else 0, 10 if symbol_match or selected else 0, exchange)


def _row_exchange_token(row: Mapping[str, Any]) -> str:
    exchange = _nullable_upper(row.get("exchange"))
    if exchange:
        return exchange
    parts = _registry_key_parts(str(row.get("registry_key") or ""))
    key_exchange = parts.get("exchange")
    return "" if key_exchange.lower() == "default" else key_exchange


def _registry_key_parts(registry_key: str) -> dict[str, str]:
    parts = str(registry_key or "").strip().split("|")
    if len(parts) != 3:
        return {"input_symbol": "", "region": "", "exchange": ""}
    return {
        "input_symbol": parts[0].strip().upper(),
        "region": parts[1].strip().lower(),
        "exchange": parts[2].strip().upper(),
    }


def _dedupe_rows_by_registry_key(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("registry_key") or "").strip()
        if key:
            deduped[key] = row
    return list(deduped.values())


def _merge_notes(raw: Any, extras: Mapping[str, Any]) -> dict[str, Any]:
    notes: dict[str, Any]
    if isinstance(raw, dict):
        notes = dict(raw)
    elif isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        notes = dict(parsed) if isinstance(parsed, dict) else {"raw": raw.strip()}
    else:
        notes = {}
    notes.update({str(key): value for key, value in extras.items()})
    return notes


def _symbol_column(frame: pd.DataFrame) -> str | None:
    for column in ("symbol", "ticker", "entity_id", "normalized_symbol"):
        if column in frame.columns:
            return column
    return None


def _symbol_group_keys(raw: Any) -> set[str]:
    symbol = str(raw or "").strip().upper()
    if not symbol or symbol in {"NONE", "NULL", "NAN"}:
        return set()
    keys = {symbol}
    if US_SHARE_CLASS_ALIAS_PATTERN.fullmatch(symbol):
        keys.add(symbol.replace(".", "-"))
        keys.add(symbol.replace("-", "."))
    return keys


def _records(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    return frame.to_dict(orient="records")


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    token = str(value).strip().lower()
    return token in {"true", "1", "yes", "y", "on"}


def _nullable_upper(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NONE", "NULL", "NAN"}:
        return None
    return text.upper()
