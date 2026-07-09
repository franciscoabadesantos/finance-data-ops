"""Controlled cleanup planning for ticker readiness residues.

The planner is intentionally side-effect free. Scripts can use the returned SQL
steps for preview or execution, but classification remains testable without a
database connection.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
import json
from typing import Any

import pandas as pd

from finance_data_ops.validation.readiness import is_placeholder_symbol

DEFAULT_SUPERSEDED_ALIASES = {"700.HK": "0700.HK"}
VALIDATED_PROMOTION_STATUSES = frozenset({"validated_market_only", "validated_full"})
CLEANUP_CLASSES = (
    "invalid_placeholder",
    "superseded_alias",
    "active_validated_without_materialized_data",
    "rejected_shadow_row_with_tracked_canonical",
    "partial_keep_review",
    "repairable_missing_technicals",
    "repairable_missing_scorecard",
)


def build_ticker_readiness_cleanup_plan(
    *,
    registry_frame: pd.DataFrame | None,
    readiness_frame: pd.DataFrame | None,
    prices_frame: pd.DataFrame | None,
    technicals_frame: pd.DataFrame | None,
    scorecard_frame: pd.DataFrame | None,
    coverage_frame: pd.DataFrame | None,
    superseded_aliases: dict[str, str] | None = None,
    retry_allowlist: set[str] | None = None,
    repair_allowlist: set[str] | None = None,
    partial_price_row_threshold: int = 30,
) -> dict[str, Any]:
    """Classify cleanup candidates and produce proposed actions.

    `feature_store.ticker_readiness` is treated as canonical context for
    tracked readiness, while materialized source/feature tables remain the
    source of row-count evidence for cleanup actions.
    """

    aliases = {_normalize_symbol(k): _normalize_symbol(v) for k, v in (superseded_aliases or {}).items()}
    aliases.update({k: v for k, v in DEFAULT_SUPERSEDED_ALIASES.items() if k not in aliases})
    retry_symbols = {_normalize_symbol(value) for value in (retry_allowlist or set()) if _normalize_symbol(value)}
    repair_symbols = {_normalize_symbol(value) for value in (repair_allowlist or set()) if _normalize_symbol(value)}

    registry_rows = _registry_rows(registry_frame)
    registry_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in registry_rows:
        registry_by_symbol[_registry_symbol(row)].append(row)

    coverage_by_symbol = _rows_by_symbol(coverage_frame)
    readiness_by_symbol = _rows_by_symbol(readiness_frame)
    price_stats = _materialized_stats(prices_frame, preferred_date_cols=("price_date", "date", "as_of_date"))
    technical_stats = _materialized_stats(
        technicals_frame,
        preferred_date_cols=("as_of_date", "date", "feature_date", "price_date"),
    )
    scorecard_stats = _materialized_stats(
        scorecard_frame,
        preferred_date_cols=("as_of_date", "date", "scorecard_date", "price_date"),
    )

    symbols = sorted(
        set(registry_by_symbol)
        | set(coverage_by_symbol)
        | set(readiness_by_symbol)
        | set(price_stats)
        | set(technical_stats)
        | set(scorecard_stats)
    )

    actions: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()

    for symbol in symbols:
        if not symbol:
            continue
        context = _symbol_context(
            symbol=symbol,
            registry_rows=registry_by_symbol.get(symbol, []),
            readiness_row=readiness_by_symbol.get(symbol, {}),
            coverage_row=coverage_by_symbol.get(symbol, {}),
            price_stats=price_stats.get(symbol, {}),
            technical_stats=technical_stats.get(symbol, {}),
            scorecard_stats=scorecard_stats.get(symbol, {}),
        )

        if is_placeholder_symbol(symbol):
            if not context["has_prices"] and not context["has_technicals"] and not context["has_scorecard"]:
                actions.append(_cleanup_registry_and_coverage_action(context, issue_class="invalid_placeholder"))
                seen_symbols.add(symbol)
            continue

        if symbol in aliases:
            actions.append(
                _superseded_alias_action(
                    context,
                    superseded_by=aliases[symbol],
                    canonical_tracked=_canonical_tracked(
                        aliases[symbol],
                        readiness_by_symbol=readiness_by_symbol,
                        price_stats=price_stats,
                        technical_stats=technical_stats,
                    ),
                )
            )
            seen_symbols.add(symbol)
            continue

        active_validated_rows = [row for row in context["registry_rows"] if _registry_active_validated(row)]
        if active_validated_rows and not context["has_prices"] and not context["has_technicals"] and not context["has_scorecard"]:
            action = _cleanup_registry_and_coverage_action(
                context,
                issue_class="active_validated_without_materialized_data",
                reason="no_materialized_data/provider_failed",
            )
            if symbol in retry_symbols:
                action["proposed_action"] = "retry_allowlisted_no_write"
                action["sql"] = []
                action["sql_preview"] = []
            actions.append(action)
            seen_symbols.add(symbol)
            continue

        rejected_rows = [row for row in context["registry_rows"] if _clean_text(row.get("status")) == "rejected"]
        if rejected_rows and _symbol_tracked(context):
            actions.append(_rejected_shadow_action(context, rejected_rows=rejected_rows))
            seen_symbols.add(symbol)
            continue

        if context["has_prices"] and context["has_technicals"] and not context["has_scorecard"]:
            actions.append(_repair_action(context, issue_class="repairable_missing_scorecard"))
            seen_symbols.add(symbol)
            continue

        if context["has_prices"] and not context["has_technicals"]:
            if symbol in repair_symbols or int(context["source_price_rows"]) >= int(partial_price_row_threshold):
                actions.append(_repair_action(context, issue_class="repairable_missing_technicals"))
            else:
                actions.append(_partial_keep_review_action(context))
            seen_symbols.add(symbol)

    grouped = {issue_class: [] for issue_class in CLEANUP_CLASSES}
    for action in actions:
        grouped[str(action["issue_class"])].append(action)
    grouped = {key: value for key, value in grouped.items() if value}

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "dry_run": True,
        "issue_counts": {key: len(value) for key, value in grouped.items()},
        "groups": grouped,
        "actions": actions,
        "symbols": sorted(seen_symbols),
    }


def sql_steps_for_plan(plan: dict[str, Any]) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for action in plan.get("actions") or []:
        for step in action.get("sql") or []:
            steps.append(dict(step))
    return steps


def _symbol_context(
    *,
    symbol: str,
    registry_rows: list[dict[str, Any]],
    readiness_row: dict[str, Any],
    coverage_row: dict[str, Any],
    price_stats: dict[str, Any],
    technical_stats: dict[str, Any],
    scorecard_stats: dict[str, Any],
) -> dict[str, Any]:
    price_rows = int(price_stats.get("row_count") or 0)
    technical_rows = int(technical_stats.get("row_count") or 0)
    scorecard_rows = int(scorecard_stats.get("row_count") or 0)
    readiness_has_prices = _truthy_first(readiness_row, ("has_prices", "source_price_available", "prices_available"))
    readiness_has_technicals = _truthy_first(
        readiness_row,
        ("has_technicals", "technical_features_available", "technicals_available"),
    )
    readiness_has_scorecard = _truthy_first(readiness_row, ("has_scorecard", "scorecard_available"))
    return {
        "ticker": symbol,
        "registry_keys": [str(row.get("registry_key") or "") for row in registry_rows if str(row.get("registry_key") or "")],
        "registry_rows": registry_rows,
        "readiness_row": readiness_row,
        "coverage_row": coverage_row,
        "has_coverage": bool(coverage_row),
        "has_prices": price_rows > 0 or readiness_has_prices,
        "readiness_has_prices": readiness_has_prices,
        "source_price_rows": price_rows,
        "source_price_latest_date": price_stats.get("latest_date"),
        "has_technicals": technical_rows > 0 or readiness_has_technicals,
        "readiness_has_technicals": readiness_has_technicals,
        "technical_feature_rows": technical_rows,
        "technical_feature_latest_date": technical_stats.get("latest_date"),
        "has_scorecard": scorecard_rows > 0 or readiness_has_scorecard,
        "readiness_has_scorecard": readiness_has_scorecard,
        "scorecard_rows": scorecard_rows,
        "scorecard_latest_date": scorecard_stats.get("latest_date"),
        "readiness_tracked": _readiness_tracked(readiness_row),
        "coverage_market_data_available": _nullable_bool(coverage_row.get("market_data_available")),
        "coverage_fundamentals_available": _nullable_bool(coverage_row.get("fundamentals_available")),
        "coverage_earnings_available": _nullable_bool(coverage_row.get("earnings_available")),
    }


def _cleanup_registry_and_coverage_action(
    context: dict[str, Any],
    *,
    issue_class: str,
    reason: str | None = None,
) -> dict[str, Any]:
    ticker = str(context["ticker"])
    cleanup_reason = reason or issue_class
    sql = []
    preview = []
    for registry_key in context["registry_keys"]:
        note = {"cleanup_class": issue_class, "cleanup_reason": cleanup_reason}
        sql.append(
            {
                "statement": (
                    "update public.ticker_registry "
                    "set status = 'rejected', validation_status = 'rejected', promotion_status = 'rejected', "
                    "validation_reason = %s, notes = coalesce(notes, '{}'::jsonb) || %s::jsonb, updated_at = now() "
                    "where registry_key = %s"
                ),
                "params": [cleanup_reason, json.dumps(note, sort_keys=True), registry_key],
            }
        )
        preview.append(
            "update public.ticker_registry "
            "set status = 'rejected', validation_status = 'rejected', promotion_status = 'rejected', "
            f"validation_reason = {_sql_literal(cleanup_reason)}, "
            f"notes = coalesce(notes, '{{}}'::jsonb) || {_sql_literal(json.dumps(note, sort_keys=True))}::jsonb, "
            f"updated_at = now() where registry_key = {_sql_literal(registry_key)};"
        )
    if context["has_coverage"]:
        sql.append({"statement": "delete from public.symbol_data_coverage where ticker = %s", "params": [ticker]})
        preview.append(f"delete from public.symbol_data_coverage where ticker = {_sql_literal(ticker)};")
    return {
        "issue_class": issue_class,
        "ticker": ticker,
        "proposed_action": "reject_deactivate_registry_rows_and_clear_stale_coverage",
        "reason": cleanup_reason,
        "registry_keys": context["registry_keys"],
        "evidence": _evidence(context),
        "sql": sql,
        "sql_preview": preview,
    }


def _superseded_alias_action(
    context: dict[str, Any],
    *,
    superseded_by: str,
    canonical_tracked: bool,
) -> dict[str, Any]:
    ticker = str(context["ticker"])
    reason = f"superseded_by:{superseded_by}"
    sql = []
    preview = []
    for registry_key in context["registry_keys"]:
        note = {
            "cleanup_class": "superseded_alias",
            "cleanup_reason": reason,
            "superseded_by": superseded_by,
        }
        sql.append(
            {
                "statement": (
                    "update public.ticker_registry "
                    "set status = 'rejected', validation_status = 'rejected', promotion_status = 'rejected', "
                    "validation_reason = %s, notes = coalesce(notes, '{}'::jsonb) || %s::jsonb, updated_at = now() "
                    "where registry_key = %s"
                ),
                "params": [reason, json.dumps(note, sort_keys=True), registry_key],
            }
        )
        preview.append(
            "update public.ticker_registry "
            "set status = 'rejected', validation_status = 'rejected', promotion_status = 'rejected', "
            f"validation_reason = {_sql_literal(reason)}, "
            f"notes = coalesce(notes, '{{}}'::jsonb) || {_sql_literal(json.dumps(note, sort_keys=True))}::jsonb, "
            f"updated_at = now() where registry_key = {_sql_literal(registry_key)};"
        )
    if context["has_coverage"]:
        sql.append({"statement": "delete from public.symbol_data_coverage where ticker = %s", "params": [ticker]})
        preview.append(f"delete from public.symbol_data_coverage where ticker = {_sql_literal(ticker)};")
    return {
        "issue_class": "superseded_alias",
        "ticker": ticker,
        "superseded_by": superseded_by,
        "canonical_tracked": canonical_tracked,
        "proposed_action": "mark_superseded_alias_and_clear_stale_coverage",
        "registry_keys": context["registry_keys"],
        "evidence": _evidence(context),
        "sql": sql,
        "sql_preview": preview,
    }


def _rejected_shadow_action(context: dict[str, Any], *, rejected_rows: list[dict[str, Any]]) -> dict[str, Any]:
    canonical_keys = [
        str(row.get("registry_key") or "")
        for row in context["registry_rows"]
        if str(row.get("registry_key") or "") and _clean_text(row.get("status")) != "rejected"
    ]
    superseded_by = next(
        (
            value
            for row in rejected_rows
            for value in [_notes_value(row.get("notes"), "superseded_by")]
            if value
        ),
        None,
    )
    return {
        "issue_class": "rejected_shadow_row_with_tracked_canonical",
        "ticker": context["ticker"],
        "proposed_action": "suppress_from_readiness_error_audit",
        "registry_keys": [str(row.get("registry_key") or "") for row in rejected_rows if str(row.get("registry_key") or "")],
        "canonical_registry_keys": [value for value in canonical_keys if value],
        "superseded_by": superseded_by,
        "evidence": _evidence(context),
        "sql": [],
        "sql_preview": [],
    }


def _repair_action(context: dict[str, Any], *, issue_class: str) -> dict[str, Any]:
    if issue_class == "repairable_missing_technicals":
        proposed = "trigger_technical_backfill_then_scorecard_build"
    else:
        proposed = "trigger_targeted_scorecard_build"
    return {
        "issue_class": issue_class,
        "ticker": context["ticker"],
        "proposed_action": proposed,
        "registry_keys": context["registry_keys"],
        "evidence": _evidence(context),
        "sql": [],
        "sql_preview": [],
    }


def _partial_keep_review_action(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "issue_class": "partial_keep_review",
        "ticker": context["ticker"],
        "proposed_action": "leave_partial_source_only_for_manual_review",
        "registry_keys": context["registry_keys"],
        "evidence": _evidence(context),
        "sql": [],
        "sql_preview": [],
    }


def _evidence(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_price_rows": context["source_price_rows"],
        "source_price_latest_date": context["source_price_latest_date"],
        "technical_feature_rows": context["technical_feature_rows"],
        "technical_feature_latest_date": context["technical_feature_latest_date"],
        "scorecard_rows": context["scorecard_rows"],
        "scorecard_latest_date": context["scorecard_latest_date"],
        "readiness_tracked": context["readiness_tracked"],
        "readiness_has_prices": context["readiness_has_prices"],
        "readiness_has_technicals": context["readiness_has_technicals"],
        "readiness_has_scorecard": context["readiness_has_scorecard"],
        "coverage_market_data_available": context["coverage_market_data_available"],
        "coverage_fundamentals_available": context["coverage_fundamentals_available"],
        "coverage_earnings_available": context["coverage_earnings_available"],
    }


def _symbol_tracked(context: dict[str, Any]) -> bool:
    return bool(context["readiness_tracked"] or (context["has_prices"] and context["has_technicals"]))


def _canonical_tracked(
    symbol: str,
    *,
    readiness_by_symbol: dict[str, dict[str, Any]],
    price_stats: dict[str, dict[str, Any]],
    technical_stats: dict[str, dict[str, Any]],
) -> bool:
    readiness = readiness_by_symbol.get(symbol, {})
    if _readiness_tracked(readiness):
        return True
    return int((price_stats.get(symbol) or {}).get("row_count") or 0) > 0 and int(
        (technical_stats.get(symbol) or {}).get("row_count") or 0
    ) > 0


def _registry_active_validated(row: dict[str, Any]) -> bool:
    status = _clean_text(row.get("status"))
    promotion = _clean_text(row.get("promotion_status"))
    validation = _clean_text(row.get("validation_status"))
    return status == "active" and (promotion in VALIDATED_PROMOTION_STATUSES or validation in VALIDATED_PROMOTION_STATUSES)


def _readiness_tracked(row: dict[str, Any]) -> bool:
    if not row:
        return False
    if _truthy_first(row, ("tracked_search_ready", "tracked", "is_tracked", "search_ready")):
        return True
    state = _clean_text(row.get("readiness_state") or row.get("state"))
    return bool(state and state.startswith("tracked"))


def _truthy_first(row: dict[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        if key in row and _nullable_bool(row.get(key)) is True:
            return True
    return False


def _registry_rows(frame: pd.DataFrame | None) -> list[dict[str, Any]]:
    if frame is None or frame.empty:
        return []
    return [dict(row) for row in frame.to_dict(orient="records")]


def _registry_symbol(row: dict[str, Any]) -> str:
    return _normalize_symbol(row.get("normalized_symbol") or row.get("input_symbol") or row.get("ticker"))


def _rows_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if frame is None or frame.empty:
        return out
    symbol_col = _symbol_column(frame)
    if symbol_col is None:
        return out
    for row in frame.to_dict(orient="records"):
        symbol = _normalize_symbol(row.get(symbol_col))
        if symbol:
            out[symbol] = dict(row)
    return out


def _materialized_stats(
    frame: pd.DataFrame | None,
    *,
    preferred_date_cols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    symbol_col = _symbol_column(frame)
    if symbol_col is None:
        return {}
    date_col = next((col for col in preferred_date_cols if col in frame.columns), None)
    local = frame.copy()
    local["_cleanup_symbol"] = local[symbol_col].map(_normalize_symbol)
    local = local.loc[local["_cleanup_symbol"].astype(bool)]
    if local.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for symbol, group in local.groupby("_cleanup_symbol", dropna=True):
        out[str(symbol)] = {
            "row_count": int(len(group.index)),
            "latest_date": _latest_date(group[date_col]) if date_col else None,
        }
    return out


def _latest_date(values: pd.Series) -> str | None:
    parsed = pd.to_datetime(values, errors="coerce", utc=True).dropna()
    if parsed.empty:
        return None
    return pd.Timestamp(parsed.max()).date().isoformat()


def _symbol_column(frame: pd.DataFrame | None) -> str | None:
    if frame is None:
        return None
    for candidate in ("ticker", "symbol", "normalized_symbol", "entity_id"):
        if candidate in frame.columns:
            return candidate
    return None


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if symbol in {"", "NAN", "NONE", "NULL"}:
        return ""
    return symbol


def _clean_text(value: object) -> str:
    text = str(value or "").strip().lower()
    return "" if text in {"", "nan", "none", "null"} else text


def _nullable_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return None
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n"}:
        return False
    return None


def _notes_value(raw_notes: object, key: str) -> str | None:
    if raw_notes is None:
        return None
    if isinstance(raw_notes, dict):
        value = raw_notes.get(key)
        return str(value) if value else None
    try:
        parsed = json.loads(str(raw_notes))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if isinstance(parsed, dict):
        value = parsed.get(key)
        return str(value) if value else None
    return None


def _sql_literal(value: object) -> str:
    if value is None:
        return "null"
    return "'" + str(value).replace("'", "''") + "'"
