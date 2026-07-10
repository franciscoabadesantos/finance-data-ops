"""Ticker materialization/readiness contracts owned by Data Ops.

This module intentionally derives readiness from materialized source/feature
rows. Registry and coverage rows are joined as context only; they do not define
tracked/product readiness by themselves.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from finance_data_ops.diagnostics.symbol_data_coverage import build_complete_symbol_data_coverage_rows

PLACEHOLDER_SYMBOL_PATTERN = re.compile(r"^\d{6,}[A-Z]$")
VALIDATED_PROMOTION_STATUSES = frozenset({"validated_market_only", "validated_full"})
PRODUCT_TRACKED_REQUIRED_COMPONENTS = ("prices", "technicals")


@dataclass(frozen=True, slots=True)
class ReadinessAudit:
    rows: list[dict[str, Any]]
    issues: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        issue_counts: dict[str, int] = {}
        for issue in self.issues:
            issue_type = str(issue.get("issue_type") or "unknown")
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "rows": self.rows,
            "issues": self.issues,
            "issue_counts": dict(sorted(issue_counts.items())),
            "tracked_tickers": [row["ticker"] for row in self.rows if bool(row.get("tracked_search_ready"))],
        }


def build_ticker_readiness_rows(
    *,
    registry_frame: pd.DataFrame | None,
    prices_frame: pd.DataFrame | None,
    technicals_frame: pd.DataFrame | None,
    scorecard_frame: pd.DataFrame | None,
    coverage_frame: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    """Build canonical data-ops readiness inputs from materialized tables.

    `tracked_search_ready` means prices and technical feature rows both exist.
    Scorecard readiness is reported separately because scorecard is a derived
    feature-store product surface and can lag source/technical readiness.
    """

    registry_by_symbol = _primary_registry_rows_by_symbol(registry_frame)
    coverage_by_symbol = _rows_by_symbol(coverage_frame)
    price_stats = _materialized_stats(prices_frame, preferred_date_cols=("price_date", "date"))
    technical_stats = _materialized_stats(
        technicals_frame,
        preferred_date_cols=("as_of_date", "date", "feature_date", "price_date"),
    )
    scorecard_stats = _materialized_stats(
        scorecard_frame,
        preferred_date_cols=("as_of_date", "date", "scorecard_date", "price_date"),
    )

    tickers = sorted(
        set(registry_by_symbol)
        | set(coverage_by_symbol)
        | set(price_stats)
        | set(technical_stats)
        | set(scorecard_stats)
    )
    rows: list[dict[str, Any]] = []
    for ticker in tickers:
        registry_row = registry_by_symbol.get(ticker, {})
        coverage_row = coverage_by_symbol.get(ticker, {})
        prices = price_stats.get(ticker, {"row_count": 0, "latest_date": None})
        technicals = technical_stats.get(ticker, {"row_count": 0, "latest_date": None})
        scorecard = scorecard_stats.get(ticker, {"row_count": 0, "latest_date": None})

        has_prices = int(prices["row_count"]) > 0
        has_technicals = int(technicals["row_count"]) > 0
        has_scorecard = int(scorecard["row_count"]) > 0
        is_placeholder = is_placeholder_symbol(ticker)
        registry_active_validated = _registry_active_validated(registry_row)
        tracked_search_ready = bool(has_prices and has_technicals and not is_placeholder)

        rows.append(
            {
                "ticker": ticker,
                "is_placeholder_symbol": is_placeholder,
                "registry_status": _clean_text(registry_row.get("status")),
                "registry_validation_status": _clean_text(registry_row.get("validation_status")),
                "registry_promotion_status": _clean_text(registry_row.get("promotion_status")),
                "registry_active_validated": registry_active_validated,
                "source_price_available": has_prices,
                "source_price_rows": int(prices["row_count"]),
                "source_price_latest_date": prices["latest_date"],
                "technical_features_available": has_technicals,
                "technical_feature_rows": int(technicals["row_count"]),
                "technical_feature_latest_date": technicals["latest_date"],
                "scorecard_available": has_scorecard,
                "scorecard_rows": int(scorecard["row_count"]),
                "scorecard_latest_date": scorecard["latest_date"],
                "tracked_search_ready": tracked_search_ready,
                "readiness_state": _readiness_state(
                    has_prices=has_prices,
                    has_technicals=has_technicals,
                    has_scorecard=has_scorecard,
                    is_placeholder=is_placeholder,
                ),
                "coverage_market_data_available": _nullable_bool(
                    coverage_row.get("market_data_available"),
                ),
                "coverage_status": _clean_text(coverage_row.get("coverage_status")),
                "coverage_reason": _clean_text(coverage_row.get("reason")),
            }
        )
    return rows


def build_readiness_audit(
    *,
    registry_frame: pd.DataFrame | None,
    prices_frame: pd.DataFrame | None,
    technicals_frame: pd.DataFrame | None,
    scorecard_frame: pd.DataFrame | None,
    coverage_frame: pd.DataFrame | None = None,
    readiness_frame: pd.DataFrame | None = None,
) -> ReadinessAudit:
    rows = build_ticker_readiness_rows(
        registry_frame=registry_frame,
        prices_frame=prices_frame,
        technicals_frame=technicals_frame,
        scorecard_frame=scorecard_frame,
        coverage_frame=coverage_frame,
    )
    registry_rows_by_symbol = _registry_rows_by_symbol(registry_frame)
    readiness_by_symbol = _rows_by_symbol(readiness_frame)
    issues: list[dict[str, Any]] = []
    for row in rows:
        ticker = str(row["ticker"])
        has_prices = bool(row["source_price_available"])
        has_technicals = bool(row["technical_features_available"])
        has_scorecard = bool(row["scorecard_available"])
        registry_active_validated = bool(row["registry_active_validated"])
        registry_status = str(row.get("registry_status") or "")

        if registry_active_validated and not has_prices:
            issues.append(_issue("registry_active_validated_without_prices", ticker, row))
        if has_prices and not has_technicals:
            issues.append(_issue("prices_without_technicals", ticker, row))
        if has_technicals and not has_scorecard:
            issues.append(_issue("technicals_without_scorecard", ticker, row))
        if (
            registry_status == "rejected"
            and (has_prices or has_technicals or has_scorecard)
            and not _is_rejected_shadow_with_tracked_canonical(
                ticker=ticker,
                registry_rows=registry_rows_by_symbol.get(ticker, []),
                readiness_row=readiness_by_symbol.get(ticker, {}),
            )
        ):
            issues.append(_issue("rejected_with_materialized_data", ticker, row))
        if bool(row.get("is_placeholder_symbol")) and not _is_cleaned_rejected_placeholder(row):
            issues.append(_issue("placeholder_like_symbol", ticker, row))
        if _coverage_disagrees(row):
            issues.append(_issue("coverage_disagrees_with_materialized_rows", ticker, row))
    return ReadinessAudit(rows=rows, issues=issues)


def rebuild_diagnostic_symbol_data_coverage_rows(
    *,
    prices_frame: pd.DataFrame,
    quotes_frame: pd.DataFrame | None = None,
    fundamentals_frame: pd.DataFrame | None = None,
    earnings_events_frame: pd.DataFrame | None = None,
    required_symbols: list[str] | None = None,
    as_of_date: str | None = None,
) -> list[dict[str, object]]:
    """Deterministically rebuild diagnostic coverage rows from current materialized inputs."""
    return build_complete_symbol_data_coverage_rows(
        required_symbols=required_symbols,
        prices_frame=prices_frame,
        quotes_frame=quotes_frame if quotes_frame is not None else pd.DataFrame(),
        fundamentals_frame=fundamentals_frame,
        earnings_events_frame=earnings_events_frame,
        as_of_date=as_of_date,
    )


def is_placeholder_symbol(ticker: str) -> bool:
    return bool(PLACEHOLDER_SYMBOL_PATTERN.fullmatch(str(ticker or "").strip().upper()))


def _issue(issue_type: str, ticker: str, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "ticker": ticker,
        "readiness_state": row.get("readiness_state"),
        "registry_status": row.get("registry_status"),
        "registry_promotion_status": row.get("registry_promotion_status"),
        "source_price_rows": row.get("source_price_rows"),
        "technical_feature_rows": row.get("technical_feature_rows"),
        "scorecard_rows": row.get("scorecard_rows"),
        "coverage_status": row.get("coverage_status"),
        "coverage_market_data_available": row.get("coverage_market_data_available"),
    }


def _coverage_disagrees(row: dict[str, Any]) -> bool:
    coverage_has_market = row.get("coverage_market_data_available")
    if coverage_has_market is None:
        return False
    return bool(coverage_has_market) != bool(row.get("source_price_available"))


def _readiness_state(
    *,
    has_prices: bool,
    has_technicals: bool,
    has_scorecard: bool,
    is_placeholder: bool,
) -> str:
    if is_placeholder:
        return "invalid_placeholder"
    if not has_prices:
        return "not_materialized"
    if not has_technicals:
        return "source_only"
    if not has_scorecard:
        return "tracked_without_scorecard"
    return "tracked_with_scorecard"


def _registry_active_validated(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    promotion = str(row.get("promotion_status") or "").strip().lower()
    return status == "active" and promotion in VALIDATED_PROMOTION_STATUSES


def _is_cleaned_rejected_placeholder(row: dict[str, Any]) -> bool:
    return bool(
        row.get("registry_status") == "rejected"
        and not row.get("source_price_available")
        and not row.get("technical_features_available")
        and not row.get("scorecard_available")
        and row.get("coverage_market_data_available") is None
        and not row.get("coverage_status")
    )


def _is_rejected_shadow_with_tracked_canonical(
    *,
    ticker: str,
    registry_rows: list[dict[str, Any]],
    readiness_row: dict[str, Any],
) -> bool:
    if not registry_rows:
        return False
    has_rejected = any(str(row.get("status") or "").strip().lower() == "rejected" for row in registry_rows)
    has_canonical = any(str(row.get("status") or "").strip().lower() != "rejected" for row in registry_rows)
    if not (has_rejected and has_canonical):
        return False
    readiness_symbol = _normalize_symbol(readiness_row.get("ticker") or readiness_row.get("symbol") or readiness_row.get("entity_id"))
    return bool(
        readiness_symbol == ticker
        and _readiness_tracked(readiness_row)
        and _readiness_scorecard_ready(readiness_row)
    )


def _registry_rows_by_symbol(frame: pd.DataFrame | None) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    if frame is None or frame.empty:
        return out
    symbol_col = "normalized_symbol" if "normalized_symbol" in frame.columns else _symbol_column(frame)
    if symbol_col is None:
        return out
    for row in frame.to_dict(orient="records"):
        symbol = _normalize_symbol(row.get(symbol_col))
        if symbol:
            out.setdefault(symbol, []).append(row)
    return out


def _primary_registry_rows_by_symbol(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    return {symbol: rows[-1] for symbol, rows in _registry_rows_by_symbol(frame).items() if rows}


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
            out[symbol] = row
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
    local["_readiness_symbol"] = local[symbol_col].map(_normalize_symbol)
    local = local.loc[local["_readiness_symbol"].astype(bool)]
    if local.empty:
        return {}
    grouped = local.groupby("_readiness_symbol", dropna=True)
    out: dict[str, dict[str, Any]] = {}
    for symbol, group in grouped:
        latest_date = _latest_date(group[date_col]) if date_col else None
        out[str(symbol)] = {"row_count": int(len(group.index)), "latest_date": latest_date}
    return out


def _latest_date(values: pd.Series) -> str | None:
    parsed = pd.to_datetime(values, errors="coerce", utc=True)
    parsed = parsed.dropna()
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


def _clean_text(value: object) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


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


def _readiness_tracked(row: dict[str, Any]) -> bool:
    if not row:
        return False
    if _truthy_first(row, ("is_tracked", "tracked_search_ready", "tracked", "search_ready")):
        return True
    state = str(row.get("readiness_state") or row.get("state") or "").strip().lower()
    return bool(state.startswith("tracked"))


def _readiness_scorecard_ready(row: dict[str, Any]) -> bool:
    if not row:
        return False
    if _truthy_first(row, ("is_scorecard_ready", "has_scorecard", "scorecard_available")):
        return True
    state = str(row.get("readiness_state") or row.get("state") or "").strip().lower()
    return state == "tracked_with_scorecard"


def _truthy_first(row: dict[str, Any], keys: tuple[str, ...]) -> bool:
    for key in keys:
        if key in row and _nullable_bool(row.get(key)) is True:
            return True
    return False
