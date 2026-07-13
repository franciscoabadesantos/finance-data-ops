"""Derived relationship-map readiness for thematic ETF catalog entries."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pandas as pd


MIN_TECHNICAL_CONSTITUENTS = 5
MIN_TECHNICAL_COVERAGE_RATIO = 0.10

ETF_THEME_READINESS_COLUMNS = [
    "etf_symbol",
    "theme",
    "active",
    "holdings_count",
    "holdings_as_of",
    "holdings_shallow",
    "priced_constituent_count",
    "technical_constituent_count",
    "tracked_constituent_count",
    "coverage_ratio",
    "relationship_map_eligible",
    "relationship_map_ineligible_reason",
    "computed_at",
]


def build_etf_theme_readiness(
    *,
    etf_holdings: pd.DataFrame,
    etf_themes: pd.DataFrame,
    market_price_daily: pd.DataFrame | None = None,
    technical_features_daily: pd.DataFrame | None = None,
    etf_holding_onboarding_identity: pd.DataFrame | None = None,
    computed_at: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Compute current relationship-map eligibility from materialized coverage."""

    if etf_themes.empty:
        return pd.DataFrame(columns=ETF_THEME_READINESS_COLUMNS)

    now = pd.Timestamp(computed_at or datetime.now(UTC))
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    holdings_by_etf = _latest_holdings_by_etf(etf_holdings)
    onboarding_by_etf = _onboardable_symbols_by_etf(etf_holding_onboarding_identity)
    priced_symbols = _symbol_set(market_price_daily, candidates=("symbol", "ticker"))
    technical_symbols = _symbol_set(technical_features_daily, candidates=("symbol", "ticker"))

    rows: list[dict[str, Any]] = []
    themes = etf_themes.copy()
    themes["etf_ticker"] = _normalized_symbol_series(themes.get("etf_ticker", pd.Series(index=themes.index)))
    themes = themes.dropna(subset=["etf_ticker"]).drop_duplicates(subset=["etf_ticker"], keep="last")

    for _, theme_row in themes.sort_values("etf_ticker").iterrows():
        etf = str(theme_row["etf_ticker"]).strip().upper()
        holdings = holdings_by_etf.get(etf, pd.DataFrame())
        holding_symbols = _symbol_set(holdings, candidates=("holding_symbol",))
        constituent_symbols = set(onboarding_by_etf.get(etf) or holding_symbols)
        holdings_count = len(holding_symbols) if holding_symbols else _integer_value(theme_row.get("holdings_count"))
        technical_count = len(constituent_symbols & technical_symbols)
        coverage_ratio = float(technical_count / holdings_count) if holdings_count > 0 else 0.0
        active = _bool_value(theme_row.get("active"), default=True)
        eligible, reason = _eligibility(active=active, holdings_count=holdings_count, technical_count=technical_count)
        if eligible and coverage_ratio < MIN_TECHNICAL_COVERAGE_RATIO:
            eligible = False
            reason = "insufficient_constituent_coverage"

        rows.append(
            {
                "etf_symbol": etf,
                "theme": _text_value(theme_row.get("theme")),
                "active": active,
                "holdings_count": int(holdings_count),
                "holdings_as_of": _latest_holding_date(holdings, fallback=theme_row.get("holdings_as_of")),
                "holdings_shallow": _bool_value(theme_row.get("holdings_shallow"), default=False),
                "priced_constituent_count": len(constituent_symbols & priced_symbols),
                "technical_constituent_count": int(technical_count),
                "tracked_constituent_count": len(constituent_symbols),
                "coverage_ratio": coverage_ratio,
                "relationship_map_eligible": bool(eligible),
                "relationship_map_ineligible_reason": reason,
                "computed_at": now,
            }
        )

    return pd.DataFrame(rows, columns=ETF_THEME_READINESS_COLUMNS)


def classify_relationship_map_theme_health(
    readiness: pd.DataFrame,
    *,
    edge_counts: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Classify relationship-map health without treating pending coverage as RED."""

    edge_counts = {str(key).strip().upper(): int(value) for key, value in (edge_counts or {}).items()}
    if readiness.empty:
        return pd.DataFrame(
            columns=[
                "etf_symbol",
                "theme",
                "relationship_map_health_bucket",
                "relationship_map_health_state",
                "relationship_map_health_severity",
            ]
        )

    rows: list[dict[str, Any]] = []
    for row in readiness.to_dict(orient="records"):
        etf = str(row.get("etf_symbol") or "").strip().upper()
        active = bool(row.get("active"))
        eligible = bool(row.get("relationship_map_eligible"))
        edge_count = int(edge_counts.get(etf, 0))
        if not active:
            bucket = "inactive"
            state = "inactive"
            severity = "ok"
        elif not eligible:
            bucket = "pendingCoverage"
            state = "pending_coverage"
            severity = "ok"
        elif edge_count <= 0:
            bucket = "noEdges"
            state = "no_edges"
            severity = "red"
        else:
            bucket = "ok"
            state = "ok"
            severity = "ok"
        rows.append(
            {
                "etf_symbol": etf,
                "theme": row.get("theme"),
                "relationship_map_health_bucket": bucket,
                "relationship_map_health_state": state,
                "relationship_map_health_severity": severity,
            }
        )
    return pd.DataFrame(rows)


def _eligibility(*, active: bool, holdings_count: int, technical_count: int) -> tuple[bool, str | None]:
    if not active:
        return False, "inactive"
    if holdings_count <= 0:
        return False, "missing_holdings"
    if technical_count < MIN_TECHNICAL_CONSTITUENTS:
        return False, "insufficient_constituent_coverage"
    return True, None


def _latest_holdings_by_etf(holdings: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if holdings.empty or "etf_ticker" not in holdings.columns:
        return {}
    frame = holdings.copy()
    frame["etf_ticker"] = _normalized_symbol_series(frame["etf_ticker"])
    frame = frame.dropna(subset=["etf_ticker"])
    if "as_of" not in frame.columns:
        return {etf: group.copy() for etf, group in frame.groupby("etf_ticker")}
    frame["_as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    out: dict[str, pd.DataFrame] = {}
    for etf, group in frame.groupby("etf_ticker"):
        latest = group["_as_of"].max()
        if pd.isna(latest):
            current = group.copy()
        else:
            current = group.loc[group["_as_of"].eq(latest)].copy()
        out[str(etf)] = current.drop(columns=["_as_of"], errors="ignore")
    return out


def _onboardable_symbols_by_etf(identity: pd.DataFrame | None) -> dict[str, set[str]]:
    if identity is None or identity.empty or "etf_ticker" not in identity.columns:
        return {}
    frame = identity.copy()
    frame["etf_ticker"] = _normalized_symbol_series(frame["etf_ticker"])
    if "is_onboardable" in frame.columns:
        frame = frame.loc[frame["is_onboardable"].fillna(False).astype(bool)].copy()
    symbol_column = "onboard_symbol" if "onboard_symbol" in frame.columns else "source_symbol"
    frame["_symbol"] = _normalized_symbol_series(frame.get(symbol_column, pd.Series(index=frame.index)))
    frame = frame.dropna(subset=["etf_ticker", "_symbol"])
    return {etf: set(group["_symbol"]) for etf, group in frame.groupby("etf_ticker")}


def _symbol_set(frame: pd.DataFrame | None, *, candidates: tuple[str, ...]) -> set[str]:
    if frame is None or frame.empty:
        return set()
    for column in candidates:
        if column in frame.columns:
            values = _normalized_symbol_series(frame[column]).dropna().tolist()
            return {str(value).strip().upper() for value in values if str(value).strip()}
    return set()


def _latest_holding_date(frame: pd.DataFrame, *, fallback: Any) -> Any:
    if not frame.empty and "as_of" in frame.columns:
        values = pd.to_datetime(frame["as_of"], errors="coerce").dropna()
        if not values.empty:
            return pd.Timestamp(values.max()).date()
    parsed = pd.to_datetime(fallback, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def _integer_value(value: Any) -> int:
    parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(parsed):
        return 0
    return int(parsed)


def _bool_value(value: Any, *, default: bool) -> bool:
    if value is None or pd.isna(value):
        return bool(default)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "t", "1", "yes", "y"}:
            return True
        if text in {"false", "f", "0", "no", "n"}:
            return False
    return bool(value)


def _text_value(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _normalized_symbol_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.upper()
    missing = text.str.lower().isin({"", "nan", "none", "nat", "<na>"})
    return text.where(~missing, None)
