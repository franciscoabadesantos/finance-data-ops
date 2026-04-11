"""Fundamentals-derived frontend summary surfaces."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd


SUMMARY_COLUMNS = [
    "ticker",
    "latest_revenue",
    "latest_eps",
    "trailing_pe",
    "market_cap",
    "revenue_growth_yoy",
    "earnings_growth_yoy",
    "latest_period_end",
    "source",
    "updated_at",
]


def compute_ticker_fundamental_summary(fundamentals_frame: pd.DataFrame) -> pd.DataFrame:
    if fundamentals_frame.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    frame = fundamentals_frame.copy()
    ticker_series = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    frame["ticker"] = ticker_series.astype(str).str.upper()
    frame["metric"] = frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower()
    frame["value"] = pd.to_numeric(frame.get("value"), errors="coerce")
    frame["period_end"] = pd.to_datetime(frame.get("period_end"), errors="coerce")
    frame["source"] = frame.get("source", frame.get("provider", "unknown")).astype(str)

    frame = frame.dropna(subset=["ticker", "metric", "value", "period_end"])
    if frame.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    rows: list[dict[str, Any]] = []
    updated_at = datetime.now(UTC).isoformat()
    for ticker, group in frame.groupby("ticker", sort=True):
        latest_revenue = _latest_metric_value(group, "revenue")
        latest_eps = _latest_metric_value(group, "eps")
        market_cap = _latest_metric_value(group, "market_cap")
        trailing_pe = _latest_metric_value(group, "trailing_pe")

        if trailing_pe is None:
            latest_net_income = _latest_metric_value(group, "net_income")
            latest_shares = _latest_metric_value(group, "shares_outstanding")
            trailing_pe = _derive_trailing_pe(
                market_cap=market_cap,
                net_income=latest_net_income,
                eps=latest_eps,
                shares_outstanding=latest_shares,
            )

        revenue_growth_yoy = _metric_growth_yoy(group, "revenue")
        earnings_growth_yoy = _metric_growth_yoy(group, "net_income")
        if earnings_growth_yoy is None:
            earnings_growth_yoy = _metric_growth_yoy(group, "eps")

        latest_period_end = group["period_end"].max()
        source = _latest_source(group)

        rows.append(
            {
                "ticker": ticker,
                "latest_revenue": latest_revenue,
                "latest_eps": latest_eps,
                "trailing_pe": trailing_pe,
                "market_cap": market_cap,
                "revenue_growth_yoy": revenue_growth_yoy,
                "earnings_growth_yoy": earnings_growth_yoy,
                "latest_period_end": (
                    pd.Timestamp(latest_period_end).date().isoformat() if not pd.isna(latest_period_end) else None
                ),
                "source": source,
                "updated_at": updated_at,
            }
        )

    return pd.DataFrame(rows, columns=SUMMARY_COLUMNS)


def _latest_metric_value(frame: pd.DataFrame, metric: str) -> float | None:
    subset = frame.loc[frame["metric"] == str(metric).lower()].sort_values(["period_end"])
    if subset.empty:
        return None
    value = pd.to_numeric(subset["value"].iloc[-1], errors="coerce")
    if pd.isna(value):
        return None
    return float(value)


def _latest_source(frame: pd.DataFrame) -> str:
    local = frame.sort_values(["period_end"])
    if local.empty:
        return "unknown"
    value = str(local["source"].iloc[-1]).strip()
    return value or "unknown"


def _metric_growth_yoy(frame: pd.DataFrame, metric: str) -> float | None:
    subset = frame.loc[frame["metric"] == str(metric).lower()].sort_values(["period_end"])
    if len(subset.index) < 2:
        return None

    latest_row = subset.iloc[-1]
    latest_date = pd.Timestamp(latest_row["period_end"])
    latest_value = _as_float(latest_row["value"])
    if latest_value is None:
        return None

    target_date = latest_date - timedelta(days=330)
    prior_window = subset.loc[subset["period_end"] <= target_date]
    if not prior_window.empty:
        prior_row = prior_window.iloc[-1]
    else:
        prior_row = subset.iloc[-2]

    prior_value = _as_float(prior_row["value"])
    if prior_value is None or prior_value == 0.0:
        return None
    return (latest_value - prior_value) / abs(prior_value)


def _derive_trailing_pe(
    *,
    market_cap: float | None,
    net_income: float | None,
    eps: float | None,
    shares_outstanding: float | None,
) -> float | None:
    if market_cap is None:
        return None

    if net_income is not None and net_income > 0:
        return market_cap / net_income

    if (
        eps is not None
        and eps > 0
        and shares_outstanding is not None
        and shares_outstanding > 0
    ):
        price = market_cap / shares_outstanding
        return price / eps if eps != 0 else None
    return None


def _as_float(value: Any) -> float | None:
    casted = pd.to_numeric(value, errors="coerce")
    if pd.isna(casted):
        return None
    return float(casted)
