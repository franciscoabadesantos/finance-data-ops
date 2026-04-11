"""Price-derived product metrics for frontend consumption."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import numpy as np
import pandas as pd


def compute_ticker_market_stats(
    prices_frame: pd.DataFrame,
    *,
    as_of_date: str | date | None = None,
) -> pd.DataFrame:
    if prices_frame.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "as_of_date",
                "last_price",
                "return_1d_pct",
                "return_1m_pct",
                "return_3m_pct",
                "return_1y_pct",
                "vol_30d_pct",
                "drawdown_1y_pct",
                "dist_from_52w_high_pct",
                "dist_from_52w_low_pct",
                "updated_at",
            ]
        )

    frame = prices_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["symbol", "date", "close"]).sort_values(["symbol", "date"])

    rows: list[dict[str, Any]] = []
    updated_at = datetime.now(UTC).isoformat()
    for symbol, group in frame.groupby("symbol", sort=True):
        close = group["close"].astype(float).reset_index(drop=True)
        if close.empty:
            continue

        trailing_252 = close.tail(252)
        pct = close.pct_change()
        latest_price = float(close.iloc[-1])
        as_of = (
            pd.Timestamp(as_of_date).date().isoformat()
            if as_of_date is not None
            else pd.Timestamp(group["date"].iloc[-1]).date().isoformat()
        )

        rows.append(
            {
                "ticker": symbol,
                "as_of_date": as_of,
                "last_price": latest_price,
                "return_1d_pct": _window_return(close, 1),
                "return_1m_pct": _window_return(close, 21),
                "return_3m_pct": _window_return(close, 63),
                "return_1y_pct": _window_return(close, 252),
                "vol_30d_pct": _volatility_annualized(pct.tail(30)),
                "drawdown_1y_pct": _drawdown(trailing_252),
                "dist_from_52w_high_pct": _distance_from_high(latest_price, trailing_252),
                "dist_from_52w_low_pct": _distance_from_low(latest_price, trailing_252),
                "updated_at": updated_at,
            }
        )
    return pd.DataFrame(rows)


def _window_return(close: pd.Series, periods: int) -> float | None:
    if len(close) <= periods:
        return None
    start = float(close.iloc[-(periods + 1)])
    end = float(close.iloc[-1])
    if start == 0.0:
        return None
    return (end / start) - 1.0


def _volatility_annualized(returns: pd.Series) -> float | None:
    vals = pd.to_numeric(returns, errors="coerce").dropna()
    if len(vals) < 2:
        return None
    return float(vals.std(ddof=1) * np.sqrt(252.0))


def _drawdown(close: pd.Series) -> float | None:
    vals = pd.to_numeric(close, errors="coerce").dropna()
    if vals.empty:
        return None
    rolling_max = vals.cummax()
    drawdown = (vals / rolling_max) - 1.0
    if drawdown.empty:
        return None
    return float(drawdown.min())


def _distance_from_high(latest_price: float, trailing: pd.Series) -> float | None:
    vals = pd.to_numeric(trailing, errors="coerce").dropna()
    if vals.empty:
        return None
    high = float(vals.max())
    if high == 0.0:
        return None
    return (latest_price / high) - 1.0


def _distance_from_low(latest_price: float, trailing: pd.Series) -> float | None:
    vals = pd.to_numeric(trailing, errors="coerce").dropna()
    if vals.empty:
        return None
    low = float(vals.min())
    if low == 0.0:
        return None
    return (latest_price / low) - 1.0
