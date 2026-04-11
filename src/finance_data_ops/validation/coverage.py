"""Coverage assessment for required symbol universes."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd


def assess_symbol_coverage(
    *,
    required_symbols: list[str],
    observed_symbols: list[str],
) -> dict[str, object]:
    required = sorted({str(v).strip().upper() for v in required_symbols if str(v).strip()})
    observed = sorted({str(v).strip().upper() for v in observed_symbols if str(v).strip()})
    observed_set = set(observed)
    missing = [symbol for symbol in required if symbol not in observed_set]
    required_count = len(required)
    present_count = required_count - len(missing)
    ratio = 1.0 if required_count == 0 else float(present_count) / float(required_count)
    if required_count == 0:
        status = "unknown"
    elif present_count == required_count:
        status = "fresh"
    elif present_count == 0:
        status = "failed_hard"
    else:
        status = "partial"
    return {
        "status": status,
        "required_count": required_count,
        "present_count": present_count,
        "coverage_ratio": ratio,
        "missing_symbols": missing,
    }


def build_symbol_coverage_rows(
    *,
    required_symbols: list[str],
    prices_frame: pd.DataFrame,
    quotes_frame: pd.DataFrame,
) -> list[dict[str, object]]:
    observed_prices = set(
        str(v).strip().upper()
        for v in prices_frame.get("symbol", pd.Series([], dtype=str)).dropna().tolist()
    )
    observed_quotes = set(
        str(v).strip().upper()
        for v in quotes_frame.get("symbol", pd.Series([], dtype=str)).dropna().tolist()
    )
    latest_price_date_by_symbol = _latest_date_by_symbol(prices_frame, date_col="date")
    latest_quote_ts_by_symbol = _latest_date_by_symbol(quotes_frame, date_col="quote_ts")
    rows: list[dict[str, object]] = []
    updated_at = datetime.now(UTC).isoformat()
    for raw_symbol in required_symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue
        has_price = symbol in observed_prices
        has_quote = symbol in observed_quotes
        has_market_data = has_price or has_quote
        latest_market_price = latest_price_date_by_symbol.get(symbol)
        latest_market_quote = latest_quote_ts_by_symbol.get(symbol)
        latest_market_date = _max_date(latest_market_price, latest_market_quote)
        if has_price and has_quote:
            status = "fresh"
            reason = "market_price_and_quote_available"
        elif has_price or has_quote:
            status = "partial"
            reason = "missing_market_quote" if has_price else "missing_market_price_daily"
        else:
            status = "failed_hard"
            reason = "missing_market_price_and_quote"
        rows.append(
            {
                "ticker": symbol,
                "market_data_available": bool(has_market_data),
                "fundamentals_available": False,
                "earnings_available": False,
                "signal_available": False,
                "market_data_last_date": latest_market_date,
                "fundamentals_last_date": None,
                "next_earnings_date": None,
                "coverage_status": status,
                "reason": reason,
                "updated_at": updated_at,
            }
        )
    return rows


def _latest_date_by_symbol(frame: pd.DataFrame, *, date_col: str) -> dict[str, str | None]:
    if frame.empty or "symbol" not in frame.columns or date_col not in frame.columns:
        return {}
    local = frame[["symbol", date_col]].copy()
    local["symbol"] = local["symbol"].astype(str).str.upper()
    local[date_col] = pd.to_datetime(local[date_col], utc=True, errors="coerce")
    local = local.dropna(subset=["symbol", date_col]).sort_values(["symbol", date_col])
    if local.empty:
        return {}
    latest = local.groupby("symbol")[date_col].max()
    out: dict[str, str | None] = {}
    for symbol, value in latest.items():
        if pd.isna(value):
            out[str(symbol)] = None
            continue
        out[str(symbol)] = pd.Timestamp(value).isoformat()
    return out


def _max_date(*values: str | None) -> str | None:
    timestamps = [pd.to_datetime(value, utc=True, errors="coerce") for value in values if value is not None]
    valid = [value for value in timestamps if not pd.isna(value)]
    if not valid:
        return None
    return pd.Timestamp(max(valid)).date().isoformat()
