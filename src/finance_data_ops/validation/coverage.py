"""Coverage assessment for required symbol universes."""

from __future__ import annotations

from datetime import UTC, date, datetime

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
    fundamentals_frame: pd.DataFrame | None = None,
    earnings_events_frame: pd.DataFrame | None = None,
    as_of_date: str | date | None = None,
) -> list[dict[str, object]]:
    observed_prices = _observed_symbols(prices_frame)
    observed_quotes = _observed_symbols(quotes_frame)
    observed_fundamentals = _observed_symbols(fundamentals_frame)
    observed_earnings = _observed_symbols(earnings_events_frame)

    latest_price_date_by_symbol = _latest_date_by_symbol(prices_frame, date_col="date")
    latest_quote_ts_by_symbol = _latest_date_by_symbol(quotes_frame, date_col="quote_ts")
    latest_fundamentals_date_by_symbol = _latest_date_by_symbol(
        fundamentals_frame,
        date_col="period_end",
    )
    next_earnings_by_symbol = _next_earnings_date_by_symbol(
        earnings_events_frame,
        as_of_date=as_of_date,
    )

    rows: list[dict[str, object]] = []
    updated_at = datetime.now(UTC).isoformat()
    for raw_symbol in required_symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue

        has_price = symbol in observed_prices
        has_quote = symbol in observed_quotes
        has_market_data = has_price or has_quote

        has_fundamentals = symbol in observed_fundamentals
        has_earnings = symbol in observed_earnings

        latest_market_price = latest_price_date_by_symbol.get(symbol)
        latest_market_quote = latest_quote_ts_by_symbol.get(symbol)
        latest_market_date = _max_date(latest_market_price, latest_market_quote)

        latest_fundamentals_date = latest_fundamentals_date_by_symbol.get(symbol)
        next_earnings_date = next_earnings_by_symbol.get(symbol)

        if fundamentals_frame is None and earnings_events_frame is None:
            # Preserve v1 market-only status semantics.
            if has_price and has_quote:
                status = "fresh"
                reason = "market_price_and_quote_available"
            elif has_price or has_quote:
                status = "partial"
                reason = "missing_market_quote" if has_price else "missing_market_price_daily"
            else:
                status = "failed_hard"
                reason = "missing_market_price_and_quote"
        else:
            if has_market_data and has_fundamentals and has_earnings:
                status = "fresh"
                reason = "market_fundamentals_earnings_available"
            else:
                missing_components: list[str] = []
                if not has_market_data:
                    missing_components.append("market_data")
                if not has_fundamentals:
                    missing_components.append("fundamentals")
                if not has_earnings:
                    missing_components.append("earnings")

                if len(missing_components) == 3:
                    status = "failed_hard"
                    reason = "missing_market_data_fundamentals_earnings"
                else:
                    status = "partial"
                    reason = f"missing_{'_'.join(missing_components)}"

        rows.append(
            {
                "ticker": symbol,
                "market_data_available": bool(has_market_data),
                "fundamentals_available": bool(has_fundamentals),
                "earnings_available": bool(has_earnings),
                "signal_available": False,
                "market_data_last_date": latest_market_date,
                "fundamentals_last_date": latest_fundamentals_date,
                "next_earnings_date": next_earnings_date,
                "coverage_status": status,
                "reason": reason,
                "updated_at": updated_at,
            }
        )
    return rows


def _latest_date_by_symbol(frame: pd.DataFrame | None, *, date_col: str) -> dict[str, str | None]:
    if frame is None or frame.empty or date_col not in frame.columns:
        return {}
    symbol_col = _symbol_column_name(frame)
    if symbol_col is None:
        return {}
    local = frame[[symbol_col, date_col]].copy()
    local[symbol_col] = local[symbol_col].astype(str).str.upper()
    local[date_col] = pd.to_datetime(local[date_col], utc=True, errors="coerce")
    local = local.dropna(subset=[symbol_col, date_col]).sort_values([symbol_col, date_col])
    if local.empty:
        return {}
    latest = local.groupby(symbol_col)[date_col].max()
    out: dict[str, str | None] = {}
    for symbol, value in latest.items():
        if pd.isna(value):
            out[str(symbol)] = None
            continue
        out[str(symbol)] = pd.Timestamp(value).isoformat()
    return out


def _next_earnings_date_by_symbol(
    frame: pd.DataFrame | None,
    *,
    as_of_date: str | date | None,
) -> dict[str, str | None]:
    if frame is None or frame.empty:
        return {}
    symbol_col = _symbol_column_name(frame)
    if symbol_col is None or "earnings_date" not in frame.columns:
        return {}

    as_of = pd.Timestamp(as_of_date).date() if as_of_date is not None else datetime.now(UTC).date()
    local = frame[[symbol_col, "earnings_date"]].copy()
    local[symbol_col] = local[symbol_col].astype(str).str.upper()
    local["earnings_date"] = pd.to_datetime(local["earnings_date"], errors="coerce").dt.date
    local = local.dropna(subset=[symbol_col, "earnings_date"])
    local = local.loc[local["earnings_date"] >= as_of]
    if local.empty:
        return {}
    next_dates = local.groupby(symbol_col)["earnings_date"].min()
    return {str(symbol): pd.Timestamp(value).date().isoformat() for symbol, value in next_dates.items()}


def _observed_symbols(frame: pd.DataFrame | None) -> set[str]:
    if frame is None or frame.empty:
        return set()
    symbol_col = _symbol_column_name(frame)
    if symbol_col is None:
        return set()
    return {
        str(value).strip().upper()
        for value in frame[symbol_col].dropna().tolist()
        if str(value).strip()
    }


def _symbol_column_name(frame: pd.DataFrame) -> str | None:
    if "ticker" in frame.columns:
        return "ticker"
    if "symbol" in frame.columns:
        return "symbol"
    return None


def _max_date(*values: str | None) -> str | None:
    timestamps = [pd.to_datetime(value, utc=True, errors="coerce") for value in values if value is not None]
    valid = [value for value in timestamps if not pd.isna(value)]
    if not valid:
        return None
    return pd.Timestamp(max(valid)).date().isoformat()
