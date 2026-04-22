"""Publish market price and quote surfaces."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_market_price_daily_payload(prices_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if prices_frame.empty:
        return []
    frame = prices_frame.copy()

    ticker = frame["ticker"] if "ticker" in frame.columns else frame.get("symbol", pd.Series(index=frame.index, dtype=object))
    source = frame["source"] if "source" in frame.columns else frame.get(
        "provider",
        pd.Series(index=frame.index, dtype=object),
    )
    fetched_at = frame["fetched_at"] if "fetched_at" in frame.columns else frame.get(
        "ingested_at",
        pd.Series(index=frame.index, dtype=object),
    )
    created_at = frame["created_at"] if "created_at" in frame.columns else fetched_at

    date_series = frame["date"] if "date" in frame.columns else pd.Series(index=frame.index, dtype=object)

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "date": pd.to_datetime(date_series, errors="coerce").dt.date,
            "close": pd.to_numeric(frame.get("close"), errors="coerce"),
            "volume": pd.to_numeric(frame.get("volume"), errors="coerce"),
            "source": source,
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "created_at": pd.to_datetime(created_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["created_at"] = payload["created_at"].fillna(payload["fetched_at"])
    payload["ticker"] = payload["ticker"].replace({"": None, "NAN": None, "NONE": None})
    payload = payload.dropna(subset=["ticker", "date", "close"])
    return payload[
        [
            "ticker",
            "date",
            "close",
            "volume",
            "source",
            "fetched_at",
            "created_at",
        ]
    ].to_dict(orient="records")


def build_market_quotes_payload(quotes_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if quotes_frame.empty:
        return []
    frame = quotes_frame.copy()

    ticker = frame["ticker"] if "ticker" in frame.columns else frame.get("symbol", pd.Series(index=frame.index, dtype=object))
    name = frame["name"] if "name" in frame.columns else ticker.astype(str).str.upper()
    source = frame["source"] if "source" in frame.columns else frame.get(
        "provider",
        pd.Series(index=frame.index, dtype=object),
    )
    fetched_at = frame["fetched_at"] if "fetched_at" in frame.columns else frame.get(
        "ingested_at",
        frame.get("quote_ts", pd.Series(index=frame.index, dtype=object)),
    )
    created_at = frame["created_at"] if "created_at" in frame.columns else fetched_at
    updated_at = frame["updated_at"] if "updated_at" in frame.columns else fetched_at

    price = pd.to_numeric(frame.get("price"), errors="coerce")
    previous_close = pd.to_numeric(frame.get("previous_close"), errors="coerce")
    if "change" in frame.columns:
        change = pd.to_numeric(frame["change"], errors="coerce")
    else:
        change = price - previous_close
    if "change_percent" in frame.columns:
        change_percent = pd.to_numeric(frame["change_percent"], errors="coerce")
    else:
        denominator = previous_close.where(previous_close != 0)
        change_percent = (change / denominator) * 100.0
    change_percent = change_percent.replace([np.inf, -np.inf], pd.NA)

    market_cap_text = frame["market_cap_text"] if "market_cap_text" in frame.columns else frame.get(
        "market_cap",
        pd.Series(index=frame.index, dtype=object),
    )

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "name": name,
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "market_cap_text": market_cap_text,
            "source": source,
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "created_at": pd.to_datetime(created_at, utc=True, errors="coerce"),
            "updated_at": pd.to_datetime(updated_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["created_at"] = payload["created_at"].fillna(payload["fetched_at"])
    payload["updated_at"] = payload["updated_at"].fillna(payload["fetched_at"])
    payload["ticker"] = payload["ticker"].replace({"": None, "NAN": None, "NONE": None})
    payload = payload.dropna(subset=["ticker"])
    return payload[
        [
            "ticker",
            "name",
            "price",
            "change",
            "change_percent",
            "market_cap_text",
            "source",
            "fetched_at",
            "created_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_market_quotes_history_payload(quotes_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if quotes_frame.empty:
        return []
    frame = quotes_frame.copy()
    ticker = frame["ticker"] if "ticker" in frame.columns else frame.get("symbol", pd.Series(index=frame.index, dtype=object))
    fetched_at = frame["fetched_at"] if "fetched_at" in frame.columns else frame.get(
        "quote_ts",
        frame.get("ingested_at", frame.get("updated_at", pd.Series(index=frame.index, dtype=object))),
    )
    source = frame["source"] if "source" in frame.columns else frame.get(
        "provider",
        pd.Series(index=frame.index, dtype=object),
    )
    price = pd.to_numeric(frame.get("price"), errors="coerce")
    previous_close = pd.to_numeric(frame.get("previous_close"), errors="coerce")
    if "change" in frame.columns:
        change = pd.to_numeric(frame["change"], errors="coerce")
    else:
        change = price - previous_close
    if "change_percent" in frame.columns:
        change_percent = pd.to_numeric(frame["change_percent"], errors="coerce")
    else:
        denominator = previous_close.where(previous_close != 0)
        change_percent = (change / denominator) * 100.0
    change_percent = change_percent.replace([np.inf, -np.inf], pd.NA)

    market_cap_source = frame["market_cap"] if "market_cap" in frame.columns else frame.get(
        "market_cap_text",
        pd.Series(index=frame.index, dtype=object),
    )

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "market_cap": _coerce_market_cap_series(market_cap_source),
            "source": source,
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["ticker"] = payload["ticker"].replace({"": None, "NAN": None, "NONE": None})
    payload = payload.dropna(subset=["ticker"])
    return payload[
        [
            "ticker",
            "fetched_at",
            "price",
            "change",
            "change_percent",
            "market_cap",
            "source",
        ]
    ].to_dict(orient="records")


def publish_prices_surfaces(
    *,
    publisher: Publisher,
    market_price_daily: pd.DataFrame,
    market_quotes: pd.DataFrame,
    refresh_materialized_view: bool = True,
) -> dict[str, Any]:
    daily_rows = build_market_price_daily_payload(market_price_daily)
    quote_rows = build_market_quotes_payload(market_quotes)
    history_rows = build_market_quotes_history_payload(market_quotes)

    daily_result = publisher.upsert(
        "market_price_daily",
        daily_rows,
        on_conflict="ticker,date",
    )
    quote_result = publisher.upsert(
        "market_quotes",
        quote_rows,
        on_conflict="ticker",
    )
    history_result = publisher.upsert(
        "market_quotes_history",
        history_rows,
        on_conflict="ticker,fetched_at",
    )
    rpc_result: dict[str, Any] | None = None
    if refresh_materialized_view:
        rpc_result = publisher.rpc("refresh_mv_latest_prices", {})
    return {
        "market_price_daily": daily_result,
        "market_quotes": quote_result,
        "market_quotes_history": history_result,
        "mv_latest_prices": rpc_result,
    }


def _coerce_market_cap_series(values: pd.Series) -> pd.Series:
    parsed_numeric = pd.to_numeric(values, errors="coerce")
    parsed_suffix = values.apply(_parse_market_cap_value)
    return parsed_numeric.where(parsed_numeric.notna(), parsed_suffix)


def _parse_market_cap_value(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        casted = float(value)
        return None if np.isnan(casted) else casted

    token = str(value).strip().upper().replace(",", "")
    if not token:
        return None
    multipliers = {
        "K": 1e3,
        "M": 1e6,
        "B": 1e9,
        "T": 1e12,
    }
    suffix = token[-1]
    if suffix in multipliers:
        try:
            base = float(token[:-1])
        except ValueError:
            return None
        return base * multipliers[suffix]
    try:
        return float(token)
    except ValueError:
        return None
