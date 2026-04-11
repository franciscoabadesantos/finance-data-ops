"""Publish market price and quote surfaces."""

from __future__ import annotations

from typing import Any

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
            "source",
            "fetched_at",
            "created_at",
        ]
    ].to_dict(orient="records")


def build_market_quotes_payload(quotes_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if quotes_frame.empty:
        return []
    frame = quotes_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["quote_ts"] = pd.to_datetime(frame["quote_ts"], utc=True, errors="coerce")
    return frame[
        [
            "symbol",
            "quote_ts",
            "price",
            "previous_close",
            "open",
            "high",
            "low",
            "volume",
            "provider",
            "ingested_at",
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

    daily_result = publisher.upsert(
        "market_price_daily",
        daily_rows,
        on_conflict="ticker,date",
    )
    quote_result = publisher.upsert(
        "market_quotes",
        quote_rows,
        on_conflict="symbol",
    )
    history_result = publisher.upsert(
        "market_quotes_history",
        quote_rows,
        on_conflict="symbol,quote_ts",
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
