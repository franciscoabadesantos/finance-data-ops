"""Publish market price and quote surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_market_price_daily_payload(prices_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if prices_frame.empty:
        return []
    frame = prices_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date.astype(str)
    return frame[
        [
            "symbol",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "provider",
            "ingested_at",
        ]
    ].to_dict(orient="records")


def build_market_quotes_payload(quotes_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if quotes_frame.empty:
        return []
    frame = quotes_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["quote_ts"] = pd.to_datetime(frame["quote_ts"], utc=True, errors="coerce").astype(str)
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
        on_conflict="symbol,date",
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
