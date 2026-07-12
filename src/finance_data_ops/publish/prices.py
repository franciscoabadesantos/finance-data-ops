"""Publish canonical market price source-cache surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_source_cache_market_price_daily_payload(prices_frame: pd.DataFrame) -> list[dict[str, Any]]:
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
            "symbol": ticker.astype(str).str.upper(),
            "price_date": pd.to_datetime(date_series, errors="coerce").dt.date,
            "open": pd.to_numeric(frame.get("open"), errors="coerce"),
            "high": pd.to_numeric(frame.get("high"), errors="coerce"),
            "low": pd.to_numeric(frame.get("low"), errors="coerce"),
            "close": pd.to_numeric(frame.get("close"), errors="coerce"),
            "adj_close": pd.to_numeric(frame.get("adj_close"), errors="coerce"),
            "volume": pd.to_numeric(frame.get("volume"), errors="coerce"),
            "source_updated_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "ingested_at": pd.to_datetime(created_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["source_updated_at"] = payload["source_updated_at"].fillna(now_utc)
    payload["ingested_at"] = payload["ingested_at"].fillna(payload["source_updated_at"])
    payload["symbol"] = payload["symbol"].replace({"": None, "NAN": None, "NONE": None})
    payload["volume"] = payload["volume"].apply(_nullable_int)
    payload = payload.dropna(subset=["symbol", "price_date", "close"])
    return payload[
        [
            "symbol",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "source_updated_at",
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
    source_cache_daily_rows = build_source_cache_market_price_daily_payload(market_price_daily)
    source_cache_daily_result = publisher.upsert(
        "source_cache.market_price_daily",
        source_cache_daily_rows,
        on_conflict="symbol,price_date",
    )
    return {
        "source_cache.market_price_daily": source_cache_daily_result,
    }


def _nullable_int(value: Any) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None
