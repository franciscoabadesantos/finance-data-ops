"""Publish price-derived product metrics."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_ticker_market_stats_snapshot_payload(stats_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if stats_frame.empty:
        return []
    frame = stats_frame.copy()
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"], errors="coerce").dt.date.astype(str)
    return frame.to_dict(orient="records")


def publish_product_metrics(
    *,
    publisher: Publisher,
    market_stats_snapshot: pd.DataFrame,
) -> dict[str, Any]:
    rows = build_ticker_market_stats_snapshot_payload(market_stats_snapshot)
    return publisher.upsert(
        "ticker_market_stats_snapshot",
        rows,
        on_conflict="symbol,as_of_date",
    )
