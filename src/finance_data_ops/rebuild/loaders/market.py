"""Chunked market rebuild loader."""

from __future__ import annotations

from typing import Any

from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.prices import publish_prices_surfaces
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.refresh.market_daily import refresh_market_daily
from finance_data_ops.refresh.quotes_latest import refresh_latest_quotes


def load_market_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: MarketDataProvider,
    cache_root: str,
    tickers: tuple[str, ...],
    start_date: str,
    end_date: str,
    max_attempts: int = 3,
    symbol_batch_size: int = 100,
) -> dict[str, Any]:
    prices_frame, prices_run = refresh_market_daily(
        symbols=list(tickers),
        start=start_date,
        end=end_date,
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
    )
    quotes_frame, quotes_run = refresh_latest_quotes(
        symbols=list(tickers),
        provider=provider,
        cache_root=cache_root,
        symbol_batch_size=int(symbol_batch_size),
    )
    publish_result = publish_prices_surfaces(
        publisher=publisher,
        market_price_daily=prices_frame,
        market_quotes=quotes_frame,
        refresh_materialized_view=False,
    )
    return {
        "refresh_runs": [prices_run.as_dict(), quotes_run.as_dict()],
        "publish_result": publish_result,
        "rows_written": int(len(prices_frame.index) + len(quotes_frame.index)),
        "touched_symbols": list(tickers),
        "touched_series": [],
        "current_window": {"start_date": start_date, "end_date": end_date},
    }

