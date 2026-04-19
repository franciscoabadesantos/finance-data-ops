"""Chunked earnings rebuild loader."""

from __future__ import annotations

from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.refresh.earnings_daily import refresh_earnings_daily


def load_earnings_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: EarningsDataProvider,
    cache_root: str,
    tickers: tuple[str, ...],
    max_attempts: int = 3,
    history_limit: int = 120,
) -> dict[str, object]:
    events_frame, history_frame, refresh_run = refresh_earnings_daily(
        symbols=list(tickers),
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
        history_limit=int(history_limit),
    )
    publish_result = publish_earnings_surfaces(
        publisher=publisher,
        earnings_events=events_frame,
        earnings_history=history_frame,
        refresh_materialized_view=False,
    )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "rows_written": int(len(events_frame.index) + len(history_frame.index)),
        "touched_symbols": list(tickers),
        "touched_series": [],
        "current_window": None,
    }

