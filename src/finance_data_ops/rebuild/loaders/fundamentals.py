"""Chunked fundamentals rebuild loader."""

from __future__ import annotations

from finance_data_ops.derived.fundamentals_summary import compute_ticker_fundamental_summary
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.refresh.fundamentals_daily import refresh_fundamentals_daily


def load_fundamentals_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: FundamentalsDataProvider,
    cache_root: str,
    tickers: tuple[str, ...],
    max_attempts: int = 3,
) -> dict[str, object]:
    fundamentals_frame, refresh_run = refresh_fundamentals_daily(
        symbols=list(tickers),
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
    )
    summary_frame = compute_ticker_fundamental_summary(fundamentals_frame)
    publish_result = publish_fundamentals_surfaces(
        publisher=publisher,
        fundamentals_history=fundamentals_frame,
        fundamentals_summary=summary_frame,
        refresh_materialized_view=False,
    )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "rows_written": int(len(fundamentals_frame.index) + len(summary_frame.index)),
        "touched_symbols": list(tickers),
        "touched_series": [],
        "current_window": None,
    }

