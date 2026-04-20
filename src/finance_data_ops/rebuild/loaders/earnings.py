"""Chunked earnings rebuild loader."""

from __future__ import annotations

from datetime import date
import logging

import pandas as pd

from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.refresh.earnings_daily import refresh_earnings_daily

logger = logging.getLogger(__name__)


def load_earnings_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: EarningsDataProvider,
    cache_root: str,
    tickers: tuple[str, ...],
    start_date: str,
    end_date: str,
    max_attempts: int = 3,
    history_limit: int = 120,
) -> dict[str, object]:
    resolved_history_limit = _resolve_earnings_history_limit(
        start_date=date.fromisoformat(str(start_date)),
        end_date=date.fromisoformat(str(end_date)),
    )
    events_frame, history_frame, refresh_run = refresh_earnings_daily(
        symbols=list(tickers),
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
        history_limit=resolved_history_limit,
    )
    provider_event_rows = int(len(events_frame.index))
    provider_history_rows = int(len(history_frame.index))
    filtered_events = _filter_earnings_window(
        events_frame,
        start_date=date.fromisoformat(str(start_date)),
        end_date=date.fromisoformat(str(end_date)),
    )
    filtered_history = _filter_earnings_window(
        history_frame,
        start_date=date.fromisoformat(str(start_date)),
        end_date=date.fromisoformat(str(end_date)),
    )
    filtered_event_rows = int(len(filtered_events.index))
    filtered_history_rows = int(len(filtered_history.index))
    logger.info(
        "Earnings rebuild chunk prepared (tickers=%s start=%s end=%s history_limit=%s provider_event_rows=%s filtered_event_rows=%s provider_history_rows=%s filtered_history_rows=%s refresh_status=%s).",
        list(tickers),
        start_date,
        end_date,
        resolved_history_limit,
        provider_event_rows,
        filtered_event_rows,
        provider_history_rows,
        filtered_history_rows,
        refresh_run.status,
    )
    if filtered_events.empty and filtered_history.empty:
        publish_result = {"status": "skipped", "reason": "window_filter_no_matches"}
    else:
        publish_result = publish_earnings_surfaces(
            publisher=publisher,
            earnings_events=filtered_events,
            earnings_history=filtered_history,
            refresh_materialized_view=False,
        )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "resolved_history_limit": resolved_history_limit,
        "provider_rows": {
            "earnings_events": provider_event_rows,
            "market_earnings_history": provider_history_rows,
        },
        "filtered_rows": {
            "earnings_events": filtered_event_rows,
            "market_earnings_history": filtered_history_rows,
        },
        "window_filter_field": "earnings_date",
        "rows_written": int(len(filtered_events.index) + len(filtered_history.index)),
        "touched_symbols": list(tickers),
        "touched_series": [],
        "current_window": {"start_date": start_date, "end_date": end_date},
    }


def _filter_earnings_window(frame: pd.DataFrame, *, start_date: date, end_date: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    local = frame.copy()
    earnings_date = pd.to_datetime(local.get("earnings_date"), errors="coerce").dt.date
    filtered = local.loc[earnings_date.notna() & (earnings_date >= start_date) & (earnings_date <= end_date)].copy()
    return filtered.reset_index(drop=True)


def _resolve_earnings_history_limit(*, start_date: date, end_date: date) -> int:
    window_days = max((end_date - start_date).days, 0) + 1
    # Earnings are quarterly. Add a buffer so narrow windows still include the
    # nearest past results plus any upcoming event rows from the provider.
    estimated_quarters = (window_days + 89) // 90
    return max(8, min(estimated_quarters + 4, 100))
