"""Chunked fundamentals rebuild loader."""

from __future__ import annotations

from datetime import date
import logging

import pandas as pd

from finance_data_ops.derived.fundamentals_summary import compute_ticker_fundamental_summary
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.refresh.fundamentals_daily import refresh_fundamentals_daily

logger = logging.getLogger(__name__)


def load_fundamentals_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: FundamentalsDataProvider,
    cache_root: str,
    tickers: tuple[str, ...],
    start_date: str,
    end_date: str,
    max_attempts: int = 3,
) -> dict[str, object]:
    fundamentals_frame, refresh_run = refresh_fundamentals_daily(
        symbols=list(tickers),
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
    )
    provider_rows = int(len(fundamentals_frame.index))
    filtered_frame = _filter_fundamentals_window(
        fundamentals_frame,
        start_date=date.fromisoformat(str(start_date)),
        end_date=date.fromisoformat(str(end_date)),
    )
    filtered_rows = int(len(filtered_frame.index))
    logger.info(
        "Fundamentals rebuild chunk prepared (tickers=%s start=%s end=%s provider_rows=%s filtered_rows=%s refresh_status=%s).",
        list(tickers),
        start_date,
        end_date,
        provider_rows,
        filtered_rows,
        refresh_run.status,
    )
    summary_frame = compute_ticker_fundamental_summary(filtered_frame)
    if filtered_frame.empty and summary_frame.empty:
        publish_result = {"status": "skipped", "reason": "window_filter_no_matches"}
    else:
        publish_result = publish_fundamentals_surfaces(
            publisher=publisher,
            fundamentals_history=filtered_frame,
            fundamentals_summary=summary_frame,
            refresh_materialized_view=False,
        )
    symbol_breakdown = _build_symbol_breakdown(
        tickers=tickers,
        provider_frame=fundamentals_frame,
        filtered_frame=filtered_frame,
        summary_frame=summary_frame,
    )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "provider_rows": provider_rows,
        "filtered_rows": filtered_rows,
        "symbol_breakdown": symbol_breakdown,
        "window_filter_field": "period_end",
        "rows_written": int(len(filtered_frame.index) + len(summary_frame.index)),
        "touched_symbols": list(tickers),
        "touched_series": [],
        "current_window": {"start_date": start_date, "end_date": end_date},
    }


def _filter_fundamentals_window(frame: pd.DataFrame, *, start_date: date, end_date: date) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    local = frame.copy()
    period_end = pd.to_datetime(local.get("period_end"), errors="coerce").dt.date
    filtered = local.loc[period_end.notna() & (period_end >= start_date) & (period_end <= end_date)].copy()
    return filtered.reset_index(drop=True)


def _build_symbol_breakdown(
    *,
    tickers: tuple[str, ...],
    provider_frame: pd.DataFrame,
    filtered_frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
) -> list[dict[str, object]]:
    provider_counts = _count_by_ticker(provider_frame)
    filtered_counts = _count_by_ticker(filtered_frame)
    summary_counts = _count_by_ticker(summary_frame)
    breakdown: list[dict[str, object]] = []
    for ticker in tickers:
        normalized = str(ticker).strip().upper()
        provider_rows = int(provider_counts.get(normalized, 0))
        filtered_rows = int(filtered_counts.get(normalized, 0))
        summary_rows = int(summary_counts.get(normalized, 0))
        breakdown.append(
            {
                "ticker": normalized,
                "provider_rows": provider_rows,
                "filtered_rows": filtered_rows,
                "summary_rows": summary_rows,
                "rows_written": filtered_rows + summary_rows,
                "zero_reason": (
                    "provider_returned_empty"
                    if provider_rows == 0
                    else ("window_filter_no_matches" if filtered_rows == 0 else None)
                ),
            }
        )
    return breakdown


def _count_by_ticker(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "ticker" not in frame.columns:
        return {}
    series = frame["ticker"].astype(str).str.strip().str.upper()
    counts = series.value_counts(dropna=False)
    return {str(index): int(value) for index, value in counts.items() if str(index).strip()}
