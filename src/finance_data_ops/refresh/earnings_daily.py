"""Daily earnings refresh job."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table


def refresh_earnings_daily(
    *,
    symbols: list[str],
    provider: EarningsDataProvider,
    cache_root: str,
    max_attempts: int = 3,
    history_limit: int = 8,
) -> tuple[pd.DataFrame, pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_earnings_daily_{uuid4().hex[:12]}"

    symbols_requested = [str(v).strip().upper() for v in symbols if str(v).strip()]
    symbols_succeeded: list[str] = []
    symbols_failed: list[str] = []
    retry_exhausted_symbols: list[str] = []
    error_messages: list[str] = []

    events_frames: list[pd.DataFrame] = []
    history_frames: list[pd.DataFrame] = []

    for symbol in symbols_requested:

        def _fetch_one() -> tuple[pd.DataFrame, pd.DataFrame]:
            events_frame, history_frame = provider.fetch_symbol_earnings(
                symbol,
                history_limit=history_limit,
            )
            if events_frame.empty and history_frame.empty:
                raise RuntimeError(f"{symbol}: provider returned zero earnings rows")
            return events_frame, history_frame

        result, error, _, exhausted_retry_path = run_with_retry(
            _fetch_one,
            max_attempts=max_attempts,
            sleep_seconds=0.0,
        )
        if error is not None or result is None:
            symbols_failed.append(symbol)
            if exhausted_retry_path:
                retry_exhausted_symbols.append(symbol)
            classification = classify_failure(error or RuntimeError("unknown refresh failure"))
            error_messages.append(f"{symbol}: {classification.message}")
            continue

        events_frame, history_frame = result
        if not events_frame.empty:
            events_frames.append(events_frame)
        if not history_frame.empty:
            history_frames.append(history_frame)
        symbols_succeeded.append(symbol)

    events = pd.concat(events_frames, ignore_index=True) if events_frames else pd.DataFrame()
    history = pd.concat(history_frames, ignore_index=True) if history_frames else pd.DataFrame()

    if not events.empty:
        write_parquet_table(
            "market_earnings_events",
            events,
            cache_root=cache_root,
            mode="replace",
            dedupe_subset=["ticker", "earnings_date"],
        )
    if not history.empty:
        write_parquet_table(
            "market_earnings_history",
            history,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["ticker", "earnings_date", "fiscal_period"],
        )

    if symbols_failed and symbols_succeeded:
        status = "partial"
    elif symbols_failed and not symbols_succeeded:
        status = "failed_retrying" if retry_exhausted_symbols else "failed_hard"
    else:
        status = "fresh"

    refresh_result = RefreshRunResult(
        run_id=run_id,
        asset_name="market_earnings",
        status=status,
        started_at=started_at.isoformat(),
        ended_at=datetime.now(UTC).isoformat(),
        symbols_requested=symbols_requested,
        symbols_succeeded=symbols_succeeded,
        symbols_failed=symbols_failed,
        retry_exhausted_symbols=retry_exhausted_symbols,
        rows_written=int(len(events.index) + len(history.index)),
        error_messages=error_messages,
    )
    return events, history, refresh_result
