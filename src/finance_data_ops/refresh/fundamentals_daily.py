"""Daily fundamentals refresh job."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table


def refresh_fundamentals_daily(
    *,
    symbols: list[str],
    provider: FundamentalsDataProvider,
    cache_root: str,
    max_attempts: int = 3,
) -> tuple[pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_fundamentals_daily_{uuid4().hex[:12]}"

    rows: list[pd.DataFrame] = []
    symbols_succeeded: list[str] = []
    symbols_failed: list[str] = []
    retry_exhausted_symbols: list[str] = []
    error_messages: list[str] = []

    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue

        def _fetch_one() -> pd.DataFrame:
            frame = provider.fetch_symbol_fundamentals(symbol)
            if frame.empty:
                raise RuntimeError(f"{symbol}: provider returned zero fundamentals rows")
            return frame

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

        rows.append(result)
        symbols_succeeded.append(symbol)

    merged = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    rows_written = int(len(merged.index))
    if not merged.empty:
        write_parquet_table(
            "market_fundamentals_v2",
            merged,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["ticker", "metric", "period_end", "period_type"],
        )

    if symbols_failed and symbols_succeeded:
        status = "partial"
    elif symbols_failed and not symbols_succeeded:
        status = "failed_retrying" if retry_exhausted_symbols else "failed_hard"
    else:
        status = "fresh"

    refresh_result = RefreshRunResult(
        run_id=run_id,
        asset_name="market_fundamentals_v2",
        status=status,
        started_at=started_at.isoformat(),
        ended_at=datetime.now(UTC).isoformat(),
        symbols_requested=[str(v).strip().upper() for v in symbols if str(v).strip()],
        symbols_succeeded=symbols_succeeded,
        symbols_failed=symbols_failed,
        retry_exhausted_symbols=retry_exhausted_symbols,
        rows_written=rows_written,
        error_messages=error_messages,
    )
    return merged, refresh_result
