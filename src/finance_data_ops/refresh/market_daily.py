"""Daily market price refresh job."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.refresh.storage import write_parquet_table


@dataclass(slots=True)
class RefreshRunResult:
    run_id: str
    asset_name: str
    status: str
    started_at: str
    ended_at: str
    symbols_requested: list[str]
    symbols_succeeded: list[str]
    symbols_failed: list[str]
    retry_exhausted_symbols: list[str]
    rows_written: int
    error_messages: list[str]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def refresh_market_daily(
    *,
    symbols: list[str],
    start: str,
    end: str,
    provider: MarketDataProvider,
    cache_root: str,
    max_attempts: int = 3,
) -> tuple[pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_market_daily_{uuid4().hex[:12]}"
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
            frame = provider.fetch_daily_prices([symbol], start=start, end=end)
            if frame.empty:
                raise RuntimeError(f"{symbol}: provider returned zero rows")
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
            "market_price_daily",
            merged,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["symbol", "date"],
        )

    if symbols_failed and symbols_succeeded:
        status = "partial"
    elif symbols_failed and not symbols_succeeded:
        status = "failed_retrying" if retry_exhausted_symbols else "failed_hard"
    else:
        status = "fresh"

    result = RefreshRunResult(
        run_id=run_id,
        asset_name="market_price_daily",
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
    return merged, result
