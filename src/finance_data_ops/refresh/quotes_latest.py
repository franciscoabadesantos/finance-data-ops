"""Latest quote refresh job."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table


def refresh_latest_quotes(
    *,
    symbols: list[str],
    provider: MarketDataProvider,
    cache_root: str,
    symbol_batch_size: int = 100,
) -> tuple[pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_quotes_latest_{uuid4().hex[:12]}"
    symbols_requested = [str(v).strip().upper() for v in symbols if str(v).strip()]

    symbols_failed: list[str] = []
    error_messages: list[str] = []
    quote_batches: list[pd.DataFrame] = []
    for batch in _chunked(symbols_requested, batch_size=max(int(symbol_batch_size), 1)):
        try:
            fetched = provider.fetch_latest_quotes(batch)
            if fetched is not None and not fetched.empty:
                quote_batches.append(fetched)
        except Exception as exc:
            classification = classify_failure(exc)
            symbols_failed.extend(batch)
            error_messages.append(f"batch[{','.join(batch)}]: {classification.message}")
    quotes = pd.concat(quote_batches, ignore_index=True) if quote_batches else pd.DataFrame()

    if quotes.empty:
        status = "failed_hard" if symbols_requested else "unknown"
        result = RefreshRunResult(
            run_id=run_id,
            asset_name="market_quotes",
            status=status,
            started_at=started_at.isoformat(),
            ended_at=datetime.now(UTC).isoformat(),
            symbols_requested=symbols_requested,
            symbols_succeeded=[],
            symbols_failed=symbols_failed,
            retry_exhausted_symbols=[],
            rows_written=0,
            error_messages=error_messages or ["quote refresh produced no rows"],
        )
        return quotes, result

    symbols_succeeded = sorted(set(str(v).upper() for v in quotes["symbol"].tolist()))
    failed_remaining = sorted(set(v for v in symbols_requested if v not in set(symbols_succeeded)).union(symbols_failed))
    status = "fresh" if not failed_remaining else "partial"

    write_parquet_table(
        "market_quotes",
        quotes,
        cache_root=cache_root,
        mode="replace",
        dedupe_subset=["symbol"],
    )
    write_parquet_table(
        "market_quotes_history",
        quotes,
        cache_root=cache_root,
        mode="append",
        dedupe_subset=["symbol", "quote_ts"],
    )

    result = RefreshRunResult(
        run_id=run_id,
        asset_name="market_quotes",
        status=status,
        started_at=started_at.isoformat(),
        ended_at=datetime.now(UTC).isoformat(),
        symbols_requested=symbols_requested,
        symbols_succeeded=symbols_succeeded,
        symbols_failed=failed_remaining,
        retry_exhausted_symbols=[],
        rows_written=int(len(quotes.index)),
        error_messages=[],
    )
    return quotes, result


def _chunked(values: list[str], *, batch_size: int) -> list[list[str]]:
    size = max(int(batch_size), 1)
    return [values[idx : idx + size] for idx in range(0, len(values), size)]
