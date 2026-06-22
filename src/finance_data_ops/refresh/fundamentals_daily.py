"""Daily fundamentals refresh job."""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import Callable
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table

LOGGER = logging.getLogger("finance_data_ops.refresh.fundamentals_daily")


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
    profile_rows: list[pd.DataFrame] = []
    holding_rows: list[pd.DataFrame] = []
    sector_weight_rows: list[pd.DataFrame] = []
    symbols_succeeded: list[str] = []
    symbols_failed: list[str] = []
    retry_exhausted_symbols: list[str] = []
    error_messages: list[str] = []

    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue

        def _fetch_one() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            frame = provider.fetch_symbol_fundamentals(symbol)
            if frame.empty:
                raise RuntimeError(f"{symbol}: provider returned zero fundamentals rows")
            profile_frame = (
                _optional_frame(lambda: provider.fetch_symbol_profile(symbol), symbol=symbol, surface="profile")
                if hasattr(provider, "fetch_symbol_profile")
                else pd.DataFrame()
            )
            holdings_frame = pd.DataFrame()
            sector_weights_frame = pd.DataFrame()
            if hasattr(provider, "fetch_symbol_etf_funds_data"):
                holdings_frame, sector_weights_frame = _optional_etf_funds_data(
                    lambda: provider.fetch_symbol_etf_funds_data(symbol),
                    symbol=symbol,
                    surface="etf_funds_data",
                )
            return frame, profile_frame, holdings_frame, sector_weights_frame

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

        fundamentals_frame, profile_frame, holdings_frame, sector_weights_frame = result
        rows.append(fundamentals_frame)
        if not profile_frame.empty:
            profile_rows.append(profile_frame)
        if not holdings_frame.empty:
            holding_rows.append(holdings_frame)
        if not sector_weights_frame.empty:
            sector_weight_rows.append(sector_weights_frame)
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

    merged_profiles = pd.concat(profile_rows, ignore_index=True) if profile_rows else pd.DataFrame()
    if not merged_profiles.empty:
        write_parquet_table(
            "ticker_profile",
            merged_profiles,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["ticker"],
        )

    merged_holdings = pd.concat(holding_rows, ignore_index=True) if holding_rows else pd.DataFrame()
    if not merged_holdings.empty:
        write_parquet_table(
            "etf_holdings",
            merged_holdings,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["etf_ticker", "holding_symbol", "as_of"],
        )

    merged_sector_weights = (
        pd.concat(sector_weight_rows, ignore_index=True) if sector_weight_rows else pd.DataFrame()
    )
    if not merged_sector_weights.empty:
        write_parquet_table(
            "etf_sector_weights",
            merged_sector_weights,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["etf_ticker", "sector", "as_of"],
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


def _optional_frame(operation: Callable[[], pd.DataFrame], *, symbol: str, surface: str) -> pd.DataFrame:
    try:
        result = operation()
    except Exception as exc:
        LOGGER.warning(
            "Optional fundamentals refresh surface failed (symbol=%s surface=%s): %r",
            symbol,
            surface,
            exc,
        )
        return pd.DataFrame()
    return result if isinstance(result, pd.DataFrame) else pd.DataFrame()


def _optional_etf_funds_data(
    operation: Callable[[], tuple[pd.DataFrame, pd.DataFrame]], *, symbol: str, surface: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        result = operation()
    except Exception as exc:
        LOGGER.warning(
            "Optional fundamentals refresh surface failed (symbol=%s surface=%s): %r",
            symbol,
            surface,
            exc,
        )
        return pd.DataFrame(), pd.DataFrame()
    if not isinstance(result, tuple) or len(result) != 2:
        return pd.DataFrame(), pd.DataFrame()
    holdings, sector_weights = result
    return (
        holdings if isinstance(holdings, pd.DataFrame) else pd.DataFrame(),
        sector_weights if isinstance(sector_weights, pd.DataFrame) else pd.DataFrame(),
    )
