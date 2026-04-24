"""Daily earnings refresh job."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import urllib.parse
import urllib.request
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.settings import load_settings


NON_EARNINGS_INSTRUMENT_TYPES = {"etf", "index_proxy", "country_fund"}


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
    settings = load_settings(cache_root=cache_root)

    symbols_requested = [str(v).strip().upper() for v in symbols if str(v).strip()]
    symbols_succeeded: list[str] = []
    symbols_failed: list[str] = []
    retry_exhausted_symbols: list[str] = []
    error_messages: list[str] = []
    registry_metadata = _fetch_ticker_registry_metadata(
        cache_root=cache_root,
        supabase_url=settings.supabase_url,
        service_role_key=settings.supabase_secret_key,
        tickers=symbols_requested,
    )

    events_frames: list[pd.DataFrame] = []
    history_frames: list[pd.DataFrame] = []

    for symbol in symbols_requested:
        instrument_type = str(registry_metadata.get(symbol, {}).get("instrument_type") or "").strip().lower()
        if instrument_type in NON_EARNINGS_INSTRUMENT_TYPES:
            symbols_succeeded.append(symbol)
            continue

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


def _fetch_ticker_registry_metadata(
    *,
    cache_root: str,
    supabase_url: str,
    service_role_key: str,
    tickers: list[str],
    timeout_seconds: int = 30,
) -> dict[str, dict[str, object]]:
    normalized_tickers = [str(value).strip().upper() for value in tickers if str(value).strip()]
    metadata: dict[str, dict[str, object]] = {}
    if not normalized_tickers:
        return metadata

    remote_rows: list[dict[str, object]] = []
    if str(supabase_url).strip() and str(service_role_key).strip():
        try:
            remote_rows = _fetch_registry_rows_from_supabase(
                supabase_url=supabase_url,
                service_role_key=service_role_key,
                tickers=normalized_tickers,
                column="normalized_symbol",
                timeout_seconds=timeout_seconds,
            )
            remote_rows.extend(
                _fetch_registry_rows_from_supabase(
                    supabase_url=supabase_url,
                    service_role_key=service_role_key,
                    tickers=normalized_tickers,
                    column="input_symbol",
                    timeout_seconds=timeout_seconds,
                )
            )
        except Exception:
            remote_rows = []

    if remote_rows:
        return _index_registry_rows(remote_rows, normalized_tickers)

    local_frame = read_parquet_table("ticker_registry", cache_root=cache_root, required=False)
    if local_frame.empty:
        return {}
    local_rows = local_frame.to_dict(orient="records")
    return _index_registry_rows(local_rows, normalized_tickers)


def _fetch_registry_rows_from_supabase(
    *,
    supabase_url: str,
    service_role_key: str,
    tickers: list[str],
    column: str,
    timeout_seconds: int,
) -> list[dict[str, object]]:
    encoded_tickers = ",".join(str(value).strip().upper() for value in tickers if str(value).strip())
    if not encoded_tickers:
        return []
    params = {
        "select": "input_symbol,normalized_symbol,instrument_type,earnings_supported",
        column: f"in.({encoded_tickers})",
    }
    base = str(supabase_url).strip().rstrip("/")
    url = f"{base}/rest/v1/ticker_registry?{urllib.parse.urlencode(params)}"
    headers = {
        "apikey": str(service_role_key).strip(),
        "Authorization": f"Bearer {str(service_role_key).strip()}",
        "Accept": "application/json",
    }
    request = urllib.request.Request(url=url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=int(timeout_seconds)) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw) if raw else []
    if not isinstance(parsed, list):
        return []
    return [row for row in parsed if isinstance(row, dict)]


def _index_registry_rows(rows: list[dict[str, object]], tickers: list[str]) -> dict[str, dict[str, object]]:
    normalized_targets = {str(value).strip().upper() for value in tickers if str(value).strip()}
    indexed: dict[str, dict[str, object]] = {}
    for row in rows:
        input_symbol = str(row.get("input_symbol") or "").strip().upper()
        normalized_symbol = str(row.get("normalized_symbol") or "").strip().upper()
        for key in (normalized_symbol, input_symbol):
            if key and key in normalized_targets and key not in indexed:
                indexed[key] = dict(row)
    return indexed
