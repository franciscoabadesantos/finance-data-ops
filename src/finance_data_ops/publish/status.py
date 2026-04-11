"""Publish operational status/freshness/coverage surfaces."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any

from finance_data_ops.publish.client import Publisher


def publish_status_surfaces(
    *,
    publisher: Publisher,
    data_source_runs: list[dict[str, Any]],
    data_asset_status: list[dict[str, Any]],
    symbol_data_coverage: list[dict[str, Any]],
) -> dict[str, Any]:
    runs_result = publisher.upsert(
        "data_source_runs",
        data_source_runs,
        on_conflict="run_id",
    )
    asset_result = publisher.upsert(
        "data_asset_status",
        data_asset_status,
        on_conflict="asset_key",
    )
    coverage_result = publisher.upsert(
        "symbol_data_coverage",
        symbol_data_coverage,
        on_conflict="ticker",
    )
    return {
        "data_source_runs": runs_result,
        "data_asset_status": asset_result,
        "symbol_data_coverage": coverage_result,
    }


def fetch_symbol_data_coverage_rows(
    *,
    supabase_url: str,
    service_role_key: str,
    tickers: list[str],
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    normalized = sorted({str(value).strip().upper() for value in tickers if str(value).strip()})
    if not normalized:
        return []

    encoded_tickers = ",".join(urllib.parse.quote(f'"{ticker}"', safe="") for ticker in normalized)
    query = (
        "select=ticker,market_data_available,market_data_last_date,"
        "earnings_available,next_earnings_date,signal_available"
        f"&ticker=in.({encoded_tickers})"
    )
    base = str(supabase_url).strip().rstrip("/")
    url = f"{base}/rest/v1/symbol_data_coverage?{query}"
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
