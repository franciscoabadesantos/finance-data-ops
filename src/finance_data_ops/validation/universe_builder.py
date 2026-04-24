"""Registry-driven symbol universe builders with safe env fallbacks."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.settings import load_settings

DEFAULT_REGIONS = ("us", "eu", "apac")
ALLOWED_PROMOTION_STATUSES = {"validated_market_only", "validated_full"}


def load_validated_symbols(
    region: str,
    *,
    require_market: bool = True,
    cache_root: str | Path | None = None,
    supabase_url: str | None = None,
    service_role_key: str | None = None,
) -> list[str]:
    normalized_region = str(region or "").strip().lower()
    if not normalized_region:
        return []

    settings = load_settings(cache_root=cache_root)
    resolved_supabase_url = str(supabase_url or settings.supabase_url).strip()
    resolved_service_key = str(service_role_key or settings.supabase_secret_key).strip()

    rows: list[dict[str, Any]] = []
    if resolved_supabase_url and resolved_service_key:
        try:
            rows = _fetch_registry_rows_from_supabase(
                supabase_url=resolved_supabase_url,
                service_role_key=resolved_service_key,
                region=normalized_region,
                require_market=bool(require_market),
            )
        except Exception:
            rows = []

    if not rows:
        frame = read_parquet_table("ticker_registry", cache_root=settings.cache_root, required=False)
        if not frame.empty:
            rows = _rows_from_frame(frame)

    return _select_symbols_from_rows(
        rows=rows,
        region=normalized_region,
        require_market=bool(require_market),
    )


def load_all_region_universes(
    *,
    require_market: bool = True,
    cache_root: str | Path | None = None,
    supabase_url: str | None = None,
    service_role_key: str | None = None,
) -> dict[str, list[str]]:
    settings = load_settings(cache_root=cache_root)
    resolved_supabase_url = str(supabase_url or settings.supabase_url).strip()
    resolved_service_key = str(service_role_key or settings.supabase_secret_key).strip()

    rows: list[dict[str, Any]] = []
    if resolved_supabase_url and resolved_service_key:
        try:
            rows = _fetch_registry_rows_from_supabase(
                supabase_url=resolved_supabase_url,
                service_role_key=resolved_service_key,
                region=None,
                require_market=bool(require_market),
            )
        except Exception:
            rows = []

    if not rows:
        frame = read_parquet_table("ticker_registry", cache_root=settings.cache_root, required=False)
        if not frame.empty:
            rows = _rows_from_frame(frame)

    universes: dict[str, list[str]] = {}
    regions = {str(value).strip().lower() for value in DEFAULT_REGIONS}
    for row in rows:
        region = str(row.get("region") or "").strip().lower()
        if region:
            regions.add(region)

    for region in sorted(regions):
        universes[region] = _select_symbols_from_rows(rows=rows, region=region, require_market=bool(require_market))
    return universes


def _fetch_registry_rows_from_supabase(
    *,
    supabase_url: str,
    service_role_key: str,
    region: str | None,
    require_market: bool,
    timeout_seconds: int = 30,
) -> list[dict[str, Any]]:
    params: dict[str, str] = {
        "select": "input_symbol,normalized_symbol,region,status,promotion_status,market_supported",
        "status": "eq.active",
        "promotion_status": "in.(validated_market_only,validated_full)",
        "normalized_symbol": "not.is.null",
    }
    if region:
        params["region"] = f"eq.{region}"
    if require_market:
        params["market_supported"] = "eq.true"

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


def _rows_from_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    safe = frame.copy()
    return safe.to_dict(orient="records")


def _select_symbols_from_rows(
    *,
    rows: list[dict[str, Any]],
    region: str,
    require_market: bool,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        row_region = str(row.get("region") or "").strip().lower()
        if row_region != region:
            continue
        status = str(row.get("status") or "").strip().lower()
        if status != "active":
            continue
        promotion = str(row.get("promotion_status") or "").strip().lower()
        if promotion not in ALLOWED_PROMOTION_STATUSES:
            continue
        symbol = str(row.get("normalized_symbol") or "").strip().upper()
        if not symbol or symbol in {"NAN", "NONE", "NULL"}:
            continue
        if require_market and not _coerce_bool(row.get("market_supported")):
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    token = str(value).strip().lower()
    return token in {"true", "1", "yes", "y"}
