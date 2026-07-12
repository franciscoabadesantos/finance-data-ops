"""Post-load finalization for rebuild jobs."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.diagnostics.symbol_data_coverage import read_complete_symbol_data_coverage_rows_from_postgres
from finance_data_ops.publish.client import PostgresPublisher
from finance_data_ops.publish.status import publish_status_surfaces, replace_symbol_data_coverage_rows
from finance_data_ops.rebuild.policies import DomainPolicy


def finalize_rebuild(
    *,
    publisher: PostgresPublisher,
    client: Any,
    policy: DomainPolicy,
    domain: str,
    touched_symbols: list[str],
    touched_series: list[str],
) -> dict[str, Any]:
    rpc_results: dict[str, Any] = {}
    derived_results: dict[str, Any] = {}
    for rpc_name in policy.refresh_materialized_views:
        rpc_results[rpc_name] = publisher.rpc(rpc_name, {})

    status_rows = _build_asset_status_rows(client=client, domain=domain, policy=policy)
    coverage_rows = _build_symbol_coverage_rows(client=client, symbols=touched_symbols) if policy.rebuild_coverage else []
    coverage_replace_result: dict[str, Any] | None = None
    if policy.rebuild_coverage and getattr(publisher, "database_dsn", ""):
        coverage_rows = read_complete_symbol_data_coverage_rows_from_postgres(database_dsn=publisher.database_dsn)
        coverage_replace_result = replace_symbol_data_coverage_rows(
            database_dsn=publisher.database_dsn,
            rows=coverage_rows,
        )
    status_result = publish_status_surfaces(
        publisher=publisher,
        data_source_runs=[],
        data_asset_status=status_rows,
        symbol_data_coverage=[] if coverage_replace_result else coverage_rows,
    )
    if coverage_replace_result is not None:
        status_result["symbol_data_coverage"] = coverage_replace_result
    return {
        "derived_results": derived_results,
        "rpc_results": rpc_results,
        "status_result": status_result,
        "touched_symbols": list(sorted(set(touched_symbols))),
        "touched_series": list(sorted(set(touched_series))),
    }


def _build_asset_status_rows(*, client: Any, domain: str, policy: DomainPolicy) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for asset_key in policy.rebuild_status_assets:
        last_available = _latest_date_for_asset(client=client, asset_key=asset_key)
        rows.append(
            {
                "asset_key": asset_key,
                "asset_type": domain.replace("-", "_"),
                "provider": "data_ops",
                "last_success_at": _as_timestamp_token(last_available),
                "last_available_date": last_available,
                "freshness_status": "fresh" if last_available is not None else "unknown",
                "coverage_status": "fresh" if last_available is not None else "unknown",
                "reason": "rebuild_finalized",
                "updated_at": pd.Timestamp.utcnow().isoformat(),
            }
        )
    return rows


def _build_symbol_coverage_rows(*, client: Any, symbols: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for symbol in sorted({str(value).strip().upper() for value in symbols if str(value).strip()}):
        market_rows = _count_for_ticker(
            client=client,
            table="source_cache.market_price_daily",
            column="symbol",
            value=symbol,
        )
        fundamentals_rows = _count_for_ticker(
            client=client,
            table="source_cache.fundamentals",
            column="symbol",
            value=symbol,
        )
        earnings_rows = _count_for_ticker(
            client=client,
            table="source_cache.earnings",
            column="symbol",
            value=symbol,
        )
        rows.append(
            {
                "ticker": symbol,
                "market_data_available": market_rows > 0,
                "fundamentals_available": fundamentals_rows > 0,
                "earnings_available": earnings_rows > 0,
                "signal_available": False,
                "market_data_last_date": _latest_for_ticker(
                    client=client,
                    table="source_cache.market_price_daily",
                    date_column="price_date",
                    symbol=symbol,
                ),
                "fundamentals_last_date": _latest_for_ticker(
                    client=client,
                    table="source_cache.fundamentals",
                    date_column="period_end",
                    symbol=symbol,
                ),
                "next_earnings_date": _latest_for_ticker(
                    client=client,
                    table="source_cache.earnings",
                    date_column="earnings_date",
                    symbol=symbol,
                ),
                "coverage_status": "fresh" if (market_rows > 0 or fundamentals_rows > 0 or earnings_rows > 0) else "unknown",
                "reason": "rebuild_finalized",
                "updated_at": pd.Timestamp.utcnow().isoformat(),
            }
        )
    return rows


def _latest_date_for_asset(*, client: Any, asset_key: str) -> str | None:
    table_map = {
        "source_cache.market_price_daily": ("source_cache.market_price_daily", "price_date"),
        "source_cache.fundamentals": ("source_cache.fundamentals", "period_end"),
        "source_cache.earnings": ("source_cache.earnings", "earnings_date"),
        "feature_store.technical_features_daily": ("feature_store.technical_features_daily", "as_of_date"),
        "feature_store.scorecard_daily": ("feature_store.scorecard_daily", "as_of_date"),
        "feature_store.ticker_page_summary": ("feature_store.ticker_page_summary", "as_of_date"),
        "macro_observations": ("macro_observations", "observation_date"),
        "macro_daily": ("macro_daily", "as_of_date"),
        "mv_latest_macro_observations": ("mv_latest_macro_observations", "observation_date"),
        "economic_release_calendar": ("economic_release_calendar", "scheduled_release_timestamp_utc"),
        "mv_latest_economic_release_calendar": ("mv_latest_economic_release_calendar", "scheduled_release_timestamp_utc"),
    }
    if asset_key not in table_map:
        return None
    table_name, column = table_map[asset_key]
    try:
        response = client.table(table_name).select(column).order(column, desc=True).limit(1).execute()
    except Exception:
        return None
    rows = response.data or []
    if not rows or not isinstance(rows[0], dict):
        return None
    value = rows[0].get(column)
    if value is None:
        return None
    return str(value)[:10]


def _latest_for_ticker(*, client: Any, table: str, date_column: str, symbol: str) -> str | None:
    try:
        response = client.table(table).select(date_column).eq("symbol", symbol).order(date_column, desc=True).limit(1).execute()
    except Exception:
        return None
    rows = response.data or []
    if rows and isinstance(rows[0], dict):
        value = rows[0].get(date_column)
        if value is not None:
            return str(value)[:10]
    return None


def _count_for_ticker(*, client: Any, table: str, column: str, value: str) -> int:
    try:
        response = client.table(table).select("*", count="exact").eq(column, value).limit(1).execute()
    except Exception:
        return 0
    count = getattr(response, "count", None)
    return int(count or 0)


def _as_timestamp_token(value: str | None) -> str | None:
    if not value:
        return None
    if "T" in value:
        return value
    return f"{value}T00:00:00+00:00"
