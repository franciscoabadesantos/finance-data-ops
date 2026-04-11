"""Publish operational status/freshness/coverage surfaces."""

from __future__ import annotations

from typing import Any

from finance_data_ops.publish.client import Publisher


def publish_status_surfaces(
    *,
    publisher: Publisher,
    data_source_runs: list[dict[str, Any]],
    data_asset_status: list[dict[str, Any]],
    symbol_data_coverage: list[dict[str, Any]],
) -> dict[str, Any]:
    runs_result = publisher.upsert("data_source_runs", data_source_runs, on_conflict="run_id")
    asset_result = publisher.upsert(
        "data_asset_status",
        data_asset_status,
        on_conflict="asset_name,as_of_date",
    )
    coverage_result = publisher.upsert(
        "symbol_data_coverage",
        symbol_data_coverage,
        on_conflict="symbol,as_of_date",
    )
    return {
        "data_source_runs": runs_result,
        "data_asset_status": asset_result,
        "symbol_data_coverage": coverage_result,
    }
