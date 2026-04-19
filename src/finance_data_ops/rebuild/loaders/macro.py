"""Chunked macro rebuild loader."""

from __future__ import annotations

from typing import Any

from finance_data_ops.providers.macro import MACRO_SERIES_CATALOG, MacroDataProvider
from finance_data_ops.publish.macro import publish_macro_surfaces
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.refresh.macro_daily import refresh_macro_daily


def load_macro_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: MacroDataProvider,
    cache_root: str,
    start_date: str,
    end_date: str,
    series_keys: tuple[str, ...],
    max_attempts: int = 3,
    force_recompute: bool = True,
    release_calendar_frame: Any = None,
) -> dict[str, Any]:
    series_catalog = tuple(spec for spec in MACRO_SERIES_CATALOG if spec.key in set(series_keys))
    catalog_frame, observations_frame, daily_frame, refresh_run = refresh_macro_daily(
        start=start_date,
        end=end_date,
        provider=provider,
        cache_root=cache_root,
        release_calendar_frame=release_calendar_frame,
        series_catalog=series_catalog,
        max_attempts=max_attempts,
        force_recompute=bool(force_recompute),
    )
    if observations_frame.empty or daily_frame.empty:
        publish_result = {"status": "skipped", "reason": "empty_chunk"}
    else:
        publish_result = publish_macro_surfaces(
            publisher=publisher,
            series_catalog=catalog_frame,
            macro_observations=observations_frame,
            macro_daily=daily_frame,
            refresh_materialized_views=False,
            required_series_keys=tuple(spec.key for spec in series_catalog if bool(spec.required_by_default)),
        )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "rows_written": int(len(catalog_frame.index) + len(observations_frame.index) + len(daily_frame.index)),
        "touched_series": list(series_keys),
        "touched_symbols": [],
        "current_window": {"start_date": start_date, "end_date": end_date},
    }
