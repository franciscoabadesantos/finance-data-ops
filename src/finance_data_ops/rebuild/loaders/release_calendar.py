"""Chunked release-calendar rebuild loader."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from finance_data_ops.providers.release_calendar import EconomicReleaseCalendarProvider
from finance_data_ops.publish.client import SupabaseRestPublisher
from finance_data_ops.publish.release_calendar import publish_release_calendar_surfaces
from finance_data_ops.refresh.release_calendar_daily import refresh_release_calendar_daily

logger = logging.getLogger(__name__)


def load_release_calendar_chunk(
    *,
    publisher: SupabaseRestPublisher,
    provider: EconomicReleaseCalendarProvider,
    cache_root: str,
    start_date: str,
    end_date: str,
    series_keys: tuple[str, ...],
    max_attempts: int = 3,
    force_recompute: bool = True,
    sleep_seconds: float = 0.0,
) -> dict[str, Any]:
    release_calendar_frame, refresh_run = refresh_release_calendar_daily(
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        cache_root=cache_root,
        max_attempts=max_attempts,
        sleep_seconds=float(sleep_seconds),
        force_recompute=bool(force_recompute),
    )
    frame = release_calendar_frame.copy()
    provider_rows = int(len(frame.index))
    if not frame.empty and series_keys:
        frame = frame.loc[frame["series_key"].astype(str).isin(set(series_keys))].copy()
    filtered_rows = int(len(frame.index))
    logger.info(
        "Release-calendar rebuild chunk prepared (series_keys=%s start=%s end=%s provider_rows=%s filtered_rows=%s refresh_status=%s succeeded=%s failed=%s).",
        list(series_keys),
        start_date,
        end_date,
        provider_rows,
        filtered_rows,
        refresh_run.status,
        refresh_run.symbols_succeeded,
        refresh_run.symbols_failed,
    )
    if frame.empty:
        publish_result = {"status": "skipped", "reason": "empty_chunk"}
    else:
        publish_result = publish_release_calendar_surfaces(
            publisher=publisher,
            economic_release_calendar=frame,
            refresh_materialized_view=False,
        )
    return {
        "refresh_run": refresh_run.as_dict(),
        "publish_result": publish_result,
        "rows_written": int(len(frame.index)),
        "touched_series": list(series_keys),
        "touched_symbols": [],
        "current_window": {"start_date": start_date, "end_date": end_date},
    }
