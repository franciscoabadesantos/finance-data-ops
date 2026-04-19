"""Daily economic release-calendar refresh job."""

from __future__ import annotations

from datetime import UTC, date, datetime
import logging
from uuid import uuid4

import pandas as pd

from finance_data_ops.ops.incidents import classify_failure, run_with_retry
from finance_data_ops.providers.release_calendar import EconomicReleaseCalendarProvider
from finance_data_ops.refresh.market_daily import RefreshRunResult
from finance_data_ops.refresh.storage import write_parquet_table

logger = logging.getLogger(__name__)


def refresh_release_calendar_daily(
    *,
    start_date: str,
    end_date: str,
    provider: EconomicReleaseCalendarProvider,
    cache_root: str,
    max_attempts: int = 3,
    sleep_seconds: float = 0.0,
    official_start_year: int | None = None,
    official_end_year: int | None = None,
    force_recompute: bool = False,
) -> tuple[pd.DataFrame, RefreshRunResult]:
    started_at = datetime.now(UTC)
    run_id = f"run_release_calendar_daily_{uuid4().hex[:12]}"

    start = date.fromisoformat(str(start_date))
    end = date.fromisoformat(str(end_date))
    if end < start:
        raise ValueError("release-calendar refresh end_date must be on/after start_date")

    def _build() -> pd.DataFrame:
        return provider.build_release_calendar(
            start_date=start,
            end_date=end,
            sleep_seconds=float(sleep_seconds),
            official_start_year=(official_start_year or start.year),
            official_end_year=(official_end_year or end.year),
        )

    frame, error, _, exhausted_retry_path = run_with_retry(
        _build,
        max_attempts=max_attempts,
        sleep_seconds=0.0,
    )

    symbols_requested = ["CPI_Headline", "CPI_Core", "UNRATE", "U6RATE", "CIVPART", "ICSA"]

    if error is not None or frame is None:
        classification = classify_failure(error or RuntimeError("unknown release-calendar refresh failure"))
        status = "failed_retrying" if exhausted_retry_path else classification.code
        refresh_result = RefreshRunResult(
            run_id=run_id,
            asset_name="economic_release_calendar",
            status=status,
            started_at=started_at.isoformat(),
            ended_at=datetime.now(UTC).isoformat(),
            symbols_requested=symbols_requested,
            symbols_succeeded=[],
            symbols_failed=symbols_requested,
            retry_exhausted_symbols=symbols_requested if exhausted_retry_path else [],
            rows_written=0,
            error_messages=[classification.message],
        )
        return pd.DataFrame(), refresh_result

    release_calendar = frame.copy()
    if release_calendar.empty:
        status = "failed_hard"
        symbols_succeeded: list[str] = []
        symbols_failed = symbols_requested
        error_messages = ["provider returned zero release-calendar rows"]
    else:
        release_calendar["series_key"] = release_calendar["series_key"].astype(str)
        symbols_succeeded = sorted(set(release_calendar["series_key"].tolist()))
        symbols_failed = sorted(set(symbols_requested).difference(set(symbols_succeeded)))
        late_rows = pd.Series(dtype=bool)
        if "availability_status" in release_calendar.columns:
            late_rows = release_calendar["availability_status"].astype(str).str.strip() == "late_missing_observation"
        late_count = int(late_rows.sum()) if not late_rows.empty else 0
        status = "partial" if (symbols_failed or late_count > 0) else "fresh"
        error_messages = [f"missing_series={','.join(symbols_failed)}"] if symbols_failed else []
        if late_count > 0:
            error_messages.append(f"late_missing_observation_rows={late_count}")

    logger.info(
        "Release-calendar refresh finished (run_id=%s start=%s end=%s status=%s provider_rows=%s succeeded=%s failed=%s errors=%s).",
        run_id,
        start.isoformat(),
        end.isoformat(),
        status,
        int(len(release_calendar.index)),
        symbols_succeeded,
        symbols_failed,
        error_messages,
    )

    write_parquet_table(
        "economic_release_calendar",
        release_calendar,
        cache_root=cache_root,
        mode="replace" if force_recompute else "append",
        dedupe_subset=["series_key", "observation_period"],
    )

    refresh_result = RefreshRunResult(
        run_id=run_id,
        asset_name="economic_release_calendar",
        status=status,
        started_at=started_at.isoformat(),
        ended_at=datetime.now(UTC).isoformat(),
        symbols_requested=symbols_requested,
        symbols_succeeded=symbols_succeeded,
        symbols_failed=symbols_failed,
        retry_exhausted_symbols=symbols_failed if status == "failed_retrying" else [],
        rows_written=int(len(release_calendar.index)),
        error_messages=error_messages,
    )
    return release_calendar, refresh_result
