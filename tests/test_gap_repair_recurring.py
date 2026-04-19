from __future__ import annotations

from datetime import UTC, datetime
import logging

import pandas as pd

from finance_data_ops.refresh.gap_repair import resolve_gap_aware_window, resolve_watermark_execution
from finance_data_ops.refresh.storage import write_parquet_table
from flows import prefect_dataops_daily


def _write_market_cache(cache_root, dates: list[str]) -> None:
    rows = []
    for value in dates:
        rows.append(
            {
                "symbol": "SPY",
                "date": value,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "adj_close": 100.5,
                "volume": 1_000_000,
                "provider": "fake",
                "ingested_at": datetime(2026, 4, 18, 6, 0, tzinfo=UTC),
            }
        )
    write_parquet_table(
        "market_price_daily",
        pd.DataFrame(rows),
        cache_root=cache_root,
        mode="replace",
        dedupe_subset=["symbol", "date"],
    )


def test_gap_aware_window_detects_gap_repair_and_earliest_missing(tmp_path) -> None:
    _write_market_cache(tmp_path, ["2026-04-14", "2026-04-15", "2026-04-17"])

    plan = resolve_gap_aware_window(
        domain="market",
        table_name="market_price_daily",
        date_column="date",
        cadence="business",
        lookback_days=3,
        explicit_start=None,
        explicit_end="2026-04-17",
        safety_overlap_days=0,
        cache_root=tmp_path,
        supabase_url="",
        service_role_key="",
    )

    assert plan.mode == "gap-repair"
    assert plan.gap_exists is True
    assert plan.earliest_missing_date == "2026-04-16"
    assert plan.start_date == "2026-04-16"
    assert plan.end_date == "2026-04-17"


def test_gap_aware_window_detects_catch_up_for_trailing_gap(tmp_path) -> None:
    _write_market_cache(tmp_path, ["2026-04-14", "2026-04-15"])

    plan = resolve_gap_aware_window(
        domain="market",
        table_name="market_price_daily",
        date_column="date",
        cadence="business",
        lookback_days=1,
        explicit_start=None,
        explicit_end="2026-04-17",
        safety_overlap_days=0,
        cache_root=tmp_path,
        supabase_url="",
        service_role_key="",
    )

    assert plan.mode == "catch-up"
    assert plan.gap_exists is True
    assert plan.earliest_missing_date == "2026-04-16"
    assert plan.start_date == "2026-04-16"
    assert plan.end_date == "2026-04-17"


def test_release_watermark_execution_returns_catch_up_when_stale(tmp_path) -> None:
    write_parquet_table(
        "economic_release_calendar",
        pd.DataFrame(
            [
                {
                    "series_key": "UNRATE",
                    "observation_period": "2026-03",
                    "release_date_local": "2026-04-03",
                }
            ]
        ),
        cache_root=tmp_path,
        mode="replace",
        dedupe_subset=["series_key", "observation_period"],
    )

    plan = resolve_watermark_execution(
        domain="release-calendar",
        table_name="economic_release_calendar",
        date_column="release_date_local",
        lookback_days=40,
        grace_days=2,
        safety_overlap_days=1,
        explicit_end="2026-04-17",
        cache_root=tmp_path,
        supabase_url="",
        service_role_key="",
    )

    assert plan.mode == "catch-up"
    assert plan.gap_exists is True
    assert plan.latest_complete_canonical_date == "2026-04-03"
    assert plan.end_date == "2026-04-17"


def test_market_prefect_flow_auto_repairs_missing_window(monkeypatch, tmp_path) -> None:
    _write_market_cache(tmp_path, ["2026-04-14", "2026-04-15", "2026-04-17"])

    captured: dict[str, object] = {}

    def _fake_run_dataops_market_daily(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(prefect_dataops_daily, "run_dataops_market_daily", _fake_run_dataops_market_daily)
    monkeypatch.setattr(prefect_dataops_daily, "get_run_logger", lambda: logging.getLogger("test-gap-repair"))

    result = prefect_dataops_daily.dataops_market_daily_flow.fn(
        symbols=["SPY"],
        start=None,
        end="2026-04-17",
        lookback_days=3,
        cache_root=str(tmp_path),
        max_attempts=1,
        publish_enabled=False,
        allow_unhealthy=True,
    )

    execution = result["execution"]
    assert execution["mode"] == "gap-repair"
    assert execution["earliest_missing_date"] == "2026-04-16"
    assert captured["start"] == "2026-04-14"  # includes safety overlap, so missed date is covered.
    assert captured["end"] == "2026-04-17"
