from __future__ import annotations

import sys
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from finance_data_ops.refresh import gap_repair
from finance_data_ops.refresh.gap_repair import (
    CanonicalWatermarkLookupError,
    resolve_watermark_execution,
)
from finance_data_ops.refresh.storage import write_parquet_table
from finance_data_ops.settings import DataOpsSettings
from flows import prefect_dataops_daily


class _FakeCursor:
    def __init__(self, rows: list[tuple[object]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.executed.append((query, params))

    def fetchall(self) -> list[tuple[object]]:
        return self.rows


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_instance = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance


def _install_fake_psycopg(monkeypatch, *, rows: list[tuple[object]]) -> _FakeCursor:
    cursor = _FakeCursor(rows)
    fake_psycopg = SimpleNamespace(connect=lambda *_args, **_kwargs: _FakeConnection(cursor))
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    return cursor


def _settings(tmp_path) -> DataOpsSettings:
    return DataOpsSettings(
        repo_root=tmp_path,
        cache_root=tmp_path,
        database_dsn="postgresql://example.local/db",
        default_symbols=[],
        default_lookback_days=14,
        default_max_attempts=1,
        symbol_batch_size=100,
        alert_webhook_url="",
    )


def test_table_level_postgres_watermark_is_used_as_canonical_source(monkeypatch, tmp_path) -> None:
    cursor = _install_fake_psycopg(monkeypatch, rows=[(date(2026, 7, 17),)])

    plan = resolve_watermark_execution(
        domain="market",
        table_name="source_cache.market_price_daily",
        date_column="price_date",
        lookback_days=14,
        grace_days=0,
        safety_overlap_days=0,
        explicit_end="2026-07-17",
        cache_root=tmp_path,
        database_dsn="postgresql://example.local/db",
    )

    assert plan.canonical_source == "postgres"
    assert plan.latest_complete_canonical_date == "2026-07-17"
    assert plan.gap_exists is False
    query, params = cursor.executed[0]
    assert 'select distinct "price_date"' in query
    assert '"source_cache"."market_price_daily"' in query
    assert params == (date(2026, 7, 3), date(2026, 7, 17))


def test_feature_build_gate_is_ready_with_only_postgres_watermarks(monkeypatch, tmp_path) -> None:
    _install_fake_psycopg(monkeypatch, rows=[(date(2026, 7, 17),)])

    gate = prefect_dataops_daily.resolve_feature_build_watermark_gate(
        as_of_date="2026-07-17",
        settings=_settings(tmp_path),
    )

    assert gate["ready"] is True
    assert gate["status"] == "ready"
    assert {item["plan"]["canonical_source"] for item in gate["requirements"]} == {"postgres"}


def test_configured_postgres_failure_does_not_fall_back_to_parquet(monkeypatch, tmp_path) -> None:
    fake_psycopg = SimpleNamespace(
        connect=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("database unavailable"))
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setattr(
        gap_repair,
        "_fetch_dates_from_parquet",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("parquet fallback must not be used")),
    )

    with pytest.raises(CanonicalWatermarkLookupError, match="parquet fallback is disabled"):
        prefect_dataops_daily.resolve_feature_build_watermark_gate(
            as_of_date="2026-07-17",
            settings=_settings(tmp_path),
        )


@pytest.mark.parametrize(
    ("table_name", "parquet_column", "requested_column"),
    [
        ("source_cache.market_price_daily", "date", "price_date"),
        ("source_cache.fundamentals", "period_end", "report_date"),
    ],
)
def test_parquet_fallback_does_not_mask_postgres_schema_mismatches(
    tmp_path,
    table_name: str,
    parquet_column: str,
    requested_column: str,
) -> None:
    write_parquet_table(
        table_name,
        pd.DataFrame({"symbol": ["SPY"], parquet_column: ["2026-07-17"]}),
        cache_root=tmp_path,
    )

    plan = resolve_watermark_execution(
        domain="test",
        table_name=table_name,
        date_column=requested_column,
        lookback_days=14,
        grace_days=0,
        safety_overlap_days=0,
        explicit_end="2026-07-17",
        cache_root=tmp_path,
        database_dsn="",
    )

    assert plan.canonical_source == "parquet"
    assert plan.latest_complete_canonical_date is None
    assert plan.gap_exists is True
