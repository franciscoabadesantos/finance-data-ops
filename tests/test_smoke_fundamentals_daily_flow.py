from __future__ import annotations

from datetime import UTC, datetime
import logging

import pandas as pd
from freezegun import freeze_time

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.refresh.storage import table_path
from flows.dataops_fundamentals_daily import run_dataops_fundamentals_daily


class FakeFundamentalsProvider:
    def fetch_symbol_fundamentals(self, symbol: str) -> pd.DataFrame:
        ticker = str(symbol).strip().upper()
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "metric": "revenue",
                    "value": 550.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "net_income",
                    "value": 110.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "eps",
                    "value": 10.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
                {
                    "ticker": ticker,
                    "metric": "market_cap",
                    "value": 2200.0,
                    "period_end": "2026-04-11",
                    "period_type": "point_in_time",
                    "fiscal_year": 2026,
                    "fiscal_quarter": "Q2",
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                },
            ]
        )

    def fetch_symbol_profile(self, symbol: str) -> pd.DataFrame:
        ticker = str(symbol).strip().upper()
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "description": f"{ticker} profile",
                    "long_business_summary": f"{ticker} profile",
                    "etf_category": "Large Blend",
                    "fund_family": "Example Funds",
                    "expense_ratio": 0.0009,
                    "inception_date": "1993-02-01",
                    "legal_type": "ETF",
                    "beta": 1.0,
                    "beta_3y": 0.95,
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    "updated_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                }
            ]
        )

    def fetch_symbol_etf_funds_data(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = str(symbol).strip().upper()
        return (
            pd.DataFrame(
                [
                    {
                        "etf_ticker": ticker,
                        "holding_symbol": "AAPL",
                        "holding_name": "Apple Inc.",
                        "weight": 0.071,
                        "as_of": "2026-04-11",
                        "source": "fake",
                        "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                        "updated_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "etf_ticker": ticker,
                        "sector": "technology",
                        "weight": 0.32,
                        "as_of": "2026-04-11",
                        "source": "fake",
                        "fetched_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                        "updated_at": datetime(2026, 4, 11, 10, 0, tzinfo=UTC),
                    }
                ]
            ),
        )


class FailingOptionalSurfacesProvider(FakeFundamentalsProvider):
    def fetch_symbol_profile(self, symbol: str) -> pd.DataFrame:
        raise RuntimeError(f"{symbol}: info failed")

    def fetch_symbol_etf_funds_data(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        raise RuntimeError(f"{symbol}: No Fund data found")


@freeze_time("2026-04-12")
def test_smoke_fundamentals_refresh_publish_status(tmp_path) -> None:
    publisher = RecordingPublisher()

    summary = run_dataops_fundamentals_daily(
        symbols=["SPY", "QQQ"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeFundamentalsProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert table_path("source_cache.fundamentals", cache_root=tmp_path).exists()
    assert not table_path("fundamentals_summary", cache_root=tmp_path).exists()
    assert table_path("ticker_profile", cache_root=tmp_path).exists()
    assert table_path("etf_holdings", cache_root=tmp_path).exists()
    assert table_path("etf_sector_weights", cache_root=tmp_path).exists()

    assert summary["refresh"]["fundamentals_daily"]["status"] == "fresh"
    assert summary["coverage"]["status"] == "fresh"
    assert summary["publish_failures"] == []
    fundamentals_upsert = next(call for call in publisher.upserts if call["table"] == "source_cache.fundamentals")
    assert fundamentals_upsert["on_conflict"] == "symbol,metric,period_end,period_type,report_date"
    assert {row["metric"] for row in fundamentals_upsert["rows"]} == {"revenue", "net_income", "eps", "market_cap"}
    fundamentals_row = fundamentals_upsert["rows"][0]
    assert set(fundamentals_row.keys()) == {
        "symbol",
        "report_date",
        "metric",
        "value",
        "value_text",
        "period_end",
        "period_type",
        "fiscal_year",
        "fiscal_quarter",
        "currency",
        "source",
        "source_updated_at",
        "ingested_at",
    }
    assert next(call for call in publisher.upserts if call["table"] == "ticker_profile")["on_conflict"] == "ticker"
    assert (
        next(call for call in publisher.upserts if call["table"] == "etf_holdings")["on_conflict"]
        == "etf_ticker,holding_symbol,as_of"
    )
    assert (
        next(call for call in publisher.upserts if call["table"] == "etf_sector_weights")["on_conflict"]
        == "etf_ticker,sector,as_of"
    )

    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    coverage_row = status_upsert["rows"][0]
    assert coverage_row["fundamentals_available"] is True
    assert coverage_row["fundamentals_last_date"] is not None

    asset_status_upsert = next(call for call in publisher.upserts if call["table"] == "data_asset_status")
    asset_keys = {row["asset_key"] for row in asset_status_upsert["rows"]}
    assert "source_cache.fundamentals" in asset_keys
    assert "fundamentals_summary" not in asset_keys

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration_row = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_fundamentals_daily")
    assert orchestration_row["status"] == "success"
    assert orchestration_row["symbols_succeeded"] == 2


def test_optional_profile_and_etf_failures_do_not_fail_fundamentals_refresh(tmp_path, caplog) -> None:
    publisher = RecordingPublisher()

    caplog.set_level(logging.WARNING, logger="finance_data_ops.refresh.fundamentals_daily")
    summary = run_dataops_fundamentals_daily(
        symbols=["KO"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FailingOptionalSurfacesProvider(),
        publisher=publisher,
        raise_on_failed_hard=True,
    )

    assert summary["refresh"]["fundamentals_daily"]["status"] == "fresh"
    assert summary["publish_failures"] == []
    assert summary["refresh"]["fundamentals_daily"]["symbols_succeeded"] == ["KO"]
    assert summary["refresh"]["fundamentals_daily"]["symbols_failed"] == []
    assert any(call["table"] == "source_cache.fundamentals" for call in publisher.upserts)
    assert not any(call["table"] == "ticker_profile" for call in publisher.upserts)
    assert not any(call["table"] == "etf_holdings" for call in publisher.upserts)
    assert not any(call["table"] == "etf_sector_weights" for call in publisher.upserts)
    assert "Optional fundamentals refresh surface failed (symbol=KO surface=profile)" in caplog.text
    assert "Optional fundamentals refresh surface failed (symbol=KO surface=etf_funds_data)" in caplog.text


def test_fundamentals_daily_refreshes_and_publishes_theme_etfs(tmp_path) -> None:
    publisher = RecordingPublisher()

    def theme_refresh() -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, object]]]:
        return (
            pd.DataFrame(
                [
                    {
                        "etf_ticker": "ARKX",
                        "holding_symbol": "RKLB",
                        "holding_name": "Rocket Lab Corp",
                        "weight": 0.12,
                        "as_of": "2026-07-02",
                        "source": "theme_etf:ark_csv",
                        "fetched_at": datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
                        "updated_at": datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "etf_ticker": "ARKX",
                        "theme": "space",
                        "wave": 1,
                        "issuer": "ARK",
                        "source_type": "ark_csv",
                        "source_ref": "ARKX",
                        "active": True,
                        "fetched_at": datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
                        "updated_at": datetime(2026, 7, 3, 10, 0, tzinfo=UTC),
                    }
                ]
            ),
            [],
        )

    summary = run_dataops_fundamentals_daily(
        symbols=["SPY"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeFundamentalsProvider(),
        publisher=publisher,
        refresh_theme_etfs=True,
        theme_etf_refresh_fn=theme_refresh,
        raise_on_failed_hard=True,
    )

    assert summary["refresh"]["theme_etf_holdings"]["status"] == "fresh"
    assert summary["rows"]["etf_themes"] == 1
    theme_upsert = next(call for call in publisher.upserts if call["table"] == "etf_themes")
    assert theme_upsert["on_conflict"] == "etf_ticker"
    assert theme_upsert["rows"][0]["theme"] == "space"
    holdings = pd.read_parquet(table_path("etf_holdings", cache_root=tmp_path))
    assert {"SPY", "ARKX"}.issubset(set(holdings["etf_ticker"]))


def test_fundamentals_coverage_merge_preserves_existing_market_flags(tmp_path) -> None:
    publisher = RecordingPublisher()
    existing_rows = [
        {
            "ticker": "SPY",
            "market_data_available": True,
            "market_data_last_date": "2026-04-10",
            "earnings_available": False,
            "next_earnings_date": None,
            "signal_available": False,
        },
        {
            "ticker": "QQQ",
            "market_data_available": True,
            "market_data_last_date": "2026-04-10",
            "earnings_available": False,
            "next_earnings_date": None,
            "signal_available": False,
        },
    ]

    run_dataops_fundamentals_daily(
        symbols=["SPY", "QQQ"],
        cache_root=str(tmp_path),
        publish_enabled=True,
        provider=FakeFundamentalsProvider(),
        publisher=publisher,
        existing_symbol_coverage_rows=existing_rows,
        raise_on_failed_hard=True,
    )

    status_upsert = next(call for call in publisher.upserts if call["table"] == "symbol_data_coverage")
    rows_by_ticker = {row["ticker"]: row for row in status_upsert["rows"]}
    assert rows_by_ticker["SPY"]["market_data_available"] is True
    assert rows_by_ticker["SPY"]["fundamentals_available"] is True
    assert rows_by_ticker["SPY"]["coverage_status"] == "partial"
    assert rows_by_ticker["SPY"]["reason"] == "missing_earnings"
    assert rows_by_ticker["QQQ"]["market_data_available"] is True
    assert rows_by_ticker["QQQ"]["fundamentals_available"] is True
