from __future__ import annotations

from datetime import UTC, datetime

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

    assert table_path("market_fundamentals_v2", cache_root=tmp_path).exists()
    assert table_path("ticker_fundamental_summary", cache_root=tmp_path).exists()
    assert table_path("ticker_profile", cache_root=tmp_path).exists()
    assert table_path("etf_holdings", cache_root=tmp_path).exists()
    assert table_path("etf_sector_weights", cache_root=tmp_path).exists()

    assert summary["refresh"]["fundamentals_daily"]["status"] == "fresh"
    assert summary["coverage"]["status"] == "fresh"
    assert summary["publish_failures"] == []
    fundamentals_upsert = next(call for call in publisher.upserts if call["table"] == "market_fundamentals_v2")
    assert fundamentals_upsert["on_conflict"] == "ticker,period,period_end,metric"
    fundamentals_row = fundamentals_upsert["rows"][0]
    assert set(fundamentals_row.keys()) == {
        "ticker",
        "period",
        "period_end",
        "metric",
        "value",
        "value_text",
        "source",
        "fetched_at",
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
    assert {"market_fundamentals_v2", "ticker_fundamental_summary", "mv_latest_fundamentals"}.issubset(
        asset_keys
    )

    runs_upsert = next(call for call in publisher.upserts if call["table"] == "data_source_runs")
    orchestration_row = next(row for row in runs_upsert["rows"] if row["job_name"] == "dataops_fundamentals_daily")
    assert orchestration_row["status"] == "success"
    assert orchestration_row["symbols_succeeded"] == 2


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
