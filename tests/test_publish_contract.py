from __future__ import annotations

from datetime import UTC, date, datetime

import numpy as np
import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher, to_json_safe
from finance_data_ops.publish.prices import publish_prices_surfaces
from finance_data_ops.publish.product_metrics import publish_product_metrics
from finance_data_ops.publish.status import publish_status_surfaces


def test_publish_contract_writes_expected_tables() -> None:
    publisher = RecordingPublisher()
    prices = pd.DataFrame(
        [
            {
                "symbol": "SPY",
                "date": "2026-04-10",
                "open": 500.0,
                "high": 510.0,
                "low": 495.0,
                "close": 505.0,
                "adj_close": 505.0,
                "volume": 1_000_000,
                "provider": "yahoo_finance",
                "ingested_at": "2026-04-10T21:00:00+00:00",
            }
        ]
    )
    quotes = pd.DataFrame(
        [
            {
                "symbol": "SPY",
                "quote_ts": "2026-04-10T21:00:00+00:00",
                "price": 505.0,
                "previous_close": 500.0,
                "open": 500.0,
                "high": 510.0,
                "low": 495.0,
                "volume": 1_000_000,
                "provider": "yahoo_finance",
                "ingested_at": "2026-04-10T21:00:00+00:00",
            }
        ]
    )
    metrics = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "as_of_date": "2026-04-10",
                "last_price": 505.0,
                "return_1d_pct": 0.01,
                "return_1m_pct": 0.03,
                "return_3m_pct": 0.08,
                "return_1y_pct": 0.15,
                "vol_30d_pct": 0.18,
                "drawdown_1y_pct": -0.09,
                "dist_from_52w_high_pct": -0.02,
                "dist_from_52w_low_pct": 0.40,
                "updated_at": "2026-04-10T21:00:00+00:00",
            }
        ]
    )

    publish_prices_surfaces(
        publisher=publisher,
        market_price_daily=prices,
        market_quotes=quotes,
        refresh_materialized_view=True,
    )
    publish_product_metrics(publisher=publisher, market_stats_snapshot=metrics)
    publish_status_surfaces(
        publisher=publisher,
        data_source_runs=[
            {
                "run_id": "run_market_daily_abc123",
                "job_name": "market_price_daily",
                "source_type": "refresh",
                "scope": "symbol_universe",
                "status": "fresh",
                "failure_classification": None,
            }
        ],
        data_asset_status=[{"asset_key": "market_price_daily", "freshness_status": "fresh"}],
        symbol_data_coverage=[
            {
                "ticker": "SPY",
                "market_data_available": True,
                "fundamentals_available": False,
                "earnings_available": False,
                "signal_available": False,
                "market_data_last_date": "2026-04-10",
                "coverage_status": "fresh",
                "reason": "market_price_and_quote_available",
            }
        ],
    )

    tables = [call["table"] for call in publisher.upserts]
    assert tables == [
        "market_price_daily",
        "market_quotes",
        "market_quotes_history",
        "ticker_market_stats_snapshot",
        "data_source_runs",
        "data_asset_status",
        "symbol_data_coverage",
    ]
    conflict_by_table = {call["table"]: call["on_conflict"] for call in publisher.upserts}
    assert conflict_by_table["market_price_daily"] == "ticker,date"
    assert conflict_by_table["data_source_runs"] == "run_id"
    assert conflict_by_table["data_asset_status"] == "asset_key"
    assert conflict_by_table["symbol_data_coverage"] == "ticker"
    assert conflict_by_table["ticker_market_stats_snapshot"] == "ticker"
    prices_call = next(call for call in publisher.upserts if call["table"] == "market_price_daily")
    price_row = prices_call["rows"][0]
    assert set(price_row.keys()) == {"ticker", "date", "close", "source", "fetched_at", "created_at"}
    assert price_row["ticker"] == "SPY"
    assert price_row["source"] == "yahoo_finance"
    metrics_call = next(call for call in publisher.upserts if call["table"] == "ticker_market_stats_snapshot")
    metric_row = metrics_call["rows"][0]
    assert metric_row["ticker"] == "SPY"
    assert "last_price" in metric_row
    assert "return_1d_pct" in metric_row
    assert "vol_30d_pct" in metric_row
    assert publisher.rpcs and publisher.rpcs[0]["name"] == "refresh_mv_latest_prices"


def test_publish_rows_are_json_safe_before_upsert() -> None:
    publisher = RecordingPublisher()
    prices = pd.DataFrame(
        [
            {
                "symbol": "SPY",
                "date": pd.Timestamp("2026-04-10"),
                "open": np.float64(500.0),
                "high": np.float64(510.0),
                "low": np.float64(495.0),
                "close": np.float64(505.0),
                "adj_close": np.float64(505.0),
                "volume": np.int64(1_000_000),
                "provider": "yahoo_finance",
                "ingested_at": pd.Timestamp("2026-04-10T21:00:00+00:00"),
            }
        ]
    )

    publish_prices_surfaces(
        publisher=publisher,
        market_price_daily=prices,
        market_quotes=pd.DataFrame(),
        refresh_materialized_view=False,
    )

    price_call = next(call for call in publisher.upserts if call["table"] == "market_price_daily")
    row = price_call["rows"][0]
    assert set(row.keys()) == {"ticker", "date", "close", "source", "fetched_at", "created_at"}
    assert row["ticker"] == "SPY"
    assert isinstance(row["date"], str)
    assert isinstance(row["fetched_at"], str)
    assert isinstance(row["created_at"], str)
    assert isinstance(row["close"], float)


def test_to_json_safe_converts_supported_scalars() -> None:
    payload = {
        "timestamp": pd.Timestamp("2026-04-10T21:00:00+00:00"),
        "dt": datetime(2026, 4, 10, 21, 0, tzinfo=UTC),
        "d": date(2026, 4, 10),
        "int_value": np.int64(5),
        "float_value": np.float64(1.5),
        "bool_value": np.bool_(True),
        "nan_value": np.nan,
        "nat_value": pd.NaT,
    }
    out = to_json_safe(payload)
    assert out["timestamp"] == "2026-04-10T21:00:00+00:00"
    assert out["dt"] == "2026-04-10T21:00:00+00:00"
    assert out["d"] == "2026-04-10"
    assert out["int_value"] == 5
    assert out["float_value"] == 1.5
    assert out["bool_value"] is True
    assert out["nan_value"] is None
    assert out["nat_value"] is None
