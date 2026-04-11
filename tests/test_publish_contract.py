from __future__ import annotations

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
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
    assert conflict_by_table["data_source_runs"] is None
    assert conflict_by_table["data_asset_status"] == "asset_key"
    assert conflict_by_table["symbol_data_coverage"] == "ticker"
    assert conflict_by_table["ticker_market_stats_snapshot"] == "ticker"
    metrics_call = next(call for call in publisher.upserts if call["table"] == "ticker_market_stats_snapshot")
    metric_row = metrics_call["rows"][0]
    assert metric_row["ticker"] == "SPY"
    assert "last_price" in metric_row
    assert "return_1d_pct" in metric_row
    assert "vol_30d_pct" in metric_row
    assert publisher.rpcs and publisher.rpcs[0]["name"] == "refresh_mv_latest_prices"
