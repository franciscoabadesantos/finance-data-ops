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
                "symbol": "SPY",
                "as_of_date": "2026-04-10",
                "latest_price": 505.0,
                "return_1d": 0.01,
                "return_1m": 0.03,
                "return_3m": 0.08,
                "return_1y": 0.15,
                "volatility_30d": 0.18,
                "drawdown_1y": -0.09,
                "distance_from_52w_high": -0.02,
                "distance_from_52w_low": 0.40,
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
                "run_id": "run_1",
                "asset_name": "market_price_daily",
                "status": "fresh",
                "failure_classification": None,
            }
        ],
        data_asset_status=[{"asset_name": "market_price_daily", "as_of_date": "2026-04-10", "freshness_status": "fresh"}],
        symbol_data_coverage=[
            {
                "symbol": "SPY",
                "as_of_date": "2026-04-10",
                "coverage_status": "fresh",
                "latest_market_date": "2026-04-10T00:00:00+00:00",
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
    assert publisher.rpcs and publisher.rpcs[0]["name"] == "refresh_mv_latest_prices"
