from __future__ import annotations

from datetime import UTC, date, datetime

import numpy as np
import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher, _build_upsert_sql, to_json_safe
from finance_data_ops.publish.prices import publish_prices_surfaces
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
                "name": "SPDR S&P 500 ETF Trust",
                "sector": None,
                "industry": None,
                "open": 500.0,
                "high": 510.0,
                "low": 495.0,
                "volume": 1_000_000,
                "provider": "yahoo_finance",
                "ingested_at": "2026-04-10T21:00:00+00:00",
            }
        ]
    )
    publish_prices_surfaces(
        publisher=publisher,
        market_price_daily=prices,
    )
    publish_status_surfaces(
        publisher=publisher,
        data_source_runs=[
            {
                "run_id": "run_market_daily_abc123",
                "job_name": "source_cache.market_price_daily",
                "source_type": "refresh",
                "scope": "symbol_universe",
                "status": "fresh",
                "failure_classification": None,
            }
        ],
        data_asset_status=[{"asset_key": "source_cache.market_price_daily", "freshness_status": "fresh"}],
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
        "source_cache.market_price_daily",
        "data_source_runs",
        "data_asset_status",
        "symbol_data_coverage",
    ]
    conflict_by_table = {call["table"]: call["on_conflict"] for call in publisher.upserts}
    assert conflict_by_table["source_cache.market_price_daily"] == "symbol,price_date"
    assert conflict_by_table["data_source_runs"] == "run_id"
    assert conflict_by_table["data_asset_status"] == "asset_key"
    assert conflict_by_table["symbol_data_coverage"] == "ticker"
    prices_call = next(call for call in publisher.upserts if call["table"] == "source_cache.market_price_daily")
    price_row = prices_call["rows"][0]
    assert set(price_row.keys()) == {
        "symbol",
        "price_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source_updated_at",
        "ingested_at",
    }
    assert price_row["symbol"] == "SPY"
    assert price_row["open"] == 500.0
    assert price_row["high"] == 510.0
    assert price_row["low"] == 495.0
    assert price_row["adj_close"] == 505.0
    assert price_row["volume"] == 1_000_000
    assert publisher.rpcs == []


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
    )

    price_call = next(call for call in publisher.upserts if call["table"] == "source_cache.market_price_daily")
    row = price_call["rows"][0]
    assert set(row.keys()) == {
        "symbol",
        "price_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source_updated_at",
        "ingested_at",
    }
    assert row["symbol"] == "SPY"
    assert isinstance(row["price_date"], str)
    assert isinstance(row["source_updated_at"], str)
    assert isinstance(row["ingested_at"], str)
    assert isinstance(row["open"], float)
    assert isinstance(row["high"], float)
    assert isinstance(row["low"], float)
    assert isinstance(row["close"], float)
    assert isinstance(row["adj_close"], float)
    assert isinstance(row["volume"], (int, float))


def test_entity_attributes_name_upsert_preserves_existing_name() -> None:
    query = _build_upsert_sql(
        schema_name="feature_store",
        table_name="entity_attributes_static",
        columns=["entity_id", "name", "country"],
        conflict_columns=["entity_id"],
        update_columns=["name", "country"],
    )

    assert '"name" = coalesce("entity_attributes_static"."name", excluded."name")' in query
    assert '"country" = excluded."country"' in query


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
