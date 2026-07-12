from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces


def test_publish_v2_contract_writes_expected_tables() -> None:
    publisher = RecordingPublisher()

    fundamentals = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "period": "2025",
                "metric": "revenue",
                "value": 550.0,
                "value_text": "550.0",
                "period_end": "2025-12-31",
                "fiscal_year": 2025,
                "fiscal_quarter": None,
                "currency": "USD",
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )
    earnings_events = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "earnings_date": "2026-05-01",
                "earnings_time": "amc",
                "fiscal_period": "2026Q1",
                "estimate_eps": 5.1,
                "estimate_revenue": 100.0,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )
    earnings_history = pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "earnings_date": "2026-02-01",
                "fiscal_period": "2025Q4",
                "actual_eps": 5.0,
                "estimate_eps": 4.8,
                "surprise_eps": 0.2,
                "actual_revenue": 96.5,
                "estimate_revenue": 95.0,
                "surprise_revenue": 1.5,
                "source": "fake",
                "fetched_at": datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
            }
        ]
    )

    publish_fundamentals_surfaces(
        publisher=publisher,
        fundamentals_history=fundamentals,
    )
    publish_earnings_surfaces(
        publisher=publisher,
        earnings_events=earnings_events,
        earnings_history=earnings_history,
    )

    tables = [call["table"] for call in publisher.upserts]
    assert tables == [
        "source_cache.fundamentals",
        "source_cache.earnings",
    ]

    conflicts = {call["table"]: call["on_conflict"] for call in publisher.upserts}
    assert conflicts["source_cache.fundamentals"] == "symbol,metric,period_end,period_type,report_date"
    assert conflicts["source_cache.earnings"] == "symbol,report_date,earnings_date,fiscal_period"

    fundamentals_row = next(call for call in publisher.upserts if call["table"] == "source_cache.fundamentals")[
        "rows"
    ][0]
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
    assert fundamentals_row["symbol"] == "SPY"

    earnings_row = next(call for call in publisher.upserts if call["table"] == "source_cache.earnings")["rows"][0]
    assert set(earnings_row.keys()) == {
        "symbol",
        "report_date",
        "earnings_date",
        "fiscal_period",
        "earnings_time",
        "actual_eps",
        "estimate_eps",
        "surprise_eps",
        "actual_revenue",
        "estimate_revenue",
        "surprise_revenue",
        "currency",
        "source",
        "source_updated_at",
        "ingested_at",
    }
    assert earnings_row["symbol"] == "SPY"

    assert publisher.rpcs == []


def test_publish_fundamentals_writes_profile_and_etf_tables() -> None:
    publisher = RecordingPublisher()
    fetched_at = datetime(2026, 6, 22, 12, 0, tzinfo=UTC)

    publish_fundamentals_surfaces(
        publisher=publisher,
        fundamentals_history=pd.DataFrame(
            [
                {
                    "ticker": "SPY",
                    "period": "2026",
                    "metric": "ex_dividend_date",
                    "value": None,
                    "value_text": "2026-06-20",
                    "period_end": "2026-06-22",
                    "source": "fake",
                    "fetched_at": fetched_at,
                }
            ]
        ),
        ticker_profile=pd.DataFrame(
            [
                {
                    "ticker": "SPY",
                    "description": "Tracks large US companies.",
                    "etf_category": "Large Blend",
                    "fund_family": "Example Funds",
                    "expense_ratio": 0.0009,
                    "inception_date": "1993-02-01",
                    "legal_type": "ETF",
                    "beta": 1.0,
                    "beta_3y": 0.95,
                    "source": "fake",
                    "fetched_at": fetched_at,
                    "updated_at": fetched_at,
                }
            ]
        ),
        etf_holdings=pd.DataFrame(
            [
                {
                    "etf_ticker": "SPY",
                    "holding_symbol": "AAPL",
                    "holding_name": "Apple Inc.",
                    "weight": 0.071,
                    "as_of": "2026-06-22",
                    "source": "fake",
                    "fetched_at": fetched_at,
                    "updated_at": fetched_at,
                }
            ]
        ),
        etf_holding_onboarding_identity=pd.DataFrame(
            [
                {
                    "etf_ticker": "ICLN",
                    "theme": "clean_energy",
                    "source_symbol": "VWS",
                    "source_name": "VESTAS WIND SYSTEMS",
                    "source_country": "DK",
                    "provider": "yahoo",
                    "provider_symbol": "VWS.CO",
                    "onboard_symbol": "VWS.CO",
                    "onboard_region": "eu",
                    "is_onboardable": True,
                    "resolution_source": "known_mapping",
                    "resolution_confidence": 0.99,
                }
            ]
        ),
        etf_sector_weights=pd.DataFrame(
            [
                {
                    "etf_ticker": "SPY",
                    "sector": "technology",
                    "weight": 0.32,
                    "as_of": "2026-06-22",
                    "source": "fake",
                    "fetched_at": fetched_at,
                    "updated_at": fetched_at,
                }
            ]
        ),
    )

    conflicts = {call["table"]: call["on_conflict"] for call in publisher.upserts}
    assert conflicts["source_cache.fundamentals"] == "symbol,metric,period_end,period_type,report_date"
    assert conflicts["ticker_profile"] == "ticker"
    assert conflicts["etf_holdings"] == "etf_ticker,holding_symbol,as_of"
    assert conflicts["etf_holding_onboarding_identity"] == "etf_ticker,source_symbol,source_country"
    assert conflicts["etf_sector_weights"] == "etf_ticker,sector,as_of"

    profile_row = next(call for call in publisher.upserts if call["table"] == "ticker_profile")["rows"][0]
    assert profile_row["ticker"] == "SPY"
    assert profile_row["description"] == "Tracks large US companies."
    assert profile_row["long_business_summary"] == "Tracks large US companies."
    assert profile_row["expense_ratio"] == 0.0009

    holding_row = next(call for call in publisher.upserts if call["table"] == "etf_holdings")["rows"][0]
    assert holding_row["holding_symbol"] == "AAPL"
    assert holding_row["weight"] == 0.071

    identity_row = next(call for call in publisher.upserts if call["table"] == "etf_holding_onboarding_identity")[
        "rows"
    ][0]
    assert identity_row["source_symbol"] == "VWS"
    assert identity_row["onboard_symbol"] == "VWS.CO"
    assert identity_row["is_onboardable"] is True

    sector_row = next(call for call in publisher.upserts if call["table"] == "etf_sector_weights")["rows"][0]
    assert sector_row["sector"] == "technology"
    assert sector_row["weight"] == 0.32
