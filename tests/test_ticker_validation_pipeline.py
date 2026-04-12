from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.validation.ticker_validation import run_single_ticker_validation


class FakeMarketProvider:
    def fetch_daily_prices(self, symbols: list[str], *, start: str, end: str) -> pd.DataFrame:
        symbol = symbols[0]
        if symbol.startswith("EGPT") or symbol.startswith("GAF"):
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "date": start,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "adj_close": 100.5,
                    "volume": 1000,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                }
            ]
        )

    def fetch_latest_quotes(self, symbols: list[str]) -> pd.DataFrame:
        symbol = symbols[0]
        if symbol in {"ANZ", "WOW", "WBC"} or symbol.startswith("EGPT") or symbol.startswith("GAF"):
            # publish-unsafe: missing previous_close -> change becomes null
            return pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "quote_ts": datetime(2026, 4, 12, tzinfo=UTC),
                        "price": 50.0,
                        "previous_close": None,
                        "open": 50.0,
                        "high": 51.0,
                        "low": 49.0,
                        "volume": 100,
                        "provider": "fake",
                        "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                    }
                ]
            )
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "quote_ts": datetime(2026, 4, 12, tzinfo=UTC),
                    "price": 50.0,
                    "previous_close": 49.0,
                    "open": 50.0,
                    "high": 51.0,
                    "low": 49.0,
                    "volume": 100,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                }
            ]
        )


class FakeFundamentalsProvider:
    SUPPORTED = {"ANZ.AX", "WBC.AX", "WOW.AX"}

    def fetch_symbol_fundamentals(self, symbol: str) -> pd.DataFrame:
        if symbol not in self.SUPPORTED:
            return pd.DataFrame()
        return pd.DataFrame(
            [
                {
                    "ticker": symbol,
                    "metric": "revenue",
                    "value": 123.0,
                    "period_end": "2025-12-31",
                    "period_type": "annual",
                    "fiscal_year": 2025,
                    "fiscal_quarter": None,
                    "currency": "USD",
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 12, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                }
            ]
        )


class FakeEarningsProvider:
    SUPPORTED = {"ANZ.AX", "WBC.AX", "WOW.AX"}

    def fetch_symbol_earnings(self, symbol: str, *, history_limit: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
        if symbol not in self.SUPPORTED:
            return pd.DataFrame(), pd.DataFrame()
        events = pd.DataFrame(
            [
                {
                    "ticker": symbol,
                    "earnings_date": "2026-05-01",
                    "estimate_eps": 2.1,
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 12, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                }
            ]
        )
        history = pd.DataFrame(
            [
                {
                    "ticker": symbol,
                    "earnings_date": "2026-02-01",
                    "actual_eps": 2.0,
                    "estimate_eps": 1.9,
                    "source": "fake",
                    "fetched_at": datetime(2026, 4, 12, tzinfo=UTC),
                    "ingested_at": datetime(2026, 4, 12, tzinfo=UTC),
                }
            ]
        )
        return events, history


def test_raw_anz_validates_to_anz_ax() -> None:
    result = run_single_ticker_validation(
        input_symbol="ANZ",
        region="apac",
        instrument_type_hint="equity",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
    )

    assert result["selected"]["candidate_symbol"] == "ANZ.AX"
    assert result["selected"]["validation_status"] == "validated_full"
    assert result["registry_row"]["normalized_symbol"] == "ANZ.AX"


def test_problematic_symbols_fail_before_promotion() -> None:
    egpt = run_single_ticker_validation(
        input_symbol="EGPT",
        region="apac",
        instrument_type_hint="etf",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
    )
    gaf = run_single_ticker_validation(
        input_symbol="GAF",
        region="apac",
        instrument_type_hint="etf",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
    )

    assert egpt["selected"]["validation_status"] == "rejected"
    assert gaf["selected"]["validation_status"] == "rejected"
    assert "market_required_failed" in egpt["selected"]["validation_reason"]
    assert "market_required_failed" in gaf["selected"]["validation_reason"]


def test_explicit_override_wins_for_index_proxy_policy() -> None:
    result = run_single_ticker_validation(
        input_symbol="SPY",
        region="us",
        instrument_type_hint="equity",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
        metadata_lookup_fn=lambda _symbol: {"quoteType": "ETF"},
    )

    assert result["selected"]["instrument_type"] == "index_proxy"
    assert result["selected"]["instrument_type_source"] == "explicit_override"
    assert result["selected"]["validation_status"] == "validated_market_only"
    assert result["selected"]["validation_reason"] == "market_supported_index_proxy"


def test_metadata_hint_classifies_adr() -> None:
    result = run_single_ticker_validation(
        input_symbol="BABA",
        region="us",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
        metadata_lookup_fn=lambda _symbol: {"quoteType": "EQUITY", "longName": "Alibaba Group Holding Limited ADR"},
    )

    assert result["selected"]["instrument_type"] == "adr"
    assert result["selected"]["instrument_type_source"] == "provider_metadata"
    assert result["selected"]["validation_status"] == "validated_market_only"
    assert "fundamentals:" in result["selected"]["validation_reason"]
    assert "earnings:" in result["selected"]["validation_reason"]


def test_metadata_hint_classifies_etf_with_earnings_optional() -> None:
    result = run_single_ticker_validation(
        input_symbol="XLK",
        region="us",
        market_provider=FakeMarketProvider(),
        fundamentals_provider=FakeFundamentalsProvider(),
        earnings_provider=FakeEarningsProvider(),
        metadata_lookup_fn=lambda _symbol: {"quoteType": "ETF", "longName": "Technology Select Sector SPDR Fund"},
    )

    assert result["selected"]["instrument_type"] == "etf"
    assert result["selected"]["instrument_type_source"] == "provider_metadata"
    assert result["selected"]["validation_status"] == "validated_market_only"
    assert result["selected"]["validation_reason"] == "market_supported"
