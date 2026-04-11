from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.providers.market import MarketDataProvider


def test_fetch_daily_prices_normalizes_provider_frame() -> None:
    def fake_download(symbol: str, **_: object) -> pd.DataFrame:
        idx = pd.to_datetime(["2026-04-07", "2026-04-08"])
        columns = pd.MultiIndex.from_tuples(
            [
                ("Open", symbol),
                ("High", symbol),
                ("Low", symbol),
                ("Close", symbol),
                ("Adj Close", symbol),
                ("Volume", symbol),
            ]
        )
        return pd.DataFrame(
            [
                [100.0, 102.0, 99.0, 101.0, 100.9, 1_000_000],
                [101.0, 103.0, 100.0, 102.0, 101.9, 1_200_000],
            ],
            index=idx,
            columns=columns,
        )

    provider = MarketDataProvider(download_fn=fake_download, quote_fn=lambda _: {})
    out = provider.fetch_daily_prices(["spy"], start="2026-04-07", end="2026-04-08")

    assert list(out["symbol"].unique()) == ["SPY"]
    assert set(("open", "high", "low", "close", "adj_close", "volume")).issubset(out.columns)
    assert out["date"].astype(str).tolist() == ["2026-04-07", "2026-04-08"]


def test_fetch_daily_prices_accepts_date_column_variants() -> None:
    def fake_download_date(_: str, **_kwargs: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "Date": "2026-04-07",
                    "Open": 100.0,
                    "High": 102.0,
                    "Low": 99.0,
                    "Close": 101.0,
                    "Adj Close": 100.9,
                    "Volume": 1_000_000,
                }
            ]
        )

    def fake_download_lower(_: str, **_kwargs: object) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "date": "2026-04-08",
                    "open": 101.0,
                    "high": 103.0,
                    "low": 100.0,
                    "close": 102.0,
                    "adj_close": 101.9,
                    "volume": 1_200_000,
                }
            ]
        )

    provider_date = MarketDataProvider(download_fn=fake_download_date, quote_fn=lambda _: {})
    out_date = provider_date.fetch_daily_prices(["spy"], start="2026-04-07", end="2026-04-07")
    assert out_date["date"].astype(str).tolist() == ["2026-04-07"]

    provider_lower = MarketDataProvider(download_fn=fake_download_lower, quote_fn=lambda _: {})
    out_lower = provider_lower.fetch_daily_prices(["spy"], start="2026-04-08", end="2026-04-08")
    assert out_lower["date"].astype(str).tolist() == ["2026-04-08"]


def test_fetch_latest_quotes_normalizes_fields() -> None:
    quote_ts = datetime(2026, 4, 10, 20, 0, tzinfo=UTC)

    def fake_quote(_: str) -> dict[str, object]:
        return {
            "price": 505.12,
            "previous_close": 500.0,
            "open": 501.0,
            "high": 506.0,
            "low": 499.0,
            "volume": 3_200_000,
            "quote_ts": quote_ts,
        }

    provider = MarketDataProvider(download_fn=lambda *_args, **_kwargs: pd.DataFrame(), quote_fn=fake_quote)
    out = provider.fetch_latest_quotes(["spy"])

    assert len(out.index) == 1
    row = out.iloc[0]
    assert row["symbol"] == "SPY"
    assert float(row["price"]) == 505.12
    assert pd.Timestamp(row["quote_ts"]).tzinfo is not None
