from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from finance_data_ops.refresh.quotes_latest import refresh_latest_quotes


class BatchFragileProvider:
    def fetch_latest_quotes(self, symbols: list[str]) -> pd.DataFrame:
        if len(symbols) > 1:
            raise RuntimeError("simulated batch provider object failure")
        symbol = str(symbols[0]).upper()
        if symbol == "BAD":
            raise RuntimeError("simulated symbol failure")
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "quote_ts": datetime(2026, 4, 11, 8, 30, tzinfo=UTC),
                    "price": 100.0,
                    "previous_close": 99.0,
                    "open": 99.5,
                    "high": 101.0,
                    "low": 98.5,
                    "volume": 1_000_000,
                    "provider": "fake",
                    "ingested_at": datetime(2026, 4, 11, 8, 30, tzinfo=UTC),
                }
            ]
        )


def test_refresh_latest_quotes_falls_back_to_per_symbol_on_batch_failure(tmp_path) -> None:
    quotes, result = refresh_latest_quotes(
        symbols=["AAPL", "BAD", "MSFT"],
        provider=BatchFragileProvider(),
        cache_root=str(tmp_path),
        symbol_batch_size=100,
    )

    assert sorted(quotes["symbol"].tolist()) == ["AAPL", "MSFT"]
    assert result.status == "partial"
    assert sorted(result.symbols_succeeded) == ["AAPL", "MSFT"]
    assert result.symbols_failed == ["BAD"]
