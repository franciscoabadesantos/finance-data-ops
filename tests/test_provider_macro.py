from __future__ import annotations

from datetime import date

import pandas as pd

from finance_data_ops.providers.macro import MacroDataProvider, SERIES_BY_KEY, catalog_frame


def test_vix3m_catalog_uses_fred_vxvcls_and_remains_required() -> None:
    spec = SERIES_BY_KEY["VIX3M"]

    assert spec.source == "fred"
    assert spec.source_code == "VXVCLS"
    assert spec.required_by_default is True
    assert spec.required_from_date == date(2007, 12, 4)

    row = catalog_frame().loc[lambda frame: frame["series_key"] == "VIX3M"].iloc[0]
    assert row["source_provider"] == "fred"
    assert row["source_code"] == "VXVCLS"
    assert bool(row["required_by_default"]) is True
    assert row["required_from_date"] == "2007-12-04"


def test_vix3m_fetches_through_fred_provider_path(monkeypatch) -> None:
    provider = MacroDataProvider()
    spec = SERIES_BY_KEY["VIX3M"]
    calls: list[tuple[str, str]] = []

    def _fake_fetch_fred_series(received_spec, *, start, end):
        calls.append((received_spec.key, received_spec.source_code))
        return pd.Series([24.65], index=pd.DatetimeIndex(["2007-12-04"]), name=received_spec.key)

    def _unexpected_fetch_yfinance_series(*_args, **_kwargs):
        raise AssertionError("VIX3M should not use yfinance")

    monkeypatch.setattr(provider, "_fetch_fred_series", _fake_fetch_fred_series)
    monkeypatch.setattr(provider, "_fetch_yfinance_series", _unexpected_fetch_yfinance_series)

    series = provider.fetch_series(spec, start="2007-12-04", end="2007-12-04")

    assert calls == [("VIX3M", "VXVCLS")]
    assert series.name == "VIX3M"
    assert float(series.iloc[0]) == 24.65
