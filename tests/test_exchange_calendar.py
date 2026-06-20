from __future__ import annotations

from datetime import date

import exchange_calendars as ec
import pandas as pd

from finance_data_ops.providers.exchange_calendar import SUPPORTED_MICS, mic_for_symbol, trading_session_rows
from finance_data_ops.publish.client import RecordingPublisher
from finance_data_ops.publish.trading_calendar import publish_trading_calendar_surfaces


def test_supported_mics_exist_in_exchange_calendars() -> None:
    names = set(ec.get_calendar_names())
    assert set(SUPPORTED_MICS).issubset(names)
    assert "XNSE" not in names


def test_mic_for_symbol_maps_yfinance_suffixes() -> None:
    assert mic_for_symbol("AAPL") == "XNYS"
    assert mic_for_symbol("ANZ.AX") == "XASX"
    assert mic_for_symbol("VOD.L") == "XLON"
    assert mic_for_symbol("SAP.DE") == "XETR"
    assert mic_for_symbol("0700.HK") == "XHKG"
    assert mic_for_symbol("INFY.NS") == "XBOM"
    assert mic_for_symbol("RELIANCE.BO") == "XBOM"
    assert mic_for_symbol("7203.T") == "XTKS"


def test_trading_session_rows_uses_exchange_calendar_sessions() -> None:
    rows = trading_session_rows("XNYS", start=date(2026, 1, 1), end=date(2026, 1, 5))
    assert [row["session_date"] for row in rows] == [date(2026, 1, 2), date(2026, 1, 5)]
    assert all(row["exchange_mic"] == "XNYS" for row in rows)


def test_publish_trading_calendar_upserts_expected_table() -> None:
    publisher = RecordingPublisher()
    frame = pd.DataFrame(
        [
            {"exchange_mic": "xnys", "session_date": "2026-01-02", "is_half_day": False},
            {"exchange_mic": "XNYS", "session_date": "2026-01-02", "is_half_day": True},
        ]
    )

    result = publish_trading_calendar_surfaces(publisher=publisher, trading_calendar=frame)

    assert result["exchange_trading_calendar"]["rows"] == 1
    assert publisher.upserts[0]["table"] == "exchange_trading_calendar"
    assert publisher.upserts[0]["on_conflict"] == "exchange_mic,session_date"
    assert publisher.upserts[0]["rows"][0]["exchange_mic"] == "XNYS"
