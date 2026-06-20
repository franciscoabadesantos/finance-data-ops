"""Publish exchange trading calendar surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_exchange_trading_calendar_payload(trading_calendar: pd.DataFrame) -> list[dict[str, Any]]:
    if trading_calendar.empty:
        return []

    frame = trading_calendar.copy()
    payload = pd.DataFrame(
        {
            "exchange_mic": frame.get("exchange_mic", pd.Series(index=frame.index, dtype=object))
            .astype(str)
            .str.strip()
            .str.upper(),
            "session_date": pd.to_datetime(frame.get("session_date"), errors="coerce").dt.date,
            "is_half_day": frame.get("is_half_day", pd.Series(False, index=frame.index)).fillna(False).astype(bool),
            "ingested_at": pd.to_datetime(frame.get("ingested_at"), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["ingested_at"] = payload["ingested_at"].fillna(pd.Timestamp.now(tz="UTC"))
    payload["exchange_mic"] = payload["exchange_mic"].replace({"": None, "NAN": None, "NONE": None})
    payload = payload.dropna(subset=["exchange_mic", "session_date"])
    payload = payload.sort_values(["exchange_mic", "session_date", "ingested_at"])
    payload = payload.drop_duplicates(subset=["exchange_mic", "session_date"], keep="last")
    return payload[
        [
            "exchange_mic",
            "session_date",
            "is_half_day",
            "ingested_at",
        ]
    ].to_dict(orient="records")


def publish_trading_calendar_surfaces(
    *,
    publisher: Publisher,
    trading_calendar: pd.DataFrame,
) -> dict[str, Any]:
    rows = build_exchange_trading_calendar_payload(trading_calendar)
    table_result = publisher.upsert(
        "exchange_trading_calendar",
        rows,
        on_conflict="exchange_mic,session_date",
    )
    return {"exchange_trading_calendar": table_result}
