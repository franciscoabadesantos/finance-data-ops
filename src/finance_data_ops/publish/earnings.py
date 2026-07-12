"""Publish canonical earnings source-cache surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_source_cache_earnings_payload(events_frame: pd.DataFrame, history_frame: pd.DataFrame) -> list[dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    if not events_frame.empty:
        frames.append(events_frame.copy())
    if not history_frame.empty:
        frames.append(history_frame.copy())
    if not frames:
        return []
    frame = pd.concat(frames, ignore_index=True, sort=False)
    ticker = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    source = frame.get("source", frame.get("provider", pd.Series(index=frame.index, dtype=object)))
    fetched_at = frame.get("fetched_at", frame.get("ingested_at", pd.Series(index=frame.index, dtype=object)))
    earnings_date = pd.to_datetime(frame.get("earnings_date"), errors="coerce").dt.date
    report_date = _resolve_report_date(frame, fallback=earnings_date)
    fiscal_period = frame.get("fiscal_period", pd.Series("unknown", index=frame.index, dtype=object))
    currency = frame.get("currency", pd.Series("USD", index=frame.index, dtype=object))

    payload = pd.DataFrame(
        {
            "symbol": ticker.astype(str).str.upper(),
            "report_date": report_date,
            "earnings_date": earnings_date,
            "fiscal_period": fiscal_period,
            "earnings_time": frame.get("earnings_time", pd.Series(index=frame.index, dtype=object)),
            "actual_eps": pd.to_numeric(frame.get("actual_eps"), errors="coerce"),
            "estimate_eps": pd.to_numeric(frame.get("estimate_eps"), errors="coerce"),
            "surprise_eps": pd.to_numeric(frame.get("surprise_eps"), errors="coerce"),
            "actual_revenue": pd.to_numeric(frame.get("actual_revenue"), errors="coerce"),
            "estimate_revenue": pd.to_numeric(frame.get("estimate_revenue"), errors="coerce"),
            "surprise_revenue": pd.to_numeric(frame.get("surprise_revenue"), errors="coerce"),
            "currency": currency,
            "source": source,
            "source_updated_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "ingested_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["source_updated_at"] = payload["source_updated_at"].fillna(now_utc)
    payload["ingested_at"] = payload["ingested_at"].fillna(payload["source_updated_at"])
    for column in ["symbol", "fiscal_period", "earnings_time", "currency", "source"]:
        payload[column] = _normalize_string_series(payload[column])
    payload["fiscal_period"] = payload["fiscal_period"].fillna("unknown")
    payload["currency"] = payload["currency"].fillna("USD").astype(str).str.upper()
    payload = payload.dropna(subset=["symbol", "report_date", "earnings_date", "fiscal_period", "currency"])
    payload = payload.sort_values(["symbol", "report_date", "earnings_date", "fiscal_period", "source_updated_at"])
    key_columns = ["symbol", "report_date", "earnings_date", "fiscal_period"]
    value_columns = [column for column in payload.columns if column not in set(key_columns)]
    payload = (
        payload.groupby(key_columns, as_index=False, sort=False)[value_columns]
        .agg(_last_non_null)
        .reset_index(drop=True)
    )
    return payload[
        [
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
        ]
    ].to_dict(orient="records")


def publish_earnings_surfaces(
    *,
    publisher: Publisher,
    earnings_events: pd.DataFrame,
    earnings_history: pd.DataFrame,
) -> dict[str, Any]:
    source_cache_rows = build_source_cache_earnings_payload(earnings_events, earnings_history)

    source_cache_result = publisher.upsert(
        "source_cache.earnings",
        source_cache_rows,
        on_conflict="symbol,report_date,earnings_date,fiscal_period",
    )

    return {
        "source_cache.earnings": source_cache_result,
    }


def _resolve_report_date(frame: pd.DataFrame, *, fallback: pd.Series) -> pd.Series:
    for column in ("report_date", "as_of_date", "fetched_at", "ingested_at"):
        if column in frame.columns:
            parsed = pd.to_datetime(frame[column], errors="coerce").dt.date
            if parsed.notna().any():
                return parsed.fillna(fallback)
    return fallback


def _normalize_string_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    missing_mask = text.str.lower().isin({"", "nan", "none", "nat", "<na>"})
    return series.where(~missing_mask, None)


def _last_non_null(series: pd.Series) -> Any:
    values = series.dropna()
    if values.empty:
        return None
    return values.iloc[-1]
