"""Publish fundamentals history and summary surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


def build_market_fundamentals_payload(fundamentals_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if fundamentals_frame.empty:
        return []

    frame = fundamentals_frame.copy()
    ticker = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    source = frame.get("source", frame.get("provider", pd.Series(index=frame.index, dtype=object)))
    fetched_at = frame.get("fetched_at", frame.get("ingested_at", pd.Series(index=frame.index, dtype=object)))
    period_type = frame["period_type"] if "period_type" in frame.columns else pd.Series("unknown", index=frame.index)

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "metric": frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "period_end": pd.to_datetime(frame.get("period_end"), errors="coerce").dt.date,
            "period_type": period_type.astype(str).str.lower(),
            "fiscal_year": pd.to_numeric(frame.get("fiscal_year"), errors="coerce").astype("Int64"),
            "fiscal_quarter": frame.get("fiscal_quarter", pd.Series(index=frame.index, dtype=object)),
            "currency": frame.get("currency", pd.Series(index=frame.index, dtype=object)),
            "source": source,
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
            "created_at": pd.to_datetime(
                frame.get("created_at", fetched_at),
                utc=True,
                errors="coerce",
            ),
            "updated_at": pd.to_datetime(
                frame.get("updated_at", fetched_at),
                utc=True,
                errors="coerce",
            ),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["created_at"] = payload["created_at"].fillna(payload["fetched_at"])
    payload["updated_at"] = payload["updated_at"].fillna(payload["fetched_at"])
    payload["ticker"] = payload["ticker"].replace({"": None, "NAN": None, "NONE": None})

    payload = payload.dropna(subset=["ticker", "metric", "period_end", "value"])
    payload = payload.sort_values(["ticker", "metric", "period_end", "period_type", "fetched_at"])
    payload = payload.drop_duplicates(
        subset=["ticker", "metric", "period_end", "period_type"],
        keep="last",
    )

    return payload[
        [
            "ticker",
            "metric",
            "value",
            "period_end",
            "period_type",
            "fiscal_year",
            "fiscal_quarter",
            "currency",
            "source",
            "fetched_at",
            "created_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_ticker_fundamental_summary_payload(summary_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if summary_frame.empty:
        return []

    frame = summary_frame.copy()
    payload = pd.DataFrame(
        {
            "ticker": frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
            .astype(str)
            .str.upper(),
            "latest_revenue": pd.to_numeric(frame.get("latest_revenue"), errors="coerce"),
            "latest_eps": pd.to_numeric(frame.get("latest_eps"), errors="coerce"),
            "trailing_pe": pd.to_numeric(frame.get("trailing_pe"), errors="coerce"),
            "market_cap": pd.to_numeric(frame.get("market_cap"), errors="coerce"),
            "revenue_growth_yoy": pd.to_numeric(frame.get("revenue_growth_yoy"), errors="coerce"),
            "earnings_growth_yoy": pd.to_numeric(frame.get("earnings_growth_yoy"), errors="coerce"),
            "latest_period_end": pd.to_datetime(frame.get("latest_period_end"), errors="coerce").dt.date,
            "source": frame.get("source", frame.get("provider", "data_ops")),
            "updated_at": pd.to_datetime(
                frame.get("updated_at", pd.Timestamp.now(tz="UTC")),
                utc=True,
                errors="coerce",
            ),
        },
        index=frame.index,
    )
    payload["updated_at"] = payload["updated_at"].fillna(pd.Timestamp.now(tz="UTC"))
    payload["ticker"] = payload["ticker"].replace({"": None, "NAN": None, "NONE": None})
    payload = payload.dropna(subset=["ticker"])

    return payload[
        [
            "ticker",
            "latest_revenue",
            "latest_eps",
            "trailing_pe",
            "market_cap",
            "revenue_growth_yoy",
            "earnings_growth_yoy",
            "latest_period_end",
            "source",
            "updated_at",
        ]
    ].to_dict(orient="records")


def publish_fundamentals_surfaces(
    *,
    publisher: Publisher,
    fundamentals_history: pd.DataFrame,
    fundamentals_summary: pd.DataFrame,
    refresh_materialized_view: bool = True,
) -> dict[str, Any]:
    history_rows = build_market_fundamentals_payload(fundamentals_history)
    summary_rows = build_ticker_fundamental_summary_payload(fundamentals_summary)

    history_result = publisher.upsert(
        "market_fundamentals_v2",
        history_rows,
        on_conflict="ticker,metric,period_end,period_type",
    )
    summary_result = publisher.upsert(
        "ticker_fundamental_summary",
        summary_rows,
        on_conflict="ticker",
    )
    rpc_result: dict[str, Any] | None = None
    if refresh_materialized_view:
        rpc_result = publisher.rpc("refresh_mv_latest_fundamentals", {})

    return {
        "market_fundamentals_v2": history_result,
        "ticker_fundamental_summary": summary_result,
        "mv_latest_fundamentals": rpc_result,
    }
