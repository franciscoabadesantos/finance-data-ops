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
    period = _resolve_period(frame)
    value_text = frame.get("value_text", pd.Series(index=frame.index, dtype=object))

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "period": period,
            "period_end": pd.to_datetime(frame.get("period_end"), errors="coerce").dt.date,
            "metric": frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "value_text": value_text,
            "source": source,
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["ticker"] = _normalize_string_series(payload["ticker"])
    payload["period"] = _normalize_string_series(payload["period"])
    payload["value_text"] = _normalize_string_series(payload["value_text"])
    payload["source"] = _normalize_string_series(payload["source"])

    payload = payload.dropna(subset=["ticker", "period", "period_end", "metric", "value"])
    payload = payload.sort_values(["ticker", "period", "period_end", "metric", "fetched_at"])
    payload = payload.drop_duplicates(
        subset=["ticker", "period", "period_end", "metric"],
        keep="last",
    )

    return payload[
        [
            "ticker",
            "period",
            "period_end",
            "metric",
            "value",
            "value_text",
            "source",
            "fetched_at",
        ]
    ].to_dict(orient="records")


def _resolve_period(frame: pd.DataFrame) -> pd.Series:
    if "period" in frame.columns:
        return frame["period"].astype(str).str.strip()

    fiscal_year = pd.to_numeric(frame.get("fiscal_year"), errors="coerce").astype("Int64")
    fiscal_quarter = frame.get("fiscal_quarter", pd.Series(index=frame.index, dtype=object))
    period_end_year = pd.to_datetime(frame.get("period_end"), errors="coerce").dt.year.astype("Int64")
    out = pd.Series(index=frame.index, dtype=object)

    quarter_mask = fiscal_year.notna() & fiscal_quarter.notna() & (fiscal_quarter.astype(str).str.strip() != "")
    out.loc[quarter_mask] = fiscal_year.loc[quarter_mask].astype(str) + fiscal_quarter.loc[quarter_mask].astype(str)

    year_mask = out.isna() & fiscal_year.notna()
    out.loc[year_mask] = fiscal_year.loc[year_mask].astype(str)

    fallback_mask = out.isna() & period_end_year.notna()
    out.loc[fallback_mask] = period_end_year.loc[fallback_mask].astype(str)

    return out.astype(str).str.strip()


def _normalize_string_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    missing_mask = text.str.lower().isin({"", "nan", "none", "nat", "<na>"})
    return series.where(~missing_mask, None)


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
        on_conflict="ticker,period,period_end,metric",
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
