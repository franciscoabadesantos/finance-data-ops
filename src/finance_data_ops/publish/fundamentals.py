"""Publish fundamentals history and summary surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.publish.client import Publisher


POINT_IN_TIME_METRICS = frozenset(
    {
        "market_cap",
        "shares_outstanding",
        "trailing_pe",
        "eps",
        "ebitda",
        "free_cash_flow",
        "dividend_yield",
        "dividend_rate",
        "trailing_annual_dividend_yield",
        "trailing_annual_dividend_rate",
        "payout_ratio",
        "beta",
        "beta_3y",
        "ytd_return",
        "three_year_avg_return",
        "five_year_avg_return",
        "ex_dividend_date",
        "payout_frequency",
    }
)

METRIC_ONLY_POINT_IN_TIME_METRICS = POINT_IN_TIME_METRICS - {"eps", "ebitda", "free_cash_flow"}


def build_market_fundamentals_payload(fundamentals_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if fundamentals_frame.empty:
        return []

    frame = _historical_fundamentals_frame(fundamentals_frame)
    if frame.empty:
        return []

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

    payload = payload.dropna(subset=["ticker", "period", "period_end", "metric"])
    payload = payload.loc[payload["value"].notna() | payload["value_text"].notna()].copy()
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


def build_ticker_fundamental_point_in_time_payload(fundamentals_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if fundamentals_frame.empty:
        return []

    frame = _point_in_time_fundamentals_frame(fundamentals_frame)
    if frame.empty:
        return []

    ticker = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    source = frame.get("source", frame.get("provider", pd.Series(index=frame.index, dtype=object)))
    fetched_at = frame.get("fetched_at", frame.get("ingested_at", pd.Series(index=frame.index, dtype=object)))
    value_text = frame.get("value_text", pd.Series(index=frame.index, dtype=object))

    payload = pd.DataFrame(
        {
            "ticker": ticker.astype(str).str.upper(),
            "metric": frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "value_text": value_text,
            "as_of_date": pd.to_datetime(frame.get("period_end"), errors="coerce").dt.date,
            "source": source,
            "fetched_at": pd.to_datetime(fetched_at, utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    now_utc = pd.Timestamp.now(tz="UTC")
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["updated_at"] = payload["fetched_at"]
    payload["ticker"] = _normalize_string_series(payload["ticker"])
    payload["metric"] = _normalize_string_series(payload["metric"])
    payload["value_text"] = _normalize_string_series(payload["value_text"])
    payload["source"] = _normalize_string_series(payload["source"])

    payload = payload.dropna(subset=["ticker", "metric", "as_of_date"])
    payload = payload.loc[payload["value"].notna() | payload["value_text"].notna()].copy()
    payload = payload.sort_values(["ticker", "metric", "as_of_date", "fetched_at"])
    payload = payload.drop_duplicates(subset=["ticker", "metric"], keep="last")

    return payload[
        [
            "ticker",
            "metric",
            "value",
            "value_text",
            "as_of_date",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_ticker_profile_payload(profile_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if profile_frame.empty:
        return []

    frame = profile_frame.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    payload = pd.DataFrame(
        {
            "ticker": frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
            .astype(str)
            .str.upper(),
            "description": frame.get(
                "description",
                frame.get("long_business_summary", pd.Series(index=frame.index, dtype=object)),
            ),
            "long_business_summary": frame.get(
                "long_business_summary",
                frame.get("description", pd.Series(index=frame.index, dtype=object)),
            ),
            "etf_category": frame.get(
                "etf_category",
                frame.get("category_name", pd.Series(index=frame.index, dtype=object)),
            ),
            "fund_family": frame.get("fund_family", pd.Series(index=frame.index, dtype=object)),
            "expense_ratio": pd.to_numeric(
                frame.get(
                    "expense_ratio",
                    frame.get("fees_expense_ratio", pd.Series(index=frame.index, dtype=object)),
                ),
                errors="coerce",
            ),
            "inception_date": pd.to_datetime(
                frame.get("inception_date", frame.get("fund_inception_date", pd.Series(index=frame.index))),
                errors="coerce",
            ).dt.date,
            "legal_type": frame.get("legal_type", pd.Series(index=frame.index, dtype=object)),
            "beta": pd.to_numeric(frame.get("beta"), errors="coerce"),
            "beta_3y": pd.to_numeric(frame.get("beta_3y"), errors="coerce"),
            "source": frame.get("source", frame.get("provider", "data_ops")),
            "fetched_at": pd.to_datetime(
                frame.get("fetched_at", frame.get("updated_at", now_utc)),
                utc=True,
                errors="coerce",
            ),
            "updated_at": pd.to_datetime(frame.get("updated_at", now_utc), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["ticker"] = _normalize_string_series(payload["ticker"])
    for column in ["description", "long_business_summary", "etf_category", "fund_family", "legal_type", "source"]:
        payload[column] = _normalize_string_series(payload[column])
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["updated_at"] = payload["updated_at"].fillna(now_utc)
    payload = payload.dropna(subset=["ticker"])
    payload = payload.drop_duplicates(subset=["ticker"], keep="last")
    return payload[
        [
            "ticker",
            "description",
            "long_business_summary",
            "etf_category",
            "fund_family",
            "expense_ratio",
            "inception_date",
            "legal_type",
            "beta",
            "beta_3y",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_etf_holdings_payload(holdings_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if holdings_frame.empty:
        return []
    frame = holdings_frame.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    payload = pd.DataFrame(
        {
            "etf_ticker": frame.get("etf_ticker", frame.get("ticker", pd.Series(index=frame.index, dtype=object)))
            .astype(str)
            .str.upper(),
            "holding_symbol": frame.get("holding_symbol", pd.Series(index=frame.index, dtype=object))
            .astype(str)
            .str.upper(),
            "holding_name": frame.get("holding_name", pd.Series(index=frame.index, dtype=object)),
            "weight": pd.to_numeric(frame.get("weight"), errors="coerce"),
            "as_of": pd.to_datetime(frame.get("as_of"), errors="coerce").dt.date,
            "source": frame.get("source", frame.get("provider", "data_ops")),
            "fetched_at": pd.to_datetime(
                frame.get("fetched_at", frame.get("updated_at", now_utc)),
                utc=True,
                errors="coerce",
            ),
            "updated_at": pd.to_datetime(frame.get("updated_at", now_utc), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["etf_ticker"] = _normalize_string_series(payload["etf_ticker"])
    payload["holding_symbol"] = _normalize_string_series(payload["holding_symbol"])
    payload["holding_name"] = _normalize_string_series(payload["holding_name"])
    payload["source"] = _normalize_string_series(payload["source"])
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["updated_at"] = payload["updated_at"].fillna(now_utc)
    payload = payload.dropna(subset=["etf_ticker", "holding_symbol", "as_of"])
    payload = payload.drop_duplicates(subset=["etf_ticker", "holding_symbol", "as_of"], keep="last")
    return payload[
        [
            "etf_ticker",
            "holding_symbol",
            "holding_name",
            "weight",
            "as_of",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_etf_sector_weights_payload(sector_weights_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if sector_weights_frame.empty:
        return []
    frame = sector_weights_frame.copy()
    now_utc = pd.Timestamp.now(tz="UTC")
    payload = pd.DataFrame(
        {
            "etf_ticker": frame.get("etf_ticker", frame.get("ticker", pd.Series(index=frame.index, dtype=object)))
            .astype(str)
            .str.upper(),
            "sector": frame.get("sector", pd.Series(index=frame.index, dtype=object)),
            "weight": pd.to_numeric(frame.get("weight"), errors="coerce"),
            "as_of": pd.to_datetime(frame.get("as_of"), errors="coerce").dt.date,
            "source": frame.get("source", frame.get("provider", "data_ops")),
            "fetched_at": pd.to_datetime(
                frame.get("fetched_at", frame.get("updated_at", now_utc)),
                utc=True,
                errors="coerce",
            ),
            "updated_at": pd.to_datetime(frame.get("updated_at", now_utc), utc=True, errors="coerce"),
        },
        index=frame.index,
    )
    payload["etf_ticker"] = _normalize_string_series(payload["etf_ticker"])
    payload["sector"] = _normalize_string_series(payload["sector"])
    payload["source"] = _normalize_string_series(payload["source"])
    payload["fetched_at"] = payload["fetched_at"].fillna(now_utc)
    payload["updated_at"] = payload["updated_at"].fillna(now_utc)
    payload = payload.dropna(subset=["etf_ticker", "sector", "as_of", "weight"])
    payload = payload.drop_duplicates(subset=["etf_ticker", "sector", "as_of"], keep="last")
    return payload[
        [
            "etf_ticker",
            "sector",
            "weight",
            "as_of",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_source_cache_fundamentals_payload(fundamentals_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if fundamentals_frame.empty:
        return []

    frame = fundamentals_frame.copy()
    ticker = frame.get("ticker", frame.get("symbol", pd.Series(index=frame.index, dtype=object)))
    source = frame.get("source", frame.get("provider", pd.Series(index=frame.index, dtype=object)))
    fetched_at = frame.get("fetched_at", frame.get("ingested_at", pd.Series(index=frame.index, dtype=object)))
    period_end = pd.to_datetime(frame.get("period_end"), errors="coerce").dt.date
    report_date = _resolve_report_date(frame, fallback=period_end)
    currency = frame.get("currency", pd.Series("USD", index=frame.index, dtype=object))

    payload = pd.DataFrame(
        {
            "symbol": ticker.astype(str).str.upper(),
            "report_date": report_date,
            "metric": frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower(),
            "value": pd.to_numeric(frame.get("value"), errors="coerce"),
            "value_text": frame.get("value_text", pd.Series(index=frame.index, dtype=object)),
            "period_end": period_end,
            "period_type": _resolve_period_type(frame),
            "fiscal_year": _resolve_fiscal_year(frame, period_end),
            "fiscal_quarter": _resolve_fiscal_quarter(frame),
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
    for column in ["symbol", "metric", "value_text", "period_type", "fiscal_quarter", "currency", "source"]:
        payload[column] = _normalize_string_series(payload[column])
    payload["period_type"] = payload["period_type"].fillna("unknown")
    payload["currency"] = payload["currency"].fillna("USD").astype(str).str.upper()
    payload = payload.dropna(subset=["symbol", "report_date", "metric", "period_end", "period_type", "currency"])
    payload = payload.loc[payload["value"].notna() | payload["value_text"].notna()].copy()
    payload = payload.sort_values(["symbol", "metric", "period_end", "period_type", "report_date", "source_updated_at"])
    payload = payload.drop_duplicates(
        subset=["symbol", "metric", "period_end", "period_type", "report_date"],
        keep="last",
    )
    return payload[
        [
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


def _resolve_report_date(frame: pd.DataFrame, *, fallback: pd.Series) -> pd.Series:
    for column in ("report_date", "as_of_date", "filing_date", "fetched_at", "ingested_at"):
        if column in frame.columns:
            parsed = pd.to_datetime(frame[column], errors="coerce").dt.date
            if parsed.notna().any():
                return parsed.fillna(fallback)
    return fallback


def _resolve_period_type(frame: pd.DataFrame) -> pd.Series:
    if "period_type" in frame.columns:
        return frame["period_type"].astype(str).str.strip().str.lower()
    if "period" in frame.columns:
        period = frame["period"].astype(str).str.strip().str.lower()
        return period.where(~period.isin({"", "nan", "none", "nat", "<na>"}), "unknown")
    return pd.Series("unknown", index=frame.index, dtype=object)


def _resolve_fiscal_year(frame: pd.DataFrame, period_end: pd.Series) -> pd.Series:
    if "fiscal_year" in frame.columns:
        return pd.to_numeric(frame["fiscal_year"], errors="coerce").astype("Int64")
    return pd.to_datetime(period_end, errors="coerce").dt.year.astype("Int64")


def _resolve_fiscal_quarter(frame: pd.DataFrame) -> pd.Series:
    if "fiscal_quarter" in frame.columns:
        return frame["fiscal_quarter"].astype(str).str.strip()
    if "period" in frame.columns:
        return frame["period"].astype(str).str.upper().str.extract(r"(Q[1-4])", expand=False)
    return pd.Series(index=frame.index, dtype=object)


def _historical_fundamentals_frame(frame: pd.DataFrame) -> pd.DataFrame:
    point_mask = _point_in_time_mask(frame)
    return frame.loc[~point_mask].copy()


def _point_in_time_fundamentals_frame(frame: pd.DataFrame) -> pd.DataFrame:
    point_mask = _point_in_time_mask(frame)
    return frame.loc[point_mask].copy()


def _point_in_time_mask(frame: pd.DataFrame) -> pd.Series:
    metric = frame.get("metric", pd.Series(index=frame.index, dtype=object)).astype(str).str.lower()
    if "period_type" in frame.columns:
        period_type = frame["period_type"].astype(str).str.strip().str.lower()
        return period_type.eq("point_in_time")
    return metric.isin(METRIC_ONLY_POINT_IN_TIME_METRICS)


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
    ticker_profile: pd.DataFrame | None = None,
    etf_holdings: pd.DataFrame | None = None,
    etf_sector_weights: pd.DataFrame | None = None,
    refresh_materialized_view: bool = True,
) -> dict[str, Any]:
    history_rows = build_market_fundamentals_payload(fundamentals_history)
    source_cache_rows = build_source_cache_fundamentals_payload(fundamentals_history)
    point_in_time_rows = build_ticker_fundamental_point_in_time_payload(fundamentals_history)
    summary_rows = build_ticker_fundamental_summary_payload(fundamentals_summary)
    profile_rows = build_ticker_profile_payload(_frame_or_empty(ticker_profile))
    holding_rows = build_etf_holdings_payload(_frame_or_empty(etf_holdings))
    sector_weight_rows = build_etf_sector_weights_payload(_frame_or_empty(etf_sector_weights))

    history_result = publisher.upsert(
        "market_fundamentals_v2",
        history_rows,
        on_conflict="ticker,period,period_end,metric",
    )
    source_cache_result = publisher.upsert(
        "source_cache.fundamentals",
        source_cache_rows,
        on_conflict="symbol,metric,period_end,period_type,report_date",
    )
    point_in_time_result: dict[str, Any] | None = None
    if point_in_time_rows:
        point_in_time_result = publisher.upsert(
            "ticker_fundamental_point_in_time",
            point_in_time_rows,
            on_conflict="ticker,metric",
        )
    summary_result = publisher.upsert(
        "ticker_fundamental_summary",
        summary_rows,
        on_conflict="ticker",
    )
    profile_result: dict[str, Any] | None = None
    if profile_rows:
        profile_result = publisher.upsert(
            "ticker_profile",
            profile_rows,
            on_conflict="ticker",
        )
    holdings_result: dict[str, Any] | None = None
    if holding_rows:
        holdings_result = publisher.upsert(
            "etf_holdings",
            holding_rows,
            on_conflict="etf_ticker,holding_symbol,as_of",
        )
    sector_weights_result: dict[str, Any] | None = None
    if sector_weight_rows:
        sector_weights_result = publisher.upsert(
            "etf_sector_weights",
            sector_weight_rows,
            on_conflict="etf_ticker,sector,as_of",
        )
    rpc_result: dict[str, Any] | None = None
    if refresh_materialized_view:
        rpc_result = publisher.rpc("refresh_mv_latest_fundamentals", {})

    return {
        "market_fundamentals_v2": history_result,
        "source_cache.fundamentals": source_cache_result,
        "ticker_fundamental_point_in_time": point_in_time_result,
        "ticker_fundamental_summary": summary_result,
        "ticker_profile": profile_result,
        "etf_holdings": holdings_result,
        "etf_sector_weights": sector_weights_result,
        "mv_latest_fundamentals": rpc_result,
    }


def _frame_or_empty(frame: pd.DataFrame | None) -> pd.DataFrame:
    return frame if frame is not None else pd.DataFrame()
