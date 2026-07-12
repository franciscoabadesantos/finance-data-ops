"""Publish canonical fundamentals source-cache surfaces."""

from __future__ import annotations

from typing import Any

import pandas as pd

from finance_data_ops.identity.provider_symbols import ONBOARDING_IDENTITY_COLUMNS

from finance_data_ops.publish.client import Publisher


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
            "holding_country": frame.get("holding_country", pd.Series(index=frame.index, dtype=object)),
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
    payload["holding_country"] = _normalize_string_series(payload["holding_country"])
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
            "holding_country",
            "weight",
            "as_of",
            "source",
            "fetched_at",
            "updated_at",
        ]
    ].to_dict(orient="records")


def build_etf_holding_onboarding_identity_payload(identity_frame: pd.DataFrame) -> list[dict[str, Any]]:
    if identity_frame.empty:
        return []
    frame = identity_frame.copy()
    for col in ONBOARDING_IDENTITY_COLUMNS:
        if col not in frame.columns:
            frame[col] = False if col == "is_onboardable" else ""
    payload = frame[ONBOARDING_IDENTITY_COLUMNS].copy()
    for col in ONBOARDING_IDENTITY_COLUMNS:
        if col == "is_onboardable":
            payload[col] = payload[col].fillna(False).astype(bool)
        elif col == "resolution_confidence":
            payload[col] = pd.to_numeric(payload[col], errors="coerce").fillna(0.0).astype(float)
        else:
            payload[col] = _normalize_string_series(payload[col])
    payload = payload.dropna(subset=["etf_ticker", "source_symbol"])
    payload = payload.drop_duplicates(subset=["etf_ticker", "source_symbol", "source_country"], keep="last")
    return payload[ONBOARDING_IDENTITY_COLUMNS].to_dict(orient="records")


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


def _normalize_string_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip()
    missing_mask = text.str.lower().isin({"", "nan", "none", "nat", "<na>"})
    return series.where(~missing_mask, None)


def publish_fundamentals_surfaces(
    *,
    publisher: Publisher,
    fundamentals_history: pd.DataFrame,
    fundamentals_summary: pd.DataFrame,
    ticker_profile: pd.DataFrame | None = None,
    etf_holdings: pd.DataFrame | None = None,
    etf_holding_onboarding_identity: pd.DataFrame | None = None,
    etf_sector_weights: pd.DataFrame | None = None,
    refresh_materialized_view: bool = True,
) -> dict[str, Any]:
    source_cache_rows = build_source_cache_fundamentals_payload(fundamentals_history)
    profile_rows = build_ticker_profile_payload(_frame_or_empty(ticker_profile))
    holding_rows = build_etf_holdings_payload(_frame_or_empty(etf_holdings))
    identity_rows = build_etf_holding_onboarding_identity_payload(_frame_or_empty(etf_holding_onboarding_identity))
    sector_weight_rows = build_etf_sector_weights_payload(_frame_or_empty(etf_sector_weights))

    source_cache_result = publisher.upsert(
        "source_cache.fundamentals",
        source_cache_rows,
        on_conflict="symbol,metric,period_end,period_type,report_date",
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
    identity_result: dict[str, Any] | None = None
    if identity_rows:
        identity_result = publisher.upsert(
            "etf_holding_onboarding_identity",
            identity_rows,
            on_conflict="etf_ticker,source_symbol,source_country",
        )
    sector_weights_result: dict[str, Any] | None = None
    if sector_weight_rows:
        sector_weights_result = publisher.upsert(
            "etf_sector_weights",
            sector_weight_rows,
            on_conflict="etf_ticker,sector,as_of",
        )

    return {
        "source_cache.fundamentals": source_cache_result,
        "ticker_profile": profile_result,
        "etf_holdings": holdings_result,
        "etf_holding_onboarding_identity": identity_result,
        "etf_sector_weights": sector_weights_result,
    }


def _frame_or_empty(frame: pd.DataFrame | None) -> pd.DataFrame:
    return frame if frame is not None else pd.DataFrame()
