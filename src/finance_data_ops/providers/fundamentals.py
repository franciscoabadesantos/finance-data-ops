"""Fundamentals provider wrappers for normalized company metric ingestion."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
import re
from typing import Any

import pandas as pd


FUNDAMENTALS_COLUMNS = [
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
    "ingested_at",
]


class FundamentalsProviderError(RuntimeError):
    """Raised when fundamentals provider output cannot be normalized safely."""


MetricAliases = dict[str, list[str]]


class FundamentalsDataProvider:
    """Provider boundary for normalized fundamentals history in Data Ops v2."""

    def __init__(
        self,
        *,
        income_statements_fn: Callable[[str], tuple[pd.DataFrame, pd.DataFrame]] | None = None,
        cashflow_statements_fn: Callable[[str], tuple[pd.DataFrame, pd.DataFrame]] | None = None,
        balance_sheet_statements_fn: Callable[[str], tuple[pd.DataFrame, pd.DataFrame]] | None = None,
        info_fn: Callable[[str], dict[str, Any]] | None = None,
        fast_info_fn: Callable[[str], dict[str, Any]] | None = None,
        provider_name: str = "yahoo_finance",
    ) -> None:
        self._income_statements_fn = income_statements_fn or self._default_income_statements_fn
        self._cashflow_statements_fn = cashflow_statements_fn or self._default_cashflow_statements_fn
        self._balance_sheet_statements_fn = (
            balance_sheet_statements_fn or self._default_balance_sheet_statements_fn
        )
        self._info_fn = info_fn or self._default_info_fn
        self._fast_info_fn = fast_info_fn or self._default_fast_info_fn
        self.provider_name = str(provider_name).strip() or "unknown_provider"

    def fetch_fundamentals(self, symbols: Iterable[str]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            local = self.fetch_symbol_fundamentals(symbol)
            if not local.empty:
                frames.append(local)
        if not frames:
            return pd.DataFrame(columns=FUNDAMENTALS_COLUMNS)
        out = pd.concat(frames, ignore_index=True)
        out = out.sort_values(["ticker", "metric", "period_end", "period_type", "fetched_at"])
        out = out.drop_duplicates(
            subset=["ticker", "metric", "period_end", "period_type"],
            keep="last",
        )
        return out.reset_index(drop=True)

    def fetch_symbol_fundamentals(self, symbol: str) -> pd.DataFrame:
        ticker = str(symbol).strip().upper()
        if not ticker:
            return pd.DataFrame(columns=FUNDAMENTALS_COLUMNS)

        ingested_at = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
        info_payload = dict(self._info_fn(ticker) or {})
        fast_info_payload = dict(self._fast_info_fn(ticker) or {})

        income_quarterly, income_annual = self._income_statements_fn(ticker)
        cashflow_quarterly, cashflow_annual = self._cashflow_statements_fn(ticker)
        balance_quarterly, balance_annual = self._balance_sheet_statements_fn(ticker)

        currency = str(
            info_payload.get("financialCurrency")
            or info_payload.get("currency")
            or "USD"
        ).strip() or "USD"

        rows: list[dict[str, Any]] = []
        rows.extend(
            _extract_statement_metrics(
                statement=income_annual,
                ticker=ticker,
                period_type="annual",
                metric_aliases=_INCOME_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )
        rows.extend(
            _extract_statement_metrics(
                statement=income_quarterly,
                ticker=ticker,
                period_type="quarterly",
                metric_aliases=_INCOME_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )
        rows.extend(
            _extract_statement_metrics(
                statement=cashflow_annual,
                ticker=ticker,
                period_type="annual",
                metric_aliases=_CASHFLOW_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )
        rows.extend(
            _extract_statement_metrics(
                statement=cashflow_quarterly,
                ticker=ticker,
                period_type="quarterly",
                metric_aliases=_CASHFLOW_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )
        rows.extend(
            _extract_statement_metrics(
                statement=balance_annual,
                ticker=ticker,
                period_type="annual",
                metric_aliases=_BALANCE_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )
        rows.extend(
            _extract_statement_metrics(
                statement=balance_quarterly,
                ticker=ticker,
                period_type="quarterly",
                metric_aliases=_BALANCE_METRICS,
                currency=currency,
                source=self.provider_name,
                ingested_at=ingested_at,
            )
        )

        point_metrics = {
            "market_cap": _coerce_float(info_payload.get("marketCap") or fast_info_payload.get("market_cap")),
            "shares_outstanding": _coerce_float(
                info_payload.get("sharesOutstanding")
                or fast_info_payload.get("shares")
                or fast_info_payload.get("shares_outstanding")
            ),
            "trailing_pe": _coerce_float(info_payload.get("trailingPE") or info_payload.get("trailingPe")),
            "eps": _coerce_float(info_payload.get("trailingEps") or info_payload.get("epsTrailingTwelveMonths")),
            "ebitda": _coerce_float(info_payload.get("ebitda")),
            "free_cash_flow": _coerce_float(info_payload.get("freeCashflow")),
        }
        point_period_end = ingested_at.date()
        point_fiscal_quarter = _fiscal_quarter(point_period_end)
        for metric, value in point_metrics.items():
            if value is None:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "metric": metric,
                    "value": float(value),
                    "period_end": point_period_end,
                    "period_type": "point_in_time",
                    "fiscal_year": int(point_period_end.year),
                    "fiscal_quarter": point_fiscal_quarter,
                    "currency": currency,
                    "source": self.provider_name,
                    "fetched_at": ingested_at,
                    "ingested_at": ingested_at,
                }
            )

        if not rows:
            return pd.DataFrame(columns=FUNDAMENTALS_COLUMNS)
        out = pd.DataFrame(rows, columns=FUNDAMENTALS_COLUMNS)
        out["period_end"] = pd.to_datetime(out["period_end"], errors="coerce").dt.date
        out = out.dropna(subset=["ticker", "metric", "period_end", "value"])
        out = out.sort_values(["ticker", "metric", "period_end", "period_type", "fetched_at"])
        out = out.drop_duplicates(
            subset=["ticker", "metric", "period_end", "period_type"],
            keep="last",
        )
        return out.reset_index(drop=True)

    def _default_income_statements_fn(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = _load_ticker(symbol)
        return _to_frame(getattr(ticker, "quarterly_income_stmt", None)), _to_frame(
            getattr(ticker, "income_stmt", None)
        )

    def _default_cashflow_statements_fn(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = _load_ticker(symbol)
        return _to_frame(getattr(ticker, "quarterly_cashflow", None)), _to_frame(
            getattr(ticker, "cashflow", None)
        )

    def _default_balance_sheet_statements_fn(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = _load_ticker(symbol)
        return _to_frame(getattr(ticker, "quarterly_balance_sheet", None)), _to_frame(
            getattr(ticker, "balance_sheet", None)
        )

    def _default_info_fn(self, symbol: str) -> dict[str, Any]:
        ticker = _load_ticker(symbol)
        payload = getattr(ticker, "info", {})
        return dict(payload or {})

    def _default_fast_info_fn(self, symbol: str) -> dict[str, Any]:
        ticker = _load_ticker(symbol)
        payload = getattr(ticker, "fast_info", {})
        return dict(payload or {})


def _load_ticker(symbol: str) -> Any:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - runtime environment dependency
        raise RuntimeError("yfinance is required for live provider calls.") from exc
    return yf.Ticker(symbol)


def _to_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if value is None:
        return pd.DataFrame()
    return pd.DataFrame(value)


def _extract_statement_metrics(
    *,
    statement: pd.DataFrame,
    ticker: str,
    period_type: str,
    metric_aliases: MetricAliases,
    currency: str,
    source: str,
    ingested_at: pd.Timestamp,
) -> list[dict[str, Any]]:
    frame = _normalize_statement_frame(statement)
    if frame.empty:
        return []

    normalized_index = {_normalize_token(index): index for index in frame.index}

    rows: list[dict[str, Any]] = []
    for metric, aliases in metric_aliases.items():
        matched_index = None
        for alias in aliases:
            token = _normalize_token(alias)
            if token in normalized_index:
                matched_index = normalized_index[token]
                break
        if matched_index is None:
            continue

        metric_series = frame.loc[matched_index]
        if isinstance(metric_series, pd.DataFrame):
            metric_series = metric_series.iloc[0]

        for raw_period, raw_value in metric_series.items():
            period_end = pd.to_datetime(raw_period, utc=False, errors="coerce")
            value = _coerce_float(raw_value)
            if pd.isna(period_end) or value is None:
                continue
            period_date = pd.Timestamp(period_end).date()
            rows.append(
                {
                    "ticker": ticker,
                    "metric": metric,
                    "value": float(value),
                    "period_end": period_date,
                    "period_type": period_type,
                    "fiscal_year": int(period_date.year),
                    "fiscal_quarter": _fiscal_quarter(period_date) if period_type == "quarterly" else None,
                    "currency": currency,
                    "source": source,
                    "fetched_at": ingested_at,
                    "ingested_at": ingested_at,
                }
            )
    return rows


def _normalize_statement_frame(frame: pd.DataFrame) -> pd.DataFrame:
    local = _to_frame(frame)
    if local.empty:
        return local

    date_like_columns = _count_date_like(local.columns)
    date_like_index = _count_date_like(local.index)
    if date_like_index > date_like_columns:
        local = local.transpose()

    local = local.copy()
    local.index = [str(idx) for idx in local.index]
    local.columns = [str(col) for col in local.columns]
    return local


def _normalize_token(value: Any) -> str:
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def _coerce_float(value: Any) -> float | None:
    casted = pd.to_numeric(value, errors="coerce")
    if pd.isna(casted):
        return None
    return float(casted)


def _fiscal_quarter(period_end: date) -> str:
    quarter = ((int(period_end.month) - 1) // 3) + 1
    return f"Q{quarter}"


def _count_date_like(values: Any) -> int:
    count = 0
    for value in values:
        parsed = pd.to_datetime(value, errors="coerce")
        if not pd.isna(parsed):
            count += 1
    return count


_INCOME_METRICS: MetricAliases = {
    "revenue": [
        "Total Revenue",
        "Revenue",
        "Operating Revenue",
    ],
    "eps": [
        "Diluted EPS",
        "Basic EPS",
        "EPS",
    ],
    "net_income": [
        "Net Income",
        "Net Income Common Stockholders",
        "Net Income Including Noncontrolling Interests",
    ],
    "gross_profit": [
        "Gross Profit",
    ],
    "operating_income": [
        "Operating Income",
        "Operating Income Loss",
    ],
    "ebitda": [
        "EBITDA",
    ],
}

_CASHFLOW_METRICS: MetricAliases = {
    "free_cash_flow": [
        "Free Cash Flow",
    ],
}

_BALANCE_METRICS: MetricAliases = {
    "total_assets": [
        "Total Assets",
    ],
    "total_liabilities": [
        "Total Liabilities Net Minority Interest",
        "Total Liabilities",
    ],
    "shares_outstanding": [
        "Ordinary Shares Number",
        "Share Issued",
        "Common Stock Shares Outstanding",
    ],
}
