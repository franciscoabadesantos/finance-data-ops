"""Fundamentals provider wrappers for normalized company metric ingestion."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
import logging
import re
from typing import Any

import pandas as pd


FUNDAMENTALS_COLUMNS = [
    "ticker",
    "metric",
    "value",
    "value_text",
    "period_end",
    "period_type",
    "fiscal_year",
    "fiscal_quarter",
    "currency",
    "source",
    "fetched_at",
    "ingested_at",
]

TICKER_PROFILE_COLUMNS = [
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

ETF_HOLDINGS_COLUMNS = [
    "etf_ticker",
    "holding_symbol",
    "holding_name",
    "weight",
    "as_of",
    "source",
    "fetched_at",
    "updated_at",
]

ETF_SECTOR_WEIGHTS_COLUMNS = [
    "etf_ticker",
    "sector",
    "weight",
    "as_of",
    "source",
    "fetched_at",
    "updated_at",
]

LOGGER = logging.getLogger("finance_data_ops.providers.fundamentals")


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
        funds_data_fn: Callable[[str], Any] | None = None,
        dividends_fn: Callable[[str], Any] | None = None,
        provider_name: str = "yahoo_finance",
    ) -> None:
        self._income_statements_fn = income_statements_fn or self._default_income_statements_fn
        self._cashflow_statements_fn = cashflow_statements_fn or self._default_cashflow_statements_fn
        self._balance_sheet_statements_fn = (
            balance_sheet_statements_fn or self._default_balance_sheet_statements_fn
        )
        self._info_fn = info_fn or self._default_info_fn
        self._fast_info_fn = fast_info_fn or self._default_fast_info_fn
        self._funds_data_fn = funds_data_fn or self._default_funds_data_fn
        self._dividends_fn = dividends_fn or self._default_dividends_fn
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
        info_payload = _optional_dict(self._info_fn, ticker, surface="info")
        fast_info_payload = _optional_dict(self._fast_info_fn, ticker, surface="fast_info")

        # Dividends come from the actual payment history (reliable), not the flaky `.info` fields.
        dividend_metrics = _dividend_metrics_from_history(
            _optional_call(self._dividends_fn, ticker, default=None, surface="dividends"),
            last_price=_first_present_value(
                _coerce_float(fast_info_payload.get("last_price")),
                _coerce_float(fast_info_payload.get("lastPrice")),
                _coerce_float(fast_info_payload.get("last_close")),
                _coerce_float(info_payload.get("currentPrice")),
                _coerce_float(info_payload.get("regularMarketPrice")),
            ),
            eps=_coerce_float(
                info_payload.get("trailingEps") or info_payload.get("epsTrailingTwelveMonths")
            ),
        )

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
            # ETFs/funds report no marketCap — their "size" is AUM (`.info['totalAssets']`).
            # Fall back to it for funds so every ETF has a reliable size (page + network sizing).
            "market_cap": _coerce_float(
                info_payload.get("marketCap")
                or fast_info_payload.get("market_cap")
                or (info_payload.get("totalAssets") if _is_fund(info_payload) else None)
            ),
            "shares_outstanding": _coerce_float(
                info_payload.get("sharesOutstanding")
                or fast_info_payload.get("shares")
                or fast_info_payload.get("shares_outstanding")
            ),
            "trailing_pe": _coerce_float(info_payload.get("trailingPE") or info_payload.get("trailingPe")),
            "eps": _coerce_float(info_payload.get("trailingEps") or info_payload.get("epsTrailingTwelveMonths")),
            "ebitda": _coerce_float(info_payload.get("ebitda")),
            "free_cash_flow": _coerce_float(info_payload.get("freeCashflow")),
            "dividend_yield": _first_present_value(
                dividend_metrics["dividend_yield"], _coerce_float(info_payload.get("dividendYield"))
            ),
            "dividend_rate": _first_present_value(
                dividend_metrics["dividend_rate"], _coerce_float(info_payload.get("dividendRate"))
            ),
            "trailing_annual_dividend_yield": _coerce_float(info_payload.get("trailingAnnualDividendYield")),
            "trailing_annual_dividend_rate": _coerce_float(info_payload.get("trailingAnnualDividendRate")),
            "payout_ratio": _first_present_value(
                dividend_metrics["payout_ratio"], _coerce_float(info_payload.get("payoutRatio"))
            ),
            "beta": _coerce_float(info_payload.get("beta")),
            "beta_3y": _coerce_float(info_payload.get("beta3Year") or info_payload.get("beta3y")),
            "ytd_return": _coerce_float(info_payload.get("ytdReturn")),
            "three_year_avg_return": _coerce_float(info_payload.get("threeYearAverageReturn")),
            "five_year_avg_return": _coerce_float(info_payload.get("fiveYearAverageReturn")),
        }
        text_point_metrics = {
            "ex_dividend_date": _coerce_date_text(info_payload.get("exDividendDate")),
            "payout_frequency": _coerce_text(info_payload.get("payoutFrequency")),
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
                    "value_text": None,
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
        for metric, value_text in text_point_metrics.items():
            if value_text is None:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "metric": metric,
                    "value": None,
                    "value_text": value_text,
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
        out = out.dropna(subset=["ticker", "metric", "period_end"])
        has_numeric = pd.to_numeric(out["value"], errors="coerce").notna()
        has_text = out["value_text"].astype(str).str.strip().replace({"nan": "", "None": "", "NaT": ""}) != ""
        out = out.loc[has_numeric | has_text].copy()
        out = out.sort_values(["ticker", "metric", "period_end", "period_type", "fetched_at"])
        out = out.drop_duplicates(
            subset=["ticker", "metric", "period_end", "period_type"],
            keep="last",
        )
        return out.reset_index(drop=True)

    def fetch_symbol_profile(self, symbol: str) -> pd.DataFrame:
        ticker = str(symbol).strip().upper()
        if not ticker:
            return pd.DataFrame(columns=TICKER_PROFILE_COLUMNS)

        fetched_at = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
        info_payload = _optional_dict(self._info_fn, ticker, surface="info")
        description = _coerce_text(
            info_payload.get("longBusinessSummary")
            or info_payload.get("description")
            or info_payload.get("summary")
        )
        row = {
            "ticker": ticker,
            "description": description,
            "long_business_summary": description,
            "etf_category": _coerce_text(info_payload.get("category") or info_payload.get("categoryName")),
            "fund_family": _coerce_text(info_payload.get("fundFamily")),
            "expense_ratio": _coerce_float(
                info_payload.get("feesExpensesInvestment")
                or info_payload.get("expenseRatio")
                or info_payload.get("annualReportExpenseRatio")
            ),
            "inception_date": _coerce_date(info_payload.get("fundInceptionDate")),
            "legal_type": _coerce_text(info_payload.get("legalType")),
            "beta": _coerce_float(info_payload.get("beta")),
            "beta_3y": _coerce_float(info_payload.get("beta3Year") or info_payload.get("beta3y")),
            "source": self.provider_name,
            "fetched_at": fetched_at,
            "updated_at": fetched_at,
        }
        if not any(row.get(column) is not None for column in TICKER_PROFILE_COLUMNS if column not in {"ticker", "source", "fetched_at", "updated_at"}):
            return pd.DataFrame(columns=TICKER_PROFILE_COLUMNS)
        return pd.DataFrame([row], columns=TICKER_PROFILE_COLUMNS)

    def fetch_symbol_etf_funds_data(self, symbol: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        ticker = str(symbol).strip().upper()
        if not ticker:
            return (
                pd.DataFrame(columns=ETF_HOLDINGS_COLUMNS),
                pd.DataFrame(columns=ETF_SECTOR_WEIGHTS_COLUMNS),
            )

        fetched_at = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
        funds_data = _optional_call(self._funds_data_fn, ticker, default=None, surface="funds_data")
        if funds_data is None:
            return (
                pd.DataFrame(columns=ETF_HOLDINGS_COLUMNS),
                pd.DataFrame(columns=ETF_SECTOR_WEIGHTS_COLUMNS),
            )

        as_of = (
            _coerce_date(_funds_value(funds_data, "as_of_date"))
            or _coerce_date(_funds_value(funds_data, "asOfDate"))
            or fetched_at.date()
        )
        holdings = _extract_etf_holdings(
            ticker=ticker,
            funds_data=funds_data,
            as_of=as_of,
            source=self.provider_name,
            fetched_at=fetched_at,
        )
        sector_weights = _extract_etf_sector_weights(
            ticker=ticker,
            funds_data=funds_data,
            as_of=as_of,
            source=self.provider_name,
            fetched_at=fetched_at,
        )
        return holdings, sector_weights

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

    def _default_funds_data_fn(self, symbol: str) -> Any:
        ticker = _load_ticker(symbol)
        getter = getattr(ticker, "get_funds_data", None)
        if callable(getter):
            return getter()
        return getattr(ticker, "funds_data", None)

    def _default_dividends_fn(self, symbol: str) -> Any:
        ticker = _load_ticker(symbol)
        return getattr(ticker, "dividends", None)


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


def _optional_call(fn: Callable[[str], Any], symbol: str, *, default: Any, surface: str) -> Any:
    try:
        return fn(symbol)
    except Exception as exc:
        LOGGER.warning(
            "Optional fundamentals provider surface failed (symbol=%s surface=%s): %r",
            symbol,
            surface,
            exc,
        )
        return default


def _optional_dict(fn: Callable[[str], Any], symbol: str, *, surface: str) -> dict[str, Any]:
    payload = _optional_call(fn, symbol, default={}, surface=surface)
    try:
        return dict(payload or {})
    except (TypeError, ValueError):
        return {}


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
                    "value_text": None,
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


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(value).date().isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        return str(int(numeric)) if numeric.is_integer() else str(numeric)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return None
    return text


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = pd.to_datetime(value, unit="s", utc=True, errors="coerce")
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def _coerce_date_text(value: Any) -> str | None:
    parsed = _coerce_date(value)
    if parsed is not None:
        return parsed.isoformat()
    return _coerce_text(value)


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


def _funds_value(funds_data: Any, name: str) -> Any:
    if funds_data is None:
        return None
    if isinstance(funds_data, dict):
        value = funds_data.get(name)
    else:
        value = getattr(funds_data, name, None)
    if callable(value):
        try:
            return value()
        except TypeError:
            return None
    return value


def _extract_etf_holdings(
    *,
    ticker: str,
    funds_data: Any,
    as_of: date,
    source: str,
    fetched_at: pd.Timestamp,
) -> pd.DataFrame:
    raw = _first_present_value(
        _funds_value(funds_data, "top_holdings"),
        _funds_value(funds_data, "topHoldings"),
    )
    frame = _records_frame(raw)
    if frame.empty:
        return pd.DataFrame(columns=ETF_HOLDINGS_COLUMNS)

    symbol_column = _first_existing_column(
        frame,
        ["symbol", "Symbol", "holding_symbol", "holdingSymbol", "ticker", "Ticker"],
    )
    name_column = _first_existing_column(
        frame,
        ["name", "Name", "holding_name", "holdingName", "Holding", "Company Name"],
    )
    weight_column = _first_existing_column(
        frame,
        ["weight", "Weight", "holdingPercent", "Holding Percent", "holding_percent", "% Assets"],
    )

    rows: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        holding_symbol = _coerce_text(row.get(symbol_column)) if symbol_column else _coerce_text(index)
        if holding_symbol is None:
            continue
        holding_name = _coerce_text(row.get(name_column)) if name_column else None
        weight = _coerce_float(row.get(weight_column)) if weight_column else None
        rows.append(
            {
                "etf_ticker": ticker,
                "holding_symbol": holding_symbol.upper(),
                "holding_name": holding_name,
                "weight": weight,
                "as_of": as_of,
                "source": source,
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    if not rows:
        return pd.DataFrame(columns=ETF_HOLDINGS_COLUMNS)
    out = pd.DataFrame(rows, columns=ETF_HOLDINGS_COLUMNS)
    out = out.drop_duplicates(subset=["etf_ticker", "holding_symbol", "as_of"], keep="last")
    return out.reset_index(drop=True)


def _extract_etf_sector_weights(
    *,
    ticker: str,
    funds_data: Any,
    as_of: date,
    source: str,
    fetched_at: pd.Timestamp,
) -> pd.DataFrame:
    raw = _first_present_value(
        _funds_value(funds_data, "sector_weightings"),
        _funds_value(funds_data, "sectorWeightings"),
    )
    frame = _sector_weights_frame(raw)
    if frame.empty:
        return pd.DataFrame(columns=ETF_SECTOR_WEIGHTS_COLUMNS)

    sector_column = _first_existing_column(frame, ["sector", "Sector", "name", "Name"])
    weight_column = _first_existing_column(frame, ["weight", "Weight", "value", "Value"])
    rows: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        sector = _coerce_text(row.get(sector_column)) if sector_column else _coerce_text(index)
        weight = _coerce_float(row.get(weight_column)) if weight_column else None
        if sector is None or weight is None:
            continue
        rows.append(
            {
                "etf_ticker": ticker,
                "sector": sector,
                "weight": weight,
                "as_of": as_of,
                "source": source,
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    if not rows:
        return pd.DataFrame(columns=ETF_SECTOR_WEIGHTS_COLUMNS)
    out = pd.DataFrame(rows, columns=ETF_SECTOR_WEIGHTS_COLUMNS)
    out = out.drop_duplicates(subset=["etf_ticker", "sector", "as_of"], keep="last")
    return out.reset_index(drop=True)


def _records_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value.copy()
    if isinstance(value, pd.Series):
        return value.to_frame().transpose()
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, dict):
        try:
            return pd.DataFrame(value)
        except ValueError:
            return pd.DataFrame([value])
    return pd.DataFrame()


def _sector_weights_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        frame = value.copy()
        if len(frame.index) == 1 and not {"sector", "Sector", "weight", "Weight"}.intersection(frame.columns):
            row = frame.iloc[0]
            return pd.DataFrame({"sector": row.index.astype(str), "weight": row.values})
        return frame
    if isinstance(value, pd.Series):
        return pd.DataFrame({"sector": value.index.astype(str), "weight": value.values})
    if isinstance(value, dict):
        return pd.DataFrame({"sector": list(value.keys()), "weight": list(value.values())})
    if isinstance(value, list):
        return pd.DataFrame(value)
    return pd.DataFrame()


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
    normalized = {_normalize_token(column): str(column) for column in frame.columns}
    for candidate in candidates:
        token = _normalize_token(candidate)
        if token in normalized:
            return normalized[token]
    return None


def _first_present_value(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        return value
    return None


def _is_fund(info_payload: dict[str, Any]) -> bool:
    """True for ETFs / mutual funds (which report AUM, not marketCap)."""
    quote_type = str(info_payload.get("quoteType") or info_payload.get("typeDisp") or "").strip().upper()
    return quote_type in {"ETF", "MUTUALFUND", "FUND"}


def _dividend_metrics_from_history(
    dividends: Any,
    *,
    last_price: float | None,
    eps: float | None,
) -> dict[str, float | None]:
    """Trailing-12-month dividend rate/yield/payout from the actual dividend history
    (yfinance `.dividends`), which is far more reliable than the flaky `.info` fields and avoids
    the bad scaling they sometimes return. `dividend_yield` is a FRACTION (rate / price)."""
    result: dict[str, float | None] = {
        "dividend_rate": None,
        "dividend_yield": None,
        "payout_ratio": None,
    }
    if dividends is None:
        return result
    try:
        series = pd.Series(dividends).dropna()
    except (TypeError, ValueError):
        return result
    if series.empty:
        return result
    series.index = pd.to_datetime(series.index, utc=True, errors="coerce")
    series = series[series.index.notna()]
    if series.empty:
        return result
    cutoff = pd.Timestamp(datetime.now(UTC)) - pd.Timedelta(days=365)
    trailing = series[series.index >= cutoff]
    rate = float(trailing.sum()) if not trailing.empty else 0.0
    if rate <= 0:
        return result
    result["dividend_rate"] = rate
    if last_price and last_price > 0:
        result["dividend_yield"] = rate / float(last_price)
    if eps and eps > 0:
        result["payout_ratio"] = rate / float(eps)
    return result


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
