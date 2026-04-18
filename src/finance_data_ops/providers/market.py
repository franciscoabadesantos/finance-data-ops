"""Market-data provider wrappers for daily bars and latest quotes."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from typing import Any

import pandas as pd

from finance_data_ops.providers.symbols import normalize_symbol_for_provider


DAILY_PRICE_COLUMNS = [
    "symbol",
    "date",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "provider",
    "ingested_at",
]

LATEST_QUOTE_COLUMNS = [
    "symbol",
    "quote_ts",
    "price",
    "previous_close",
    "open",
    "high",
    "low",
    "volume",
    "provider",
    "ingested_at",
]


class MarketProviderError(RuntimeError):
    """Raised when provider output cannot be normalized safely."""


class MarketDataProvider:
    """Provider boundary for market prices/quotes in Data Ops v1."""

    def __init__(
        self,
        *,
        download_fn: Callable[..., pd.DataFrame] | None = None,
        quote_fn: Callable[[str], dict[str, Any]] | None = None,
        provider_name: str = "yahoo_finance",
    ) -> None:
        self._download_fn = download_fn or self._default_download_fn
        self._quote_fn = quote_fn or self._default_quote_fn
        self.provider_name = str(provider_name).strip() or "unknown_provider"

    def fetch_daily_prices(
        self,
        symbols: Iterable[str],
        *,
        start: str | datetime,
        end: str | datetime,
    ) -> pd.DataFrame:
        start_ts = pd.Timestamp(start).tz_localize(None)
        end_ts = pd.Timestamp(end).tz_localize(None)
        if end_ts < start_ts:
            raise ValueError("end date must be on/after start date")

        ingested_at = datetime.now(UTC)
        normalized_frames: list[pd.DataFrame] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            for provider_symbol in _provider_symbol_candidates(symbol):
                try:
                    raw = self._download_fn(
                        provider_symbol,
                        start=start_ts.date().isoformat(),
                        end=(end_ts + pd.Timedelta(days=1)).date().isoformat(),
                        interval="1d",
                        auto_adjust=False,
                        progress=False,
                        threads=False,
                    )
                except Exception:
                    continue
                if raw is None or raw.empty:
                    continue
                normalized_frames.append(
                    self._normalize_daily_prices(raw=raw, symbol=symbol, ingested_at=ingested_at)
                )
                break

        if not normalized_frames:
            return pd.DataFrame(columns=DAILY_PRICE_COLUMNS)
        out = pd.concat(normalized_frames, ignore_index=True)
        out = out.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"], keep="last")
        return out.reset_index(drop=True)

    def fetch_latest_quotes(self, symbols: Iterable[str]) -> pd.DataFrame:
        ingested_at = datetime.now(UTC)
        rows: list[dict[str, Any]] = []
        for raw_symbol in symbols:
            symbol = str(raw_symbol).strip().upper()
            if not symbol:
                continue
            for provider_symbol in _provider_symbol_candidates(symbol):
                try:
                    payload = self._quote_fn(provider_symbol)
                except Exception:
                    continue
                if payload is None:
                    continue
                rows.append(self._normalize_quote(payload=payload, symbol=symbol, ingested_at=ingested_at))
                break
        if not rows:
            return pd.DataFrame(columns=LATEST_QUOTE_COLUMNS)
        out = pd.DataFrame(rows, columns=LATEST_QUOTE_COLUMNS)
        out = out.sort_values(["symbol", "quote_ts"]).drop_duplicates(subset=["symbol"], keep="last")
        return out.reset_index(drop=True)

    def _default_download_fn(self, symbol: str, **kwargs: Any) -> pd.DataFrame:
        try:
            import yfinance as yf
        except ImportError as exc:  # pragma: no cover - runtime environment dependency
            raise RuntimeError("yfinance is required for live provider calls.") from exc
        frame = yf.download(symbol, **kwargs)
        if isinstance(frame, pd.DataFrame):
            return frame
        return pd.DataFrame(frame)

    def _default_quote_fn(self, symbol: str) -> dict[str, Any]:
        try:
            import yfinance as yf
        except ImportError as exc:  # pragma: no cover - runtime environment dependency
            raise RuntimeError("yfinance is required for live provider calls.") from exc

        ticker = yf.Ticker(symbol)
        fast = _safe_fast_info_payload(ticker)
        info = _safe_info_payload(ticker)
        history = _safe_quote_history(ticker)

        latest_row: pd.Series | None = history.iloc[-1] if not history.empty else None
        previous_row: pd.Series | None = history.iloc[-2] if len(history.index) >= 2 else None

        quote_time = fast.get("lastTradeTime") or fast.get("last_trade_time")
        parsed_quote_ts = pd.to_datetime(quote_time, utc=True, errors="coerce")
        if pd.isna(parsed_quote_ts) and latest_row is not None:
            parsed_quote_ts = pd.to_datetime(latest_row.name, utc=True, errors="coerce")
        if pd.isna(parsed_quote_ts):
            parsed_quote_ts = pd.Timestamp.utcnow()

        history_close = _coerce_float(latest_row.get("Close")) if latest_row is not None else None
        previous_history_close = (
            _coerce_float(previous_row.get("Close")) if previous_row is not None else None
        )

        return {
            "price": _first_float(
                fast.get("lastPrice"),
                fast.get("last_price"),
                fast.get("regularMarketPrice"),
                history_close,
                info.get("currentPrice"),
                info.get("regularMarketPrice"),
            ),
            "previous_close": _first_float(
                fast.get("previousClose"),
                fast.get("previous_close"),
                previous_history_close,
                info.get("previousClose"),
                info.get("regularMarketPreviousClose"),
            ),
            "open": _first_float(
                fast.get("open"),
                _coerce_float(latest_row.get("Open")) if latest_row is not None else None,
                info.get("open"),
                info.get("regularMarketOpen"),
            ),
            "high": _first_float(
                fast.get("dayHigh"),
                fast.get("day_high"),
                _coerce_float(latest_row.get("High")) if latest_row is not None else None,
                info.get("dayHigh"),
                info.get("regularMarketDayHigh"),
            ),
            "low": _first_float(
                fast.get("dayLow"),
                fast.get("day_low"),
                _coerce_float(latest_row.get("Low")) if latest_row is not None else None,
                info.get("dayLow"),
                info.get("regularMarketDayLow"),
            ),
            "volume": _first_float(
                fast.get("lastVolume"),
                fast.get("last_volume"),
                fast.get("volume"),
                _coerce_float(latest_row.get("Volume")) if latest_row is not None else None,
                info.get("volume"),
                info.get("regularMarketVolume"),
            ),
            "quote_ts": parsed_quote_ts,
        }

    def _normalize_daily_prices(
        self,
        *,
        raw: pd.DataFrame,
        symbol: str,
        ingested_at: datetime,
    ) -> pd.DataFrame:
        frame = raw.copy()
        if isinstance(frame.index, pd.DatetimeIndex):
            frame = frame.reset_index()

        frame.columns = [_normalize_column_name(col) for col in frame.columns]

        date_col = "date" if "date" in frame.columns else None
        if date_col is None:
            for candidate in ("datetime", "timestamp", "index", "level_0"):
                if candidate in frame.columns:
                    date_col = candidate
                    break
        if date_col is None:
            for candidate in frame.columns:
                token = str(candidate).lower()
                if "date" not in token and "time" not in token:
                    continue
                parsed = pd.to_datetime(frame[candidate], utc=False, errors="coerce")
                if parsed.notna().any():
                    date_col = str(candidate)
                    break
        if date_col is None:
            raise MarketProviderError(
                f"{symbol}: provider frame has no date column after normalization "
                f"(columns={list(frame.columns)!r})"
            )

        adj_close = frame.get("adj_close", frame.get("adjclose", frame.get("close")))

        out = pd.DataFrame(
            {
                "symbol": symbol,
                "date": pd.to_datetime(frame[date_col], utc=False, errors="coerce").dt.date,
                "open": pd.to_numeric(frame.get("open"), errors="coerce"),
                "high": pd.to_numeric(frame.get("high"), errors="coerce"),
                "low": pd.to_numeric(frame.get("low"), errors="coerce"),
                "close": pd.to_numeric(frame.get("close"), errors="coerce"),
                "adj_close": pd.to_numeric(adj_close, errors="coerce"),
                "volume": pd.to_numeric(frame.get("volume"), errors="coerce"),
                "provider": self.provider_name,
                "ingested_at": pd.Timestamp(ingested_at).tz_convert("UTC"),
            }
        )
        out = out.dropna(subset=["date", "close"])
        return out[DAILY_PRICE_COLUMNS]

    def _normalize_quote(
        self,
        *,
        payload: dict[str, Any],
        symbol: str,
        ingested_at: datetime,
    ) -> dict[str, Any]:
        quote_ts = pd.to_datetime(payload.get("quote_ts"), utc=True, errors="coerce")
        if pd.isna(quote_ts):
            quote_ts = pd.Timestamp.utcnow()
        return {
            "symbol": symbol,
            "quote_ts": pd.Timestamp(quote_ts).tz_convert("UTC"),
            "price": _coerce_float(payload.get("price")),
            "previous_close": _coerce_float(payload.get("previous_close")),
            "open": _coerce_float(payload.get("open")),
            "high": _coerce_float(payload.get("high")),
            "low": _coerce_float(payload.get("low")),
            "volume": _coerce_float(payload.get("volume")),
            "provider": self.provider_name,
            "ingested_at": pd.Timestamp(ingested_at).tz_convert("UTC"),
        }


def _coerce_float(value: Any) -> float | None:
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(casted):
        return None
    return float(casted)


def _first_float(*values: Any) -> float | None:
    for raw in values:
        casted = _coerce_float(raw)
        if casted is not None:
            return casted
    return None


def _normalize_column_name(column: Any) -> str:
    if isinstance(column, tuple):
        parts = [str(part).strip() for part in column if str(part).strip()]
        if parts:
            raw = parts[0]
        else:
            raw = ""
    else:
        raw = str(column).strip()
    return raw.lower().replace(" ", "_")


def _provider_symbol_candidates(symbol: str) -> list[str]:
    candidates = normalize_symbol_for_provider(symbol, region=None, exchange=None)
    if symbol not in candidates:
        candidates.append(symbol)
    return candidates


def _safe_fast_info_payload(ticker: Any) -> dict[str, Any]:
    try:
        payload = getattr(ticker, "fast_info", {})
    except Exception:
        return {}
    try:
        return dict(payload or {})
    except Exception:
        return {}


def _safe_info_payload(ticker: Any) -> dict[str, Any]:
    try:
        payload = getattr(ticker, "info", {})
    except Exception:
        return {}
    try:
        return dict(payload or {})
    except Exception:
        return {}


def _safe_quote_history(ticker: Any) -> pd.DataFrame:
    history_payload: Any
    try:
        history_payload = ticker.history(period="5d", interval="1d", auto_adjust=False, actions=False)
    except TypeError:
        try:
            history_payload = ticker.history(period="5d", interval="1d", auto_adjust=False)
        except Exception:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()
    if isinstance(history_payload, pd.DataFrame):
        return history_payload
    return pd.DataFrame(history_payload)
