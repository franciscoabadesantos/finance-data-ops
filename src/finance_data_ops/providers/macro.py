"""Macro provider wrappers and canonical series catalog."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pandas as pd

MacroSource = Literal["fred", "yfinance"]
MacroFrequency = Literal["daily", "weekly", "monthly"]


@dataclass(frozen=True, slots=True)
class MacroSeriesSpec:
    key: str
    source: MacroSource
    source_code: str
    frequency: MacroFrequency
    required_by_default: bool
    optional: bool = False
    description: str = ""


STALENESS_MAX_BDAYS_BY_FREQUENCY: dict[MacroFrequency, int] = {
    "daily": 5,
    "weekly": 10,
    "monthly": 45,
}


MACRO_SERIES_CATALOG: tuple[MacroSeriesSpec, ...] = (
    MacroSeriesSpec("VIX", "yfinance", "^VIX", "daily", True, description="CBOE Volatility Index close."),
    MacroSeriesSpec("VIX3M", "yfinance", "^VIX3M", "daily", True, description="CBOE 3M volatility index close."),
    MacroSeriesSpec("VVIX", "yfinance", "^VVIX", "daily", True, description="CBOE VVIX close."),
    MacroSeriesSpec("10Y_Treasury_Yield", "fred", "DGS10", "daily", True, description="10Y treasury yield."),
    MacroSeriesSpec("2Y_Treasury_Yield", "fred", "DGS2", "daily", True, description="2Y treasury yield."),
    MacroSeriesSpec("High_Yield_Spread", "fred", "BAMLH0A0HYM2", "daily", True, description="US high yield OAS."),
    MacroSeriesSpec("TED_Spread", "fred", "TEDRATE", "daily", False, optional=True, description="TED spread optional."),
    MacroSeriesSpec("CPI_Headline", "fred", "CPIAUCSL", "monthly", False, description="Headline CPI."),
    MacroSeriesSpec("CPI_Core", "fred", "CPILFESL", "monthly", False, description="Core CPI."),
    MacroSeriesSpec("UNRATE", "fred", "UNRATE", "monthly", True, description="U-3 unemployment rate."),
    MacroSeriesSpec("U6RATE", "fred", "U6RATE", "monthly", False, description="U-6 unemployment rate."),
    MacroSeriesSpec("ICSA", "fred", "ICSA", "weekly", False, description="Initial jobless claims."),
    MacroSeriesSpec("CIVPART", "fred", "CIVPART", "monthly", False, description="Labor force participation."),
    MacroSeriesSpec("WTI", "fred", "DCOILWTICO", "daily", False, description="WTI spot price."),
    MacroSeriesSpec("Gasoline_US_Regular", "fred", "GASREGW", "weekly", False, description="US gasoline weekly."),
    MacroSeriesSpec("NatGas_HenryHub", "fred", "DHHNGSP", "daily", False, description="Henry Hub nat gas."),
    MacroSeriesSpec("DBC", "yfinance", "DBC", "daily", False, description="DBC ETF close."),
)


SERIES_BY_KEY: dict[str, MacroSeriesSpec] = {spec.key: spec for spec in MACRO_SERIES_CATALOG}
DEFAULT_REQUIRED_SERIES_KEYS: tuple[str, ...] = tuple(spec.key for spec in MACRO_SERIES_CATALOG if spec.required_by_default)
RELEASE_TIMED_SERIES: frozenset[str] = frozenset({"CPI_Headline", "CPI_Core", "UNRATE", "U6RATE", "CIVPART", "ICSA"})


class MacroProviderError(RuntimeError):
    """Raised when macro provider output cannot be normalized safely."""


class MacroDataProvider:
    """Provider boundary for macro series ingestion."""

    def __init__(self, *, provider_name: str = "mixed_fred_yfinance") -> None:
        self.provider_name = str(provider_name).strip() or "unknown_provider"

    def fetch_series(
        self,
        spec: MacroSeriesSpec,
        *,
        start: str | datetime,
        end: str | datetime,
    ) -> pd.Series:
        start_ts = pd.Timestamp(start).tz_localize(None)
        end_ts = pd.Timestamp(end).tz_localize(None)
        if end_ts < start_ts:
            raise ValueError("end date must be on/after start date")

        if spec.source == "fred":
            return self._fetch_fred_series(spec, start=start_ts, end=end_ts)
        if spec.source == "yfinance":
            return self._fetch_yfinance_series(spec, start=start_ts, end=end_ts)
        raise MacroProviderError(f"Unsupported macro source={spec.source!r} for {spec.key}.")

    def _fetch_fred_series(self, spec: MacroSeriesSpec, *, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        import io

        try:
            import requests
        except Exception as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("requests is required for FRED macro fetches.") from exc

        response = requests.get(
            "https://fred.stlouisfed.org/graph/fredgraph.csv",
            params={
                "id": spec.source_code,
                "cosd": start.date().isoformat(),
                "coed": end.date().isoformat(),
            },
            timeout=30,
        )
        response.raise_for_status()
        frame = pd.read_csv(io.StringIO(response.text))
        if frame is None or frame.empty:
            return pd.Series(dtype="float64", name=spec.key)
        date_col = "DATE" if "DATE" in frame.columns else frame.columns[0]
        value_col = spec.source_code if spec.source_code in frame.columns else frame.columns[-1]
        frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce")
        frame[value_col] = pd.to_numeric(frame[value_col], errors="coerce")
        frame = frame.loc[~pd.isna(frame[date_col])].copy()
        if frame.empty:
            return pd.Series(dtype="float64", name=spec.key)

        series = pd.Series(frame[value_col].to_numpy(), index=pd.DatetimeIndex(frame[date_col]), name=spec.key)
        series = series.dropna()
        if series.empty:
            return pd.Series(dtype="float64", name=spec.key)
        series.index = pd.to_datetime(series.index, errors="coerce")
        series = series[~pd.isna(series.index)]
        series.index = pd.DatetimeIndex(series.index).tz_localize(None)
        series = series.sort_index()
        series = series[~series.index.duplicated(keep="last")]
        series.name = spec.key
        return series

    def _fetch_yfinance_series(self, spec: MacroSeriesSpec, *, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
        try:
            import yfinance as yf
        except Exception as exc:  # pragma: no cover - runtime dependency
            raise RuntimeError("yfinance is required for yfinance macro fetches.") from exc

        history = yf.Ticker(spec.source_code).history(
            start=start.date().isoformat(),
            end=(end + pd.Timedelta(days=1)).date().isoformat(),
            interval="1d",
            # Match legacy Finance macro fetch behavior (yfinance default adjusted-close surface).
            auto_adjust=True,
        )
        if history is None or history.empty or "Close" not in history.columns:
            return pd.Series(dtype="float64", name=spec.key)

        series = pd.to_numeric(history["Close"], errors="coerce")
        series = series.dropna()
        if series.empty:
            return pd.Series(dtype="float64", name=spec.key)
        series.index = pd.to_datetime(series.index, errors="coerce")
        series = series[~pd.isna(series.index)]
        idx = pd.DatetimeIndex(series.index)
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        series.index = idx
        series = series.sort_index()
        series = series[~series.index.duplicated(keep="last")]
        series.name = spec.key
        return series


def catalog_frame(catalog: Iterable[MacroSeriesSpec] | None = None) -> pd.DataFrame:
    series_catalog = tuple(catalog or MACRO_SERIES_CATALOG)
    now_utc = datetime.now(UTC).isoformat()
    rows: list[dict[str, object]] = []
    for spec in series_catalog:
        rows.append(
            {
                "series_key": spec.key,
                "source_provider": spec.source,
                "source_code": spec.source_code,
                "frequency": spec.frequency,
                "required_by_default": bool(spec.required_by_default),
                "optional": bool(spec.optional),
                "staleness_max_bdays": int(STALENESS_MAX_BDAYS_BY_FREQUENCY[spec.frequency]),
                "release_calendar_source": _default_release_calendar_source(spec.key),
                "description": spec.description,
                "updated_at": now_utc,
            }
        )
    return pd.DataFrame(rows)


def _default_release_calendar_source(series_key: str) -> str | None:
    key = str(series_key).strip()
    if key in {"CPI_Headline", "CPI_Core"}:
        return "bls_cpi_release_calendar_v1"
    if key in {"UNRATE", "U6RATE", "CIVPART"}:
        return "bls_unrate_release_calendar_v1"
    if key == "ICSA":
        return "dol_icsa_release_calendar_v1"
    return None
