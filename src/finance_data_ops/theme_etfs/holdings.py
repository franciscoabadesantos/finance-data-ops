"""Fetch and normalize thematic ETF holdings into the canonical ETF table shape."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime
import io
import logging
import re
from typing import Any

import pandas as pd
import requests

from finance_data_ops.refresh.storage import write_parquet_table
from finance_data_ops.theme_etfs.config import THEME_ETFS, ThemeETF


ETF_THEME_COLUMNS = [
    "etf_ticker",
    "theme",
    "wave",
    "issuer",
    "source_type",
    "source_ref",
    "active",
    "fetched_at",
    "updated_at",
]

LOGGER = logging.getLogger("finance_data_ops.theme_etfs.holdings")
FetchBytes = Callable[[str], bytes]


def fetch_theme_etf_holdings(
    *,
    theme_etfs: Iterable[ThemeETF] = THEME_ETFS,
    fetch_bytes: FetchBytes | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    fetch_impl = fetch_bytes or _fetch_url_bytes
    holding_frames: list[pd.DataFrame] = []
    theme_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for spec in theme_etfs:
        try:
            holdings = fetch_single_theme_etf_holdings(spec, fetch_bytes=fetch_impl)
        except Exception as exc:
            LOGGER.warning("Theme ETF holdings unavailable (theme=%s etf=%s): %r", spec.theme, spec.etf_ticker, exc)
            failures.append({"theme": spec.theme, "etf_ticker": spec.etf_ticker, "error": repr(exc)})
            continue
        if holdings.empty:
            failures.append({"theme": spec.theme, "etf_ticker": spec.etf_ticker, "error": "empty_holdings"})
            continue
        holding_frames.append(holdings)
        theme_rows.append(_theme_row(spec, active=True))

    holdings_frame = pd.concat(holding_frames, ignore_index=True) if holding_frames else _empty_holdings_frame()
    if not holdings_frame.empty:
        holdings_frame = holdings_frame.drop_duplicates(
            subset=["etf_ticker", "holding_symbol", "as_of"],
            keep="last",
        ).reset_index(drop=True)
    themes_frame = pd.DataFrame(theme_rows, columns=ETF_THEME_COLUMNS) if theme_rows else pd.DataFrame(columns=ETF_THEME_COLUMNS)
    return holdings_frame, themes_frame, failures


def fetch_single_theme_etf_holdings(spec: ThemeETF, *, fetch_bytes: FetchBytes | None = None) -> pd.DataFrame:
    fetch_impl = fetch_bytes or _fetch_url_bytes
    now = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
    if spec.source_type == "global_x_csv":
        csv_url = _resolve_global_x_csv_url(spec.source_ref, fetch_bytes=fetch_impl)
        raw = fetch_impl(csv_url)
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(frame, spec=spec, fetched_at=now, source_ref=csv_url)
    if spec.source_type == "ark_csv":
        raw = fetch_impl(spec.source_ref)
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(frame, spec=spec, fetched_at=now, source_ref=spec.source_ref)
    if spec.source_type in {"vaneck_xlsx", "state_street_xlsx"}:
        raw = fetch_impl(spec.source_ref)
        frame = _read_excel_with_header_detection(raw)
        return _normalize_holdings_frame(frame, spec=spec, fetched_at=now, source_ref=spec.source_ref)
    if spec.source_type == "yfinance_funds_data":
        frame = _fetch_yfinance_funds_data(spec.etf_ticker)
        return _normalize_holdings_frame(frame, spec=spec, fetched_at=now, source_ref=spec.source_ref)
    raise ValueError(f"Unsupported theme ETF source_type: {spec.source_type}")


def write_theme_etf_outputs(*, holdings: pd.DataFrame, themes: pd.DataFrame, cache_root: str) -> dict[str, str | int]:
    paths: dict[str, str | int] = {}
    if not holdings.empty:
        path = write_parquet_table(
            "etf_holdings",
            holdings,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["etf_ticker", "holding_symbol", "as_of"],
        )
        paths["etf_holdings"] = str(path)
        paths["etf_holdings_rows"] = int(len(holdings.index))
    if not themes.empty:
        path = write_parquet_table(
            "etf_themes",
            themes,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["etf_ticker"],
        )
        paths["etf_themes"] = str(path)
        paths["etf_themes_rows"] = int(len(themes.index))
    return paths


def _theme_row(spec: ThemeETF, *, active: bool) -> dict[str, Any]:
    now = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
    return {
        "etf_ticker": spec.etf_ticker.upper(),
        "theme": spec.theme,
        "wave": int(spec.wave),
        "issuer": spec.issuer,
        "source_type": spec.source_type,
        "source_ref": spec.source_ref,
        "active": bool(active),
        "fetched_at": now,
        "updated_at": now,
    }


def _resolve_global_x_csv_url(slug: str, *, fetch_bytes: FetchBytes) -> str:
    page_url = f"https://www.globalxetfs.com/funds/{str(slug).strip().lower()}?download_full_holdings=true"
    text = fetch_bytes(page_url).decode("utf-8", errors="replace")
    match = re.search(r"https://assets\.globalxetfs\.com/(?:funds/)?holdings/[^\"\\]+\.csv", text)
    if not match:
        raise RuntimeError(f"Global X holdings CSV URL not found for {slug}.")
    return match.group(0)


def _read_csv_with_header_detection(raw: bytes) -> pd.DataFrame:
    text = raw.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()
    header_index = _find_header_index(lines)
    return pd.read_csv(io.StringIO("\n".join(lines[header_index:])))


def _read_excel_with_header_detection(raw: bytes) -> pd.DataFrame:
    workbook = pd.ExcelFile(io.BytesIO(raw))
    best: pd.DataFrame | None = None
    for sheet in workbook.sheet_names:
        raw_frame = pd.read_excel(workbook, sheet_name=sheet, header=None)
        if raw_frame.empty:
            continue
        lines = [",".join("" if pd.isna(value) else str(value) for value in row) for row in raw_frame.to_numpy()]
        header_index = _find_header_index(lines)
        candidate = pd.read_excel(workbook, sheet_name=sheet, header=header_index)
        if _first_existing_column(candidate, _SYMBOL_COLUMNS) and _first_existing_column(candidate, _WEIGHT_COLUMNS):
            best = candidate
            break
    if best is None:
        raise RuntimeError("No holdings-like sheet found in workbook.")
    return best


_SYMBOL_COLUMNS = [
    "ticker",
    "Ticker",
    "symbol",
    "Symbol",
    "holding_symbol",
    "Holding Ticker",
    "Identifier",
]
_NAME_COLUMNS = [
    "company",
    "Company",
    "name",
    "Name",
    "holding_name",
    "Holding Name",
    "Security Name",
    "Description",
]
_WEIGHT_COLUMNS = [
    "weight (%)",
    "Weight (%)",
    "% of Net Assets",
    "% of net assets",
    "Holding Percent",
    "holdingPercent",
    "Weight",
    "weight",
    "Percent",
    "Pct",
]
_DATE_COLUMNS = ["date", "Date", "as_of", "asOfDate", "As Of", "Fund Holdings Data as of"]


def _normalize_holdings_frame(
    frame: pd.DataFrame,
    *,
    spec: ThemeETF,
    fetched_at: pd.Timestamp,
    source_ref: str,
) -> pd.DataFrame:
    if frame.empty:
        return _empty_holdings_frame()

    frame = _promote_symbol_index(frame)
    symbol_column = _first_existing_column(frame, _SYMBOL_COLUMNS)
    name_column = _first_existing_column(frame, _NAME_COLUMNS)
    weight_column = _first_existing_column(frame, _WEIGHT_COLUMNS)
    date_column = _first_existing_column(frame, _DATE_COLUMNS)
    if symbol_column is None or weight_column is None:
        raise RuntimeError(f"Missing required symbol/weight columns for {spec.etf_ticker}.")

    rows: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        symbol = _normalize_holding_symbol(row.get(symbol_column))
        name = _coerce_text(row.get(name_column)) if name_column else None
        if not _is_equity_like_holding(symbol=symbol, name=name):
            continue
        as_of = _coerce_date(row.get(date_column)) if date_column else None
        rows.append(
            {
                "etf_ticker": spec.etf_ticker.upper(),
                "holding_symbol": symbol,
                "holding_name": name,
                "weight": _coerce_weight(row.get(weight_column)),
                "as_of": as_of or fetched_at.date(),
                "source": f"theme_etf:{spec.source_type}",
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    if not rows:
        return _empty_holdings_frame()
    out = pd.DataFrame(rows)
    out = out.dropna(subset=["holding_symbol", "as_of"])
    out = out.drop_duplicates(subset=["etf_ticker", "holding_symbol", "as_of"], keep="last")
    return out[
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
    ].reset_index(drop=True)


def _promote_symbol_index(frame: pd.DataFrame) -> pd.DataFrame:
    index_name = str(frame.index.name or "").strip()
    if index_name and _normalize_token(index_name) in {_normalize_token(value) for value in _SYMBOL_COLUMNS}:
        return frame.reset_index()
    return frame


def _fetch_yfinance_funds_data(ticker: str) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError("yfinance is required for yfinance_funds_data theme ETF sources.") from exc
    funds_data = yf.Ticker(ticker).get_funds_data()
    holdings = getattr(funds_data, "top_holdings", None)
    if holdings is None:
        holdings = getattr(funds_data, "topHoldings", None)
    if holdings is None:
        return pd.DataFrame()
    return holdings.copy() if isinstance(holdings, pd.DataFrame) else pd.DataFrame(holdings)


def _fetch_url_bytes(url: str) -> bytes:
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 finance-data-ops/0.1",
            "Accept": "text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        },
        timeout=60,
    )
    response.raise_for_status()
    return bytes(response.content)


def _find_header_index(lines: list[str]) -> int:
    for index, line in enumerate(lines):
        token = _normalize_token(line)
        if any(symbol in token for symbol in ("ticker", "symbol", "identifier")) and any(
            weight in token for weight in ("weight", "netassets", "pct")
        ):
            return index
    return 0


def _first_existing_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {_normalize_token(column): str(column) for column in frame.columns}
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
        token = _normalize_token(candidate)
        if token in normalized:
            return normalized[token]
    return None


_BLOOMBERG_SUFFIX_TO_YAHOO = {
    "US": "",
    "UW": "",
    "UN": "",
    "UR": "",
    "UQ": "",
    "LN": ".L",
    "L": ".L",
    "GR": ".DE",
    "GY": ".DE",
    "DE": ".DE",
    "FP": ".PA",
    "NA": ".AS",
    "SW": ".SW",
    "SE": ".ST",
    "SS": ".SS",
    "CH": ".SW",
    "HK": ".HK",
    "JP": ".T",
    "JT": ".T",
    "AU": ".AX",
    "AT": ".AX",
    "CN": ".TO",
    "CT": ".TO",
    "KS": ".KS",
    "KQ": ".KQ",
    "TT": ".TW",
    "TW": ".TW",
    "IT": ".MI",
    "IM": ".MI",
    "SM": ".MC",
    "DC": ".CO",
    "NO": ".OL",
}


def _normalize_holding_symbol(value: Any) -> str | None:
    text = _coerce_text(value)
    if text is None:
        return None
    token = text.upper().replace("/", "-").strip()
    token = re.sub(r"\s+", " ", token)
    if " " in token:
        base, suffix = token.rsplit(" ", 1)
        if suffix in _BLOOMBERG_SUFFIX_TO_YAHOO:
            return f"{base}{_BLOOMBERG_SUFFIX_TO_YAHOO[suffix]}"
    return token


def _is_equity_like_holding(*, symbol: str | None, name: str | None) -> bool:
    if not symbol:
        return False
    token = symbol.strip().upper()
    name_token = str(name or "").strip().upper()
    if token in {"", "NAN", "NONE", "NULL", "CASH", "USD", "EUR", "GBP", "JPY", "CAD", "AUD"}:
        return False
    if name_token in {"CASH", "US DOLLAR", "U.S. DOLLAR"}:
        return False
    if "CASH" in name_token or "TREASURY BILL" in name_token or "MONEY MARKET" in name_token:
        return False
    return bool(re.search(r"[A-Z]", token))


def _coerce_weight(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("%", "").replace(",", "")
    numeric = pd.to_numeric(text, errors="coerce")
    if pd.isna(numeric):
        return None
    weight = float(numeric)
    if weight > 1.0:
        return weight / 100.0
    return weight


def _coerce_date(value: Any) -> date | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def _coerce_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat", "<na>"}:
        return None
    return text


def _normalize_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _empty_holdings_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "etf_ticker",
            "holding_symbol",
            "holding_name",
            "weight",
            "as_of",
            "source",
            "fetched_at",
            "updated_at",
        ]
    )
