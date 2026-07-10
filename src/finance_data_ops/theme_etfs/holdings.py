"""Fetch and normalize thematic ETF holdings into the canonical ETF table shape."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
import io
import logging
import re
from typing import Any

import pandas as pd
import requests

from finance_data_ops.geography import country_from_source_or_symbol, normalize_country
from finance_data_ops.identity.provider_symbols import build_holding_onboarding_identities
from finance_data_ops.refresh.storage import read_parquet_table, write_parquet_table
from finance_data_ops.symbology import is_placeholder_identifier, normalize_listing_symbol, normalize_symbol_with_country
from finance_data_ops.theme_etfs.config import THEME_ETFS, ThemeETF


ETF_THEME_COLUMNS = [
    "etf_ticker",
    "theme",
    "wave",
    "issuer",
    "source_type",
    "source_ref",
    "holdings_count",
    "holdings_as_of",
    "holdings_source_depth",
    "holdings_shallow",
    "active",
    "fetched_at",
    "updated_at",
]

LOGGER = logging.getLogger("finance_data_ops.theme_etfs.holdings")
FetchBytes = Callable[[str], bytes]
MAX_SINGLE_HOLDING_WEIGHT = 0.50
MAX_TOTAL_HOLDINGS_WEIGHT = 1.35


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
        theme_rows.append(_theme_row(spec, holdings=holdings, active=True))

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
    try:
        return _fetch_single_theme_etf_holdings_from_source(
            spec,
            source_type=spec.source_type,
            source_ref=spec.source_ref,
            fetch_bytes=fetch_impl,
            fetched_at=now,
            shallow=False,
        )
    except Exception:
        if not spec.fallback_source_type:
            raise
        LOGGER.warning(
            "Theme ETF issuer holdings unavailable; using fallback (theme=%s etf=%s source=%s fallback=%s)",
            spec.theme,
            spec.etf_ticker,
            spec.source_type,
            spec.fallback_source_type,
            exc_info=True,
        )
        fallback = _fetch_single_theme_etf_holdings_from_source(
            spec,
            source_type=spec.fallback_source_type,
            source_ref=spec.fallback_source_ref or spec.etf_ticker,
            fetch_bytes=fetch_impl,
            fetched_at=now,
            shallow=True,
        )
        if fallback.empty:
            raise RuntimeError(f"Fallback holdings empty for {spec.etf_ticker}.")
        return fallback


def _fetch_single_theme_etf_holdings_from_source(
    spec: ThemeETF,
    *,
    source_type: str,
    source_ref: str,
    fetch_bytes: FetchBytes,
    fetched_at: pd.Timestamp,
    shallow: bool,
) -> pd.DataFrame:
    if source_type == "global_x_csv":
        csv_url = _resolve_global_x_csv_url(source_ref, fetch_bytes=fetch_bytes)
        raw = fetch_bytes(csv_url)
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=csv_url,
            shallow=shallow,
        )
    if source_type == "ark_csv":
        raw = fetch_bytes(source_ref)
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=shallow,
        )
    if source_type in {"vaneck_xlsx", "state_street_xlsx"}:
        raw = fetch_bytes(source_ref)
        frame = _read_excel_with_header_detection(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=shallow,
        )
    if source_type == "ishares_csv":
        csv_url = _resolve_ishares_csv_url(spec.etf_ticker, source_ref)
        raw = fetch_bytes(csv_url)
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=csv_url,
            shallow=shallow,
        )
    if source_type == "first_trust_html":
        raw = fetch_bytes(source_ref)
        frame = _read_first_trust_holdings_html(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=shallow,
        )
    if source_type == "advisorshares_csv":
        raw = fetch_bytes(source_ref)
        frame = _read_advisorshares_holdings_csv(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=shallow,
        )
    if source_type == "roundhill_csv":
        csv_url = _resolve_roundhill_csv_url(spec.etf_ticker, source_ref, fetch_bytes=fetch_bytes)
        raw = fetch_bytes(csv_url)
        frame = _read_roundhill_holdings_csv(raw, account=spec.etf_ticker)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=csv_url,
            shallow=shallow,
        )
    if source_type == "issuer_csv":
        raw = fetch_bytes(_resolve_generic_issuer_ref(spec, source_ref))
        frame = _read_csv_with_header_detection(raw)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=shallow,
        )
    if source_type == "yfinance_funds_data":
        frame = _fetch_yfinance_funds_data(spec.etf_ticker)
        return _normalize_holdings_frame(
            frame,
            spec=spec,
            fetched_at=fetched_at,
            source_type=source_type,
            source_ref=source_ref,
            shallow=True,
        )
    raise ValueError(f"Unsupported theme ETF source_type: {source_type}")


def write_theme_etf_outputs(
    *,
    holdings: pd.DataFrame,
    themes: pd.DataFrame,
    cache_root: str,
    replace_refreshed_holdings: bool = True,
    theme_etfs: Iterable[ThemeETF] | None = None,
    deactivate_missing_themes: bool = False,
) -> dict[str, str | int]:
    paths: dict[str, str | int] = {}
    themes_to_write = themes.copy()
    if bool(deactivate_missing_themes):
        themes_to_write = _append_inactive_theme_tombstones(
            themes=themes_to_write,
            cache_root=cache_root,
            theme_etfs=theme_etfs or THEME_ETFS,
        )
    _validate_active_themes_have_holdings(themes=themes_to_write, holdings=holdings)
    deactivated_tickers = _inactive_theme_tickers(themes_to_write)
    if not holdings.empty:
        holdings_to_write = holdings.copy()
        mode = "append"
        if bool(replace_refreshed_holdings):
            refreshed_etfs = {
                str(value).strip().upper()
                for value in holdings_to_write["etf_ticker"].dropna().tolist()
                if str(value).strip()
            }
            refreshed_etfs.update(deactivated_tickers)
            existing = read_parquet_table("etf_holdings", cache_root=cache_root, required=False)
            if not existing.empty and refreshed_etfs and "etf_ticker" in existing.columns:
                existing = existing.loc[
                    ~existing["etf_ticker"].astype(str).str.upper().isin(refreshed_etfs)
                ].copy()
                holdings_to_write = pd.concat([existing, holdings_to_write], ignore_index=True)
                mode = "replace"
        path = write_parquet_table(
            "etf_holdings",
            holdings_to_write,
            cache_root=cache_root,
            mode=mode,
            dedupe_subset=["etf_ticker", "holding_symbol", "as_of"],
        )
        paths["etf_holdings"] = str(path)
        paths["etf_holdings_rows"] = int(len(holdings.index))
        paths["etf_holdings_cache_rows"] = int(len(holdings_to_write.index))
    if not themes_to_write.empty:
        path = write_parquet_table(
            "etf_themes",
            themes_to_write,
            cache_root=cache_root,
            mode="append",
            dedupe_subset=["etf_ticker"],
        )
        paths["etf_themes"] = str(path)
        paths["etf_themes_rows"] = int(len(themes_to_write.index))
    if not holdings.empty:
        identity = build_holding_onboarding_identities(
            holdings=holdings,
            etf_themes=themes_to_write,
            entity_attributes=read_parquet_table("entity_attributes_static", cache_root=cache_root, required=False),
            ticker_registry=read_parquet_table("ticker_registry", cache_root=cache_root, required=False),
        )
        path = write_parquet_table(
            "etf_holding_onboarding_identity",
            identity,
            cache_root=cache_root,
            mode="replace",
            dedupe_subset=["etf_ticker", "source_symbol", "source_country"],
        )
        paths["etf_holding_onboarding_identity"] = str(path)
        paths["etf_holding_onboarding_identity_rows"] = int(len(identity.index))
    return paths


def _append_inactive_theme_tombstones(
    *,
    themes: pd.DataFrame,
    cache_root: str,
    theme_etfs: Iterable[ThemeETF],
) -> pd.DataFrame:
    existing = read_parquet_table("etf_themes", cache_root=cache_root, required=False)
    if existing.empty or "etf_ticker" not in existing.columns:
        return themes

    successful_tickers = _theme_tickers(themes)
    active_mask = (
        existing["active"].fillna(True).astype(bool)
        if "active" in existing.columns
        else pd.Series(True, index=existing.index)
    )
    existing_tickers = existing["etf_ticker"].astype(str).str.strip().str.upper()
    stale = existing.loc[active_mask & ~existing_tickers.isin(successful_tickers)].copy()
    if stale.empty:
        return themes

    stale = stale.drop_duplicates(subset=["etf_ticker"], keep="last")
    spec_by_ticker = {spec.etf_ticker.upper(): spec for spec in theme_etfs}
    now = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
    tombstones: list[dict[str, Any]] = []
    for _, row in stale.iterrows():
        ticker = str(row.get("etf_ticker", "")).strip().upper()
        spec = spec_by_ticker.get(ticker)
        tombstone = {column: row.get(column) for column in ETF_THEME_COLUMNS}
        if spec is not None:
            tombstone.update(
                {
                    "theme": spec.theme,
                    "wave": int(spec.wave),
                    "issuer": spec.issuer,
                    "source_type": spec.source_type,
                    "source_ref": spec.source_ref,
                }
            )
        tombstone.update(
            {
                "etf_ticker": ticker,
                "holdings_count": 0,
                "holdings_as_of": None,
                "holdings_source_depth": "unavailable",
                "holdings_shallow": True,
                "active": False,
                "fetched_at": now,
                "updated_at": now,
            }
        )
        tombstones.append(tombstone)

    tombstone_frame = pd.DataFrame(tombstones, columns=ETF_THEME_COLUMNS)
    if themes.empty:
        return tombstone_frame
    return pd.concat([themes, tombstone_frame], ignore_index=True)


def _theme_tickers(themes: pd.DataFrame) -> set[str]:
    if themes.empty or "etf_ticker" not in themes.columns:
        return set()
    return {
        str(value).strip().upper()
        for value in themes["etf_ticker"].dropna().tolist()
        if str(value).strip()
    }


def _inactive_theme_tickers(themes: pd.DataFrame) -> set[str]:
    if themes.empty or "etf_ticker" not in themes.columns or "active" not in themes.columns:
        return set()
    inactive = themes.loc[~themes["active"].fillna(True).astype(bool)]
    return _theme_tickers(inactive)


def _validate_active_themes_have_holdings(*, themes: pd.DataFrame, holdings: pd.DataFrame) -> None:
    if themes.empty:
        return
    if "active" not in themes.columns or "etf_ticker" not in themes.columns:
        return
    active = themes.loc[themes["active"].fillna(True).astype(bool)].copy()
    if active.empty:
        return
    holding_tickers = _theme_tickers(holdings)
    missing = sorted(set(active["etf_ticker"].astype(str).str.strip().str.upper()) - holding_tickers)
    if missing:
        raise RuntimeError(f"Active ETF themes have no refreshed holdings: {', '.join(missing)}")
    if "holdings_count" in active.columns:
        counts = pd.to_numeric(active["holdings_count"], errors="coerce").fillna(0)
        zero_count = sorted(active.loc[counts <= 0, "etf_ticker"].astype(str).str.strip().str.upper().tolist())
        if zero_count:
            raise RuntimeError(f"Active ETF themes report zero holdings: {', '.join(zero_count)}")


def _theme_row(spec: ThemeETF, *, holdings: pd.DataFrame, active: bool) -> dict[str, Any]:
    now = pd.Timestamp(datetime.now(UTC)).tz_convert("UTC")
    source_type = _single_frame_attr(holdings, "source_type", spec.source_type)
    source_ref = _single_frame_attr(holdings, "source_ref", spec.source_ref)
    source_depth = _single_frame_attr(holdings, "source_depth", "full")
    as_of_values = pd.to_datetime(holdings.get("as_of"), errors="coerce").dropna()
    return {
        "etf_ticker": spec.etf_ticker.upper(),
        "theme": spec.theme,
        "wave": int(spec.wave),
        "issuer": spec.issuer,
        "source_type": source_type,
        "source_ref": source_ref,
        "holdings_count": int(holdings["holding_symbol"].nunique()) if "holding_symbol" in holdings.columns else int(len(holdings.index)),
        "holdings_as_of": pd.Timestamp(as_of_values.max()).date() if not as_of_values.empty else None,
        "holdings_source_depth": source_depth,
        "holdings_shallow": source_depth == "shallow",
        "active": bool(active),
        "fetched_at": now,
        "updated_at": now,
    }


def _single_frame_attr(frame: pd.DataFrame, key: str, default: str) -> str:
    value = frame.attrs.get(key)
    if value:
        return str(value)
    if key in frame.columns and not frame.empty:
        values = [str(item) for item in frame[key].dropna().unique().tolist()]
        if values:
            return values[0]
    return default


def _resolve_global_x_csv_url(slug: str, *, fetch_bytes: FetchBytes) -> str:
    ticker = str(slug).strip().lower()
    failures: list[str] = []
    for candidate_date in _global_x_candidate_dates(datetime.now(UTC).date()):
        csv_url = (
            "https://assets.globalxetfs.com/funds/holdings/"
            f"{ticker}_full-holdings_{candidate_date:%Y%m%d}.csv"
        )
        try:
            fetch_bytes(csv_url)
            return csv_url
        except Exception as exc:
            failures.append(f"{csv_url}: {exc!r}")
    raise RuntimeError(f"Global X holdings CSV URL not found for {slug}. Tried: {'; '.join(failures)}")


def _global_x_candidate_dates(start_date: date, *, lookback_days: int = 10) -> list[date]:
    dates: list[date] = []
    for offset in range(lookback_days + 1):
        candidate = start_date - timedelta(days=offset)
        if candidate.weekday() < 5:
            dates.append(candidate)
    return dates


def _resolve_roundhill_csv_url(ticker: str, source_ref: str, *, fetch_bytes: FetchBytes) -> str:
    ticker = str(ticker).strip().upper()
    page_url = str(source_ref).strip() or f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"
    if not page_url.startswith("http"):
        page_url = f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/"
    page_text = fetch_bytes(page_url).decode("utf-8", errors="replace")
    if "Download CSV" not in page_text:
        raise RuntimeError(f"Roundhill holdings CSV link not found on {page_url}.")

    failures: list[str] = []
    for candidate_date in _roundhill_candidate_dates(datetime.now(UTC).date()):
        csv_url = (
            "https://www.roundhillinvestments.com/assets/data/"
            f"FilepointRoundhill.40RU.RU_Holdings_{candidate_date:%m%d%Y}.csv"
        )
        try:
            raw = fetch_bytes(csv_url)
            if _bytes_look_like_html(raw):
                raise RuntimeError("Roundhill dated CSV returned HTML.")
            return csv_url
        except Exception as exc:
            failures.append(f"{csv_url}: {exc!r}")
    raise RuntimeError(f"Roundhill holdings CSV URL not found for {ticker}. Tried: {'; '.join(failures)}")


def _roundhill_candidate_dates(start_date: date, *, lookback_days: int = 15) -> list[date]:
    dates: list[date] = []
    for offset in range(lookback_days + 1):
        candidate = start_date - timedelta(days=offset)
        if candidate.weekday() < 5:
            dates.append(candidate)
    return dates


def _resolve_ishares_csv_url(ticker: str, source_ref: str) -> str:
    raw = str(source_ref).strip()
    if raw.startswith("http"):
        return raw
    product_id, _, slug = raw.partition("/")
    if not product_id or not slug:
        raise RuntimeError(f"iShares source_ref must be '<product_id>/<slug>', got {source_ref!r}.")
    return (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document"
        "?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares&locale=en_US"
        f"&portfolioId={product_id}&userType=individual&asOfDate=&component=holdings"
    )


def _resolve_generic_issuer_ref(spec: ThemeETF, source_ref: str) -> str:
    raw = str(source_ref).strip()
    if raw.startswith("http"):
        return raw
    ticker = (raw or spec.etf_ticker).upper()
    issuer = spec.issuer.lower()
    if issuer == "invesco":
        return f"https://www.invesco.com/us/financial-products/etfs/holdings/main/holdings/0?action=download&ticker={ticker}"
    if issuer == "first trust":
        return f"https://www.ftportfolios.com/Common/ContentFileLoader.aspx?ContentGUID=holdings-{ticker.lower()}"
    if issuer == "amplify":
        return f"https://amplifyetfs.com/{ticker.lower()}-holdings.csv"
    if issuer == "advisorshares":
        return f"https://advisorshares.com/wp-content/uploads/fund-holdings/{ticker}.csv"
    if issuer == "roundhill":
        return f"https://www.roundhillinvestments.com/etf/{ticker}/full-holdings.csv"
    if issuer == "u.s. global":
        return f"https://www.usglobaletfs.com/holdings/{ticker}.csv"
    return raw


def _read_csv_with_header_detection(raw: bytes) -> pd.DataFrame:
    text = raw.decode("utf-8-sig", errors="replace")
    if _text_looks_like_html(text):
        raise RuntimeError("Holdings endpoint returned HTML instead of tabular holdings.")
    lines = text.splitlines()
    header_index = _find_header_index(lines)
    frame = pd.read_csv(io.StringIO("\n".join(lines[header_index:])))
    as_of = _extract_as_of_from_lines(lines[: header_index + 1])
    if as_of is not None:
        frame.attrs["as_of"] = as_of
    return frame


def _bytes_look_like_html(raw: bytes) -> bool:
    return _text_looks_like_html(raw.decode("utf-8-sig", errors="replace"))


def _text_looks_like_html(text: str) -> bool:
    stripped = text.lstrip().lower()
    return stripped.startswith("<!doctype html") or stripped.startswith("<html")


def _read_advisorshares_holdings_csv(raw: bytes) -> pd.DataFrame:
    frame = _read_csv_with_header_detection(raw)
    date_column = _first_existing_column(frame, _DATE_COLUMNS)
    if date_column and frame.attrs.get("as_of") is None:
        for value in frame[date_column].dropna().tolist():
            as_of = _coerce_date(value)
            if as_of is not None:
                frame.attrs["as_of"] = as_of
                break

    asset_group_column = _first_existing_column(frame, ["Asset Group"])
    if asset_group_column is None:
        return frame

    out = frame.copy()
    asset_groups = out[asset_group_column].astype(str).str.strip().str.upper()
    out["Asset Class"] = asset_groups.map(lambda value: "Equity" if value in {"FS", "S"} else "Other")
    return out


def _read_roundhill_holdings_csv(raw: bytes, *, account: str) -> pd.DataFrame:
    frame = _read_csv_with_header_detection(raw)
    account_column = _first_existing_column(frame, ["Account"])
    if account_column is None:
        raise RuntimeError("Roundhill holdings CSV missing Account column.")

    account_token = str(account).strip().upper()
    out = frame.loc[frame[account_column].astype(str).str.upper().eq(account_token)].copy()
    if out.empty:
        raise RuntimeError(f"Roundhill holdings CSV has no rows for {account_token}.")

    date_column = _first_existing_column(out, _DATE_COLUMNS)
    if date_column:
        dates = pd.to_datetime(out[date_column], utc=True, errors="coerce").dropna()
        if not dates.empty:
            out.attrs["as_of"] = pd.Timestamp(dates.max()).date()

    money_market_column = _first_existing_column(out, ["MoneyMarketFlag"])
    symbol_column = _first_existing_column(out, _SYMBOL_COLUMNS)
    if money_market_column or symbol_column:
        money_market = (
            out[money_market_column].astype(str).str.strip().str.upper().eq("Y")
            if money_market_column
            else pd.Series(False, index=out.index)
        )
        symbols = (
            out[symbol_column].astype(str).str.strip().str.upper()
            if symbol_column
            else pd.Series("", index=out.index)
        )
        out["Asset Class"] = [
            "Other" if is_money_market or symbol in {"CASH&OTHER"} else "Equity"
            for is_money_market, symbol in zip(money_market.tolist(), symbols.tolist(), strict=False)
        ]
    return out


def _read_first_trust_holdings_html(raw: bytes) -> pd.DataFrame:
    text = raw.decode("utf-8", errors="replace")
    if not text.lstrip().lower().startswith(("<!doctype html", "<html")):
        raise RuntimeError("First Trust holdings endpoint returned non-HTML content.")

    frames = pd.read_html(io.StringIO(text), attrs={"class": "fundSilverGrid"})
    if not frames:
        raise RuntimeError("First Trust holdings table not found.")

    as_of = _extract_as_of_from_lines(text.splitlines())
    for frame in frames:
        candidate = _promote_first_trust_header(frame)
        if _first_existing_column(candidate, _SYMBOL_COLUMNS) and _first_existing_column(candidate, _WEIGHT_COLUMNS):
            if as_of is not None:
                candidate.attrs["as_of"] = as_of
            return candidate
    raise RuntimeError("First Trust holdings table missing identifier/weight columns.")


def _promote_first_trust_header(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    first_row = [str(value).strip() for value in frame.iloc[0].tolist()]
    if "Identifier" in first_row and "Weighting" in first_row:
        out = frame.iloc[1:].copy()
        out.columns = first_row
        return out.reset_index(drop=True)
    return frame


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
            as_of = _extract_as_of_from_lines(lines[: header_index + 1])
            if as_of is not None:
                candidate.attrs["as_of"] = as_of
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
    "Stock Ticker",
    "StockTicker",
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
    "Security Description",
    "SecurityName",
    "Description",
    "Issuer Name",
    "Issue Name",
]
_WEIGHT_COLUMNS = [
    "weight (%)",
    "Weight (%)",
    "Weight (%) ",
    "% of Net Assets",
    "% of net assets",
    "Weight (%)",
    "Holding Percent",
    "holdingPercent",
    "Weight",
    "weight",
    "Percent",
    "Pct",
    "Portfolio Weight %",
    "Weighting",
    "Weightings",
]
_DATE_COLUMNS = ["date", "Date", "as_of", "asOfDate", "As Of", "Fund Holdings Data as of"]
_ASSET_CLASS_COLUMNS = ["asset_class", "Asset Class", "Class", "Security Type", "Type"]
_COUNTRY_COLUMNS = [
    "country",
    "Country",
    "location",
    "Location",
    "Country/Region",
    "Country / Region",
    "Domicile",
    "Geography",
    "Market",
]


def _extract_as_of_from_lines(lines: list[str]) -> date | None:
    for line in lines:
        line = re.sub(r"<[^>]+>", " ", line)
        date_match = re.search(
            r"(?:as\s+of|data\s+as\s+of|holdings\s+as\s+of)[^0-9]*"
            r"([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}|[0-9]{4}-[0-9]{1,2}-[0-9]{1,2})",
            line,
            flags=re.IGNORECASE,
        )
        if date_match:
            parsed = _coerce_date(date_match.group(1))
            if parsed:
                return parsed
        match = re.search(
            r"(?:as\s+of|data\s+as\s+of|holdings\s+as\s+of)[^A-Za-z0-9]*(.+)$",
            line,
            flags=re.IGNORECASE,
        )
        if match:
            parsed = _coerce_date(match.group(1).strip().strip(",").strip('"'))
            if parsed:
                return parsed
    return None


def _normalize_holdings_frame(
    frame: pd.DataFrame,
    *,
    spec: ThemeETF,
    fetched_at: pd.Timestamp,
    source_type: str,
    source_ref: str,
    shallow: bool,
) -> pd.DataFrame:
    if frame.empty:
        return _empty_holdings_frame()

    frame = _promote_symbol_index(frame)
    symbol_column = _first_existing_column(frame, _SYMBOL_COLUMNS)
    name_column = _first_existing_column(frame, _NAME_COLUMNS)
    weight_column = _first_existing_column(frame, _WEIGHT_COLUMNS)
    date_column = _first_existing_column(frame, _DATE_COLUMNS)
    asset_class_column = _first_existing_column(frame, _ASSET_CLASS_COLUMNS)
    country_column = _first_existing_column(frame, _COUNTRY_COLUMNS)
    if symbol_column is None or weight_column is None:
        raise RuntimeError(f"Missing required symbol/weight columns for {spec.etf_ticker}.")

    default_as_of = frame.attrs.get("as_of")
    rows: list[dict[str, Any]] = []
    for index, row in frame.iterrows():
        source_country = normalize_country(row.get(country_column)) if country_column else ""
        symbol = _normalize_holding_symbol(row.get(symbol_column), country=source_country)
        name = _coerce_text(row.get(name_column)) if name_column else None
        asset_class = _coerce_text(row.get(asset_class_column)) if asset_class_column else None
        if not _is_equity_like_holding(symbol=symbol, name=name, asset_class=asset_class):
            continue
        parsed_weight = _coerce_raw_weight(row.get(weight_column))
        if parsed_weight is None:
            continue
        as_of = _coerce_date(row.get(date_column)) if date_column else None
        if as_of is None:
            as_of = _coerce_date(default_as_of)
        rows.append(
            {
                "etf_ticker": spec.etf_ticker.upper(),
                "holding_symbol": symbol,
                "holding_name": name,
                "holding_country": country_from_source_or_symbol(source_country, symbol),
                "_raw_weight": parsed_weight[0],
                "_raw_weight_is_percent": parsed_weight[1],
                "as_of": as_of or fetched_at.date(),
                "source": f"theme_etf:{source_type}{':shallow' if shallow else ''}",
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    if not rows:
        return _empty_holdings_frame()
    out = pd.DataFrame(rows)
    out = out.dropna(subset=["holding_symbol", "as_of"])
    out = out.drop_duplicates(subset=["etf_ticker", "holding_symbol", "as_of"], keep="last")
    out["weight"] = _normalize_weight_values(
        out["_raw_weight"].tolist(),
        out["_raw_weight_is_percent"].tolist(),
    )
    out = out.drop(columns=["_raw_weight", "_raw_weight_is_percent"])
    out.attrs["source_type"] = source_type
    out.attrs["source_ref"] = source_ref
    out.attrs["source_depth"] = "shallow" if shallow else "full"
    _validate_holdings_weights(out, spec=spec)
    return out[
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
    ].reset_index(drop=True)


def _promote_symbol_index(frame: pd.DataFrame) -> pd.DataFrame:
    index_name = str(frame.index.name or "").strip()
    if index_name and _normalize_token(index_name) in {_normalize_token(value) for value in _SYMBOL_COLUMNS}:
        return frame.reset_index()
    return frame


def _validate_holdings_weights(frame: pd.DataFrame, *, spec: ThemeETF) -> None:
    if frame.empty or "weight" not in frame.columns:
        return
    weights = pd.to_numeric(frame["weight"], errors="coerce").dropna()
    if weights.empty:
        return
    max_weight = float(weights.max())
    total_weight = float(weights.sum())
    if max_weight > MAX_SINGLE_HOLDING_WEIGHT:
        row = frame.loc[pd.to_numeric(frame["weight"], errors="coerce").idxmax()]
        symbol = row.get("holding_symbol")
        name = row.get("holding_name")
        message = (
            f"Implausible ETF holding weight for {spec.etf_ticker}: "
            f"{symbol} {name} weight={max_weight:.4f}"
        )
        LOGGER.warning(message)
        raise RuntimeError(message)
    if total_weight > MAX_TOTAL_HOLDINGS_WEIGHT:
        message = f"Implausible ETF holdings weight sum for {spec.etf_ticker}: sum={total_weight:.4f}"
        LOGGER.warning(message)
        raise RuntimeError(message)


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
            "X-Requested-With": "XMLHttpRequest",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
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


def _normalize_holding_symbol(value: Any, *, country: Any = None) -> str | None:
    text = _coerce_text(value)
    if text is None:
        return None
    token = normalize_symbol_with_country(text, country) if country else normalize_listing_symbol(text)
    return token or None


def _is_equity_like_holding(*, symbol: str | None, name: str | None, asset_class: str | None = None) -> bool:
    if not symbol:
        return False
    token = symbol.strip().upper()
    name_token = str(name or "").strip().upper()
    asset_class_token = str(asset_class or "").strip().upper()
    if asset_class_token and asset_class_token not in {"EQUITY", "STOCK"}:
        return False
    currency_tokens = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "HKD", "SEK", "NOK", "DKK", "KRW", "CNY"}
    if token in {"", "NAN", "NONE", "NULL", "CASH", "CASH_USD"} | currency_tokens:
        return False
    if is_placeholder_identifier(token):
        return False
    if re.fullmatch(r"-?[A-Z]{3}\s*CASH-?", token):
        return False
    if re.fullmatch(r"[A-Z]{3}\s*(CURNCY|CURRENCY)", token):
        return False
    if name_token in {"CASH", "US DOLLAR", "U.S. DOLLAR"}:
        return False
    non_equity_markers = (
        "CASH",
        "CSH FND",
        "TREASURY BILL",
        "TREASURY SL AGENCY",
        "MONEY MARKET",
        "REPURCHASE AGREEMENT",
        "COLLATERAL",
        "FUTURE",
        "SWAP",
        "WARRANT",
        "OPTION",
        "RIGHTS",
        "RECEIVABLE",
        "PAYABLE",
    )
    if any(marker in name_token for marker in non_equity_markers):
        return False
    if re.search(r"[A-Z]", token):
        return True
    return asset_class_token in {"EQUITY", "STOCK"} and bool(re.fullmatch(r"[0-9.\\-]+", token))


def _coerce_weight(value: Any) -> float | None:
    parsed = _coerce_raw_weight(value)
    if parsed is None:
        return None
    return _normalize_weight_values([parsed[0]], [parsed[1]])[0]


def _coerce_raw_weight(value: Any) -> tuple[float, bool] | None:
    if value is None:
        return None
    raw_text = str(value).strip()
    is_percent = "%" in raw_text
    text = raw_text.replace("%", "").replace(",", "")
    numeric = pd.to_numeric(text, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric), is_percent


def _normalize_weight_values(values: list[float], percent_flags: list[bool]) -> list[float]:
    if not values:
        return []
    raw_weights = pd.Series(values, dtype="float64")
    total_weight = float(raw_weights.sum())
    max_weight = float(raw_weights.max())
    uses_percent_scale = (
        any(bool(flag) for flag in percent_flags)
        or total_weight > MAX_TOTAL_HOLDINGS_WEIGHT
        or max_weight > 1.0
    )
    scale = 100.0 if uses_percent_scale else 1.0
    return [round(float(value) / scale, 10) for value in raw_weights.tolist()]


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
            "holding_country",
            "weight",
            "as_of",
            "source",
            "fetched_at",
            "updated_at",
        ]
    )
