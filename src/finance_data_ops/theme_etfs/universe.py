"""Wave-gated thematic ETF constituent expansion for the ticker registry."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_payload
from finance_data_ops.validation.ticker_registry import build_registry_key, read_ticker_registry


MetadataLookup = Callable[[str], Mapping[str, Any] | dict[str, Any] | None]

NON_EQUITY_SYMBOLS = {"", "NAN", "NONE", "NULL", "CASH", "USD", "EUR", "GBP", "JPY", "CAD", "AUD"}
SUPPORTED_WAVES = {1, 2}


def build_wave_universe_additions(
    *,
    holdings: pd.DataFrame,
    etf_themes: pd.DataFrame,
    existing_registry: pd.DataFrame | None = None,
    cache_root: str | Path | None = None,
    wave: int = 1,
    max_new_tickers: int = 125,
    batch_size: int = 25,
    metadata_lookup: MetadataLookup | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if int(wave) not in SUPPORTED_WAVES:
        raise ValueError("wave must be 1 or 2.")
    if max_new_tickers < 1:
        raise ValueError("max_new_tickers must be positive.")
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")

    registry = (
        existing_registry.copy()
        if existing_registry is not None
        else read_ticker_registry(cache_root=cache_root or "data_cache")
    )
    current_symbols = _current_registry_symbols(registry, ignore_theme_wave=wave)
    candidates = _rank_wave_candidates(holdings=holdings, etf_themes=etf_themes, wave=wave)
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame(), _summary(wave=wave, candidates=0, selected=0, skipped_existing=0)

    rows: list[dict[str, Any]] = []
    skipped_existing = 0
    selected_count = 0
    lookup = metadata_lookup or _default_metadata_lookup
    for _, candidate in candidates.iterrows():
        symbol = str(candidate["normalized_symbol"]).strip().upper()
        if symbol in current_symbols:
            skipped_existing += 1
            continue
        metadata = _safe_metadata_lookup(lookup, symbol)
        registry_row = _build_registry_row(symbol=symbol, candidate=candidate, metadata=metadata, wave=wave)
        rows.append(registry_row)
        current_symbols.add(symbol)
        selected_count += 1
        if selected_count >= max_new_tickers:
            break

    registry_rows = pd.DataFrame(rows)
    if not registry_rows.empty:
        registry_rows["theme_ramp_batch"] = ((registry_rows.index // int(batch_size)) + 1).astype(int)
    entity_rows = pd.DataFrame(build_entity_attributes_static_payload(rows))
    summary = _summary(
        wave=wave,
        candidates=int(len(candidates.index)),
        selected=int(len(registry_rows.index)),
        skipped_existing=skipped_existing,
    )
    summary["max_new_tickers"] = int(max_new_tickers)
    summary["batch_size"] = int(batch_size)
    summary["batches"] = int(registry_rows["theme_ramp_batch"].max()) if "theme_ramp_batch" in registry_rows else 0
    return registry_rows, entity_rows, summary


def _rank_wave_candidates(*, holdings: pd.DataFrame, etf_themes: pd.DataFrame, wave: int) -> pd.DataFrame:
    if holdings.empty or etf_themes.empty:
        return pd.DataFrame()
    themes = etf_themes.copy()
    themes["etf_ticker"] = themes["etf_ticker"].astype(str).str.upper()
    themes["wave"] = pd.to_numeric(themes["wave"], errors="coerce").fillna(0).astype(int)
    wave_etfs = themes.loc[themes["wave"] == int(wave), ["etf_ticker", "theme", "wave"]]
    if wave_etfs.empty:
        return pd.DataFrame()

    frame = holdings.copy()
    frame["etf_ticker"] = frame["etf_ticker"].astype(str).str.upper()
    frame = frame.merge(wave_etfs, on="etf_ticker", how="inner")
    if frame.empty:
        return pd.DataFrame()
    frame["normalized_symbol"] = frame["holding_symbol"].map(_normalize_candidate_symbol)
    frame["weight"] = pd.to_numeric(frame.get("weight"), errors="coerce").fillna(0.0).clip(lower=0.0)
    frame = frame.loc[frame.apply(_is_candidate_equity_row, axis=1)].copy()
    if frame.empty:
        return pd.DataFrame()

    grouped = (
        frame.groupby("normalized_symbol", as_index=False)
        .agg(
            aggregate_weight=("weight", "sum"),
            theme_count=("theme", "nunique"),
            etf_count=("etf_ticker", "nunique"),
            themes=("theme", lambda values: ",".join(sorted({str(v) for v in values if str(v).strip()}))),
            source_etfs=("etf_ticker", lambda values: ",".join(sorted({str(v) for v in values if str(v).strip()}))),
            holding_name=("holding_name", _first_non_empty),
        )
        .sort_values(["theme_count", "aggregate_weight", "normalized_symbol"], ascending=[False, False, True])
        .reset_index(drop=True)
    )
    return grouped


def _build_registry_row(
    *,
    symbol: str,
    candidate: pd.Series,
    metadata: Mapping[str, Any],
    wave: int,
) -> dict[str, Any]:
    now_iso = datetime.now(UTC).isoformat()
    country = _metadata_country(symbol, metadata)
    region = _region_for_country(country)
    exchange = _metadata_text(metadata, "exchange", "fullExchangeName")
    exchange_mic = _metadata_text(metadata, "exchangeMic", "exchange_mic")
    currency = _metadata_text(metadata, "currency", "financialCurrency") or "USD"
    sector = _metadata_text(metadata, "sector")
    instrument_type = _metadata_instrument_type(symbol, metadata)
    themes = str(candidate.get("themes") or "")
    source_etfs = str(candidate.get("source_etfs") or "")
    notes = (
        "created_by=theme_etf_universe;"
        f"wave={int(wave)};"
        f"themes={themes};"
        f"source_etfs={source_etfs};"
        f"aggregate_weight={float(candidate.get('aggregate_weight') or 0.0):.8f};"
        f"theme_count={int(candidate.get('theme_count') or 0)}"
    )
    return {
        "registry_key": build_registry_key(input_symbol=symbol, region=region.lower(), exchange=exchange),
        "input_symbol": symbol,
        "normalized_symbol": symbol,
        "region": region.lower(),
        "exchange": exchange,
        "exchange_mic": exchange_mic,
        "currency": currency,
        "instrument_type": instrument_type,
        "status": "active",
        "market_supported": True,
        "fundamentals_supported": instrument_type in {"equity", "adr"},
        "earnings_supported": instrument_type in {"equity", "adr"},
        "validation_status": "validated_market_only",
        "validation_reason": "theme_etf_constituent_wave_gate",
        "promotion_status": "validated_market_only",
        "last_validated_at": now_iso,
        "notes": notes,
        "updated_at": now_iso,
        "country": country,
        "sector": sector,
    }


def _current_registry_symbols(registry: pd.DataFrame, *, ignore_theme_wave: int | None = None) -> set[str]:
    if registry.empty or "normalized_symbol" not in registry.columns:
        return set()
    active = registry.copy()
    if "status" in active.columns:
        active = active.loc[active["status"].astype(str).str.lower() == "active"]
    if ignore_theme_wave is not None and "notes" in active.columns:
        notes = active["notes"].astype(str)
        generated_same_wave = notes.str.contains("created_by=theme_etf_universe", regex=False) & notes.str.contains(
            f"wave={int(ignore_theme_wave)};",
            regex=False,
        )
        active = active.loc[~generated_same_wave]
    return {
        str(value).strip().upper()
        for value in active["normalized_symbol"].dropna().tolist()
        if str(value).strip().upper() not in NON_EQUITY_SYMBOLS
    }


def _normalize_candidate_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _is_candidate_equity_row(row: pd.Series) -> bool:
    symbol = _normalize_candidate_symbol(row.get("normalized_symbol"))
    name = str(row.get("holding_name") or "").strip().upper()
    if symbol in NON_EQUITY_SYMBOLS:
        return False
    if " " in symbol:
        return False
    if any(token in name for token in ("CASH", "TREASURY BILL", "MONEY MARKET", "FUTURE", "SWAP")):
        return False
    if symbol.startswith("^"):
        return False
    return True


def _default_metadata_lookup(symbol: str) -> dict[str, Any]:
    try:
        import yfinance as yf
    except Exception:
        return {}
    try:
        return dict(getattr(yf.Ticker(symbol), "info", {}) or {})
    except Exception:
        return {}


def _safe_metadata_lookup(lookup: MetadataLookup, symbol: str) -> dict[str, Any]:
    try:
        payload = lookup(symbol)
    except Exception:
        return {}
    return dict(payload or {}) if isinstance(payload, Mapping) else {}


def _metadata_instrument_type(symbol: str, metadata: Mapping[str, Any]) -> str:
    quote_type = str(metadata.get("quoteType") or metadata.get("quote_type") or "").strip().upper()
    if quote_type == "ADR":
        return "adr"
    if quote_type in {"ETF", "MUTUALFUND", "FUND", "INDEX"}:
        return "unknown"
    if symbol.endswith(".L") or symbol.endswith(".DE") or symbol.endswith(".PA"):
        return "equity"
    return "equity"


def _metadata_country(symbol: str, metadata: Mapping[str, Any]) -> str:
    country = _metadata_text(metadata, "country", "countryCode", "country_code")
    if country:
        return _normalize_country(country)
    for suffix, country_code in _SUFFIX_TO_COUNTRY.items():
        if symbol.endswith(suffix):
            return country_code
    return "US"


_COUNTRY_NAME_TO_CODE = {
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
    "USA": "US",
    "TAIWAN": "TW",
    "JAPAN": "JP",
    "UNITED KINGDOM": "GB",
    "GREAT BRITAIN": "GB",
    "SWITZERLAND": "CH",
    "GERMANY": "DE",
    "FRANCE": "FR",
    "ITALY": "IT",
    "NETHERLANDS": "NL",
    "CANADA": "CA",
    "AUSTRALIA": "AU",
    "SOUTH KOREA": "KR",
    "KOREA": "KR",
    "CHINA": "CN",
    "HONG KONG": "HK",
    "ISRAEL": "IL",
    "BRAZIL": "BR",
}


def _normalize_country(country: str) -> str:
    token = str(country or "").strip().upper()
    if len(token) == 2:
        return token
    return _COUNTRY_NAME_TO_CODE.get(token, token)


_SUFFIX_TO_COUNTRY = {
    ".L": "GB",
    ".DE": "DE",
    ".PA": "FR",
    ".AS": "NL",
    ".SW": "CH",
    ".ST": "SE",
    ".HK": "HK",
    ".T": "JP",
    ".AX": "AU",
    ".TO": "CA",
    ".KS": "KR",
    ".KQ": "KR",
    ".TW": "TW",
    ".MI": "IT",
    ".MC": "ES",
    ".CO": "DK",
    ".OL": "NO",
}


def _region_for_country(country: str) -> str:
    normalized = str(country or "US").strip().upper()
    if normalized in {"US", "CA"}:
        return "us"
    if normalized in {"GB", "DE", "FR", "NL", "CH", "SE", "IT", "ES", "DK", "NO", "PT"}:
        return "eu"
    return "apac"


def _metadata_text(metadata: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = metadata.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "null"}:
            return text
    return None


def _first_non_empty(values: pd.Series) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text and text.lower() not in {"nan", "none", "null"}:
            return text
    return None


def _summary(*, wave: int, candidates: int, selected: int, skipped_existing: int) -> dict[str, Any]:
    return {
        "wave": int(wave),
        "candidate_tickers": int(candidates),
        "new_tickers_selected": int(selected),
        "skipped_existing": int(skipped_existing),
    }
