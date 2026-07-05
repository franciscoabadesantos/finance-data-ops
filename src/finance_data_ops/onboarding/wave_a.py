"""Wave A onboarding: ITB homebuilders plus US-listed GDX gold miners."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import re
from typing import Any

import pandas as pd

from finance_data_ops.geography import infer_country_from_symbol
from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_payload
from finance_data_ops.symbology import is_placeholder_identifier, normalize_listing_symbol
from finance_data_ops.validation.ticker_registry import build_registry_key


WAVE_A_NAME = "wave_a_itb_gdx_us"
WAVE_A_ETFS = ("ITB", "GDX")
FULL_HISTORY_START_DATE = "1900-01-01"
DEEP_EARNINGS_HISTORY_LIMIT = 120

_US_REGION = "us"
_ACTIVE_PROMOTIONS = {"validated_market_only", "validated_full"}
_NON_EQUITY_SYMBOLS = {
    "",
    "NAN",
    "NONE",
    "NULL",
    "CASH",
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "CAD",
    "AUD",
    "CHF",
    "HKD",
}


@dataclass(frozen=True, slots=True)
class WaveAOnboardingPayloads:
    targets: pd.DataFrame
    registry_rows: list[dict[str, Any]]
    entity_rows: list[dict[str, Any]]
    symbols_to_backfill: list[str]
    summary: dict[str, Any]


def build_wave_a_onboarding_payloads(
    *,
    holdings: pd.DataFrame,
    existing_registry: pd.DataFrame | None = None,
) -> WaveAOnboardingPayloads:
    targets = build_wave_a_targets(holdings)
    existing_symbols = _existing_universe_symbols(existing_registry)
    new_targets = targets.loc[~targets["symbol"].isin(existing_symbols)].copy()
    registry_rows = build_wave_a_registry_rows(new_targets)
    entity_rows = build_entity_attributes_static_payload(registry_rows)
    symbols_to_backfill = sorted(new_targets["symbol"].astype(str).str.upper().tolist())
    summary = {
        "wave": WAVE_A_NAME,
        "target_etfs": list(WAVE_A_ETFS),
        "target_symbols": int(len(targets.index)),
        "existing_symbols_skipped": int(len(targets.index) - len(new_targets.index)),
        "new_symbols_to_onboard": int(len(new_targets.index)),
        "symbols_to_backfill": symbols_to_backfill,
        "full_history_start_date": FULL_HISTORY_START_DATE,
        "earnings_history_limit": DEEP_EARNINGS_HISTORY_LIMIT,
        "by_source_etf": targets.groupby("source_etfs")["symbol"].nunique().sort_index().to_dict()
        if not targets.empty
        else {},
    }
    return WaveAOnboardingPayloads(
        targets=targets,
        registry_rows=registry_rows,
        entity_rows=entity_rows,
        symbols_to_backfill=symbols_to_backfill,
        summary=summary,
    )


def build_wave_a_targets(holdings: pd.DataFrame) -> pd.DataFrame:
    if holdings.empty:
        return _empty_targets()
    frame = holdings.copy()
    if "etf_ticker" not in frame.columns or "holding_symbol" not in frame.columns:
        return _empty_targets()
    frame["etf_ticker"] = frame["etf_ticker"].astype(str).str.strip().str.upper()
    frame = frame.loc[frame["etf_ticker"].isin(WAVE_A_ETFS)].copy()
    if frame.empty:
        return _empty_targets()

    frame["symbol"] = frame["holding_symbol"].map(normalize_listing_symbol)
    frame["holding_name"] = frame.get("holding_name", pd.Series(index=frame.index, dtype=object)).map(_text_or_none)
    frame["weight"] = pd.to_numeric(frame.get("weight"), errors="coerce")
    frame = frame.loc[frame.apply(_is_wave_a_target_row, axis=1)].copy()
    if frame.empty:
        return _empty_targets()

    grouped = (
        frame.groupby("symbol", as_index=False)
        .agg(
            name=("holding_name", _first_non_empty),
            aggregate_weight=("weight", "sum"),
            source_etfs=("etf_ticker", lambda values: ",".join(sorted(set(map(str, values))))),
        )
        .sort_values(["source_etfs", "aggregate_weight", "symbol"], ascending=[True, False, True])
        .reset_index(drop=True)
    )
    return grouped[["symbol", "name", "source_etfs", "aggregate_weight"]].copy()


def build_wave_a_registry_rows(targets: pd.DataFrame) -> list[dict[str, Any]]:
    if targets.empty:
        return []
    now_iso = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for _, target in targets.iterrows():
        symbol = str(target.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        source_etfs = str(target.get("source_etfs") or "")
        aggregate_weight = float(target.get("aggregate_weight") or 0.0)
        rows.append(
            {
                "registry_key": build_registry_key(input_symbol=symbol, region=_US_REGION, exchange=None),
                "input_symbol": symbol,
                "normalized_symbol": symbol,
                "region": _US_REGION,
                "exchange": None,
                "exchange_mic": "XNYS",
                "currency": "USD",
                "instrument_type": "equity",
                "status": "active",
                "market_supported": True,
                "fundamentals_supported": True,
                "earnings_supported": True,
                "validation_status": "validated_full",
                "validation_reason": "wave_a_itb_gdx_us_full_history_backfill",
                "promotion_status": "validated_full",
                "last_validated_at": now_iso,
                "notes": (
                    f"created_by={WAVE_A_NAME};"
                    f"source_etfs={source_etfs};"
                    f"aggregate_weight={aggregate_weight:.8f};"
                    f"full_history_start_date={FULL_HISTORY_START_DATE}"
                ),
                "updated_at": now_iso,
                "country": infer_country_from_symbol(symbol),
                "name": _text_or_none(target.get("name")),
            }
        )
    return rows


def _is_wave_a_target_row(row: pd.Series) -> bool:
    etf = str(row.get("etf_ticker") or "").strip().upper()
    symbol = str(row.get("symbol") or "").strip().upper()
    name = str(row.get("holding_name") or "").strip().upper()
    if not _is_us_listed_equity_symbol(symbol):
        return False
    if any(marker in name for marker in ("CASH", "MONEY MARKET", "FUTURE", "SWAP", "RIGHTS", "WARRANT")):
        return False
    if etf == "ITB":
        return True
    if etf == "GDX":
        return True
    return False


def _is_us_listed_equity_symbol(symbol: str) -> bool:
    token = normalize_listing_symbol(symbol)
    if token in _NON_EQUITY_SYMBOLS:
        return False
    if is_placeholder_identifier(token):
        return False
    if "." in token or " " in token or token.startswith("-") or token.startswith("^"):
        return False
    if not re.fullmatch(r"[A-Z][A-Z0-9-]{0,9}", token):
        return False
    return bool(re.search(r"[A-Z]", token))


def _existing_universe_symbols(registry: pd.DataFrame | None) -> set[str]:
    if registry is None or registry.empty:
        return set()
    out: set[str] = set()
    for _, row in registry.iterrows():
        status = str(row.get("status") or "").strip().lower()
        promotion = str(row.get("promotion_status") or "").strip().lower()
        if status != "active" or promotion not in _ACTIVE_PROMOTIONS:
            continue
        symbol = normalize_listing_symbol(row.get("normalized_symbol") or row.get("input_symbol"))
        if symbol:
            out.add(symbol)
    return out


def _empty_targets() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol", "name", "source_etfs", "aggregate_weight"])


def _first_non_empty(values: pd.Series) -> str | None:
    for value in values:
        text = _text_or_none(value)
        if text:
            return text
    return None


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NAN", "NONE", "NULL"}:
        return None
    return text
