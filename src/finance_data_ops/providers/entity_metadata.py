"""One place that knows how to ask the provider what a company *is*.

Descriptive metadata -- name, sector, industry, exchange, currency -- was being
fetched by three separate copies of the same function, and they did not agree
on what to keep. `theme_etfs.universe` kept `sector` and dropped `industry`.
`onboarding.wave_a` never looked at all, building its registry rows straight
from ETF holdings. `validation.ticker_validation` merged in `fast_info` for
`quoteType` and kept neither.

The result reached the relationship map: `industry` was 0% populated across
1,738 entities, so the industry tier of the atlas's naming logic could never
fire, and `sector` sat at 24%. The gold-miner field -- ITB and GDX
constituents, onboarded by wave_a, which asks the provider nothing -- had 34 of
39 members with no sector at all, and announced "Healthcare" on three votes.

None of that was a provider limitation. The provider returns both fields for
every symbol tried, US and non-US alike.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Mapping

LOGGER = logging.getLogger(__name__)

MetadataLookup = Callable[[str], Mapping[str, Any] | dict[str, Any] | None]

#: Descriptive fields worth persisting on the entity, with the provider aliases
#: each has been seen under. Order matters: first non-empty wins.
DESCRIPTIVE_ALIASES: dict[str, tuple[str, ...]] = {
    "name": ("name", "shortName", "longName"),
    "sector": ("sector", "sectorKey", "sector_key"),
    "industry": ("industry", "industryKey", "industry_key"),
    "description": ("description", "longBusinessSummary", "long_business_summary"),
}


def default_metadata_lookup(symbol: str) -> dict[str, Any]:
    """Provider metadata for one symbol, merged from `info` and `fast_info`.

    Import and call failures are logged rather than returned as an empty dict
    on their own: an empty result has to be readable as "the provider has
    nothing", and it cannot be if it also means "the call raised".
    """
    try:
        import yfinance as yf
    except ImportError:
        LOGGER.warning("yfinance is not installed; entity metadata will be empty for %s.", symbol)
        return {}
    try:
        ticker = yf.Ticker(symbol)
        info = dict(getattr(ticker, "info", {}) or {})
        fast = dict(getattr(ticker, "fast_info", {}) or {})
    except Exception:
        LOGGER.exception("Provider metadata lookup failed for %s.", symbol)
        return {}
    merged = dict(info)
    for key in ("quoteType", "exchange", "market"):
        if key not in merged and key in fast:
            merged[key] = fast.get(key)
    return merged


def safe_metadata_lookup(lookup: MetadataLookup, symbol: str) -> dict[str, Any]:
    """Call `lookup`, turning a failure into an empty mapping *and* a log line."""
    try:
        payload = lookup(symbol)
    except Exception:
        LOGGER.exception("Entity metadata lookup raised for %s.", symbol)
        return {}
    return dict(payload or {}) if isinstance(payload, Mapping) else {}


def _first_non_empty(metadata: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = metadata.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"none", "nan", "null"}:
            return text
    return None


def descriptive_fields(metadata: Mapping[str, Any]) -> dict[str, str | None]:
    """The descriptive fields an entity row carries, from provider metadata.

    Returning every key with an explicit None keeps a missing field visible in
    the registry row rather than absent from it, which is what let
    `extras.get("sector")` quietly yield NULL for an entire onboarding path.
    """
    return {field: _first_non_empty(metadata, aliases) for field, aliases in DESCRIPTIVE_ALIASES.items()}
