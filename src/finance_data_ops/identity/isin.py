"""Listing-to-ISIN enrichment for Entity Layer V0.1."""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import Any

from finance_data_ops.identity.models import ListingCandidate

ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

_PLACEHOLDER_ISINS = {"", "-", "N/A", "NA", "NAN", "NONE", "NULL"}

_ALLOWED_ISIN_PREFIXES_BY_SUFFIX = {
    ".AX": {"AU"},
    ".AS": {"NL"},
    ".CO": {"DK"},
    ".DE": {"DE"},
    ".HK": {"HK", "CN", "KY", "BM", "GB"},
    ".KQ": {"KR"},
    ".KS": {"KR"},
    ".L": {"GB", "JE", "GG", "IE"},
    ".LS": {"PT"},
    ".MI": {"IT"},
    ".NS": {"IN"},
    ".PA": {"FR"},
    ".SS": {"CN"},
    ".SZ": {"CN"},
    ".T": {"JP"},
    ".TA": {"IL"},
    ".TO": {"CA"},
}

_LISTING_COUNTRY_BY_SUFFIX = {
    ".AX": "AU",
    ".AS": "NL",
    ".CO": "DK",
    ".DE": "DE",
    ".HK": "HK",
    ".KQ": "KR",
    ".KS": "KR",
    ".L": "GB",
    ".LS": "PT",
    ".MI": "IT",
    ".NS": "IN",
    ".PA": "FR",
    ".SS": "CN",
    ".SZ": "CN",
    ".T": "JP",
    ".TA": "IL",
    ".TO": "CA",
}

_ALLOWED_ISIN_PREFIXES_BY_COUNTRY = {
    "AU": {"AU"},
    "CA": {"CA"},
    "CN": {"CN"},
    "DE": {"DE"},
    "DK": {"DK"},
    "FR": {"FR"},
    "GB": {"GB", "JE", "GG", "IE"},
    "HK": {"HK", "CN", "KY", "BM", "GB"},
    "IL": {"IL"},
    "IN": {"IN"},
    "IT": {"IT"},
    "JP": {"JP"},
    "KR": {"KR"},
    "NL": {"NL"},
    "PT": {"PT"},
    "US": {"US"},
}


@dataclass(frozen=True, slots=True)
class IsinRecord:
    symbol: str
    provider: str
    request_payload: dict[str, Any]
    response_payload: dict[str, Any] | None
    isin: str = ""
    status: str = "not_found"
    error_message: str = ""
    source: str = "yfinance"


class YFinanceIsinClient:
    def __init__(
        self,
        *,
        fixture_isins: dict[str, Any] | None = None,
        offline: bool = False,
        provider: str = "yfinance",
    ) -> None:
        self.fixture_isins = {str(k).strip().upper(): v for k, v in (fixture_isins or {}).items()}
        self.offline = bool(offline)
        self.provider = provider

    def enrich_candidates(self, candidates: list[ListingCandidate]) -> list[IsinRecord]:
        return [self.enrich_candidate(candidate) for candidate in candidates]

    def enrich_candidate(self, candidate: ListingCandidate) -> IsinRecord:
        symbol = _clean_text(candidate.symbol, upper=True)
        provider_symbol = _clean_text(candidate.provider_symbol or candidate.symbol, upper=True)
        request_payload = {"provider_symbol": provider_symbol}
        if self.fixture_isins:
            return self._from_fixture(candidate=candidate, symbol=symbol, request_payload=request_payload)
        if self.offline:
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=None,
                status="not_found",
                error_message="offline_without_fixture",
            )

        try:
            import yfinance as yf

            ticker = yf.Ticker(provider_symbol)
            response_payload: dict[str, Any] = {}
            raw_isin = ""
            get_isin = getattr(ticker, "get_isin", None)
            if callable(get_isin):
                value = get_isin()
                response_payload["get_isin"] = value
                raw_isin = _clean_text(value, upper=True)
            if not raw_isin:
                value = getattr(ticker, "isin", None)
                response_payload["isin"] = value
                raw_isin = _clean_text(value, upper=True)
            if not raw_isin:
                info = getattr(ticker, "info", None)
                if isinstance(info, dict):
                    response_payload["info"] = {
                        key: info.get(key)
                        for key in ("isin", "quoteType", "symbol")
                        if key in info
                    }
                    raw_isin = _clean_text(info.get("isin"), upper=True)
            validation = validate_isin_for_listing(raw_isin, candidate)
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=response_payload,
                isin=validation["isin"],
                status=validation["status"],
                error_message=validation["reason"],
                source=self.provider,
            )
        except Exception as exc:
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=None,
                status="error",
                error_message=str(exc),
            )

    def _from_fixture(
        self,
        *,
        candidate: ListingCandidate,
        symbol: str,
        request_payload: dict[str, Any],
    ) -> IsinRecord:
        raw = self.fixture_isins.get(symbol)
        if raw is None:
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=None,
                status="not_found",
                error_message="fixture_not_found",
            )
        if isinstance(raw, IsinRecord):
            return replace(raw, symbol=symbol, provider=self.provider, request_payload=request_payload)
        if isinstance(raw, dict):
            validation = validate_isin_for_listing(raw.get("isin"), candidate)
            explicit_status = _clean_text(raw.get("status"), upper=False)
            status = explicit_status or validation["status"]
            error_message = _clean_text(raw.get("error_message"), upper=False) or (
                "" if status == "success" else validation["reason"]
            )
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=raw,
                isin=validation["isin"],
                status=status,
                error_message=error_message,
                source=_clean_text(raw.get("source"), upper=False) or self.provider,
            )
        validation = validate_isin_for_listing(raw, candidate)
        return IsinRecord(
            symbol=symbol,
            provider=self.provider,
            request_payload=request_payload,
            response_payload={"isin": raw},
            isin=validation["isin"],
            status=validation["status"],
            error_message="" if validation["status"] == "success" else validation["reason"],
            source=self.provider,
        )


def isin_cache_rows(records: list[IsinRecord]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": record.symbol,
            "provider": record.provider,
            "request_payload": record.request_payload,
            "response_payload": record.response_payload,
            "isin": record.isin or None,
            "status": record.status,
            "error_message": record.error_message or None,
        }
        for record in records
    ]


def validate_isin_for_listing(raw_isin: Any, candidate: ListingCandidate) -> dict[str, str]:
    isin = _clean_text(raw_isin, upper=True)
    if isin in _PLACEHOLDER_ISINS:
        return {"status": "not_found", "isin": "", "reason": "placeholder_isin"}
    if not ISIN_PATTERN.fullmatch(isin):
        return {"status": "suspect", "isin": isin, "reason": "invalid_isin_format"}
    if not _valid_isin_check_digit(isin):
        return {"status": "suspect", "isin": isin, "reason": "invalid_isin_check_digit"}
    if not _isin_matches_listing_context(isin, candidate):
        reason = (
            "provider_returned_alternate_market_instrument"
            if _listing_country(candidate) == "US"
            else "provider_listing_mismatch"
        )
        return {"status": "suspect", "isin": isin, "reason": reason}
    return {"status": "success", "isin": isin, "reason": ""}


def allowed_isin_prefixes_for_listing(candidate: ListingCandidate) -> set[str]:
    """Return conservative ISIN country prefixes expected for a provider listing."""

    return _allowed_isin_prefixes(candidate)


def isin_prefix_policy_for_listing(candidate: ListingCandidate) -> dict[str, Any]:
    """Return auditable listing country and ISIN-prefix policy for confirmation gates."""

    symbol = _clean_text(candidate.provider_symbol or candidate.symbol, upper=True)
    for suffix in sorted(_ALLOWED_ISIN_PREFIXES_BY_SUFFIX, key=len, reverse=True):
        if symbol.endswith(suffix):
            return {
                "derived_listing_country": _LISTING_COUNTRY_BY_SUFFIX.get(suffix, ""),
                "allowed_isin_prefixes": sorted(_ALLOWED_ISIN_PREFIXES_BY_SUFFIX[suffix]),
                "source": "provider_symbol_suffix",
                "suffix": suffix,
            }
    country = _listing_country(candidate)
    prefixes = _ALLOWED_ISIN_PREFIXES_BY_COUNTRY.get(country, set())
    return {
        "derived_listing_country": country,
        "allowed_isin_prefixes": sorted(prefixes),
        "source": "candidate_country" if country else "missing",
        "suffix": "",
    }


def _valid_isin_check_digit(isin: str) -> bool:
    expanded = ""
    for char in isin:
        if char.isdigit():
            expanded += char
        else:
            expanded += str(ord(char) - 55)
    total = 0
    for index, digit in enumerate(map(int, reversed(expanded))):
        value = digit * 2 if index % 2 == 1 else digit
        total += value // 10 + value % 10
    return total % 10 == 0


def _isin_matches_listing_context(isin: str, candidate: ListingCandidate) -> bool:
    allowed = _allowed_isin_prefixes(candidate)
    if not allowed:
        return False
    return isin[:2] in allowed


def _allowed_isin_prefixes(candidate: ListingCandidate) -> set[str]:
    return set(isin_prefix_policy_for_listing(candidate)["allowed_isin_prefixes"])


def _listing_country(candidate: ListingCandidate) -> str:
    return _clean_text(candidate.country, upper=True)


def _clean_text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.upper() in _PLACEHOLDER_ISINS:
        return ""
    return text.upper() if upper else text
