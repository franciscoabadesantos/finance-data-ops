"""Listing-to-ISIN enrichment for Entity Layer V0.1."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from finance_data_ops.identity.models import ListingCandidate


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
            return self._from_fixture(symbol=symbol, request_payload=request_payload)
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
            isin = ""
            get_isin = getattr(ticker, "get_isin", None)
            if callable(get_isin):
                value = get_isin()
                response_payload["get_isin"] = value
                isin = _clean_text(value, upper=True)
            if not isin:
                value = getattr(ticker, "isin", None)
                response_payload["isin"] = value
                isin = _clean_text(value, upper=True)
            if not isin:
                info = getattr(ticker, "info", None)
                if isinstance(info, dict):
                    response_payload["info"] = {key: info.get(key) for key in ("isin", "quoteType", "symbol") if key in info}
                    isin = _clean_text(info.get("isin"), upper=True)
            if not isin:
                return IsinRecord(
                    symbol=symbol,
                    provider=self.provider,
                    request_payload=request_payload,
                    response_payload=response_payload,
                    status="not_found",
                    error_message="isin_not_available",
                )
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=response_payload,
                isin=isin,
                status="success",
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

    def _from_fixture(self, *, symbol: str, request_payload: dict[str, Any]) -> IsinRecord:
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
            status = _clean_text(raw.get("status"), upper=False) or ("success" if _clean_text(raw.get("isin"), upper=True) else "not_found")
            return IsinRecord(
                symbol=symbol,
                provider=self.provider,
                request_payload=request_payload,
                response_payload=raw,
                isin=_clean_text(raw.get("isin"), upper=True),
                status=status,
                error_message=_clean_text(raw.get("error_message"), upper=False),
                source=_clean_text(raw.get("source"), upper=False) or self.provider,
            )
        isin = _clean_text(raw, upper=True)
        return IsinRecord(
            symbol=symbol,
            provider=self.provider,
            request_payload=request_payload,
            response_payload={"isin": raw},
            isin=isin,
            status="success" if isin else "not_found",
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


def _clean_text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    try:
        if value != value:
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a"}:
        return ""
    return text.upper() if upper else text
