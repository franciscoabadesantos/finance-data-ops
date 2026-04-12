"""Single-symbol validation pipeline with normalization and domain checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd

from finance_data_ops.providers.earnings import EarningsDataProvider
from finance_data_ops.providers.fundamentals import FundamentalsDataProvider
from finance_data_ops.providers.market import MarketDataProvider
from finance_data_ops.providers.symbols import (
    load_symbol_normalization_config,
    normalize_input_symbol,
    normalize_symbol_for_provider,
)
from finance_data_ops.publish.prices import build_market_quotes_payload
from finance_data_ops.validation.ticker_registry import build_registry_key

SUPPORTED_INSTRUMENT_TYPES = {"equity", "adr", "etf", "index_proxy", "country_fund", "unknown"}
SUPPORTED_VALIDATION_STATUSES = {
    "pending_validation",
    "validated_market_only",
    "validated_full",
    "rejected",
}
SUPPORTED_PROMOTION_STATUSES = {
    "pending_validation",
    "validated_market_only",
    "validated_full",
    "rejected",
}

DOMAIN_EXPECTATION_POLICY = {
    "equity": {"market": "required", "fundamentals": "preferred", "earnings": "preferred"},
    "adr": {"market": "required", "fundamentals": "preferred", "earnings": "preferred"},
    "etf": {"market": "required", "fundamentals": "optional", "earnings": "optional"},
    "country_fund": {"market": "required", "fundamentals": "optional", "earnings": "optional"},
    "index_proxy": {"market": "required", "fundamentals": "ignored", "earnings": "ignored"},
    "unknown": {"market": "required", "fundamentals": "optional", "earnings": "optional"},
}

INDEX_PROXY_SYMBOLS = {
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "VOO",
    "VTI",
}

COUNTRY_FUND_SYMBOLS = {
    "VGK",
    "EZU",
    "FEZ",
    "EWJ",
    "EWT",
    "MCHI",
    "KWEB",
    "FXI",
    "ILF",
    "EWZ",
    "EWW",
    "EZA",
    "AFK",
    "EIS",
    "EPHE",
    "THD",
    "EIDO",
    "EWY",
    "EWA",
    "EWS",
    "EWM",
}

PUBLISH_REQUIRED_QUOTE_FIELDS = ("price", "change")


@dataclass(slots=True)
class DomainCheckResult:
    supported: bool
    reason: str
    rows: int
    details: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CandidateValidation:
    candidate_symbol: str
    instrument_type: str
    instrument_type_source: str
    market: DomainCheckResult
    fundamentals: DomainCheckResult
    earnings: DomainCheckResult
    validation_status: str
    promotion_status: str
    validation_reason: str
    score: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "candidate_symbol": self.candidate_symbol,
            "instrument_type": self.instrument_type,
            "instrument_type_source": self.instrument_type_source,
            "market": self.market.as_dict(),
            "fundamentals": self.fundamentals.as_dict(),
            "earnings": self.earnings.as_dict(),
            "validation_status": self.validation_status,
            "promotion_status": self.promotion_status,
            "validation_reason": self.validation_reason,
            "score": self.score,
        }


def run_single_ticker_validation(
    *,
    input_symbol: str,
    region: str | None,
    exchange: str | None = None,
    instrument_type_hint: str | None = None,
    history_limit: int = 12,
    market_provider: MarketDataProvider | None = None,
    fundamentals_provider: FundamentalsDataProvider | None = None,
    earnings_provider: EarningsDataProvider | None = None,
    metadata_lookup_fn: Callable[[str], Mapping[str, Any] | dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    normalized_input = normalize_input_symbol(input_symbol)
    if not normalized_input:
        raise ValueError("input_symbol is required.")

    normalized_region = str(region or "us").strip().lower()

    normalized_exchange = str(exchange).strip().upper() if exchange else None
    candidates = normalize_symbol_for_provider(
        normalized_input,
        region=normalized_region,
        exchange=normalized_exchange,
    )
    if not candidates:
        raise RuntimeError(f"No symbol candidates generated for {normalized_input}.")

    market_impl = market_provider or MarketDataProvider()
    fundamentals_impl = fundamentals_provider or FundamentalsDataProvider()
    earnings_impl = earnings_provider or EarningsDataProvider()

    validations: list[CandidateValidation] = []
    for candidate in candidates:
        metadata = _lookup_instrument_metadata(candidate_symbol=candidate, metadata_lookup_fn=metadata_lookup_fn)
        instrument_type, instrument_type_source = classify_instrument_type(
            input_symbol=normalized_input,
            candidate_symbol=candidate,
            instrument_type_hint=instrument_type_hint,
            exchange=normalized_exchange,
            metadata=metadata,
        )
        market_result = validate_market_support(candidate_symbol=candidate, provider=market_impl)
        fundamentals_result = validate_fundamentals_support(candidate_symbol=candidate, provider=fundamentals_impl)
        earnings_result = validate_earnings_support(
            candidate_symbol=candidate,
            provider=earnings_impl,
            history_limit=history_limit,
        )

        validation_status, promotion_status, validation_reason = classify_candidate_status(
            instrument_type=instrument_type,
            market_result=market_result,
            fundamentals_result=fundamentals_result,
            earnings_result=earnings_result,
        )
        score = _candidate_score(
            validation_status=validation_status,
            market_supported=market_result.supported,
            fundamentals_supported=fundamentals_result.supported,
            earnings_supported=earnings_result.supported,
        )
        validations.append(
            CandidateValidation(
                candidate_symbol=candidate,
                instrument_type=instrument_type,
                instrument_type_source=instrument_type_source,
                market=market_result,
                fundamentals=fundamentals_result,
                earnings=earnings_result,
                validation_status=validation_status,
                promotion_status=promotion_status,
                validation_reason=validation_reason,
                score=score,
            )
        )

    selected = _select_best_candidate(validations)
    now_iso = datetime.now(UTC).isoformat()
    registry_row = {
        "registry_key": build_registry_key(
            input_symbol=normalized_input,
            region=normalized_region,
            exchange=normalized_exchange,
        ),
        "input_symbol": normalized_input,
        "normalized_symbol": selected.candidate_symbol,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "instrument_type": selected.instrument_type,
        "status": "active" if selected.validation_status != "rejected" else "rejected",
        "market_supported": bool(selected.market.supported),
        "fundamentals_supported": bool(selected.fundamentals.supported),
        "earnings_supported": bool(selected.earnings.supported),
        "validation_status": selected.validation_status,
        "validation_reason": selected.validation_reason,
        "promotion_status": selected.promotion_status,
        "last_validated_at": now_iso,
        "notes": _build_notes(
            candidates=candidates,
            selected=selected,
            policy=DOMAIN_EXPECTATION_POLICY.get(selected.instrument_type, DOMAIN_EXPECTATION_POLICY["unknown"]),
        ),
        "updated_at": now_iso,
    }
    return {
        "input_symbol": normalized_input,
        "region": normalized_region,
        "exchange": normalized_exchange,
        "instrument_type_hint": str(instrument_type_hint or "").strip().lower() or None,
        "candidates": candidates,
        "candidate_validations": [item.as_dict() for item in validations],
        "selected": selected.as_dict(),
        "registry_row": registry_row,
    }


def validate_market_support(*, candidate_symbol: str, provider: MarketDataProvider) -> DomainCheckResult:
    end_date = date.today()
    start_date = end_date - timedelta(days=35)
    try:
        prices = provider.fetch_daily_prices(
            [candidate_symbol],
            start=start_date.isoformat(),
            end=end_date.isoformat(),
        )
    except Exception as exc:
        return DomainCheckResult(
            supported=False,
            reason=f"market_daily_error:{exc}",
            rows=0,
            details={"stage": "daily_prices"},
        )

    if prices.empty:
        return DomainCheckResult(
            supported=False,
            reason="market_daily_empty",
            rows=0,
            details={"stage": "daily_prices"},
        )

    try:
        quotes = provider.fetch_latest_quotes([candidate_symbol])
    except Exception as exc:
        return DomainCheckResult(
            supported=False,
            reason=f"market_quotes_error:{exc}",
            rows=int(len(prices.index)),
            details={"stage": "latest_quotes", "daily_rows": int(len(prices.index))},
        )
    if quotes.empty:
        return DomainCheckResult(
            supported=False,
            reason="market_quotes_empty",
            rows=int(len(prices.index)),
            details={"stage": "latest_quotes", "daily_rows": int(len(prices.index))},
        )

    quote_payload = build_market_quotes_payload(quotes)
    if not quote_payload:
        return DomainCheckResult(
            supported=False,
            reason="market_quotes_payload_empty",
            rows=int(len(prices.index)),
            details={"stage": "publish_precheck"},
        )
    payload_row = next(
        (row for row in quote_payload if str(row.get("ticker", "")).strip().upper() == candidate_symbol),
        quote_payload[0],
    )
    missing_required = [field for field in PUBLISH_REQUIRED_QUOTE_FIELDS if _is_missing(payload_row.get(field))]
    if missing_required:
        return DomainCheckResult(
            supported=False,
            reason=f"market_quotes_publish_unsafe_missing_{','.join(missing_required)}",
            rows=int(len(prices.index)),
            details={
                "stage": "publish_precheck",
                "missing_fields": missing_required,
                "payload_sample": {field: payload_row.get(field) for field in ("price", "change", "change_percent")},
            },
        )

    return DomainCheckResult(
        supported=True,
        reason="market_supported_publish_safe",
        rows=int(len(prices.index)),
        details={
            "daily_rows": int(len(prices.index)),
            "quote_rows": int(len(quotes.index)),
        },
    )


def validate_fundamentals_support(
    *,
    candidate_symbol: str,
    provider: FundamentalsDataProvider,
) -> DomainCheckResult:
    try:
        frame = provider.fetch_symbol_fundamentals(candidate_symbol)
    except Exception as exc:
        return DomainCheckResult(
            supported=False,
            reason=f"fundamentals_error:{exc}",
            rows=0,
            details={},
        )
    if frame.empty:
        return DomainCheckResult(supported=False, reason="fundamentals_empty", rows=0, details={})
    return DomainCheckResult(
        supported=True,
        reason="fundamentals_supported",
        rows=int(len(frame.index)),
        details={"metrics": int(frame["metric"].nunique()) if "metric" in frame.columns else None},
    )


def validate_earnings_support(
    *,
    candidate_symbol: str,
    provider: EarningsDataProvider,
    history_limit: int,
) -> DomainCheckResult:
    try:
        events, history = provider.fetch_symbol_earnings(candidate_symbol, history_limit=max(int(history_limit), 1))
    except Exception as exc:
        return DomainCheckResult(
            supported=False,
            reason=f"earnings_error:{exc}",
            rows=0,
            details={},
        )
    event_rows = int(len(events.index))
    history_rows = int(len(history.index))
    if event_rows == 0 and history_rows == 0:
        return DomainCheckResult(
            supported=False,
            reason="earnings_empty",
            rows=0,
            details={"events_rows": event_rows, "history_rows": history_rows},
        )
    return DomainCheckResult(
        supported=True,
        reason="earnings_supported",
        rows=event_rows + history_rows,
        details={"events_rows": event_rows, "history_rows": history_rows},
    )


def classify_instrument_type(
    *,
    input_symbol: str,
    candidate_symbol: str,
    instrument_type_hint: str | None = None,
    exchange: str | None = None,
    metadata: Mapping[str, Any] | dict[str, Any] | None = None,
) -> tuple[str, str]:
    overrides = _load_instrument_type_overrides()
    normalized_input = normalize_input_symbol(input_symbol)
    normalized_candidate = normalize_input_symbol(candidate_symbol)

    for key in (normalized_candidate, normalized_input):
        override = overrides.get(key)
        if override:
            return override, "explicit_override"

    if instrument_type_hint:
        normalized_hint = str(instrument_type_hint).strip().lower()
        if normalized_hint in SUPPORTED_INSTRUMENT_TYPES:
            return normalized_hint, "input_hint"

    metadata_hint = _infer_instrument_type_from_metadata(
        symbol=normalized_candidate,
        metadata=dict(metadata or {}),
    )
    if metadata_hint:
        return metadata_hint, "provider_metadata"

    heuristic = _infer_instrument_type_from_symbol(
        symbol=normalized_candidate,
        exchange=exchange,
    )
    if heuristic:
        return heuristic, "symbol_heuristic"

    return "unknown", "fallback_unknown"


def classify_candidate_status(
    *,
    instrument_type: str,
    market_result: DomainCheckResult,
    fundamentals_result: DomainCheckResult,
    earnings_result: DomainCheckResult,
) -> tuple[str, str, str]:
    policy = DOMAIN_EXPECTATION_POLICY.get(instrument_type, DOMAIN_EXPECTATION_POLICY["unknown"])
    if policy.get("market") == "required" and not market_result.supported:
        reason = f"market_required_failed:{market_result.reason}"
        return "rejected", "rejected", reason

    required_failures: list[str] = []
    if policy.get("fundamentals") == "required" and not fundamentals_result.supported:
        required_failures.append(f"fundamentals:{fundamentals_result.reason}")
    if policy.get("earnings") == "required" and not earnings_result.supported:
        required_failures.append(f"earnings:{earnings_result.reason}")
    if required_failures:
        return "rejected", "rejected", ",".join(required_failures)

    if market_result.supported and fundamentals_result.supported and earnings_result.supported:
        return "validated_full", "validated_full", "all_domains_supported"

    if market_result.supported:
        preferred_missing: list[str] = []
        if policy.get("fundamentals") == "preferred" and not fundamentals_result.supported:
            preferred_missing.append(f"fundamentals:{fundamentals_result.reason}")
        if policy.get("earnings") == "preferred" and not earnings_result.supported:
            preferred_missing.append(f"earnings:{earnings_result.reason}")
        if preferred_missing:
            return "validated_market_only", "validated_market_only", ",".join(preferred_missing)
        if instrument_type == "index_proxy":
            return "validated_market_only", "validated_market_only", "market_supported_index_proxy"
        return "validated_market_only", "validated_market_only", "market_supported"

    return "rejected", "rejected", f"market_failed:{market_result.reason}"


def _lookup_instrument_metadata(
    *,
    candidate_symbol: str,
    metadata_lookup_fn: Callable[[str], Mapping[str, Any] | dict[str, Any] | None] | None,
) -> dict[str, Any]:
    lookup_fn = metadata_lookup_fn or _default_metadata_lookup
    try:
        payload = lookup_fn(candidate_symbol)
    except Exception:
        return {}
    if isinstance(payload, Mapping):
        return {str(k): v for k, v in payload.items()}
    return {}


def _load_instrument_type_overrides() -> dict[str, str]:
    cfg = load_symbol_normalization_config()
    raw = dict(cfg.get("instrument_type_overrides") or {})
    out: dict[str, str] = {}
    for key, value in raw.items():
        symbol = normalize_input_symbol(str(key))
        instrument = str(value).strip().lower()
        if symbol and instrument in SUPPORTED_INSTRUMENT_TYPES:
            out[symbol] = instrument
    return out


def _default_metadata_lookup(symbol: str) -> dict[str, Any]:
    try:
        import yfinance as yf
    except Exception:
        return {}
    try:
        ticker = yf.Ticker(symbol)
        info = dict(getattr(ticker, "info", {}) or {})
        fast = dict(getattr(ticker, "fast_info", {}) or {})
    except Exception:
        return {}
    merged = dict(info)
    if "quoteType" not in merged and "quoteType" in fast:
        merged["quoteType"] = fast.get("quoteType")
    if "exchange" not in merged and "exchange" in fast:
        merged["exchange"] = fast.get("exchange")
    if "market" not in merged and "market" in fast:
        merged["market"] = fast.get("market")
    return merged


def _infer_instrument_type_from_metadata(*, symbol: str, metadata: dict[str, Any]) -> str | None:
    if not metadata:
        return None

    quote_type = _normalize_metadata_token(
        metadata.get("quoteType")
        or metadata.get("quote_type")
        or metadata.get("securityType")
        or metadata.get("security_type")
    )
    short_name = str(metadata.get("shortName") or metadata.get("short_name") or "").upper()
    long_name = str(metadata.get("longName") or metadata.get("long_name") or "").upper()
    category = str(metadata.get("category") or "").upper()
    fund_family = str(metadata.get("fundFamily") or metadata.get("fund_family") or "").upper()
    is_adr_flag = bool(metadata.get("isAdr") or metadata.get("is_adr"))
    descriptor = " ".join(item for item in (short_name, long_name, category, fund_family) if item)

    if quote_type in {"INDEX"}:
        return "index_proxy"
    if quote_type in {"ETF", "MUTUALFUND", "FUND"}:
        if symbol in COUNTRY_FUND_SYMBOLS or _looks_like_country_fund_text(descriptor):
            return "country_fund"
        return "etf"
    if quote_type in {"ADR"}:
        return "adr"
    if quote_type in {"EQUITY", "COMMONSTOCK", "STOCK"}:
        if is_adr_flag or " ADR" in f" {descriptor} " or " ADS" in f" {descriptor} ":
            return "adr"
        return "equity"

    if " ETF" in f" {descriptor} " or " FUND" in f" {descriptor} ":
        if symbol in COUNTRY_FUND_SYMBOLS or _looks_like_country_fund_text(descriptor):
            return "country_fund"
        return "etf"
    if " INDEX" in f" {descriptor} " and symbol in INDEX_PROXY_SYMBOLS:
        return "index_proxy"
    if " ADR" in f" {descriptor} " or " ADS" in f" {descriptor} ":
        return "adr"
    return None


def _infer_instrument_type_from_symbol(*, symbol: str, exchange: str | None) -> str | None:
    if symbol in INDEX_PROXY_SYMBOLS or symbol.startswith("^"):
        return "index_proxy"
    if symbol in COUNTRY_FUND_SYMBOLS or (symbol.startswith("EW") and len(symbol) <= 4):
        return "country_fund"
    if "." in symbol:
        suffix = symbol.split(".")[-1]
        known_suffixes = _known_exchange_suffixes()
        if suffix in known_suffixes:
            return "equity"
    if str(exchange or "").strip().upper():
        return "equity"
    return None


def _known_exchange_suffixes() -> set[str]:
    cfg = load_symbol_normalization_config()
    suffix_by_exchange = dict(cfg.get("suffix_by_exchange") or {})
    known: set[str] = set()
    for value in suffix_by_exchange.values():
        token = str(value).strip().upper()
        if token.startswith(".") and len(token) > 1:
            known.add(token[1:])
    return known


def _normalize_metadata_token(raw: object) -> str:
    return str(raw or "").strip().upper().replace(" ", "")


def _looks_like_country_fund_text(raw: str) -> bool:
    text = str(raw or "").upper()
    return " MSCI " in f" {text} " or " COUNTRY " in f" {text} "


def _select_best_candidate(candidates: list[CandidateValidation]) -> CandidateValidation:
    if not candidates:
        raise RuntimeError("No candidate validations to select from.")
    status_rank = {
        "validated_full": 3,
        "validated_market_only": 2,
        "pending_validation": 1,
        "rejected": 0,
    }
    return max(candidates, key=lambda item: (status_rank.get(item.validation_status, -1), item.score))


def _candidate_score(
    *,
    validation_status: str,
    market_supported: bool,
    fundamentals_supported: bool,
    earnings_supported: bool,
) -> int:
    base = {
        "validated_full": 300,
        "validated_market_only": 200,
        "pending_validation": 100,
        "rejected": 0,
    }.get(validation_status, 0)
    return base + (50 if market_supported else 0) + (10 if fundamentals_supported else 0) + (10 if earnings_supported else 0)


def _build_notes(*, candidates: list[str], selected: CandidateValidation, policy: dict[str, str]) -> str:
    return (
        f"candidates={','.join(candidates)};"
        f"selected={selected.candidate_symbol};"
        f"instrument_type={selected.instrument_type};"
        f"instrument_type_source={selected.instrument_type_source};"
        f"policy={policy};"
        f"market={selected.market.reason};"
        f"fundamentals={selected.fundamentals.reason};"
        f"earnings={selected.earnings.reason}"
    )


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False
