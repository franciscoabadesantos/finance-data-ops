"""OpenFIGI mapping client and raw cache row builder."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import replace
from typing import Any

import requests

from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping, OpenFigiRequest

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
DEFAULT_BATCH_SIZE = 25
DEFAULT_REQUEST_SLEEP_SECONDS = 6.5

_OPENFIGI_EXCH_CODE_BY_YAHOO_SUFFIX = {
    ".AX": "AU",
    ".AS": "NA",
    ".CO": "DC",
    ".DE": "GY",
    ".HK": "HK",
    ".L": "LN",
    ".T": "JT",
    ".TO": "CN",
}

_OPENFIGI_EXCH_CODE_BY_EXCHANGE = {
    "AMEX": "US",
    "ASE": "US",
    "NASDAQ": "US",
    "NMS": "US",
    "NYQ": "US",
    "NYSE": "US",
    "PCX": "US",
    "US": "US",
}

_OPENFIGI_EXCH_CODE_BY_MIC = {
    "XAMS": "NA",
    "XASX": "AU",
    "XCSE": "DC",
    "XETR": "GY",
    "XHKG": "HK",
    "XLON": "LN",
    "XNYS": "US",
    "XNAS": "US",
    "XTKS": "JT",
    "XTSE": "CN",
}


class OpenFigiClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        fixture_mappings: dict[str, Any] | None = None,
        dry_run: bool = False,
        batch_size: int = DEFAULT_BATCH_SIZE,
        request_sleep_seconds: float = DEFAULT_REQUEST_SLEEP_SECONDS,
        session: requests.Session | None = None,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.environ.get("OPENFIGI_API_KEY") or "").strip()
        self.fixture_mappings = {str(k).upper(): v for k, v in (fixture_mappings or {}).items()}
        self.dry_run = bool(dry_run)
        self.batch_size = max(1, min(int(batch_size), DEFAULT_BATCH_SIZE))
        self.request_sleep_seconds = max(0.0, float(request_sleep_seconds))
        self.session = session or requests.Session()

    def map_candidates(self, candidates: list[ListingCandidate]) -> list[OpenFigiMapping]:
        requests_by_symbol = [build_openfigi_request(candidate) for candidate in candidates]
        if self.fixture_mappings:
            return [self._mapping_from_fixture(request) for request in requests_by_symbol]
        if self.dry_run:
            return [_not_found_mapping(request, "dry_run_without_fixture") for request in requests_by_symbol]

        mappings: list[OpenFigiMapping] = []
        for start in range(0, len(requests_by_symbol), self.batch_size):
            batch = requests_by_symbol[start : start + self.batch_size]
            mappings.extend(self._map_live_batch(batch))
            if start + self.batch_size < len(requests_by_symbol) and self.request_sleep_seconds:
                time.sleep(self.request_sleep_seconds)
        return mappings

    def _map_live_batch(self, batch: list[OpenFigiRequest]) -> list[OpenFigiMapping]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-OPENFIGI-APIKEY"] = self.api_key
        payload = [request.payload for request in batch]
        try:
            response = self.session.post(OPENFIGI_MAPPING_URL, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            return [_error_mapping(request, str(exc)) for request in batch]

        mappings: list[OpenFigiMapping] = []
        if not isinstance(body, list):
            return [_error_mapping(request, "unexpected_openfigi_response_shape", response_payload={"body": body}) for request in batch]

        for request, item in zip(batch, body):
            mappings.append(_mapping_from_openfigi_item(request, item))
        if len(body) < len(batch):
            for request in batch[len(body) :]:
                mappings.append(_error_mapping(request, "missing_openfigi_batch_response"))
        return mappings

    def _mapping_from_fixture(self, request: OpenFigiRequest) -> OpenFigiMapping:
        raw = self.fixture_mappings.get(request.symbol.upper())
        if raw is None:
            return _not_found_mapping(request, "fixture_not_found")
        if isinstance(raw, OpenFigiMapping):
            return replace(raw, symbol=request.symbol, request_hash=request.request_hash, payload=request.payload)
        if isinstance(raw, Exception):
            return _error_mapping(request, str(raw))
        if isinstance(raw, dict) and raw.get("status") == "error":
            return _error_mapping(request, str(raw.get("error_message") or "fixture_error"), response_payload=raw)
        if isinstance(raw, dict) and "data" in raw:
            return _mapping_from_openfigi_item(request, raw)
        if isinstance(raw, dict):
            return _mapping_from_data(request, raw, response_payload={"data": [raw]})
        return _error_mapping(request, f"unsupported_fixture_mapping: {type(raw).__name__}")


def build_openfigi_request(candidate: ListingCandidate) -> OpenFigiRequest:
    normalized = normalize_openfigi_request_inputs(
        provider_symbol=candidate.provider_symbol or candidate.symbol,
        exchange=candidate.exchange,
        exchange_mic=candidate.exchange_mic,
        country=candidate.country,
        currency=candidate.currency,
    )
    payload: dict[str, Any] = {
        "idType": "TICKER",
        "idValue": normalized["openfigi_ticker"],
    }
    if normalized["mic_code"]:
        payload["micCode"] = normalized["mic_code"]
    elif normalized["exch_code"]:
        payload["exchCode"] = normalized["exch_code"]
    if normalized["currency"]:
        payload["currency"] = normalized["currency"]
    normalized_payload = {key: value for key, value in payload.items() if _clean_text(value)}
    request_hash = hashlib.sha256(json.dumps(normalized_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return OpenFigiRequest(symbol=candidate.symbol, payload=normalized_payload, request_hash=request_hash)


def normalize_openfigi_request_inputs(
    *,
    provider_symbol: Any,
    exchange: Any = None,
    exchange_mic: Any = None,
    country: Any = None,
    currency: Any = None,
) -> dict[str, str]:
    """Normalize provider/Yahoo listing identity into one OpenFIGI ticker request.

    OpenFIGI rejects payloads with both `micCode` and `exchCode`. We prefer a
    valid MIC when one is explicitly available; otherwise we fall back to a
    generic OpenFIGI/Bloomberg exchange code inferred from suffix, MIC, exchange,
    or country. `marketSecDes` is intentionally omitted for V0.
    """

    symbol = _clean_text(provider_symbol, upper=True)
    exchange_text = _clean_text(exchange, upper=True)
    mic = _clean_text(exchange_mic, upper=True)
    country_text = _clean_text(country, upper=True)
    currency_text = _clean_text(currency, upper=True)

    suffix = _matched_yahoo_suffix(symbol)
    ticker = _ticker_without_suffix(symbol, suffix)
    suffix_exch_code = _OPENFIGI_EXCH_CODE_BY_YAHOO_SUFFIX.get(suffix, "")

    mic_code = mic if _valid_mic(mic) else ""
    exch_code = ""
    if not mic_code:
        exch_code = (
            suffix_exch_code
            or _OPENFIGI_EXCH_CODE_BY_MIC.get(mic, "")
            or _OPENFIGI_EXCH_CODE_BY_EXCHANGE.get(exchange_text, "")
            or _OPENFIGI_EXCH_CODE_BY_EXCHANGE.get(country_text, "")
        )

    return {
        "openfigi_ticker": ticker,
        "mic_code": mic_code,
        "exch_code": exch_code,
        "currency": currency_text,
    }


def openfigi_cache_rows(mappings: list[OpenFigiMapping]) -> list[dict[str, Any]]:
    rows = []
    for mapping in mappings:
        rows.append(
            {
                "request_hash": mapping.request_hash,
                "request_payload": mapping.payload,
                "response_payload": mapping.response_payload,
                "status": mapping.status,
                "error_message": mapping.error_message or None,
            }
        )
    return rows


def _mapping_from_openfigi_item(request: OpenFigiRequest, item: Any) -> OpenFigiMapping:
    if not isinstance(item, dict):
        return _error_mapping(request, "unexpected_openfigi_item_shape", response_payload={"item": item})
    if item.get("error"):
        message = str(item.get("error") or "")
        status = "not_found" if "not found" in message.lower() else "error"
        return OpenFigiMapping(
            symbol=request.symbol,
            request_hash=request.request_hash,
            status=status,
            payload=request.payload,
            response_payload=item,
            error_message=message,
        )
    data = item.get("data")
    if not isinstance(data, list) or not data:
        return OpenFigiMapping(
            symbol=request.symbol,
            request_hash=request.request_hash,
            status="not_found",
            payload=request.payload,
            response_payload=item,
            error_message="no_openfigi_data",
        )
    if len(data) > 1:
        mapping = _mapping_from_data(request, data[0], response_payload=item)
        return replace(mapping, status="ambiguous", error_message="multiple_openfigi_matches")
    return _mapping_from_data(request, data[0], response_payload=item)


def _mapping_from_data(request: OpenFigiRequest, data: dict[str, Any], *, response_payload: dict[str, Any]) -> OpenFigiMapping:
    metadata = dict(data)
    return OpenFigiMapping(
        symbol=request.symbol,
        request_hash=request.request_hash,
        status="success",
        payload=request.payload,
        response_payload=response_payload,
        figi=_clean_text(data.get("figi"), upper=True),
        composite_figi=_clean_text(data.get("compositeFIGI") or data.get("compositeFigi"), upper=True),
        share_class_figi=_clean_text(data.get("shareClassFIGI") or data.get("shareClassFigi"), upper=True),
        isin=_clean_text(data.get("isin") or data.get("ID_ISIN"), upper=True),
        lei=_clean_text(data.get("lei") or data.get("LEI"), upper=True),
        legal_entity_id=_clean_text(
            data.get("legalEntityId")
            or data.get("entityId")
            or data.get("issuerId")
            or data.get("issuerFigi"),
            upper=True,
        ),
        ticker=_clean_text(data.get("ticker"), upper=True),
        name=_clean_text(data.get("name") or data.get("securityDescription")),
        exchange=_clean_text(data.get("exchCode") or data.get("exchange"), upper=True),
        exchange_mic=_clean_text(data.get("micCode") or data.get("exchangeMIC") or data.get("exchangeMic"), upper=True),
        country=_clean_text(data.get("country") or data.get("marketSecDes"), upper=True),
        currency=_clean_text(data.get("currency"), upper=True),
        home_country=_clean_text(
            data.get("homeCountry")
            or data.get("countryOfIncorporation")
            or data.get("domicileCountry"),
            upper=True,
        ),
        security_type=_clean_text(data.get("securityType2") or data.get("securityType"), upper=True),
        metadata=metadata,
    )


def _not_found_mapping(request: OpenFigiRequest, message: str) -> OpenFigiMapping:
    return OpenFigiMapping(
        symbol=request.symbol,
        request_hash=request.request_hash,
        status="not_found",
        payload=request.payload,
        response_payload=None,
        error_message=message,
    )


def _error_mapping(
    request: OpenFigiRequest,
    message: str,
    *,
    response_payload: dict[str, Any] | None = None,
) -> OpenFigiMapping:
    return OpenFigiMapping(
        symbol=request.symbol,
        request_hash=request.request_hash,
        status="error",
        payload=request.payload,
        response_payload=response_payload,
        error_message=message,
    )


def _matched_yahoo_suffix(symbol: str) -> str:
    for suffix in sorted(_OPENFIGI_EXCH_CODE_BY_YAHOO_SUFFIX, key=len, reverse=True):
        if symbol.endswith(suffix):
            return suffix
    return ""


def _ticker_without_suffix(symbol: str, suffix: str) -> str:
    if not suffix:
        return symbol
    ticker = symbol[: -len(suffix)]
    if suffix == ".CO":
        return ticker.replace("-", "").replace(".", "")
    return ticker


def _valid_mic(value: str) -> bool:
    if not value or value in {"XXXX", "XUNK"}:
        return False
    return len(value) == 4 and value.isalnum()


def _clean_text(value: Any, *, upper: bool = False) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
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
