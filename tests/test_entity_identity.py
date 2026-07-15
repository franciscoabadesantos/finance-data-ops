from __future__ import annotations

from pathlib import Path

from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.identity.openfigi import OpenFigiClient, build_openfigi_request
from finance_data_ops.identity.publisher import publish_entity_identity
from finance_data_ops.identity.resolver import build_entity_identity
from finance_data_ops.identity.universe import build_candidate_universe_from_frames
from finance_data_ops.publish.client import RecordingPublisher
import pandas as pd


def test_openfigi_request_omits_nan_and_never_sends_mic_and_exchange() -> None:
    request = build_openfigi_request(
        ListingCandidate(
            symbol="SAP",
            provider_symbol="SAP",
            exchange="NYQ",
            exchange_mic=float("nan"),
            country="US",
            currency="nan",
        )
    )

    assert request.payload == {"idType": "TICKER", "idValue": "SAP", "exchCode": "US"}
    assert "micCode" not in request.payload
    assert "marketSecDes" not in request.payload
    assert "NAN" not in request.payload.values()


def test_openfigi_request_prefers_valid_mic_over_exchange() -> None:
    request = build_openfigi_request(
        ListingCandidate(
            symbol="SAP",
            provider_symbol="SAP",
            exchange="NYQ",
            exchange_mic="XNYS",
            currency="USD",
        )
    )

    assert request.payload == {"idType": "TICKER", "idValue": "SAP", "micCode": "XNYS", "currency": "USD"}
    assert "exchCode" not in request.payload


def test_openfigi_unauthenticated_default_batch_size_is_five() -> None:
    assert OpenFigiClient(api_key="").batch_size == 5
    assert OpenFigiClient(api_key="token").batch_size == 25
    assert OpenFigiClient(api_key="", batch_size=12).batch_size == 12


def test_yahoo_suffixes_normalize_for_openfigi_requests() -> None:
    cases = {
        "SAP.DE": ("SAP", "GY"),
        "ASML.AS": ("ASML", "NA"),
        "NOVO-B.CO": ("NOVOB", "DC"),
        "TLS.AX": ("TLS", "AU"),
        "HSBA.L": ("HSBA", "LN"),
        "0005.HK": ("5", "HK"),
        "7203.T": ("7203", "JT"),
        "RY.TO": ("RY", "CN"),
        "MSFT": ("MSFT", "US"),
    }
    for provider_symbol, (openfigi_ticker, exch_code) in cases.items():
        request = build_openfigi_request(
            ListingCandidate(
                symbol=provider_symbol,
                provider_symbol=provider_symbol,
                exchange="NMS" if provider_symbol == "MSFT" else "",
                exchange_mic="",
                currency="USD",
            )
        )
        assert request.payload["idValue"] == openfigi_ticker
        assert request.payload["exchCode"] == exch_code
        assert "micCode" not in request.payload
        assert "marketSecDes" not in request.payload


def test_supported_local_suffixes_use_mic_without_exchange_code() -> None:
    cases = {
        "005930.KS": ("005930", "XKRX"),
        "091990.KQ": ("091990", "XKOS"),
        "000001.SZ": ("000001", "XSHE"),
        "600519.SS": ("600519", "XSHG"),
        "SON.LS": ("SON", "XLIS"),
        "MC.PA": ("MC", "XPAR"),
        "NEXI.MI": ("NEXI", "XMIL"),
        "TEVA.TA": ("TEVA", "XTAE"),
        "RELIANCE.NS": ("RELIANCE", "XNSE"),
    }
    for provider_symbol, (openfigi_ticker, mic_code) in cases.items():
        request = build_openfigi_request(
            ListingCandidate(
                symbol=provider_symbol,
                provider_symbol=provider_symbol,
                exchange="",
                exchange_mic="",
            )
        )

        assert request.payload["idValue"] == openfigi_ticker
        assert request.payload["micCode"] == mic_code
        assert "exchCode" not in request.payload
        assert "marketSecDes" not in request.payload


def test_openfigi_retries_korea_ks_on_kosdaq_without_mutating_provider_symbol() -> None:
    session = _OpenFigiVariantSession(success_when=lambda payload: payload.get("micCode") == "XKOS")
    client = OpenFigiClient(api_key="", batch_size=1, request_sleep_seconds=0, session=session)

    mappings = client.map_candidates([_candidate("053800.KS", exchange_mic="", country="KR", currency="KRW")])

    assert mappings[0].status == "success"
    assert mappings[0].symbol == "053800.KS"
    assert mappings[0].payload["idValue"] == "053800"
    assert mappings[0].payload["micCode"] == "XKOS"
    assert session.payloads[0]["micCode"] == "XKRX"
    assert session.payloads[1]["micCode"] == "XKOS"


def test_openfigi_retries_china_numeric_ticker_without_leading_zeroes() -> None:
    session = _OpenFigiVariantSession(success_when=lambda payload: payload.get("idValue") == "858")
    client = OpenFigiClient(api_key="", batch_size=1, request_sleep_seconds=0, session=session)

    mappings = client.map_candidates([_candidate("000858.SZ", exchange_mic="", country="CN", currency="CNY")])

    assert mappings[0].status == "success"
    assert mappings[0].symbol == "000858.SZ"
    assert mappings[0].payload["idValue"] == "858"
    assert mappings[0].payload["micCode"] == "XSHE"
    assert session.payloads[0]["idValue"] == "000858"
    assert session.payloads[1]["idValue"] == "858"


def test_hong_kong_numeric_ticker_strips_leading_zeroes_only_for_openfigi_request() -> None:
    request = build_openfigi_request(
        ListingCandidate(
            symbol="0005.HK",
            provider_symbol="0005.HK",
            country="HK",
            currency="HKD",
        )
    )

    assert request.symbol == "0005.HK"
    assert request.payload["idValue"] == "5"
    assert request.payload["exchCode"] == "HK"


def test_candidate_universe_treats_pandas_nan_as_missing() -> None:
    candidates = build_candidate_universe_from_frames(
        ticker_registry=pd.DataFrame(
            [
                {
                    "input_symbol": "SAP.DE",
                    "normalized_symbol": "nan",
                    "exchange": float("nan"),
                    "exchange_mic": pd.NA,
                    "currency": "NAN",
                    "region": "eu",
                    "status": "active",
                    "promotion_status": "validated_full",
                    "market_supported": True,
                }
            ]
        )
    )

    assert candidates[0].symbol == "SAP.DE"
    assert candidates[0].exchange == ""
    assert candidates[0].exchange_mic == ""
    assert candidates[0].currency == ""


def test_candidate_universe_uses_entity_attributes_static_name_metadata() -> None:
    candidates = build_candidate_universe_from_frames(
        ticker_readiness=pd.DataFrame([{"symbol": "005930.KS", "tracked": True}]),
        entity_attributes_static=pd.DataFrame(
            [
                {
                    "entity_id": "005930.KS",
                    "display_name": "Samsung Electronics Co Ltd",
                }
            ]
        ),
    )

    assert candidates[0].symbol == "005930.KS"
    assert candidates[0].name == "Samsung Electronics Co Ltd"


def test_openfigi_413_batch_is_split_without_poisoning_whole_batch() -> None:
    candidates = [_candidate("AAA"), _candidate("BBB"), _candidate("CCC")]
    client = OpenFigiClient(
        api_key="",
        batch_size=3,
        request_sleep_seconds=0,
        session=_PayloadTooLargeThenSuccessSession(),
    )

    mappings = client.map_candidates(candidates)
    result = build_entity_identity(
        candidates=candidates,
        mappings=mappings,
        batch_split_retries=client.batch_split_retries,
    )

    assert [mapping.status for mapping in mappings] == ["success", "success", "success"]
    assert result.summary()["batch_split_retries"] == 2
    assert result.summary()["security_resolved_listings"] == 3
    assert result.summary()["entity_unresolved_openfigi_error"] == 0


def test_sap_pair_resolves_to_same_entity() -> None:
    result = _resolve(
        [_candidate("SAP", country="US"), _candidate("SAP.DE", country="DE")],
        {
            "SAP": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", composite="SAP-US", country="US", home="DE"),
            "SAP.DE": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", composite="SAP-DE", country="DE", home="DE"),
        },
    )

    assert len(result.entities) == 1
    assert {listing.symbol for listing in result.listings} == {"SAP", "SAP.DE"}
    assert {listing.entity_id for listing in result.listings} == {result.entities[0].entity_id}


def test_nvo_pair_resolves_to_same_entity() -> None:
    result = _resolve(
        [_candidate("NVO", country="US"), _candidate("NOVO-B.CO", country="DK")],
        {
            "NVO": _mapping("NVO", "Novo Nordisk A/S", legal="NOVO-LEGAL", share="NOVO-SHARE", country="US", home="DK"),
            "NOVO-B.CO": _mapping("NOVOB", "Novo Nordisk A/S", legal="NOVO-LEGAL", share="NOVO-SHARE", country="DK", home="DK"),
        },
    )

    assert len(result.entities) == 1
    assert {listing.symbol for listing in result.listings} == {"NVO", "NOVO-B.CO"}


def test_asml_pair_resolves_to_same_entity() -> None:
    result = _resolve(
        [_candidate("ASML", country="US"), _candidate("ASML.AS", country="NL")],
        {
            "ASML": _mapping("ASML", "ASML Holding NV", legal="ASML-LEGAL", share="ASML-SHARE", country="US", home="NL"),
            "ASML.AS": _mapping("ASML", "ASML Holding NV", legal="ASML-LEGAL", share="ASML-SHARE", country="NL", home="NL"),
        },
    )

    assert len(result.entities) == 1
    assert result.entities[0].home_country == "NL"


def test_tls_suffix_collision_does_not_merge_when_identifiers_differ() -> None:
    result = _resolve(
        [_candidate("TLS", country="US"), _candidate("TLS.AX", country="AU")],
        {
            "TLS": _mapping("TLS", "Telos Corp", legal="TELOS-LEGAL", share="TELOS-SHARE", country="US", home="US"),
            "TLS.AX": _mapping("TLS", "Telstra Group Ltd", legal="TELSTRA-LEGAL", share="TELSTRA-SHARE", country="AU", home="AU"),
        },
    )

    assert len(result.entities) == 2
    assert len({listing.entity_id for listing in result.listings}) == 2


def test_share_classes_are_distinct_listings_under_entity_structure() -> None:
    result = _resolve(
        [
            _candidate("GOOG"),
            _candidate("GOOGL"),
            _candidate("LEN"),
            _candidate("LENB"),
        ],
        {
            "GOOG": _mapping("GOOG", "Alphabet Inc", legal="ALPHABET-LEGAL", share="GOOG-SHARE", composite="GOOG-COMP"),
            "GOOGL": _mapping("GOOGL", "Alphabet Inc", legal="ALPHABET-LEGAL", share="GOOGL-SHARE", composite="GOOGL-COMP"),
            "LEN": _mapping("LEN", "Lennar Corp", legal="LENNAR-LEGAL", share="LEN-SHARE", composite="LEN-COMP"),
            "LENB": _mapping("LENB", "Lennar Corp", legal="LENNAR-LEGAL", share="LENB-SHARE", composite="LENB-COMP"),
        },
    )

    by_entity: dict[str, set[str]] = {}
    for listing in result.listings:
        by_entity.setdefault(listing.entity_id, set()).add(listing.symbol)
    assert {"GOOG", "GOOGL"} in by_entity.values()
    assert {"LEN", "LENB"} in by_entity.values()
    assert {listing.share_class_figi for listing in result.listings if listing.symbol in {"GOOG", "GOOGL"}} == {
        "GOOG-SHARE",
        "GOOGL-SHARE",
    }


def test_security_figis_without_entity_identifier_do_not_resolve_adr_home_pair() -> None:
    result = _resolve(
        [_candidate("SAP", country="US"), _candidate("SAP.DE", country="DE")],
        {
            "SAP": _security_mapping("SAP", "SAP SE", share="SAP-ADR-SHARE", composite="SAP-ADR-COMP", country="US"),
            "SAP.DE": _security_mapping("SAP", "SAP SE", share="SAP-DE-SHARE", composite="SAP-DE-COMP", country="DE"),
        },
    )

    assert result.entities == []
    assert result.listings == []
    assert result.unresolved_symbols == ["SAP", "SAP.DE"]
    issue_types = {audit.issue_type for audit in result.audits}
    assert "security_only_group_not_resolved" in issue_types
    assert "same_name_different_security_identifier" in issue_types
    assert "possible_same_company_not_resolved" in issue_types
    assert "adr_home_candidate_missing_entity_identifier" in issue_types


def test_live_like_security_only_samples_create_suggestions_not_entities() -> None:
    result = _resolve(
        [
            _candidate("SAP", country="US"),
            _candidate("SAP.DE", country="DE"),
            _candidate("ASML", country="US"),
            _candidate("ASML.AS", country="NL"),
            _candidate("NVO", country="US"),
            _candidate("NOVO-B.CO", country="DK"),
            _candidate("TLS", country="US"),
            _candidate("TLS.AX", country="AU"),
        ],
        {
            "SAP": _security_mapping("SAP", "SAP SE-SPONSORED ADR", share="BBG001S6RD41", composite="BBG000BDSLD7", country="US", security_type="ADR"),
            "SAP.DE": _security_mapping("SAP", "SAP SE", share="BBG001S6RK27", composite="BBG000BG7DY8", country="DE"),
            "ASML": _security_mapping("ASML", "ASML HOLDING NV-NY REG SHS", share="BBG001SCG0R3", composite="BBG000K6MRN4", country="US", security_type="Depositary Receipt"),
            "ASML.AS": _security_mapping("ASML", "ASML HOLDING NV", share="BBG001S7Q066", composite="BBG000C1HSN8", country="NL"),
            "NVO": _security_mapping("NVO", "NOVO-NORDISK A/S-SPONS ADR", share="BBG001S5TSK0", composite="BBG000BQBKR3", country="US", security_type="ADR"),
            "NOVO-B.CO": _security_mapping("NOVOB", "NOVO NORDISK A/S-B", share="BBG001S6RN12", composite="BBG000F8TYC6", country="DK"),
            "TLS": _security_mapping("TLS", "TELOS CORP", share="BBG00TLSUS01", composite="BBG00TLSUS02", country="US"),
            "TLS.AX": _security_mapping("TLS", "TELSTRA GROUP LTD", share="BBG00TLSAU01", composite="BBG00TLSAU02", country="AU"),
        },
    )

    summary = result.summary()
    assert summary["security_resolved_listings"] == 8
    assert summary["entity_resolved_listings"] == 0
    assert summary["entity_unresolved_security_only"] == 8
    assert summary["strong_company_identifier_groups"] == 0
    assert result.entities == []
    assert result.listings == []
    same_name_audits = [
        audit
        for audit in result.audits
        if audit.issue_type == "same_name_different_security_identifier"
    ]
    assert {tuple(audit.details["symbols"]) for audit in same_name_audits} >= {
        ("SAP", "SAP.DE"),
        ("ASML", "ASML.AS"),
        ("NOVO-B.CO", "NVO"),
    }
    assert all(set(audit.details.get("symbols", [])) != {"TLS", "TLS.AX"} for audit in same_name_audits)


def test_lenb_not_found_is_isolated_from_successful_batch_members() -> None:
    result = _resolve(
        [_candidate("LEN"), _candidate("LENB")],
        {
            "LEN": _security_mapping("LEN", "LENNAR CORP-A", share="LEN-SHARE", composite="LEN-COMP"),
            "LENB": {"error": "No identifier found."},
        },
    )

    summary = result.summary()
    assert summary["security_resolved_listings"] == 1
    assert summary["entity_unresolved_no_openfigi_match"] == 1
    assert summary["entity_unresolved_openfigi_error"] == 0
    assert result.unresolved_symbols == ["LEN", "LENB"]


def test_strong_legal_entity_id_creates_entity_records() -> None:
    result = _resolve(
        [_candidate("SAP", country="US"), _candidate("SAP.DE", country="DE")],
        {
            "SAP": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-ADR-SHARE", composite="SAP-ADR-COMP", country="US", home="DE"),
            "SAP.DE": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-DE-SHARE", composite="SAP-DE-COMP", country="DE", home="DE"),
        },
    )

    summary = result.summary()
    assert summary["security_resolved_listings"] == 2
    assert summary["entity_resolved_listings"] == 2
    assert summary["strong_company_identifier_groups"] == 1
    assert len(result.entities) == 1


def test_openfigi_error_for_one_symbol_does_not_abort_batch() -> None:
    result = _resolve(
        [_candidate("ERR"), _candidate("OK")],
        {
            "ERR": {"status": "error", "error_message": "rate limited"},
            "OK": _mapping("OK", "Okay Corp", legal="OK-LEGAL", share="OK-SHARE"),
        },
    )

    assert result.unresolved_symbols == ["ERR"]
    assert [listing.symbol for listing in result.listings] == ["OK"]
    assert any(audit.issue_type == "openfigi_unresolved_mapping" and audit.symbol == "ERR" for audit in result.audits)


def test_primary_listing_policy_prefers_complete_home_listing() -> None:
    result = _resolve(
        [
            _candidate("SAP", country="US", has_prices=True, has_technicals=True),
            _candidate("SAP.DE", country="DE", has_prices=True, has_technicals=True),
        ],
        {
            "SAP": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", country="US", home="DE"),
            "SAP.DE": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", country="DE", home="DE"),
        },
    )

    assert result.entities[0].primary_listing_symbol == "SAP.DE"
    assert result.entities[0].primary_listing_reason == "complete_home_listing"


def test_primary_listing_policy_uses_adr_when_only_complete_listing() -> None:
    result = _resolve(
        [
            _candidate("SAP", country="US", has_prices=True, has_technicals=True),
            _candidate("SAP.DE", country="DE", has_prices=False, has_technicals=False),
        ],
        {
            "SAP": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", country="US", home="DE"),
            "SAP.DE": _mapping("SAP", "SAP SE", legal="SAP-LEGAL", share="SAP-SHARE", country="DE", home="DE"),
        },
    )

    assert result.entities[0].home_country == "DE"
    assert result.entities[0].primary_listing_symbol == "SAP"
    assert result.entities[0].primary_listing_reason == "only_complete_listing"


def test_fuzzy_only_matches_create_audit_rows_not_resolved_mappings() -> None:
    result = _resolve(
        [
            _candidate("ABC", name="Example PLC"),
            _candidate("ABC.L", name="Example PLC"),
        ],
        {
            "ABC": {"ticker": "ABC", "name": "Example PLC"},
            "ABC.L": {"ticker": "ABC", "name": "Example PLC"},
        },
    )

    assert result.entities == []
    assert result.listings == []
    assert result.unresolved_symbols == ["ABC", "ABC.L"]
    assert any(audit.issue_type == "fuzzy_match_suggestion_not_resolved" for audit in result.audits)


def test_provider_symbol_normalization_creates_audit_without_resolution() -> None:
    result = _resolve(
        [_candidate("NOVO-B.CO", country="DK")],
        {
            "NOVO-B.CO": _security_mapping("NOVOB", "Novo Nordisk A/S", share="NOVO-SHARE", composite="NOVO-COMP", country="DK"),
        },
    )

    assert result.entities == []
    assert result.summary()["provider_symbol_normalized"] == 1
    assert any(audit.issue_type == "provider_symbol_normalized" for audit in result.audits)


def test_publisher_only_targets_entity_layer_tables() -> None:
    result = _resolve(
        [_candidate("OK")],
        {"OK": _mapping("OK", "Okay Corp", legal="OK-LEGAL", share="OK-SHARE")},
    )
    publisher = RecordingPublisher()

    publish_entity_identity(publisher=publisher, result=result)

    assert [call["table"] for call in publisher.upserts] == [
        "source_cache.openfigi_mapping_raw",
        "feature_store.entity_master",
        "feature_store.entity_listing",
    ]
    assert [call["table"] for call in publisher.inserts] == ["feature_store.entity_identity_audit"]


def test_runtime_schema_contains_entity_layer_tables_indexes_and_grants() -> None:
    sql = Path("sql/000_runtime_schema.sql").read_text()

    for snippet in [
        "create table if not exists source_cache.openfigi_mapping_raw",
        "create table if not exists source_cache.gleif_entity_raw",
        "create table if not exists source_cache.listing_isin_raw",
        "create table if not exists source_cache.gleif_isin_lei_raw",
        "create table if not exists source_cache.gleif_lei_isin_raw",
        "create table if not exists feature_store.entity_master",
        "create table if not exists feature_store.entity_listing",
        "create table if not exists feature_store.entity_identity_audit",
        "create table if not exists feature_store.entity_identity_review",
        "create table if not exists feature_store.entity_identity_review_audit",
        "create table if not exists feature_store.entity_identity_publication_batch",
        "create table if not exists feature_store.entity_identity_publication_current",
        "publication_batch_id text",
        "review_key text unique",
        "attach_method text",
        "attach_confidence text",
        "evidence_payload jsonb not null default '{}'::jsonb",
        "source_freshness jsonb not null default '{}'::jsonb",
        "idx_listing_isin_raw_isin",
        "idx_gleif_isin_lei_raw_lei",
        "idx_entity_listing_entity_id",
        "idx_entity_listing_isin",
        "idx_entity_listing_figi",
        "idx_entity_listing_composite_figi",
        "idx_entity_listing_share_class_figi",
        "idx_entity_listing_exchange_mic",
        "idx_entity_master_home_country",
        "idx_entity_master_publication_batch_id",
        "idx_entity_listing_publication_batch_id",
        "idx_entity_identity_audit_symbol_issue_type",
        "idx_entity_identity_review_symbol_state",
        "idx_entity_identity_review_audit_review_id",
        "idx_entity_identity_review_publication_batch_id",
        "idx_entity_identity_publication_batch_scope_current",
        "grant select, insert, update, delete",
        "finance_data_ops_worker",
        "feature_store.entity_identity_publication_batch",
        "feature_store.entity_identity_publication_current",
    ]:
        assert snippet in sql


def _resolve(candidates: list[ListingCandidate], fixtures: dict[str, dict]):
    client = OpenFigiClient(fixture_mappings=fixtures)
    mappings = client.map_candidates(candidates)
    return build_entity_identity(candidates=candidates, mappings=mappings)


def _candidate(
    symbol: str,
    *,
    country: str = "US",
    currency: str = "USD",
    exchange_mic: str = "XNYS",
    name: str = "",
    has_prices: bool = True,
    has_technicals: bool = True,
) -> ListingCandidate:
    return ListingCandidate(
        symbol=symbol,
        provider_symbol=symbol,
        country=country,
        currency=currency,
        exchange_mic=exchange_mic,
        name=name,
        has_prices=has_prices,
        has_technicals=has_technicals,
    )


def _mapping(
    ticker: str,
    name: str,
    *,
    legal: str,
    share: str,
    composite: str = "",
    country: str = "US",
    home: str = "US",
) -> dict:
    return {
        "ticker": ticker,
        "name": name,
        "legalEntityId": legal,
        "figi": f"{share}-FIGI",
        "shareClassFIGI": share,
        "compositeFIGI": composite or f"{share}-COMP",
        "country": country,
        "homeCountry": home,
        "currency": "USD",
        "micCode": "XNYS",
        "securityType2": "Common Stock",
    }


def _security_mapping(
    ticker: str,
    name: str,
    *,
    share: str,
    composite: str,
    country: str = "US",
    security_type: str = "Common Stock",
) -> dict:
    return {
        "ticker": ticker,
        "name": name,
        "figi": f"{share}-FIGI",
        "shareClassFIGI": share,
        "compositeFIGI": composite,
        "country": country,
        "currency": "USD",
        "exchCode": "US" if country == "US" else country,
        "securityType2": security_type,
    }


class _PayloadTooLargeThenSuccessSession:
    def post(self, _url: str, *, headers: dict, json: list[dict], timeout: int) -> "_FakeOpenFigiResponse":
        if len(json) > 1:
            return _FakeOpenFigiResponse(413, {"error": "Payload Too Large"})
        ticker = json[0]["idValue"]
        return _FakeOpenFigiResponse(
            200,
            [
                {
                    "data": [
                        {
                            "ticker": ticker,
                            "name": f"{ticker} CORP",
                            "figi": f"{ticker}-FIGI",
                            "compositeFIGI": f"{ticker}-COMP",
                            "shareClassFIGI": f"{ticker}-SHARE",
                            "securityType2": "Common Stock",
                        }
                    ]
                }
            ],
        )


class _OpenFigiVariantSession:
    def __init__(self, *, success_when) -> None:
        self.success_when = success_when
        self.payloads: list[dict] = []

    def post(self, _url: str, *, headers: dict, json: list[dict], timeout: int) -> "_FakeOpenFigiResponse":
        payload = dict(json[0])
        self.payloads.append(payload)
        if not self.success_when(payload):
            return _FakeOpenFigiResponse(200, [{"data": []}])
        ticker = payload["idValue"]
        return _FakeOpenFigiResponse(
            200,
            [
                {
                    "data": [
                        {
                            "ticker": ticker,
                            "name": f"{ticker} CORP",
                            "figi": f"{ticker}-FIGI",
                            "compositeFIGI": f"{ticker}-COMP",
                            "shareClassFIGI": f"{ticker}-SHARE",
                            "micCode": payload.get("micCode"),
                            "securityType2": "Common Stock",
                        }
                    ]
                }
            ],
        )


class _FakeOpenFigiResponse:
    def __init__(self, status_code: int, payload: object) -> None:
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> object:
        return self._payload
