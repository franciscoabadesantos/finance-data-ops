from __future__ import annotations

from pathlib import Path

from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity
from finance_data_ops.identity.resolver import build_entity_identity
from finance_data_ops.publish.client import RecordingPublisher


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
        "create table if not exists feature_store.entity_master",
        "create table if not exists feature_store.entity_listing",
        "create table if not exists feature_store.entity_identity_audit",
        "idx_entity_listing_entity_id",
        "idx_entity_listing_isin",
        "idx_entity_listing_figi",
        "idx_entity_listing_composite_figi",
        "idx_entity_listing_share_class_figi",
        "idx_entity_listing_exchange_mic",
        "idx_entity_master_home_country",
        "idx_entity_identity_audit_symbol_issue_type",
        "grant select, insert, update, delete",
        "finance_data_ops_worker",
        "grant select on feature_store.entity_master, feature_store.entity_listing, feature_store.entity_identity_audit",
    ]:
        assert snippet in sql


def _resolve(candidates: list[ListingCandidate], fixtures: dict[str, dict]) :
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
