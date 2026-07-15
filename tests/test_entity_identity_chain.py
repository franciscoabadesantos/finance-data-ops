from __future__ import annotations

from finance_data_ops.identity.chain import (
    _heuristic_attach_audit,
    acceptance_fixture_candidates,
    acceptance_gleif_fixtures,
    acceptance_gleif_legal_name_fixtures,
    acceptance_gleif_lei_isin_fixtures,
    acceptance_isin_fixtures,
    acceptance_openfigi_fixtures,
    acceptance_pairs_for_symbols,
    measure_entity_identity_chain,
)
from finance_data_ops.identity.models import ListingCandidate, OpenFigiMapping
from finance_data_ops.identity.gleif import (
    GleifIsinLeiClient,
    GleifIsinLeiRecord,
    GleifLegalNameRecord,
    GleifLeiIsinRecord,
)
from finance_data_ops.identity.isin import IsinRecord, YFinanceIsinClient, isin_prefix_policy_for_listing
from finance_data_ops.identity.names import (
    legal_name_query_from_listing,
    legal_name_query_variants_from_listing,
    name_normalization_audit_flags,
    normalize_legal_name_conservative,
)
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import (
    build_entity_publication_batch_id,
    build_side_by_side_entity_publication_plan,
    publish_entity_identity_controlled,
    publish_entity_identity_raw_caches,
    publish_entity_identity_side_by_side,
)
from finance_data_ops.publish.client import RecordingPublisher
from scripts.measure_entity_identity_chain import (
    _audit_forward_lookup_isins,
    _gleif_lei_expansion_lookup_leis,
    _gleif_lei_expansion_lookup_plan,
    _merge_gleif_records,
)

_PREFIX_MISMATCH_ISIN_REASONS = {
    "provider_returned_alternate_market_instrument",
    "provider_listing_mismatch",
    "isin_prefix_mismatch",
}


def test_acceptance_fixture_chain_reports_symbol_pair_outcomes() -> None:
    measurement = _fixture_measurement()
    summary = measurement.summary

    assert summary["candidate_count"] == 14
    assert summary["isin_found_count"] == 4
    assert summary["lei_found_count"] == 4
    assert summary["anchor_isin_count"] == 4
    assert summary["anchor_lei_count"] == 4
    assert summary["listings_attached_direct_isin"] == 4
    assert summary["listings_attached_via_lei_expansion"] == 4
    assert summary["listings_attached_name_anchor_confirmed"] == 6
    assert summary["entity_groups_formed"] == 7
    assert summary["acceptance_pairs_grouped"] == 7
    assert summary["pairs_grouped_direct_anchor_plus_lei_expansion"] == 4
    assert summary["pairs_grouped_name_anchor_confirmed"] == 3
    assert summary["pairs_grouped_direct_lei"] == 0
    assert summary["pairs_blocked_no_valid_anchor"] == 0
    assert summary["adr_home_pairs_grouped"] == 4
    assert summary["share_class_pairs_grouped"] == 2
    assert summary["adrs_mapping_to_depositary_or_ambiguous_lei_count"] == 0
    assert summary["unresolved_no_isin_count"] == 6
    assert summary["unresolved_no_lei_count"] == 0
    assert summary["name_anchor_precision_audit_count"] == 6
    assert summary["precision_audit_count"] == 6
    assert summary["heuristic_attach_audit_count"] == 6
    assert summary["non_direct_attach_audit_count"] == 10
    assert summary["publication_gate_status"] == "ready_machine_safe"
    assert summary["publication_gate_review_required_count"] == 0
    assert summary["gleif_lei_expansion_requested_count"] == 7
    assert summary["gleif_lei_expansion_requested_by_origin"]["prefix_compatible_direct_anchor"] == 4
    assert summary["gleif_lei_expansion_requested_by_origin"]["legal_name_candidate"] == 3
    assert summary["gleif_lei_expansion_requested_by_origin"]["prefix_mismatch_isolated_candidate"] == 0
    assert summary["gleif_lei_expansion_status_counts"] == {"success": 7}
    assert summary["attached_count"] == 14
    assert summary["attached_rate"] == 1.0
    assert summary["fixable_free_count"] == 0
    assert summary["fixable_free_rate"] == 0.0
    assert summary["provider_or_curated_count"] == 0
    assert summary["provider_or_curated_rate"] == 0.0
    assert summary["review_count"] == 0
    assert summary["review_rate"] == 0.0
    assert summary["accepted_pairs_passed"] is True
    assert summary["guardrail_pairs_unmerged"] is True
    assert measurement.name_anchor_precision_audit
    assert measurement.heuristic_attach_audit
    assert measurement.publication_gate["publication_allowed_without_review"] is True

    by_pair = {tuple(row["pair"]): row for row in measurement.pair_rows}
    assert by_pair[("SAP", "SAP.DE")]["grouped"] is True
    assert by_pair[("SAP", "SAP.DE")]["right_attach_method"] == "lei_expansion"
    assert by_pair[("ASML", "ASML.AS")]["grouped"] is True
    assert by_pair[("NVO", "NOVO-B.CO")]["grouped"] is True
    assert by_pair[("RIO", "RIO.L")]["grouped"] is True
    assert by_pair[("0005.HK", "HSBA.L")]["grouped"] is True
    assert by_pair[("0005.HK", "HSBA.L")]["grouping_path"] == "name_anchor_confirmed"
    assert by_pair[("GOOG", "GOOGL")]["grouped"] is True
    assert by_pair[("GOOG", "GOOGL")]["grouping_path"] == "name_anchor_confirmed"
    assert by_pair[("LEN", "LENB")]["grouped"] is True
    assert by_pair[("LEN", "LENB")]["grouping_path"] == "name_anchor_confirmed"


def test_symbol_rows_include_required_measurement_fields() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    sap = next(row for row in measurement.symbol_rows if row["symbol"] == "SAP")

    assert sap["provider_symbol"] == "SAP"
    assert sap["openfigi_ticker"] == "SAP"
    assert sap["openfigi_exchange"] == "US"
    assert sap["figi"]
    assert sap["compositeFIGI"]
    assert sap["shareClassFIGI"]
    assert sap["isin_source"] == "fixture_yfinance"
    assert sap["isin"] == "US8030542042"
    assert sap["lei_source"] == "fixture_gleif"
    assert sap["lei"] == "529900D6BF99LW9R2E68"
    assert sap["legal_name"] == "SAP SE"
    assert sap["openfigi_name"] == "SAP SE-SPONSORED ADR"
    assert sap["internal_candidate_name"] == "SAP SE-SPONSORED ADR"
    assert sap["listing_name_used_for_legal_name_search"] == "Sap Se"
    assert sap["legal_name_anchor_status"] == "not_requested_direct_isin"
    assert sap["decision_bucket"] == "attached"


def test_isin_missing_and_lei_missing_are_measured_separately() -> None:
    measurement = _fixture_measurement(
        ["AAA", "BBB"],
        isin_fixtures={
            "AAA": {"status": "not_found", "error_message": "no_isin"},
            "BBB": {"isin": "USBBBBBBBBB1", "source": "fixture_yfinance"},
        },
        gleif_fixtures={"USBBBBBBBBB1": {"status": "not_found", "error_message": "no_lei"}},
    )

    assert measurement.summary["unresolved_no_isin_count"] == 1
    assert measurement.summary["unresolved_no_lei_count"] == 1


def test_live_like_prefix_mismatch_yfinance_isins_are_diagnostic_or_missing() -> None:
    measurement = _fixture_measurement(
        ["GOOG", "GOOGL", "ASML.AS", "0005.HK", "SAP.DE", "HSBA.L", "LEN", "NOVO-B.CO"],
        isin_fixtures={
            "GOOG": {"isin": "CA02080M1005", "source": "fixture_yfinance"},
            "GOOGL": {"isin": "CA02080M1005", "source": "fixture_yfinance"},
            "ASML.AS": {"isin": "AR0725224551", "source": "fixture_yfinance"},
            "0005.HK": {"isin": "ARDEUT112257", "source": "fixture_yfinance"},
            "SAP.DE": {"isin": "-", "source": "fixture_yfinance"},
            "HSBA.L": {"isin": "-", "source": "fixture_yfinance"},
            "LEN": {"isin": "-", "source": "fixture_yfinance"},
            "NOVO-B.CO": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={},
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["GOOG"]["isin_status"] == "suspect"
    assert rows["GOOG"]["isin_error_reason"] == "provider_returned_alternate_market_instrument"
    assert rows["GOOG"]["isin_prefix_match"] is False
    assert rows["GOOG"]["isin_prefix_diagnostic"] == ""
    assert rows["GOOG"]["isin"] == ""
    assert rows["GOOG"]["raw_isin"] == "CA02080M1005"
    assert rows["ASML.AS"]["isin_status"] == "suspect"
    assert rows["ASML.AS"]["isin_error_reason"] == "provider_listing_mismatch"
    assert rows["0005.HK"]["isin_status"] == "suspect"
    assert rows["0005.HK"]["isin_error_reason"] == "provider_listing_mismatch"
    assert rows["SAP.DE"]["isin_status"] == "not_found"
    assert rows["SAP.DE"]["isin_error_reason"] == "placeholder_isin"
    assert measurement.summary["isin_suspect_count"] == 4
    assert measurement.summary["isin_found_count"] == 0


def test_gleif_lei_records_endpoint_fixture_parses_lei_and_legal_name() -> None:
    session = _FakeGleifSession(
        {
            "data": [
                {
                    "id": "5493006MHB84DD0ZWV18",
                    "attributes": {
                        "lei": "5493006MHB84DD0ZWV18",
                        "entity": {"legalName": {"name": "Alphabet Inc."}},
                    },
                }
            ]
        }
    )
    client = GleifIsinLeiClient(session=session)

    record = client.lookup_isin("US02079K1079")

    assert session.requested_url.endswith("/lei-records")
    assert session.requested_params == {"filter[isin]": "US02079K1079"}
    assert record.status == "success"
    assert record.lei == "5493006MHB84DD0ZWV18"
    assert record.legal_name == "Alphabet Inc."
    assert record.response_payload is not None


def test_gleif_lei_isins_endpoint_paginates_and_parses_expanded_isins() -> None:
    session = _FakeGleifSession(
        [
            {
                "data": [{"id": "US02079K1079", "type": "isins"}],
                "meta": {"pagination": {"currentPage": 1, "totalPages": 2}},
            },
            {
                "data": [{"attributes": {"isin": "US02079K3059"}}],
                "meta": {"pagination": {"currentPage": 2, "totalPages": 2}},
            },
        ]
    )
    client = GleifIsinLeiClient(session=session, page_size=1)

    record = client.lookup_lei_isin("5493006MHB84DD0ZWV18")

    assert session.requested_urls == [
        "https://api.gleif.org/api/v1/lei-records/5493006MHB84DD0ZWV18/isins",
        "https://api.gleif.org/api/v1/lei-records/5493006MHB84DD0ZWV18/isins",
    ]
    assert session.requested_params_list == [
        {"page[size]": 1, "page[number]": 1},
        {"page[size]": 1, "page[number]": 2},
    ]
    assert record.status == "success"
    assert record.source == "gleif_lei_record_isins"
    assert record.isin_list == ["US02079K1079", "US02079K3059"]


def test_gleif_lei_isins_retries_429_retry_after_then_success() -> None:
    sleeps: list[float] = []
    session = _FakeGleifSession(
        [
            {"errors": [{"title": "Too Many Requests"}]},
            {"data": [{"id": "US02079K1079", "type": "isins"}]},
        ],
        status_codes=[429, 200],
        headers=[{"Retry-After": "2"}, {}],
    )
    client = GleifIsinLeiClient(
        session=session,
        lei_isin_max_retries=2,
        retry_backoff_seconds=0.1,
        retry_jitter_seconds=0.0,
        request_sleep_seconds=0.0,
        sleep_func=sleeps.append,
    )

    record = client.lookup_lei_isin("5493006MHB84DD0ZWV18")

    assert record.status == "success"
    assert record.isin_list == ["US02079K1079"]
    assert sleeps == [2.0]
    assert len(session.requested_urls) == 2


def test_gleif_lei_isins_persistent_429_is_rate_limited_not_not_found() -> None:
    sleeps: list[float] = []
    session = _FakeGleifSession(
        {"errors": [{"title": "Too Many Requests"}]},
        status_codes=[429, 429, 429],
    )
    client = GleifIsinLeiClient(
        session=session,
        lei_isin_max_retries=2,
        retry_backoff_seconds=0.5,
        retry_jitter_seconds=0.0,
        request_sleep_seconds=0.0,
        sleep_func=sleeps.append,
    )

    record = client.lookup_lei_isin("5493006MHB84DD0ZWV18")

    assert record.status == "rate_limited"
    assert record.isin_list == []
    assert "429 Client Error: Too Many Requests" in record.error_message
    assert sleeps == [0.5, 1.0]
    assert len(session.requested_urls) == 3


def test_gleif_lei_isins_throttles_between_unique_lei_requests() -> None:
    sleeps: list[float] = []
    session = _FakeGleifSession({"data": [{"id": "US02079K1079", "type": "isins"}]})
    client = GleifIsinLeiClient(
        session=session,
        request_sleep_seconds=1.25,
        retry_jitter_seconds=0.0,
        sleep_func=sleeps.append,
    )

    records = client.lookup_lei_isins(["LEI1", "LEI1", "LEI2", "LEI3"])

    assert [record.status for record in records] == ["success", "success", "success"]
    assert sleeps == [1.25, 1.25]
    assert len(session.requested_urls) == 3


def test_gleif_legal_name_search_tries_variants_until_success() -> None:
    session = _FakeGleifSession(
        [
            {"data": []},
            {
                "data": [
                    {
                        "id": "HDLEI000000001",
                        "attributes": {
                            "lei": "HDLEI000000001",
                            "entity": {
                                "legalName": {"name": "THE HOME DEPOT, INC."},
                                "legalAddress": {"country": "US"},
                                "headquartersAddress": {"country": "US"},
                                "jurisdiction": "US-DE",
                                "status": "ACTIVE",
                            },
                            "registration": {"status": "ISSUED"},
                        },
                    }
                ]
            },
        ]
    )
    client = GleifIsinLeiClient(session=session)

    records = client.search_legal_names(["Home Depot Inc The", "The Home Depot Inc"])

    assert len(records) == 1
    assert records[0].status == "success"
    assert records[0].candidates[0]["lei"] == "HDLEI000000001"
    assert session.requested_params_list[0]["filter[entity.legalName]"] == "Home Depot Inc The"
    assert session.requested_params_list[1]["filter[entity.legalName]"] == "The Home Depot Inc"


def test_legal_name_query_preserves_corporate_suffix_but_confirmation_normalizes_it() -> None:
    assert legal_name_query_from_listing("ALPHABET INC-CL C") == "Alphabet Inc"
    assert legal_name_query_from_listing("LENNAR CORP-A") == "Lennar Corporation"
    assert legal_name_query_from_listing("HSBC HOLDINGS PLC") == "Hsbc Holdings Plc"
    assert legal_name_query_from_listing("ELI LILLY & CO") == "Eli Lilly And Company"
    assert legal_name_query_variants_from_listing("HOME DEPOT INC/THE")[:3] == [
        "Home Depot Inc The",
        "Home Depot Inc",
        "The Home Depot Inc",
    ]
    assert "Sony Group" in legal_name_query_variants_from_listing("SONY GROUP CORP")
    assert "Sk Hynix" in legal_name_query_variants_from_listing("SK HYNIX INC")
    assert "Telos" in legal_name_query_variants_from_listing("TELOS CORPORATION")
    schlumberger_variants = legal_name_query_variants_from_listing("Schlumberger Nv")
    assert "Schlumberger N.V." in schlumberger_variants
    assert "Schlumberger" in schlumberger_variants
    assert normalize_legal_name_conservative("ALPHABET INC-CL C") == "ALPHABET"
    assert normalize_legal_name_conservative("ALPHABET INC.") == "ALPHABET"
    assert normalize_legal_name_conservative("THE HOME DEPOT INC.") == "HOME DEPOT"
    assert normalize_legal_name_conservative("HOME DEPOT INC/THE") == "HOME DEPOT"
    assert normalize_legal_name_conservative("ELI LILLY AND CO") == "ELI LILLY AND"
    assert normalize_legal_name_conservative("ELI LILLY AND COMPANY") == "ELI LILLY AND"
    assert normalize_legal_name_conservative("NECキャピタルソリューション株式会社") == "NEC キャピタルソリューション"
    assert normalize_legal_name_conservative("NEC株式会社") == "NEC"
    flags = name_normalization_audit_flags("NECキャピタルソリューション株式会社", "NEC")
    assert flags["cjk_name_collapsed_to_latin_acronym"] is True
    assert flags["distinctive_tokens_removed"] is True


def test_adr_isin_can_map_to_underlying_lei() -> None:
    for left, right in [("SAP", "SAP.DE"), ("ASML", "ASML.AS"), ("NVO", "NOVO-B.CO"), ("RIO", "RIO.L")]:
        measurement = _fixture_measurement([left, right])
        pair = measurement.pair_rows[0]

        assert pair["grouped"] is True
        assert pair["reason"] == "shared_lei"


def test_adr_isin_can_map_to_depositary_or_ambiguous_lei() -> None:
    measurement = _fixture_measurement(
        ["SAP", "SAP.DE"],
        gleif_fixtures={
            "US8030542042": {
                "lei": "SAPADRDEPOSITARYLEI1",
                "legal_name": "DEPOSITARY BANK FOR SAP ADR",
                "lei_role": "depositary",
                "source": "fixture_gleif",
            },
        },
        gleif_lei_isin_fixtures={
            "LEI:SAPADRDEPOSITARYLEI1": {
                "legal_name": "DEPOSITARY BANK FOR SAP ADR",
                "isin_list": ["US8030542042"],
            }
        },
    )
    pair = measurement.pair_rows[0]

    assert pair["grouped"] is False
    assert pair["reason"] == "missing_isin_or_anchor"
    assert measurement.summary["adrs_mapping_to_depositary_or_ambiguous_lei_count"] == 1


def test_share_classes_with_different_isins_group_by_same_lei() -> None:
    measurement = _fixture_measurement(["GOOG", "GOOGL", "LEN", "LENB"])
    by_pair = {tuple(row["pair"]): row for row in measurement.pair_rows}

    assert by_pair[("GOOG", "GOOGL")]["left_isin"] == ""
    assert by_pair[("GOOG", "GOOGL")]["left_attach_method"] == "name_anchor_confirmed"
    assert by_pair[("GOOG", "GOOGL")]["right_attach_method"] == "name_anchor_confirmed"
    assert by_pair[("GOOG", "GOOGL")]["grouped"] is True
    assert by_pair[("LEN", "LENB")]["left_isin"] == ""
    assert by_pair[("LEN", "LENB")]["left_attach_method"] == "name_anchor_confirmed"
    assert by_pair[("LEN", "LENB")]["right_attach_method"] == "name_anchor_confirmed"
    assert by_pair[("LEN", "LENB")]["grouped"] is True


def test_same_fuzzy_name_with_different_lei_does_not_group() -> None:
    measurement = _fixture_measurement(
        ["AAA", "AAA.L"],
        isin_fixtures={
            "AAA": {"isin": "US0000000002", "source": "fixture_yfinance"},
            "AAA.L": {"isin": "GB0000000009", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "US0000000002": {"lei": "LEIUSDIFFERENT1", "legal_name": "EXAMPLE PLC"},
            "GB0000000009": {"lei": "LEIGBDIFFERENT2", "legal_name": "EXAMPLE PLC"},
        },
        pairs=[("AAA", "AAA.L", "cross_listing")],
    )

    assert measurement.pair_rows[0]["grouped"] is False
    assert measurement.pair_rows[0]["reason"] == "different_lei"


def test_isolated_direct_isin_prefix_mismatch_can_attach_after_baseline_paths() -> None:
    symbols = ["ACN", "BNTX", "DLO", "TRI", "WPM", "NE"]
    measurement = _fixture_measurement(
        symbols,
        openfigi_fixtures={
            "ACN": _security_fixture("ACN", "ACCENTURE PLC"),
            "BNTX": _security_fixture("BNTX", "BIONTECH SE"),
            "DLO": _security_fixture("DLO", "DLOCAL LTD"),
            "TRI": _security_fixture("TRI", "THOMSON REUTERS CORP"),
            "WPM": _security_fixture("WPM", "WHEATON PRECIOUS METALS CORP"),
            "NE": _security_fixture("NE", "NOBLE CORP PLC"),
        },
        isin_fixtures={
            "ACN": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"},
            "BNTX": {"isin": "DE000A2PSR20", "source": "fixture_yfinance"},
            "DLO": {"isin": "KYG290181018", "source": "fixture_yfinance"},
            "TRI": {"isin": "CA8849038085", "source": "fixture_yfinance"},
            "WPM": {"isin": "CA9628791027", "source": "fixture_yfinance"},
            "NE": {"isin": "GB00BMXNWH07", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "IE00B4BNMY34": {"lei": "ACNLEI00000001", "legal_name": "ACCENTURE PUBLIC LIMITED COMPANY"},
            "DE000A2PSR20": {"lei": "BNTXLEI0000001", "legal_name": "BIONTECH SE"},
            "KYG290181018": {"lei": "DLOLEI00000001", "legal_name": "DLOCAL LIMITED"},
            "CA8849038085": {"lei": "TRILEI00000001", "legal_name": "THOMSON REUTERS CORPORATION"},
            "CA9628791027": {"lei": "WPMLEI00000001", "legal_name": "WHEATON PRECIOUS METALS CORP."},
            "GB00BMXNWH07": {"lei": "NELEI000000001", "legal_name": "NOBLE CORPORATION PLC"},
        },
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC"),
            ListingCandidate(symbol="BNTX", provider_symbol="BNTX", country="US", currency="USD", name="BIONTECH SE"),
            ListingCandidate(symbol="DLO", provider_symbol="DLO", country="US", currency="USD", name="DLOCAL LTD"),
            ListingCandidate(symbol="TRI", provider_symbol="TRI", country="US", currency="USD", name="THOMSON REUTERS CORP"),
            ListingCandidate(symbol="WPM", provider_symbol="WPM", country="US", currency="USD", name="WHEATON PRECIOUS METALS CORP"),
            ListingCandidate(symbol="NE", provider_symbol="NE", country="US", currency="USD", name="NOBLE CORP PLC"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    for symbol in symbols:
        assert rows[symbol]["entity_attach_method"] == "isin_direct_prefix_mismatch_name_confirmed"
        assert rows[symbol]["attachment_provenance"] == "isin_direct_prefix_mismatch_name_confirmed"
        assert rows[symbol]["attachment_confidence"] == "high"
        assert rows[symbol]["isin_status"] == "suspect"
        assert rows[symbol]["isin_error_reason"] == "provider_returned_alternate_market_instrument"
        assert rows[symbol]["isin"] == rows[symbol]["raw_isin"]
        assert rows[symbol]["direct_prefix_mismatch_candidate_status"] == "confirmed"
        assert rows[symbol]["compatible_isin_gate_status"] == "diagnostic_prefix_mismatch"
        assert rows[symbol]["decision_bucket"] == "attached"
    assert measurement.summary["listings_attached_direct_isin_prefix_mismatch_name_confirmed"] == 6
    assert measurement.summary["attached_count"] == 6


def test_shop_prefix_mismatch_without_name_confirmation_stays_manual_review() -> None:
    measurement = _fixture_measurement(
        ["SHOP"],
        openfigi_fixtures={"SHOP": _security_fixture("SHOP", "SHOPIFY INC")},
        isin_fixtures={"SHOP": {"isin": "CA82509L1076", "source": "fixture_yfinance"}},
        gleif_fixtures={
            "CA82509L1076": {
                "lei": "SHOPWRONGLEI001",
                "legal_name": "SHOP APOTHEKE EUROPE N.V.",
            }
        },
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="SHOP", provider_symbol="SHOP", country="US", currency="USD", name="SHOPIFY INC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_lei"] == ""
    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["direct_lei"] == ""
    assert row["direct_prefix_mismatch_candidate_status"] == "rejected"
    assert row["direct_prefix_mismatch_candidate_reject_reason"] == "direct_prefix_mismatch_name_unconfirmed"
    assert row["decision_bucket"] == "needs_manual_review"
    assert measurement.summary["manual_review_by_listing_group_kind"] == {"single_listing": 1}


def test_direct_prefix_mismatch_does_not_override_ambiguous_legal_name_candidate() -> None:
    measurement = _fixture_measurement(
        ["ACN"],
        openfigi_fixtures={"ACN": _security_fixture("ACN", "ACCENTURE PLC")},
        isin_fixtures={"ACN": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"}},
        gleif_fixtures={"IE00B4BNMY34": {"lei": "ACNLEI00000001", "legal_name": "ACCENTURE PUBLIC LIMITED COMPANY"}},
        gleif_lei_isin_fixtures={
            "LEI:ACNLEI00000001": {"legal_name": "ACCENTURE PUBLIC LIMITED COMPANY", "isin_list": ["US0000000011"]},
            "LEI:ACNLEI00000002": {"legal_name": "ACCENTURE PLC", "isin_list": ["US0000000029"]},
        },
        gleif_legal_name_fixtures={
            "NAME:ACCENTURE": {
                "candidates": [
                    _legal_candidate("ACNLEI00000001", "ACCENTURE PLC", country="US"),
                    _legal_candidate("ACNLEI00000002", "ACCENTURE PLC", country="US"),
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_ambiguous"
    assert row["direct_prefix_mismatch_candidate_status"] == "not_requested"
    assert row["decision_bucket"] == "needs_manual_review"


def test_direct_prefix_mismatch_requires_us_listing() -> None:
    measurement = _fixture_measurement(
        ["CSL.AX"],
        openfigi_fixtures={"CSL.AX": _security_fixture("CSL", "CSL LTD", country="AU")},
        isin_fixtures={"CSL.AX": {"isin": "GB00BMXNWH07", "source": "fixture_yfinance"}},
        gleif_fixtures={"GB00BMXNWH07": {"lei": "CSLLEI0000001", "legal_name": "CSL LIMITED"}},
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="CSL.AX", provider_symbol="CSL.AX", country="AU", currency="AUD", name="CSL LTD"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["direct_prefix_mismatch_candidate_status"] == "rejected"
    assert row["direct_prefix_mismatch_candidate_reject_reason"] == "direct_prefix_mismatch_non_us_listing"
    assert row["entity_lei"] == ""


def test_us_foreign_issuer_can_attach_from_legal_name_candidate_when_direct_isin_lei_missing() -> None:
    measurement = _fixture_measurement(
        ["ACN", "DLO", "TRI"],
        openfigi_fixtures={
            "ACN": _security_fixture("ACN", "ACCENTURE PLC"),
            "DLO": _security_fixture("DLO", "DLOCAL LTD"),
            "TRI": _security_fixture("TRI", "THOMSON REUTERS CORP"),
        },
        isin_fixtures={
            "ACN": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"},
            "DLO": {"isin": "KYG290181018", "source": "fixture_yfinance"},
            "TRI": {"isin": "CA8849038812", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "IE00B4BNMY34": {"status": "not_found", "error_message": "no_lei"},
            "KYG290181018": {"status": "not_found", "error_message": "no_lei"},
            "CA8849038812": {"status": "not_found", "error_message": "no_lei"},
        },
        gleif_lei_isin_fixtures={
            "LEI:549300JY6CF6DO4YFQ03": {"legal_name": "ACCENTURE PLC", "isin_list": ["IE00B4BNMY34"]},
            "LEI:529900D15DJKVN3RCO35": {"legal_name": "DLocal Limited", "isin_list": ["KYG290181018"]},
            "LEI:549300561UZND4C7B569": {"legal_name": "THOMSON REUTERS CORPORATION", "isin_list": ["CA8849038812"]},
        },
        gleif_legal_name_fixtures={
            "NAME:ACCENTURE": {
                "candidates": [_legal_candidate("549300JY6CF6DO4YFQ03", "ACCENTURE PLC", country="IE")]
            },
            "NAME:DLOCAL": {
                "candidates": [_legal_candidate("529900D15DJKVN3RCO35", "DLocal Limited", country="KY")]
            },
            "NAME:THOMSON REUTERS": {
                "candidates": [_legal_candidate("549300561UZND4C7B569", "THOMSON REUTERS CORPORATION", country="CA")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC"),
            ListingCandidate(symbol="DLO", provider_symbol="DLO", country="US", currency="USD", name="DLOCAL LTD"),
            ListingCandidate(symbol="TRI", provider_symbol="TRI", country="US", currency="USD", name="THOMSON REUTERS CORP"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    for symbol in ("ACN", "DLO", "TRI"):
        assert rows[symbol]["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
        assert rows[symbol]["attachment_confidence"] == "high"
        assert rows[symbol]["direct_prefix_mismatch_candidate_status"] == "confirmed_via_legal_name_candidate"
        assert rows[symbol]["compatible_isin_gate_status"] == "foreign_issuer_prefix_mismatch_confirmed"
        assert rows[symbol]["legal_name_candidate_lei"]
        assert rows[symbol]["legal_name_candidate_lei_in_expansion_request"] is True
        assert rows[symbol]["legal_name_anchor_status"] == "confirmed"
    assert measurement.summary["attached_count"] == 3
    assert measurement.summary["listings_attached_foreign_issuer_name_anchor_confirmed"] == 3


def test_us_foreign_issuer_can_attach_from_legal_name_candidate_without_provider_isin() -> None:
    measurement = _fixture_measurement(
        ["BNTX", "WPM"],
        openfigi_fixtures={
            "BNTX": _security_fixture("BNTX", "BIONTECH SE"),
            "WPM": _security_fixture("WPM", "WHEATON PRECIOUS METALS CORP"),
        },
        isin_fixtures={
            "BNTX": {"status": "not_found", "error_message": "provider_isin_missing"},
            "WPM": {"status": "not_found", "error_message": "provider_isin_missing"},
        },
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:BNTXLEI0000001": {"legal_name": "BIONTECH SE", "isin_list": ["DE000A2PSR20"]},
            "LEI:WPMLEI00000001": {"legal_name": "WHEATON PRECIOUS METALS CORP.", "isin_list": ["CA9628791027"]},
        },
        gleif_legal_name_fixtures={
            "NAME:BIONTECH": {
                "candidates": [_legal_candidate("BNTXLEI0000001", "BIONTECH SE", country="DE")]
            },
            "NAME:WHEATON PRECIOUS METALS": {
                "candidates": [_legal_candidate("WPMLEI00000001", "WHEATON PRECIOUS METALS CORP.", country="CA")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="BNTX", provider_symbol="BNTX", country="US", currency="USD", name="BIONTECH SE"),
            ListingCandidate(
                symbol="WPM",
                provider_symbol="WPM",
                country="US",
                currency="USD",
                name="WHEATON PRECIOUS METALS CORP",
            ),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["BNTX"]["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert rows["BNTX"]["matched_compatible_isins"] == ["DE000A2PSR20"]
    assert rows["WPM"]["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert rows["WPM"]["matched_compatible_isins"] == ["CA9628791027"]
    assert measurement.summary["attached_count"] == 2


def test_us_foreign_issuer_acn_attaches_when_legal_name_candidate_is_not_exact_suffix_match() -> None:
    candidate = ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC")
    measurement = measure_entity_identity_chain(
        candidates=[candidate],
        openfigi_mappings=[
            OpenFigiMapping(
                symbol="ACN",
                request_hash="fixture",
                status="success",
                payload={"exchCode": "US"},
                ticker="ACN",
                name="ACCENTURE PLC",
                country="US",
                currency="USD",
            )
        ],
        isin_records=[
            IsinRecord(
                symbol="ACN",
                provider="fixture_yfinance",
                request_payload={"symbol": "ACN"},
                response_payload={"isin": "IE00B4BNMY34"},
                isin="IE00B4BNMY34",
                status="suspect",
                error_message="provider_returned_alternate_market_instrument",
            )
        ],
        gleif_records=[
            GleifIsinLeiRecord(isin="IE00B4BNMY34", status="not_found", error_message="no_lei")
        ],
        gleif_lei_isin_records=[
            GleifLeiIsinRecord(
                lei="549300JY6CF6DO4YFQ03",
                legal_name="ACCENTURE PUBLIC LIMITED COMPANY",
                isin_list=["IE00B4BNMY34"],
                status="success",
            )
        ],
        gleif_legal_name_records=[
            GleifLegalNameRecord(
                query_name="Accenture",
                normalized_query_name="ACCENTURE",
                status="success",
                candidates=[
                    {
                        **_legal_candidate("549300JY6CF6DO4YFQ03", "ACCENTURE PUBLIC LIMITED COMPANY", country="IE"),
                        "normalized_legal_name": "ACCENTURE PUBLIC",
                    }
                ],
            )
        ],
        gleif_lei_expansion_request_leis=["549300JY6CF6DO4YFQ03"],
        gleif_lei_expansion_request_origin_leis={"legal_name_candidate": ["549300JY6CF6DO4YFQ03"]},
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert row["candidate_lei"] == "549300JY6CF6DO4YFQ03"
    assert row["legal_name_candidate_lei"] == "549300JY6CF6DO4YFQ03"
    assert row["matched_compatible_isins"] == ["IE00B4BNMY34"]
    assert row["compatible_isin_gate_status"] == "foreign_issuer_prefix_mismatch_confirmed"
    assert row["foreign_issuer_candidate_evaluated"] is True
    assert row["foreign_issuer_candidate_lei"] == "549300JY6CF6DO4YFQ03"
    assert row["foreign_issuer_name_match_status"] == "matched"
    assert row["foreign_issuer_name_match_normalized_listing_names"] == ["ACCENTURE"]
    assert row["foreign_issuer_name_match_normalized_legal_name"] == "ACCENTURE"
    assert row["foreign_issuer_lei_expansion_status"] == "success"
    assert row["foreign_issuer_raw_isin"] == "IE00B4BNMY34"
    assert row["foreign_issuer_raw_isin_in_expansion"] is True
    assert row["foreign_issuer_issuer_country_prefixes"] == ["IE"]
    assert row["foreign_issuer_expanded_issuer_country_isin_count"] == 1
    assert row["foreign_issuer_final_gate_status"] == "confirmed"


def test_acn_shaped_generic_candidate_routes_into_foreign_issuer_fallback() -> None:
    measurement = _fixture_measurement(
        ["ACN"],
        openfigi_fixtures={"ACN": _security_fixture("ACN", "ACCENTURE PLC")},
        isin_fixtures={"ACN": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"}},
        gleif_fixtures={"IE00B4BNMY34": {"status": "not_found", "error_message": "no_lei"}},
        gleif_lei_isin_fixtures={
            "LEI:549300JY6CF6DO4YFQ03": {
                "legal_name": "ACCENTURE PLC",
                "isin_list": ["IE00B4BNMY34"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:ACCENTURE": {
                "candidates": [
                    {
                        **_legal_candidate("549300JY6CF6DO4YFQ03", "ACCENTURE PLC", country="IE"),
                        "entity_status": "",
                        "registration_status": "",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert row["candidate_lei"] == "549300JY6CF6DO4YFQ03"
    assert row["legal_name_candidate_lei"] == "549300JY6CF6DO4YFQ03"
    assert row["foreign_issuer_candidate_evaluated"] is True
    assert row["foreign_issuer_raw_isin_in_expansion"] is True
    assert row["foreign_issuer_final_gate_status"] == "confirmed"


def test_us_foreign_issuer_raw_isin_in_lei_expansion_confirms_when_gleif_country_prefix_is_incomplete() -> None:
    measurement = _fixture_measurement(
        ["DLO", "TRI"],
        openfigi_fixtures={
            "DLO": _security_fixture("DLO", "DLOCAL LTD"),
            "TRI": _security_fixture("TRI", "THOMSON REUTERS CORP"),
        },
        isin_fixtures={
            "DLO": {"isin": "KYG290181018", "source": "fixture_yfinance"},
            "TRI": {"isin": "CA8849038812", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "KYG290181018": {"status": "not_found", "error_message": "no_lei"},
            "CA8849038812": {"status": "not_found", "error_message": "no_lei"},
        },
        gleif_lei_isin_fixtures={
            "LEI:529900D15DJKVN3RCO35": {"legal_name": "DLocal Limited", "isin_list": ["KYG290181018"]},
            "LEI:549300561UZND4C7B569": {"legal_name": "THOMSON REUTERS CORPORATION", "isin_list": ["CA8849038812"]},
        },
        gleif_legal_name_fixtures={
            "NAME:DLOCAL": {
                "candidates": [
                    {
                        **_legal_candidate("529900D15DJKVN3RCO35", "DLocal Limited", country="UY"),
                        "jurisdiction": "",
                        "jurisdiction_country": "",
                    }
                ]
            },
            "NAME:THOMSON REUTERS": {
                "candidates": [
                    {
                        **_legal_candidate("549300561UZND4C7B569", "THOMSON REUTERS CORPORATION", country="GB"),
                        "jurisdiction": "",
                        "jurisdiction_country": "",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="DLO", provider_symbol="DLO", country="US", currency="USD", name="DLOCAL LTD"),
            ListingCandidate(symbol="TRI", provider_symbol="TRI", country="US", currency="USD", name="THOMSON REUTERS CORP"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["DLO"]["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert rows["DLO"]["matched_compatible_isins"] == ["KYG290181018"]
    assert rows["DLO"]["foreign_issuer_raw_isin_in_expansion"] is True
    assert rows["DLO"]["foreign_issuer_expanded_issuer_country_isin_count"] == 0
    assert rows["TRI"]["entity_attach_method"] == "foreign_issuer_name_anchor_confirmed"
    assert rows["TRI"]["matched_compatible_isins"] == ["CA8849038812"]
    assert rows["TRI"]["foreign_issuer_raw_isin_in_expansion"] is True
    assert rows["TRI"]["foreign_issuer_expanded_issuer_country_isin_count"] == 0


def test_foreign_issuer_generic_candidate_keeps_gate_closed_without_lei_expansion() -> None:
    measurement = _fixture_measurement(
        ["DLO", "WPM"],
        openfigi_fixtures={
            "DLO": _security_fixture("DLO", "DLOCAL LTD"),
            "WPM": _security_fixture("WPM", "WHEATON PRECIOUS METALS CORP"),
        },
        isin_fixtures={
            "DLO": {"isin": "KYG290181018", "source": "fixture_yfinance"},
            "WPM": {"status": "not_found", "error_message": "provider_isin_missing"},
        },
        gleif_fixtures={"KYG290181018": {"status": "not_found", "error_message": "no_lei"}},
        gleif_lei_isin_fixtures={
            "LEI:529900D15DJKVN3RCO35": {
                "status": "not_found",
                "error_message": "no_gleif_lei_isin_mapping",
                "legal_name": "DLocal Limited",
                "isin_list": [],
            },
            "LEI:549300XSFG5ZCGVYD886": {
                "status": "not_found",
                "error_message": "no_gleif_lei_isin_mapping",
                "legal_name": "WHEATON PRECIOUS METALS CORP.",
                "isin_list": [],
            },
        },
        gleif_legal_name_fixtures={
            "NAME:DLOCAL": {
                "candidates": [_legal_candidate("529900D15DJKVN3RCO35", "DLocal Limited", country="KY")]
            },
            "NAME:WHEATON PRECIOUS METALS": {
                "candidates": [_legal_candidate("549300XSFG5ZCGVYD886", "WHEATON PRECIOUS METALS CORP.", country="CA")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="DLO", provider_symbol="DLO", country="US", currency="USD", name="DLOCAL LTD"),
            ListingCandidate(
                symbol="WPM",
                provider_symbol="WPM",
                country="US",
                currency="USD",
                name="WHEATON PRECIOUS METALS CORP",
            ),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    for symbol in ("DLO", "WPM"):
        assert rows[symbol]["entity_attach_method"] == "unattached_no_anchor"
        assert rows[symbol]["foreign_issuer_candidate_evaluated"] is True
        assert rows[symbol]["foreign_issuer_lei_expansion_status"] == "not_found"
        assert rows[symbol]["foreign_issuer_reject_reason"] == "lei_expansion_not_success"
        assert rows[symbol]["foreign_issuer_final_gate_status"] == "rejected"


def test_foreign_issuer_rejection_diagnostics_explain_missing_confirming_isin() -> None:
    measurement = _fixture_measurement(
        ["TRI"],
        openfigi_fixtures={"TRI": _security_fixture("TRI", "THOMSON REUTERS CORP")},
        isin_fixtures={"TRI": {"isin": "CA8849038812", "source": "fixture_yfinance"}},
        gleif_fixtures={"CA8849038812": {"status": "not_found", "error_message": "no_lei"}},
        gleif_lei_isin_fixtures={
            "LEI:549300561UZND4C7B569": {
                "legal_name": "THOMSON REUTERS CORPORATION",
                "isin_list": ["US8849030001"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:THOMSON REUTERS": {
                "candidates": [_legal_candidate("549300561UZND4C7B569", "THOMSON REUTERS CORPORATION", country="CA")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="TRI", provider_symbol="TRI", country="US", currency="USD", name="THOMSON REUTERS CORP"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["entity_attach_reason"] == "legal_name_match_country_incompatible"
    assert row["foreign_issuer_candidate_evaluated"] is True
    assert row["foreign_issuer_candidate_lei"] == "549300561UZND4C7B569"
    assert row["foreign_issuer_name_match_status"] == "matched"
    assert row["foreign_issuer_lei_expansion_status"] == "success"
    assert row["foreign_issuer_lei_expansion_isin_count"] == 1
    assert row["foreign_issuer_raw_isin"] == "CA8849038812"
    assert row["foreign_issuer_raw_isin_in_expansion"] is False
    assert row["foreign_issuer_issuer_country_prefixes"] == ["CA"]
    assert row["foreign_issuer_expanded_issuer_country_isin_count"] == 0
    assert row["foreign_issuer_reject_reason"] == "no_confirming_foreign_issuer_isin"
    assert row["foreign_issuer_final_gate_status"] == "rejected"
    assert measurement.summary["foreign_issuer_fallback_evaluated_count"] == 1
    assert measurement.summary["foreign_issuer_fallback_attached_count"] == 0
    assert measurement.summary["foreign_issuer_fallback_rejected_count"] == 1
    assert measurement.summary["foreign_issuer_fallback_reject_reason_counts"] == {
        "no_confirming_foreign_issuer_isin": 1
    }
    assert measurement.summary["foreign_issuer_fallback_expansion_status_counts"] == {"success": 1}
    assert measurement.summary["foreign_issuer_fallback_raw_isin_in_expansion_counts"] == {"false": 1}


def test_us_foreign_issuer_ambiguous_legal_name_candidates_stay_manual_review() -> None:
    measurement = _fixture_measurement(
        ["ACN"],
        openfigi_fixtures={"ACN": _security_fixture("ACN", "ACCENTURE PLC")},
        isin_fixtures={"ACN": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"}},
        gleif_fixtures={"IE00B4BNMY34": {"status": "not_found", "error_message": "no_lei"}},
        gleif_lei_isin_fixtures={
            "LEI:ACNLEI00000001": {"legal_name": "ACCENTURE PLC", "isin_list": ["IE00B4BNMY34"]},
            "LEI:ACNLEI00000002": {"legal_name": "ACCENTURE PLC", "isin_list": ["IE0000000002"]},
        },
        gleif_legal_name_fixtures={
            "NAME:ACCENTURE": {
                "candidates": [
                    _legal_candidate("ACNLEI00000001", "ACCENTURE PLC", country="IE"),
                    _legal_candidate("ACNLEI00000002", "ACCENTURE PLC", country="IE"),
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ACN", provider_symbol="ACN", country="US", currency="USD", name="ACCENTURE PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_ambiguous"
    assert row["decision_bucket"] == "needs_manual_review"
    assert row["direct_prefix_mismatch_candidate_status"] == "not_requested"


def test_direct_prefix_mismatch_does_not_skip_name_anchor_fallback() -> None:
    symbols = ["ALKS", "BCRX", "BE", "COIN", "CSL.AX"]
    measurement = _fixture_measurement(
        symbols,
        openfigi_fixtures={
            "ALKS": _security_fixture("ALKS", "ALKERMES PLC"),
            "BCRX": _security_fixture("BCRX", "BIOCRYST PHARMACEUTICALS INC"),
            "BE": _security_fixture("BE", "BLOOM ENERGY CORP"),
            "COIN": _security_fixture("COIN", "COINBASE GLOBAL INC"),
            "CSL.AX": _security_fixture("CSL", "CSL LTD", country="AU"),
        },
        isin_fixtures={
            "ALKS": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"},
            "BCRX": {"isin": "DE000A2PSR20", "source": "fixture_yfinance"},
            "BE": {"isin": "KYG290181018", "source": "fixture_yfinance"},
            "COIN": {"isin": "CA8849038085", "source": "fixture_yfinance"},
            "CSL.AX": {"isin": "GB00BMXNWH07", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "IE00B4BNMY34": {"lei": "DIRECTWRONGLEI1", "legal_name": "UNRELATED HOLDINGS PLC"},
            "DE000A2PSR20": {"lei": "DIRECTWRONGLEI2", "legal_name": "UNRELATED HOLDINGS PLC"},
            "KYG290181018": {"lei": "DIRECTWRONGLEI3", "legal_name": "UNRELATED HOLDINGS PLC"},
            "CA8849038085": {"lei": "DIRECTWRONGLEI4", "legal_name": "UNRELATED HOLDINGS PLC"},
            "GB00BMXNWH07": {"lei": "DIRECTWRONGLEI5", "legal_name": "UNRELATED HOLDINGS PLC"},
        },
        gleif_lei_isin_fixtures={
            "LEI:DIRECTWRONGLEI1": {"legal_name": "UNRELATED HOLDINGS PLC", "isin_list": ["IE00B4BNMY34"]},
            "LEI:DIRECTWRONGLEI2": {"legal_name": "UNRELATED HOLDINGS PLC", "isin_list": ["DE000A2PSR20"]},
            "LEI:DIRECTWRONGLEI3": {"legal_name": "UNRELATED HOLDINGS PLC", "isin_list": ["KYG290181018"]},
            "LEI:DIRECTWRONGLEI4": {"legal_name": "UNRELATED HOLDINGS PLC", "isin_list": ["CA8849038085"]},
            "LEI:DIRECTWRONGLEI5": {"legal_name": "UNRELATED HOLDINGS PLC", "isin_list": ["GB00BMXNWH07"]},
            "LEI:ALKSLEI000001": {"legal_name": "ALKERMES PLC", "isin_list": ["US01642T1088"]},
            "LEI:BCRXLEI000001": {
                "legal_name": "BIOCRYST PHARMACEUTICALS INC",
                "isin_list": ["US09058V1035"],
            },
            "LEI:BELEI0000001": {"legal_name": "BLOOM ENERGY CORPORATION", "isin_list": ["US0937121079"]},
            "LEI:COINLEI000001": {"legal_name": "COINBASE GLOBAL INC", "isin_list": ["US19260Q1076"]},
            "LEI:CSLLEI0000001": {"legal_name": "CSL LIMITED", "isin_list": ["AU000000CSL8"]},
        },
        gleif_legal_name_fixtures={
            "NAME:ALKERMES": {
                "candidates": [_legal_candidate("ALKSLEI000001", "ALKERMES PLC", country="US")]
            },
            "NAME:BIOCRYST PHARMACEUTICALS": {
                "candidates": [
                    _legal_candidate("BCRXLEI000001", "BIOCRYST PHARMACEUTICALS INC", country="US")
                ]
            },
            "NAME:BLOOM ENERGY": {
                "candidates": [_legal_candidate("BELEI0000001", "BLOOM ENERGY CORPORATION", country="US")]
            },
            "NAME:COINBASE GLOBAL": {
                "candidates": [_legal_candidate("COINLEI000001", "COINBASE GLOBAL INC", country="US")]
            },
            "NAME:CSL": {
                "candidates": [_legal_candidate("CSLLEI0000001", "CSL LIMITED", country="AU")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ALKS", provider_symbol="ALKS", country="US", currency="USD", name="ALKERMES PLC"),
            ListingCandidate(
                symbol="BCRX",
                provider_symbol="BCRX",
                country="US",
                currency="USD",
                name="BIOCRYST PHARMACEUTICALS INC",
            ),
            ListingCandidate(symbol="BE", provider_symbol="BE", country="US", currency="USD", name="BLOOM ENERGY CORP"),
            ListingCandidate(symbol="COIN", provider_symbol="COIN", country="US", currency="USD", name="COINBASE GLOBAL INC"),
            ListingCandidate(symbol="CSL.AX", provider_symbol="CSL.AX", country="AU", currency="AUD", name="CSL LTD"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    for symbol in symbols:
        assert rows[symbol]["isin_status"] == "suspect"
        assert rows[symbol]["raw_isin"]
        assert rows[symbol]["direct_prefix_mismatch_candidate_status"] == "not_requested"
        assert rows[symbol]["legal_name_anchor_status"] == "confirmed"
        assert rows[symbol]["entity_attach_method"] == "name_anchor_confirmed"
        assert rows[symbol]["attachment_provenance"] == "name_anchor_confirmed"
        assert rows[symbol]["attachment_confidence"] == "medium"
        assert rows[symbol]["legal_name_candidate_lei"]
        assert rows[symbol]["legal_name_candidate_lei_in_expansion_request"] is True
        assert rows[symbol]["legal_name_candidate_lei_expansion_status"] == "success"
        assert rows[symbol]["legal_name_candidate_lei_expansion_isin_count"] > 0
        assert rows[symbol]["legal_name_candidate_compatible_isin_count"] > 0
        assert rows[symbol]["legal_name_candidate_compatible_isin_sample"]
    assert measurement.summary["listings_attached_name_anchor_confirmed"] == 5


def test_prefix_mismatch_lei_is_excluded_from_main_lei_expansion_lookup() -> None:
    candidates = {
        "ALKS": ListingCandidate(symbol="ALKS", provider_symbol="ALKS", country="US", currency="USD"),
    }
    isin_records = YFinanceIsinClient(
        fixture_isins={"ALKS": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"}},
        offline=True,
    ).enrich_candidates(list(candidates.values()))
    direct_lei_by_isin = {"IE00B4BNMY34": "DIRECTWRONGLEI1"}

    plan = _gleif_lei_expansion_lookup_plan(
        isin_records=isin_records,
        candidates_by_symbol=candidates,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=["ALKSLEI000001"],
    )
    lookup_leis = _gleif_lei_expansion_lookup_leis(
        isin_records=isin_records,
        candidates_by_symbol=candidates,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=["ALKSLEI000001"],
    )

    assert lookup_leis == ["ALKSLEI000001"]
    assert "DIRECTWRONGLEI1" not in lookup_leis
    assert plan["origin_leis"]["prefix_mismatch_isolated_candidate"] == []
    assert plan["excluded_origin_leis"]["prefix_mismatch_isolated_candidate"] == ["DIRECTWRONGLEI1"]


def test_name_anchor_candidate_diagnostics_survive_missing_expansion_and_prefix_mismatch_failure() -> None:
    measurement = _fixture_measurement(
        ["ALKS"],
        openfigi_fixtures={"ALKS": _security_fixture("ALKS", "ALKERMES PLC")},
        isin_fixtures={"ALKS": {"isin": "IE00B4BNMY34", "source": "fixture_yfinance"}},
        gleif_fixtures={"IE00B4BNMY34": {"lei": "DIRECTWRONGLEI1", "legal_name": "UNRELATED HOLDINGS PLC"}},
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={
            "NAME:ALKERMES": {
                "candidates": [_legal_candidate("ALKSLEI000001", "ALKERMES PLC", country="US")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ALKS", provider_symbol="ALKS", country="US", currency="USD", name="ALKERMES PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["entity_attach_reason"] == "gleif_lei_found_but_no_compatible_isin"
    assert row["candidate_lei"] == "ALKSLEI000001"
    assert row["candidate_legal_name"] == "ALKERMES PLC"
    assert row["legal_name_candidate_lei"] == "ALKSLEI000001"
    assert row["legal_name_candidate_lei_in_expansion_request"] is True
    assert row["legal_name_candidate_lei_expansion_status"] == "not_found"
    assert row["legal_name_candidate_lei_expansion_isin_count"] == 0
    assert row["legal_name_candidate_compatible_isin_count"] == 0
    assert row["legal_name_candidate_compatible_isin_sample"] == []
    assert row["matched_compatible_isins"] == []
    assert row["direct_prefix_mismatch_status"] == "rejected"
    assert row["direct_prefix_mismatch_reject_reason"] == "direct_prefix_mismatch_name_unconfirmed"
    assert row["direct_prefix_mismatch_lei"] == "DIRECTWRONGLEI1"
    assert row["direct_prefix_mismatch_legal_name"] == "UNRELATED HOLDINGS PLC"


def test_rate_limited_lei_expansion_is_diagnostic_not_missing_identity() -> None:
    measurement = _fixture_measurement(
        ["ALKS"],
        openfigi_fixtures={"ALKS": _security_fixture("ALKS", "ALKERMES PLC")},
        isin_fixtures={"ALKS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:ALKSLEI000001": {
                "status": "rate_limited",
                "error_message": "429 Client Error: Too Many Requests",
                "isin_list": [],
            },
        },
        gleif_legal_name_fixtures={
            "NAME:ALKERMES": {
                "candidates": [_legal_candidate("ALKSLEI000001", "ALKERMES PLC", country="US")]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="ALKS", provider_symbol="ALKS", country="US", currency="USD", name="ALKERMES PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["legal_name_anchor_status"] == "rejected"
    assert row["legal_name_anchor_reject_reason"] == "gleif_lei_found_but_no_compatible_isin"
    assert row["legal_name_candidate_lei"] == "ALKSLEI000001"
    assert row["legal_name_candidate_lei_in_expansion_request"] is True
    assert row["legal_name_candidate_lei_expansion_status"] == "rate_limited"
    assert row["legal_name_candidate_lei_expansion_error"] == "429 Client Error: Too Many Requests"
    assert row["legal_name_candidate_lei_expansion_isin_count"] == 0
    assert row["matched_compatible_isins"] == []
    assert measurement.summary["gleif_lei_expansion_status_counts"] == {"rate_limited": 1}


def test_tls_tls_ax_remains_non_merged_with_different_lei() -> None:
    measurement = _fixture_measurement(
        ["TLS", "TLS.AX"],
        isin_fixtures={
            "TLS": {"isin": "US87969B1017", "source": "fixture_yfinance"},
            "TLS.AX": {"isin": "AU000000TLS2", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "US87969B1017": {"lei": "TELOSLEI000000001", "legal_name": "TELOS CORP"},
            "AU000000TLS2": {"lei": "TELSTRALEI000001", "legal_name": "TELSTRA GROUP LTD"},
        },
        pairs=[("TLS", "TLS.AX", "bare_collision")],
    )

    assert measurement.pair_rows[0]["grouped"] is False
    assert measurement.pair_rows[0]["reason"] == "different_lei"


def test_tls_tls_ax_remains_non_merged_through_name_anchor_path() -> None:
    measurement = _fixture_measurement(
        ["TLS", "TLS.AX"],
        isin_fixtures={
            "TLS": {"isin": "-", "source": "fixture_yfinance"},
            "TLS.AX": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:TELOSLEI000000001": {"legal_name": "TELOS CORPORATION", "isin_list": ["US87969B1017"]},
            "LEI:TELSTRALEI000001": {"legal_name": "TELSTRA GROUP LTD", "isin_list": ["AU000000TLS2"]},
        },
        gleif_legal_name_fixtures={
            "NAME:TELOS": {
                "candidates": [
                    {
                        "lei": "TELOSLEI000000001",
                        "legal_name": "TELOS CORPORATION",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
            "NAME:TELSTRA GROUP": {
                "candidates": [
                    {
                        "lei": "TELSTRALEI000001",
                        "legal_name": "TELSTRA GROUP LTD",
                        "legal_country": "AU",
                        "headquarters_country": "AU",
                        "jurisdiction": "AU",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="TLS", provider_symbol="TLS", country="US", currency="USD", name="TELOS CORPORATION"),
            ListingCandidate(symbol="TLS.AX", provider_symbol="TLS.AX", country="AU", currency="AUD", name="TELSTRA GROUP LTD"),
        ],
        pairs=[("TLS", "TLS.AX", "bare_collision")],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["TLS"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["TLS.AX"]["entity_attach_method"] == "name_anchor_confirmed"
    assert measurement.pair_rows[0]["grouped"] is False
    assert measurement.pair_rows[0]["reason"] == "different_lei"


def test_short_legal_name_candidate_cannot_attach_without_compatible_expanded_isin() -> None:
    measurement = _fixture_measurement(
        ["TELOS.L"],
        isin_fixtures={"TELOS.L": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:TELOSLEI000000001": {"legal_name": "TELOS CORPORATION", "isin_list": ["US87969B1017"]},
        },
        gleif_legal_name_fixtures={
            "NAME:TELOS": {
                "candidates": [
                    {
                        "lei": "TELOSLEI000000001",
                        "legal_name": "TELOS CORPORATION",
                        "legal_country": "GB",
                        "headquarters_country": "GB",
                        "jurisdiction": "GB",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="TELOS.L", provider_symbol="TELOS.L", country="GB", currency="GBP", name="TELOS"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["legal_name_anchor_status"] == "rejected"
    assert row["compatible_isin_gate_status"] == "rejected"
    assert row["compatible_isin_gate_reject_reason"] == "no_compatible_expanded_isin_for_listing_country"
    assert row["entity_lei"] == ""


def test_lenb_openfigi_not_found_can_still_group_when_name_anchor_is_confirmed() -> None:
    measurement = _fixture_measurement(["LEN", "LENB"])
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["LENB"]["openfigi_status"] == "not_found"
    assert rows["LENB"]["isin"] == ""
    assert rows["LENB"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["LENB"]["attachment_provenance"] == "name_anchor_confirmed"
    assert rows["LENB"]["attachment_confidence"] == "medium"
    assert rows["LENB"]["listing_name_used_for_legal_name_search"] == "Lennar Corporation"
    assert rows["LENB"]["legal_name_anchor_status"] == "confirmed"
    assert measurement.pair_rows[0]["grouped"] is True


def test_ambiguous_lei_expansion_does_not_attach() -> None:
    measurement = _fixture_measurement(
        ["AAA", "AAB", "AAA.L"],
        isin_fixtures={
            "AAA": {"isin": "US0000000002", "source": "fixture_yfinance"},
            "AAB": {"isin": "US0000000010", "source": "fixture_yfinance"},
            "AAA.L": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "US0000000002": {"lei": "LEIEXAMPLE000001", "legal_name": "EXAMPLE PLC"},
            "US0000000010": {"lei": "LEIEXAMPLE000002", "legal_name": "EXAMPLE PLC"},
        },
        gleif_lei_isin_fixtures={
            "LEI:LEIEXAMPLE000001": {"legal_name": "EXAMPLE PLC", "isin_list": ["GB0000000009"]},
            "LEI:LEIEXAMPLE000002": {"legal_name": "EXAMPLE PLC", "isin_list": ["GB0000000017"]},
        },
        extra_candidates=[
            ListingCandidate(symbol="AAA", provider_symbol="AAA", country="US", currency="USD", name="EXAMPLE PLC"),
            ListingCandidate(symbol="AAB", provider_symbol="AAB", country="US", currency="USD", name="EXAMPLE PLC"),
            ListingCandidate(symbol="AAA.L", provider_symbol="AAA.L", country="GB", currency="GBP", name="EXAMPLE PLC"),
        ],
        pairs=[],
    )
    row = next(row for row in measurement.symbol_rows if row["symbol"] == "AAA.L")

    assert row["entity_attach_method"] == "unattached_ambiguous"
    assert measurement.summary["listings_unattached_ambiguous"] == 1


def test_no_anchor_listing_remains_unresolved() -> None:
    measurement = _fixture_measurement(
        ["ZZZ.L"],
        isin_fixtures={"ZZZ.L": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="ZZZ.L", provider_symbol="ZZZ.L", country="GB", currency="GBP", name="UNANCHORED PLC"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["entity_attach_reason"] == "openfigi_not_found"
    assert row["entity_attach_reasons"] == [
        "openfigi_not_found",
        "legal_name_search_no_match",
        "provider_isin_missing",
    ]
    assert row["decision_bucket"] == "fixable_free"
    assert measurement.summary["tail_without_anchor_count"] == 1
    assert measurement.summary["tail_without_anchor_examples"][0]["symbol"] == "ZZZ.L"
    assert measurement.summary["decision_bucket_counts"]["fixable_free"] == 1
    assert row["listing_group_kind"] == "single_listing"
    assert row["listing_group_size_in_measurement"] == 1
    assert measurement.summary["tail_without_anchor_by_listing_group_kind"] == {"single_listing": 1}


def test_tail_breakdown_identifies_multi_listing_candidates() -> None:
    measurement = _fixture_measurement(
        ["TAIL", "TAIL.L"],
        openfigi_fixtures={
            "TAIL": {"error": "No identifier found."},
            "TAIL.L": {"error": "No identifier found."},
        },
        isin_fixtures={
            "TAIL": {"isin": "-", "source": "fixture_yfinance"},
            "TAIL.L": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={},
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="TAIL", provider_symbol="TAIL", country="US", currency="USD", name="TAIL COMPANY PLC"),
            ListingCandidate(symbol="TAIL.L", provider_symbol="TAIL.L", country="GB", currency="GBP", name="TAIL COMPANY PLC"),
        ],
        pairs=[("TAIL", "TAIL.L", "cross_listing")],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["TAIL"]["listing_group_kind"] == "multi_listing_candidate"
    assert rows["TAIL.L"]["listing_group_kind"] == "multi_listing_candidate"
    assert rows["TAIL"]["listing_group_size_in_measurement"] == 2
    assert rows["TAIL"]["listing_group_symbols_in_measurement"] == ["TAIL", "TAIL.L"]
    assert measurement.summary["tail_without_anchor_by_listing_group_kind"] == {"multi_listing_candidate": 2}


def test_valid_isin_without_gleif_lei_rolls_up_to_provider_or_curated_identity() -> None:
    measurement = _fixture_measurement(
        ["0700.HK"],
        isin_fixtures={"0700.HK": {"isin": "KYG875721634", "source": "fixture_yfinance"}},
        gleif_fixtures={"KYG875721634": {"status": "not_found", "error_message": "no_gleif_isin_mapping"}},
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={},
        extra_candidates=[
            ListingCandidate(symbol="0700.HK", provider_symbol="0700.HK", country="HK", currency="HKD", name="TENCENT HOLDINGS LTD"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["isin_status"] == "success"
    assert row["entity_attach_reason"] == "valid_isin_no_gleif_lei"
    assert "valid_isin_no_gleif_lei" in row["entity_attach_reasons"]
    assert row["decision_bucket"] == "requires_provider_or_curated_identity"
    assert measurement.summary["requires_provider_or_curated_identity"] == 1
    assert measurement.summary["provider_or_curated_by_listing_group_kind"] == {"single_listing": 1}


def test_ambiguous_legal_name_anchor_is_manual_review() -> None:
    measurement = _fixture_measurement(
        ["AMB"],
        isin_fixtures={"AMB": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:ACMELEI00000001": {"legal_name": "ACME CORPORATION", "isin_list": ["US0000000002"]},
            "LEI:ACMELEI00000002": {"legal_name": "ACME CORPORATION", "isin_list": ["US0000000010"]},
        },
        gleif_legal_name_fixtures={
            "NAME:ACME": {
                "candidates": [
                    {
                        "lei": "ACMELEI00000001",
                        "legal_name": "ACME CORPORATION",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    },
                    {
                        "lei": "ACMELEI00000002",
                        "legal_name": "ACME CORPORATION",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    },
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="AMB", provider_symbol="AMB", country="US", currency="USD", name="ACME CORP-A"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "unattached_ambiguous"
    assert row["entity_attach_reason"] == "legal_name_search_ambiguous"
    assert row["legal_name_anchor_status"] == "ambiguous"
    assert row["decision_bucket"] == "needs_manual_review"
    assert measurement.summary["needs_manual_review"] == 1
    assert measurement.summary["manual_review_by_listing_group_kind"] == {"single_listing": 1}


def test_name_anchor_precision_audit_and_isin_samples_are_capped() -> None:
    expanded_isins = [f"US{index:09d}{index % 10}" for index in range(20)]
    measurement = _fixture_measurement(
        ["MEGA"],
        isin_fixtures={"MEGA": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:MEGALEI00000001": {"legal_name": "MEGA CORPORATION", "isin_list": expanded_isins}
        },
        gleif_legal_name_fixtures={
            "NAME:MEGA": {
                "candidates": [
                    {
                        "lei": "MEGALEI00000001",
                        "legal_name": "MEGA CORPORATION",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="MEGA", provider_symbol="MEGA", country="US", currency="USD", name="MEGA CORP-A"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]
    audit = measurement.name_anchor_precision_audit[0]

    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert row["lei_expanded_isin_count"] == 20
    assert row["compatible_expanded_isin_count"] == 20
    assert len(row["lei_expanded_isins"]) == 10
    assert len(row["matched_compatible_isins"]) == 10
    assert audit["symbol"] == "MEGA"
    assert audit["matched_gleif_legal_name"] == "MEGA CORPORATION"
    assert audit["compatible_expanded_isin_count"] == 20
    assert audit["compatible_isin_candidate_count"] == 20
    assert len(audit["compatible_isin_candidate_sample"]) == 10


def test_openfigi_success_name_variants_can_confirm_legal_name_anchor() -> None:
    measurement = _fixture_measurement(
        ["HD", "LLY"],
        openfigi_fixtures={
            "HD": {
                "ticker": "HD",
                "name": "HOME DEPOT INC/THE",
                "figi": "HD-FIGI",
                "compositeFIGI": "HD-COMP",
                "shareClassFIGI": "HD-SHARE",
                "country": "US",
                "securityType2": "Common Stock",
            },
            "LLY": {
                "ticker": "LLY",
                "name": "ELI LILLY & CO",
                "figi": "LLY-FIGI",
                "compositeFIGI": "LLY-COMP",
                "shareClassFIGI": "LLY-SHARE",
                "country": "US",
                "securityType2": "Common Stock",
            },
        },
        isin_fixtures={
            "HD": {"isin": "-", "source": "fixture_yfinance"},
            "LLY": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:HDLEI000000001": {"legal_name": "THE HOME DEPOT, INC.", "isin_list": ["US4370761029"]},
            "LEI:LLYLEI00000001": {"legal_name": "ELI LILLY AND COMPANY", "isin_list": ["US5324571083"]},
        },
        gleif_legal_name_fixtures={
            "NAME:HOME DEPOT": {
                "candidates": [
                    {
                        "lei": "HDLEI000000001",
                        "legal_name": "THE HOME DEPOT, INC.",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
            "NAME:ELI LILLY AND": {
                "candidates": [
                    {
                        "lei": "LLYLEI00000001",
                        "legal_name": "ELI LILLY AND COMPANY",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-IN",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="HD", provider_symbol="HD", country="US", currency="USD"),
            ListingCandidate(symbol="LLY", provider_symbol="LLY", country="US", currency="USD"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["HD"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["HD"]["listing_name_used_for_legal_name_search"] == "Home Depot Inc The"
    assert rows["HD"]["matched_compatible_isins"] == ["US4370761029"]
    assert rows["LLY"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["LLY"]["listing_name_used_for_legal_name_search"] == "Eli Lilly And Company"
    assert rows["LLY"]["matched_compatible_isins"] == ["US5324571083"]


def test_local_openfigi_name_suffix_variants_can_confirm_legal_name_anchor() -> None:
    measurement = _fixture_measurement(
        ["000660.KS", "6758.T", "TLS"],
        openfigi_fixtures={
            "000660.KS": {
                "ticker": "000660",
                "name": "SK HYNIX INC",
                "figi": "HYNIX-FIGI",
                "compositeFIGI": "HYNIX-COMP",
                "shareClassFIGI": "HYNIX-SHARE",
                "country": "KR",
                "securityType2": "Common Stock",
            },
            "6758.T": {
                "ticker": "6758",
                "name": "SONY GROUP CORP",
                "figi": "SONY-FIGI",
                "compositeFIGI": "SONY-COMP",
                "shareClassFIGI": "SONY-SHARE",
                "country": "JP",
                "securityType2": "Common Stock",
            },
            "TLS": {
                "ticker": "TLS",
                "name": "TELOS CORPORATION",
                "figi": "TELOS-FIGI",
                "compositeFIGI": "TELOS-COMP",
                "shareClassFIGI": "TELOS-SHARE",
                "country": "US",
                "securityType2": "Common Stock",
            },
        },
        isin_fixtures={
            "000660.KS": {"isin": "-", "source": "fixture_yfinance"},
            "6758.T": {"isin": "-", "source": "fixture_yfinance"},
            "TLS": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:SKHYNIXLEI0001": {"legal_name": "SK HYNIX INC.", "isin_list": ["KR7000660001"]},
            "LEI:SONYLEI000001": {"legal_name": "SONY GROUP CORPORATION", "isin_list": ["JP3435000009"]},
            "LEI:TELOSLEI00001": {"legal_name": "TELOS CORPORATION", "isin_list": ["US87969B1017"]},
        },
        gleif_legal_name_fixtures={
            "NAME:SK HYNIX": {
                "candidates": [
                    {
                        "lei": "SKHYNIXLEI0001",
                        "legal_name": "SK HYNIX INC.",
                        "legal_country": "KR",
                        "headquarters_country": "KR",
                        "jurisdiction": "KR",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
            "NAME:SONY GROUP": {
                "candidates": [
                    {
                        "lei": "SONYLEI000001",
                        "legal_name": "SONY GROUP CORPORATION",
                        "legal_country": "JP",
                        "headquarters_country": "JP",
                        "jurisdiction": "JP",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
            "NAME:TELOS": {
                "candidates": [
                    {
                        "lei": "TELOSLEI00001",
                        "legal_name": "TELOS CORPORATION",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-MD",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            },
        },
        extra_candidates=[
            ListingCandidate(symbol="000660.KS", provider_symbol="000660.KS", currency="KRW"),
            ListingCandidate(symbol="6758.T", provider_symbol="6758.T", currency="JPY"),
            ListingCandidate(symbol="TLS", provider_symbol="TLS", country="US", currency="USD"),
        ],
        pairs=[],
    )
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["000660.KS"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["000660.KS"]["listing_name_used_for_legal_name_search"] == "Sk Hynix Inc"
    assert rows["000660.KS"]["matched_compatible_isins"] == ["KR7000660001"]
    assert rows["6758.T"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["6758.T"]["listing_name_used_for_legal_name_search"] == "Sony Group Corporation"
    assert rows["6758.T"]["matched_compatible_isins"] == ["JP3435000009"]
    assert rows["TLS"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["TLS"]["listing_name_used_for_legal_name_search"] == "Telos Corporation"
    assert rows["TLS"]["matched_compatible_isins"] == ["US87969B1017"]


def test_internal_name_can_drive_legal_name_anchor_when_openfigi_not_found() -> None:
    measurement = _fixture_measurement(
        ["005930.KS"],
        openfigi_fixtures={"005930.KS": {"error": "No identifier found."}},
        isin_fixtures={"005930.KS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:SAMSUNGLEI0001": {
                "legal_name": "SAMSUNG ELECTRONICS CO., LTD.",
                "isin_list": ["KR7005930003"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:SAMSUNG ELECTRONICS": {
                "candidates": [
                    {
                        "lei": "SAMSUNGLEI0001",
                        "legal_name": "SAMSUNG ELECTRONICS CO., LTD.",
                        "legal_country": "KR",
                        "headquarters_country": "KR",
                        "jurisdiction": "KR",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(
                symbol="005930.KS",
                provider_symbol="005930.KS",
                currency="KRW",
                name="SAMSUNG ELECTRONICS CO LTD",
            ),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["openfigi_status"] == "not_found"
    assert row["derived_listing_country"] == "KR"
    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert row["listing_name_used_for_legal_name_search"] == "Samsung Electronics Company Limited"
    assert row["matched_compatible_isins"] == ["KR7005930003"]


def test_name_anchor_does_not_confirm_ns_listing_with_only_us_isins() -> None:
    measurement = _fixture_measurement(
        ["RELIANCE.NS"],
        isin_fixtures={"RELIANCE.NS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:RELIANCELEI0001": {
                "legal_name": "RELIANCE INDUSTRIES LIMITED",
                "isin_list": ["US7594701077", "US759470AB33"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:RELIANCE INDUSTRIES": {
                "candidates": [
                    {
                        "lei": "RELIANCELEI0001",
                        "legal_name": "RELIANCE INDUSTRIES LIMITED",
                        "legal_country": "IN",
                        "headquarters_country": "IN",
                        "jurisdiction": "IN",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(
                symbol="RELIANCE.NS",
                provider_symbol="RELIANCE.NS",
                currency="INR",
                name="RELIANCE INDUSTRIES LIMITED",
            ),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["derived_listing_country"] == "IN"
    assert row["allowed_isin_prefixes"] == ["IN"]
    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["compatible_expanded_isin_count"] == 0
    assert row["compatible_isin_gate_status"] == "rejected"
    assert row["compatible_isin_gate_reject_reason"] == "no_compatible_expanded_isin_for_listing_country"
    assert "gleif_lei_found_but_no_compatible_isin" in row["entity_attach_reasons"]


def test_name_anchor_confirms_ns_listing_only_with_in_compatible_isin() -> None:
    measurement = _fixture_measurement(
        ["RELIANCE.NS"],
        isin_fixtures={"RELIANCE.NS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:RELIANCELEI0001": {
                "legal_name": "RELIANCE INDUSTRIES LIMITED",
                "isin_list": ["US7594701077", "INE002A01018"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:RELIANCE INDUSTRIES": {
                "candidates": [
                    {
                        "lei": "RELIANCELEI0001",
                        "legal_name": "RELIANCE INDUSTRIES LIMITED",
                        "legal_country": "IN",
                        "headquarters_country": "IN",
                        "jurisdiction": "IN",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(
                symbol="RELIANCE.NS",
                provider_symbol="RELIANCE.NS",
                currency="INR",
                name="RELIANCE INDUSTRIES LIMITED",
            ),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert row["derived_listing_country"] == "IN"
    assert row["matched_compatible_isins"] == ["INE002A01018"]
    assert row["expected_listing_country_prefix_present"] is True


def test_name_anchor_confirms_ls_listing_only_with_pt_compatible_isin() -> None:
    blocked = _fixture_measurement(
        ["SON.LS"],
        isin_fixtures={"SON.LS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:SONAELEI0001": {"legal_name": "SONAE", "isin_list": ["US0000000002"]}
        },
        gleif_legal_name_fixtures={
            "NAME:SONAE": {
                "candidates": [
                    {
                        "lei": "SONAELEI0001",
                        "legal_name": "SONAE",
                        "legal_country": "PT",
                        "headquarters_country": "PT",
                        "jurisdiction": "PT",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="SON.LS", provider_symbol="SON.LS", currency="EUR", name="SONAE"),
        ],
        pairs=[],
    )
    confirmed = _fixture_measurement(
        ["SON.LS"],
        isin_fixtures={"SON.LS": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:SONAELEI0001": {"legal_name": "SONAE", "isin_list": ["PTSON0AM0001"]}
        },
        gleif_legal_name_fixtures={
            "NAME:SONAE": {
                "candidates": [
                    {
                        "lei": "SONAELEI0001",
                        "legal_name": "SONAE",
                        "legal_country": "PT",
                        "headquarters_country": "PT",
                        "jurisdiction": "PT",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="SON.LS", provider_symbol="SON.LS", currency="EUR", name="SONAE"),
        ],
        pairs=[],
    )

    assert blocked.symbol_rows[0]["derived_listing_country"] == "PT"
    assert blocked.symbol_rows[0]["allowed_isin_prefixes"] == ["PT"]
    assert blocked.symbol_rows[0]["entity_attach_method"] == "unattached_no_anchor"
    assert confirmed.symbol_rows[0]["entity_attach_method"] == "name_anchor_confirmed"
    assert confirmed.symbol_rows[0]["matched_compatible_isins"] == ["PTSON0AM0001"]


def test_hk_listing_prefix_policy_is_explicit_for_cayman_issuer_isin() -> None:
    measurement = _fixture_measurement(
        ["0700.HK"],
        isin_fixtures={"0700.HK": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:TENCENTLEI0001": {
                "legal_name": "TENCENT HOLDINGS LIMITED",
                "isin_list": ["KYG875721634"],
            }
        },
        gleif_legal_name_fixtures={
            "NAME:TENCENT HOLDINGS": {
                "candidates": [
                    {
                        "lei": "TENCENTLEI0001",
                        "legal_name": "TENCENT HOLDINGS LIMITED",
                        "legal_country": "KY",
                        "headquarters_country": "CN",
                        "jurisdiction": "KY",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="0700.HK", provider_symbol="0700.HK", currency="HKD", name="TENCENT HOLDINGS LTD"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["derived_listing_country"] == "HK"
    assert row["allowed_isin_prefixes"] == ["BM", "CN", "GB", "HK", "KY"]
    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert row["matched_compatible_isins"] == ["KYG875721634"]
    assert row["compatible_isin_gate_status"] == "passed"


def test_name_anchor_without_country_or_supported_suffix_needs_manual_review() -> None:
    measurement = _fixture_measurement(
        ["LOCALONLY"],
        isin_fixtures={"LOCALONLY": {"isin": "-", "source": "fixture_yfinance"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:LOCALLEI0001": {"legal_name": "LOCAL ONLY LIMITED", "isin_list": ["US0000000002"]}
        },
        gleif_legal_name_fixtures={
            "NAME:LOCAL ONLY": {
                "candidates": [
                    {
                        "lei": "LOCALLEI0001",
                        "legal_name": "LOCAL ONLY LIMITED",
                        "legal_country": "US",
                        "headquarters_country": "US",
                        "jurisdiction": "US-DE",
                        "entity_status": "ACTIVE",
                        "registration_status": "ISSUED",
                    }
                ]
            }
        },
        extra_candidates=[
            ListingCandidate(symbol="LOCALONLY", provider_symbol="LOCALONLY", name="LOCAL ONLY LIMITED"),
        ],
        pairs=[],
    )
    row = measurement.symbol_rows[0]

    assert row["allowed_isin_prefixes"] == []
    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["entity_attach_reason"] == "missing_listing_country_for_isin_gate"
    assert row["compatible_isin_gate_status"] == "rejected"
    assert row["decision_bucket"] == "needs_manual_review"


def test_prefix_mismatch_isin_without_gleif_lei_does_not_create_anchor_lei() -> None:
    measurement = _fixture_measurement(
        ["GOOG", "GOOGL"],
        isin_fixtures={
            "GOOG": {"isin": "CA02080M1005", "source": "fixture_yfinance"},
            "GOOGL": {"isin": "-", "source": "fixture_yfinance"},
        },
        gleif_fixtures=acceptance_gleif_fixtures(),
        gleif_lei_isin_fixtures=acceptance_gleif_lei_isin_fixtures(),
        gleif_legal_name_fixtures={},
    )

    assert measurement.summary["anchor_isin_count"] == 0
    assert measurement.summary["anchor_lei_count"] == 0
    assert measurement.summary["pairs_blocked_no_valid_anchor"] == 1
    assert all(not row["entity_lei"] for row in measurement.symbol_rows)
    assert measurement.pair_rows[0]["grouped"] is False
    assert measurement.pair_rows[0]["reason"] == "no_valid_anchor_isin"


def test_cache_apply_publishes_only_raw_cache_tables() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    publisher = RecordingPublisher()

    publish_entity_identity_raw_caches(publisher=publisher, measurement=measurement)

    assert [call["table"] for call in publisher.upserts] == [
        "source_cache.openfigi_mapping_raw",
        "source_cache.listing_isin_raw",
        "source_cache.gleif_isin_lei_raw",
        "source_cache.gleif_lei_isin_raw",
    ]
    assert all(not call["table"].startswith("feature_store.entity_") for call in publisher.upserts)
    assert publisher.inserts == []


def test_publication_readiness_audit_includes_all_non_direct_attaches() -> None:
    measurement = _fixture_measurement()
    audited_symbols = {row["symbol"] for row in measurement.heuristic_attach_audit}
    non_direct_symbols = {
        row["symbol"]
        for row in measurement.symbol_rows
        if row["entity_attach_method"]
        in {
            "lei_expansion",
            "name_anchor_confirmed",
            "foreign_issuer_name_anchor_confirmed",
            "isin_direct_prefix_mismatch_name_confirmed",
        }
    }

    assert audited_symbols == non_direct_symbols
    assert all(row["deterministic_support"] for row in measurement.heuristic_attach_audit)
    assert all(row["review_status"] == "machine_verifiably_safe" for row in measurement.heuristic_attach_audit)

    goog = next(row for row in measurement.heuristic_attach_audit if row["symbol"] == "GOOG")
    assert goog["attach_method"] == "name_anchor_confirmed"
    assert goog["conservative_name_match"] is True
    assert goog["entity_group_symbols"] == ["GOOG", "GOOGL"]
    assert goog["matched_compatible_isins"]


def test_side_by_side_publication_plan_keeps_provisional_single_listing_evidence() -> None:
    measurement = _fixture_measurement(
        ["ACME"],
        extra_candidates=[
            ListingCandidate(
                symbol="ACME",
                provider_symbol="ACME",
                country="US",
                currency="USD",
                exchange="NMS",
                name="ACME CORP",
                source="test_fixture",
            )
        ],
        isin_fixtures={"ACME": {"status": "not_found", "error_message": "no_isin"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={},
        gleif_legal_name_fixtures={
            "ACME": {
                "status": "success",
                "query": "ACME",
                "candidates": [_legal_candidate("549300ACME000000001", "ACME CORP", country="US")],
            }
        },
    )

    plan = build_side_by_side_entity_publication_plan(measurement)

    assert plan["publication_gate"]["status"] == "ready_machine_safe"
    assert plan["feature_store.entity_master"][0]["entity_id"] == "provisional:ACME"
    assert plan["feature_store.entity_master"][0]["resolution_status"] == "provisional"
    assert plan["feature_store.entity_listing"][0]["attach_method"] == "provisional_single_listing_candidate"
    assert plan["feature_store.entity_listing"][0]["resolution_status"] == "provisional"
    assert plan["feature_store.entity_identity_publication_batch"][0]["batch_id"] == plan["batch_id"]
    assert plan["verification_summary"]["candidate_lei_retained_provisional_count"] == 1


def test_controlled_entity_publish_dry_run_writes_nothing_and_reports_counts() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    publisher = RecordingPublisher()

    result = publish_entity_identity_controlled(publisher=publisher, measurement=measurement)

    assert result["status"] == "dry_run"
    assert result["planned_counts"]["feature_store.entity_master"] == 1
    assert result["planned_counts"]["feature_store.entity_listing"] == 2
    assert result["verification_summary"]["unresolved_multi_listing_entities_count"] == 0
    assert publisher.upserts == []
    assert publisher.inserts == []


def test_controlled_entity_publish_blocks_when_gate_not_ready() -> None:
    measurement = _fixture_measurement(
        ["6701.T"],
        extra_candidates=[
            ListingCandidate(
                symbol="6701.T",
                provider_symbol="6701.T",
                country="JP",
                currency="JPY",
                exchange="JP",
                name="NEC CORP",
                source="test_fixture",
            )
        ],
        openfigi_fixtures={"6701.T": _security_fixture("6701", "NEC CORP", country="JP")},
        isin_fixtures={"6701.T": {"status": "not_found", "error_message": "no_isin"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:NEC-CORP-LEI": {
                "lei": "NEC-CORP-LEI",
                "legal_name": "NEC CORPORATION",
                "isin_list": ["JP3733000008"],
                "status": "success",
            }
        },
        gleif_legal_name_fixtures={
            "NEC": {
                "status": "success",
                "query": "NEC",
                "candidates": [_legal_candidate("NEC-CORP-LEI", "NEC CORPORATION", country="JP")],
            }
        },
    )
    publisher = RecordingPublisher()

    result = publish_entity_identity_controlled(
        publisher=publisher,
        measurement=measurement,
        apply_entities=True,
    )

    assert result["status"] == "blocked"
    assert "publication_gate_not_ready" in result["publication_blockers"]
    assert "heuristic_review_required" in result["publication_blockers"]
    assert publisher.upserts == []
    assert publisher.inserts == []


def test_controlled_entity_publish_cache_first_then_side_by_side_tables() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    publisher = RecordingPublisher()

    result = publish_entity_identity_controlled(
        publisher=publisher,
        measurement=measurement,
        apply_caches=True,
        apply_entities=True,
        batch_id="test-batch-1",
    )

    assert result["status"] == "published_side_by_side"
    assert result["batch_id"] == "test-batch-1"
    assert [call["table"] for call in publisher.upserts[:4]] == [
        "source_cache.openfigi_mapping_raw",
        "source_cache.listing_isin_raw",
        "source_cache.gleif_isin_lei_raw",
        "source_cache.gleif_lei_isin_raw",
    ]
    assert [call["table"] for call in publisher.upserts[4:]] == [
        "feature_store.entity_identity_publication_batch",
        "feature_store.entity_master",
        "feature_store.entity_listing",
        "feature_store.entity_identity_publication_current",
    ]
    assert publisher.upserts[4]["on_conflict"] == "batch_id"
    assert publisher.upserts[5]["on_conflict"] == "entity_id"
    assert publisher.upserts[6]["on_conflict"] == "symbol"
    assert publisher.upserts[7]["on_conflict"] == "scope_key"
    assert publisher.inserts == []
    assert publisher.upserts[4]["rows"][0]["actual_counts"]["feature_store.entity_listing"] == 2
    assert publisher.upserts[5]["rows"][0]["publication_batch_id"] == "test-batch-1"
    assert all(row["publication_batch_id"] == "test-batch-1" for row in publisher.upserts[6]["rows"])


def test_controlled_entity_publish_rerun_uses_idempotent_upserts() -> None:
    measurement = _fixture_measurement(["GOOG", "GOOGL"])
    first = RecordingPublisher()
    second = RecordingPublisher()

    publish_entity_identity_controlled(
        publisher=first,
        measurement=measurement,
        apply_entities=True,
        batch_id="repeatable-batch",
    )
    publish_entity_identity_controlled(
        publisher=second,
        measurement=measurement,
        apply_entities=True,
        batch_id="repeatable-batch",
    )

    assert [(call["table"], call["on_conflict"]) for call in first.upserts] == [
        (call["table"], call["on_conflict"]) for call in second.upserts
    ]
    assert first.upserts[0]["rows"][0]["batch_id"] == "repeatable-batch"
    assert second.upserts[0]["rows"][0]["batch_id"] == "repeatable-batch"
    assert first.inserts == []
    assert second.inserts == []


def test_publication_batch_id_is_deterministic_for_same_measurement() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])

    assert build_entity_publication_batch_id(measurement) == build_entity_publication_batch_id(measurement)


def test_cjk_legal_name_does_not_collapse_to_short_latin_acronym_attach() -> None:
    measurement = _fixture_measurement(
        ["6701.T"],
        extra_candidates=[
            ListingCandidate(
                symbol="6701.T",
                provider_symbol="6701.T",
                country="JP",
                currency="JPY",
                exchange="JP",
                name="NEC CORP",
                source="test_fixture",
            )
        ],
        openfigi_fixtures={
            "6701.T": _security_fixture("6701", "NEC CORP", country="JP"),
        },
        isin_fixtures={"6701.T": {"status": "not_found", "error_message": "no_isin"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:353800ZI7YB13OFXXR18": {
                "lei": "353800ZI7YB13OFXXR18",
                "legal_name": "NECキャピタルソリューション株式会社",
                "isin_list": ["JP3164740001"],
                "status": "success",
            }
        },
        gleif_legal_name_fixtures={
            "NEC": {
                "status": "success",
                "query": "NEC",
                "candidates": [
                    _legal_candidate(
                        "353800ZI7YB13OFXXR18",
                        "NECキャピタルソリューション株式会社",
                        country="JP",
                    )
                ],
            }
        },
    )

    row = measurement.symbol_rows[0]

    assert normalize_legal_name_conservative("NECキャピタルソリューション株式会社") != "NEC"
    assert row["entity_attach_method"] == "unattached_no_anchor"
    assert row["entity_lei"] == ""
    assert measurement.heuristic_attach_audit == []


def test_short_acronym_name_anchor_blocks_publication_gate_until_reviewed() -> None:
    measurement = _fixture_measurement(
        ["6701.T"],
        extra_candidates=[
            ListingCandidate(
                symbol="6701.T",
                provider_symbol="6701.T",
                country="JP",
                currency="JPY",
                exchange="JP",
                name="NEC CORP",
                source="test_fixture",
            )
        ],
        openfigi_fixtures={
            "6701.T": _security_fixture("6701", "NEC CORP", country="JP"),
        },
        isin_fixtures={"6701.T": {"status": "not_found", "error_message": "no_isin"}},
        gleif_fixtures={},
        gleif_lei_isin_fixtures={
            "LEI:NEC-CORP-LEI": {
                "lei": "NEC-CORP-LEI",
                "legal_name": "NEC CORPORATION",
                "isin_list": ["JP3733000008"],
                "status": "success",
            }
        },
        gleif_legal_name_fixtures={
            "NEC": {
                "status": "success",
                "query": "NEC",
                "candidates": [_legal_candidate("NEC-CORP-LEI", "NEC CORPORATION", country="JP")],
            }
        },
    )

    row = measurement.symbol_rows[0]
    audit = measurement.heuristic_attach_audit[0]

    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert audit["normalized_name_too_short"] is True
    assert audit["normalized_name_acronym_only"] is True
    assert audit["review_status"] == "needs_review"
    assert audit["review_reason"] == "name_normalization_requires_review"
    assert measurement.publication_gate["status"] == "blocked_pending_review"
    assert measurement.summary["cjk_apac_heuristic_attach_audit_count"] == 1
    assert measurement.cjk_apac_heuristic_attach_audit[0]["symbol"] == "6701.T"


def test_acronym_name_anchor_is_machine_safe_with_direct_isin_lei_support() -> None:
    audit = _heuristic_attach_audit(
        [
            {
                "symbol": "CSL.AX",
                "provider_symbol": "CSL.AX",
                "entity_attach_method": "name_anchor_confirmed",
                "entity_lei": "529900ECSECK5ZDQTE14",
                "candidate_lei": "529900ECSECK5ZDQTE14",
                "candidate_legal_name": "CSL LIMITED",
                "internal_candidate_name": "CSL LTD",
                "openfigi_name": "CSL LTD",
                "listing_name_used_for_legal_name_search": "Csl Limited",
                "legal_name_anchor_status": "confirmed",
                "matched_compatible_isins": ["AU000000CSL8"],
                "raw_isin": "AU000000CSL8",
                "isin": "AU000000CSL8",
                "direct_lei": "529900ECSECK5ZDQTE14",
                "derived_listing_country": "AU",
                "allowed_isin_prefixes": ["AU"],
            }
        ]
    )

    row = audit[0]

    assert row["normalized_name_too_short"] is True
    assert row["normalized_name_acronym_only"] is True
    assert row["strong_deterministic_isin_support"] is True
    assert row["review_status"] == "machine_verifiably_safe"
    assert row["review_reason"] == ""


def test_acronym_name_anchor_is_machine_safe_with_forward_resolved_matched_isin_support() -> None:
    audit = _heuristic_attach_audit(
        [
            {
                "symbol": "CSL.AX",
                "provider_symbol": "CSL.AX",
                "entity_attach_method": "name_anchor_confirmed",
                "entity_lei": "529900ECSECK5ZDQTE14",
                "candidate_lei": "529900ECSECK5ZDQTE14",
                "candidate_legal_name": "CSL LIMITED",
                "internal_candidate_name": "CSL LTD",
                "openfigi_name": "CSL LTD",
                "listing_name_used_for_legal_name_search": "Csl Limited",
                "legal_name_anchor_status": "confirmed",
                "matched_compatible_isins": ["AU000000CSL8"],
                "derived_listing_country": "AU",
                "allowed_isin_prefixes": ["AU"],
            }
        ],
        gleif_by_isin={
            "AU000000CSL8": GleifIsinLeiRecord(
                isin="AU000000CSL8",
                lei="529900ECSECK5ZDQTE14",
                legal_name="CSL LIMITED",
                status="success",
            )
        },
    )

    row = audit[0]

    assert row["normalized_name_too_short"] is True
    assert row["normalized_name_acronym_only"] is True
    assert row["strong_deterministic_isin_support"] is True
    assert row["review_status"] == "machine_verifiably_safe"


def test_live_shaped_csl_matched_isin_forward_lookup_makes_gate_safe() -> None:
    measurement = _fixture_measurement(
        ["CSL.AX"],
        extra_candidates=[
            ListingCandidate(
                symbol="CSL.AX",
                provider_symbol="CSL.AX",
                country="AU",
                currency="AUD",
                exchange="ASX",
                name="CSL LTD",
                source="test_fixture",
            )
        ],
        openfigi_fixtures={"CSL.AX": _security_fixture("CSL", "CSL LTD", country="AU")},
        isin_fixtures={"CSL.AX": {"status": "not_found", "error_message": "no_isin"}},
        gleif_fixtures={
            "AU000000CSL8": {
                "lei": "529900ECSECK5ZDQTE14",
                "legal_name": "CSL LIMITED",
                "status": "success",
            }
        },
        gleif_lei_isin_fixtures={
            "LEI:529900ECSECK5ZDQTE14": {
                "lei": "529900ECSECK5ZDQTE14",
                "legal_name": "CSL LIMITED",
                "isin_list": ["AU000000CSL8"],
                "status": "success",
            }
        },
        gleif_legal_name_fixtures={
            "NAME:CSL": {
                "status": "success",
                "query": "CSL",
                "candidates": [_legal_candidate("529900ECSECK5ZDQTE14", "CSL LIMITED", country="AU")],
            }
        },
    )

    row = measurement.symbol_rows[0]
    audit = measurement.heuristic_attach_audit[0]

    assert row["entity_attach_method"] == "name_anchor_confirmed"
    assert row["matched_compatible_isins"] == ["AU000000CSL8"]
    assert audit["normalized_name_acronym_only"] is True
    assert audit["normalized_name_too_short"] is True
    assert audit["strong_deterministic_isin_support"] is True
    assert audit["review_status"] == "machine_verifiably_safe"
    assert measurement.publication_gate["status"] == "ready_machine_safe"


def test_acronym_name_anchor_without_known_isin_support_still_requires_review() -> None:
    audit = _heuristic_attach_audit(
        [
            {
                "symbol": "6701.T",
                "provider_symbol": "6701.T",
                "entity_attach_method": "name_anchor_confirmed",
                "entity_lei": "NEC-CORP-LEI",
                "candidate_lei": "NEC-CORP-LEI",
                "candidate_legal_name": "NEC CORPORATION",
                "internal_candidate_name": "NEC CORP",
                "openfigi_name": "NEC CORP",
                "listing_name_used_for_legal_name_search": "Nec Corporation",
                "legal_name_anchor_status": "confirmed",
                "matched_compatible_isins": ["JP3733000008"],
                "derived_listing_country": "JP",
                "allowed_isin_prefixes": ["JP"],
            }
        ]
    )

    row = audit[0]

    assert row["normalized_name_too_short"] is True
    assert row["normalized_name_acronym_only"] is True
    assert row["strong_deterministic_isin_support"] is False
    assert row["review_status"] == "needs_review"
    assert row["review_reason"] == "name_normalization_requires_review"


def test_acronym_name_anchor_with_expansion_only_isin_still_requires_review() -> None:
    audit = _heuristic_attach_audit(
        [
            {
                "symbol": "CSL.AX",
                "provider_symbol": "CSL.AX",
                "entity_attach_method": "name_anchor_confirmed",
                "entity_lei": "529900ECSECK5ZDQTE14",
                "candidate_lei": "529900ECSECK5ZDQTE14",
                "candidate_legal_name": "CSL LIMITED",
                "internal_candidate_name": "CSL LTD",
                "openfigi_name": "CSL LTD",
                "listing_name_used_for_legal_name_search": "Csl Limited",
                "legal_name_anchor_status": "confirmed",
                "matched_compatible_isins": ["AU000000CSL8"],
                "expanded_candidate_isins": ["AU000000CSL8"],
                "lei_expanded_isins": ["AU000000CSL8"],
                "derived_listing_country": "AU",
                "allowed_isin_prefixes": ["AU"],
            }
        ]
    )

    row = audit[0]

    assert row["normalized_name_acronym_only"] is True
    assert row["strong_deterministic_isin_support"] is False
    assert row["review_status"] == "needs_review"


def test_side_by_side_publisher_blocks_unreviewed_heuristic_gate() -> None:
    measurement = _fixture_measurement(["GOOG", "GOOGL"])
    blocked_gate = {
        **measurement.publication_gate,
        "status": "blocked_pending_review",
        "publication_allowed_without_review": False,
        "review_required_count": 1,
    }
    blocked_measurement = type(measurement)(
        symbol_rows=measurement.symbol_rows,
        pair_rows=measurement.pair_rows,
        summary=measurement.summary,
        name_anchor_precision_audit=measurement.name_anchor_precision_audit,
        heuristic_attach_audit=measurement.heuristic_attach_audit,
        cjk_apac_heuristic_attach_audit=measurement.cjk_apac_heuristic_attach_audit,
        publication_gate=blocked_gate,
        openfigi_cache_rows=measurement.openfigi_cache_rows,
        isin_cache_rows=measurement.isin_cache_rows,
        gleif_cache_rows=measurement.gleif_cache_rows,
        gleif_lei_isin_cache_rows=measurement.gleif_lei_isin_cache_rows,
    )
    publisher = RecordingPublisher()

    result = publish_entity_identity_side_by_side(publisher=publisher, measurement=blocked_measurement)

    assert result["status"] == "blocked"
    assert result["reason"] == "publication_gate_blocked"
    assert publisher.upserts == []
    assert publisher.inserts == []


def _fixture_measurement(
    symbols: list[str] | None = None,
    *,
    isin_fixtures: dict | None = None,
    gleif_fixtures: dict | None = None,
    gleif_lei_isin_fixtures: dict | None = None,
    gleif_legal_name_fixtures: dict | None = None,
    openfigi_fixtures: dict | None = None,
    extra_candidates: list[ListingCandidate] | None = None,
    pairs: list[tuple[str, str, str]] | None = None,
):
    candidates = acceptance_fixture_candidates(symbols=symbols)
    if extra_candidates:
        candidates = list(extra_candidates)
    present = {candidate.symbol for candidate in candidates}
    for symbol in symbols or []:
        if symbol not in present:
            candidates.append(
                ListingCandidate(
                    symbol=symbol,
                    provider_symbol=symbol,
                    country="US",
                    currency="USD",
                    exchange="NMS",
                    source="test_fixture",
                )
            )
    openfigi_fixture_mappings = acceptance_openfigi_fixtures()
    if openfigi_fixtures:
        openfigi_fixture_mappings.update(openfigi_fixtures)
    openfigi = OpenFigiClient(fixture_mappings=openfigi_fixture_mappings, request_sleep_seconds=0)
    openfigi_mappings = openfigi.map_candidates(candidates)
    isin = YFinanceIsinClient(fixture_isins=isin_fixtures or acceptance_isin_fixtures())
    isin_records = isin.enrich_candidates(candidates)
    fixture_mappings = {}
    fixture_mappings.update(gleif_fixtures if gleif_fixtures is not None else acceptance_gleif_fixtures())
    fixture_mappings.update(
        gleif_lei_isin_fixtures
        if gleif_lei_isin_fixtures is not None
        else acceptance_gleif_lei_isin_fixtures()
    )
    fixture_mappings.update(
        gleif_legal_name_fixtures
        if gleif_legal_name_fixtures is not None
        else acceptance_gleif_legal_name_fixtures()
    )
    gleif = GleifIsinLeiClient(fixture_mappings=fixture_mappings)
    gleif_records = gleif.lookup_isins(_gleif_lookup_isins(isin_records))
    direct_lei_by_isin = {record.isin: record.lei for record in gleif_records if record.lei and record.status == "success"}
    openfigi_by_symbol = {mapping.symbol: mapping for mapping in openfigi_mappings}
    candidates_by_symbol = {candidate.symbol: candidate for candidate in candidates}
    direct_lei_symbols = {
        record.symbol
        for record in isin_records
        if _direct_isin_can_skip_legal_name(
            candidate=candidates_by_symbol.get(record.symbol),
            record=record,
            direct_lei_by_isin=direct_lei_by_isin,
        )
    }
    legal_name_records = gleif.search_legal_names(
        [
            query
            for candidate in candidates
            if candidate.symbol not in direct_lei_symbols
            for query in legal_name_query_variants_from_listing(
                (openfigi_by_symbol.get(candidate.symbol).name if openfigi_by_symbol.get(candidate.symbol) else ""),
                candidate.name,
            )
        ]
    )
    legal_name_candidate_leis = [
        candidate["lei"]
        for record in legal_name_records
        for candidate in record.candidates
        if candidate.get("lei")
    ]
    gleif_lei_expansion_plan = _gleif_lei_expansion_lookup_plan(
        isin_records=isin_records,
        candidates_by_symbol=candidates_by_symbol,
        direct_lei_by_isin=direct_lei_by_isin,
        legal_name_candidate_leis=legal_name_candidate_leis,
    )
    gleif_lei_isin_records = gleif.lookup_lei_isins(gleif_lei_expansion_plan["request_leis"])
    selected_symbols = [candidate.symbol for candidate in candidates]
    measurement = measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        gleif_lei_isin_records=gleif_lei_isin_records,
        gleif_legal_name_records=legal_name_records,
        gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
        gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
        gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
        pairs=pairs if pairs is not None else acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=openfigi.batch_split_retries,
    )
    audit_forward_isins = _audit_forward_lookup_isins(measurement, gleif_records)
    if audit_forward_isins:
        gleif_records = _merge_gleif_records(gleif_records, gleif.lookup_isins(audit_forward_isins))
        measurement = measure_entity_identity_chain(
            candidates=candidates,
            openfigi_mappings=openfigi_mappings,
            isin_records=isin_records,
            gleif_records=gleif_records,
            gleif_lei_isin_records=gleif_lei_isin_records,
            gleif_legal_name_records=legal_name_records,
            gleif_lei_expansion_request_leis=gleif_lei_expansion_plan["request_leis"],
            gleif_lei_expansion_request_origin_leis=gleif_lei_expansion_plan["origin_leis"],
            gleif_lei_expansion_excluded_origin_leis=gleif_lei_expansion_plan["excluded_origin_leis"],
            pairs=pairs if pairs is not None else acceptance_pairs_for_symbols(selected_symbols),
            batch_split_retries=openfigi.batch_split_retries,
        )
    return measurement


def _security_fixture(ticker: str, name: str, *, country: str = "US") -> dict:
    return {
        "ticker": ticker,
        "name": name,
        "figi": f"{ticker}-FIGI",
        "compositeFIGI": f"{ticker}-COMP",
        "shareClassFIGI": f"{ticker}-SHARE",
        "country": country,
        "currency": "USD",
        "securityType2": "Common Stock",
    }


def _gleif_lookup_isins(isin_records) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for record in isin_records:
        if not record.isin:
            continue
        should_lookup = record.status == "success" or (
            record.status == "suspect" and record.error_message in _PREFIX_MISMATCH_ISIN_REASONS
        )
        if should_lookup and record.isin not in seen:
            out.append(record.isin)
            seen.add(record.isin)
    return out


def _legal_candidate(lei: str, legal_name: str, *, country: str) -> dict:
    return {
        "lei": lei,
        "legal_name": legal_name,
        "legal_country": country,
        "headquarters_country": country,
        "jurisdiction": country,
        "entity_status": "ACTIVE",
        "registration_status": "ISSUED",
    }


def _direct_isin_can_skip_legal_name(*, candidate, record, direct_lei_by_isin: dict[str, str]) -> bool:
    if not candidate or not record.isin or record.status != "success" or not direct_lei_by_isin.get(record.isin):
        return False
    allowed_prefixes = set(isin_prefix_policy_for_listing(candidate)["allowed_isin_prefixes"])
    return bool(allowed_prefixes and record.isin[:2] in allowed_prefixes)


class _FakeGleifSession:
    def __init__(
        self,
        payload: object,
        status_code: int = 200,
        *,
        status_codes: list[int] | None = None,
        headers: list[dict[str, str]] | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.status_codes = status_codes or []
        self.headers = headers or []
        self.requested_url = ""
        self.requested_params = {}
        self.requested_urls: list[str] = []
        self.requested_params_list: list[dict] = []
        self._call_count = 0

    def get(self, url: str, *, params: dict, timeout: int) -> "_FakeGleifResponse":
        self.requested_url = url
        self.requested_params = dict(params)
        self.requested_urls.append(url)
        self.requested_params_list.append(dict(params))
        payload = self.payload
        if isinstance(payload, list):
            index = min(self._call_count, len(payload) - 1)
            payload = payload[index]
        status_code = self.status_codes[min(self._call_count, len(self.status_codes) - 1)] if self.status_codes else self.status_code
        headers = self.headers[min(self._call_count, len(self.headers) - 1)] if self.headers else {}
        self._call_count += 1
        return _FakeGleifResponse(payload, status_code, headers=headers)


class _FakeGleifResponse:
    def __init__(self, payload: object, status_code: int, *, headers: dict[str, str] | None = None) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            message = "429 Client Error: Too Many Requests" if self.status_code == 429 else f"HTTP {self.status_code}"
            raise RuntimeError(message)

    def json(self) -> object:
        return self.payload
