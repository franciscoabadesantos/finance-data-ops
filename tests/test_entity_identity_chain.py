from __future__ import annotations

from finance_data_ops.identity.chain import (
    acceptance_fixture_candidates,
    acceptance_gleif_fixtures,
    acceptance_gleif_legal_name_fixtures,
    acceptance_gleif_lei_isin_fixtures,
    acceptance_isin_fixtures,
    acceptance_openfigi_fixtures,
    acceptance_pairs_for_symbols,
    measure_entity_identity_chain,
)
from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.identity.gleif import GleifIsinLeiClient
from finance_data_ops.identity.isin import YFinanceIsinClient
from finance_data_ops.identity.names import legal_name_query_from_listing, normalize_legal_name_conservative
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity_raw_caches
from finance_data_ops.publish.client import RecordingPublisher


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


def test_live_like_bad_yfinance_isins_are_suspect_or_missing() -> None:
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
    assert rows["GOOG"]["isin"] == ""
    assert rows["GOOG"]["raw_isin"] == "CA02080M1005"
    assert rows["ASML.AS"]["isin_status"] == "suspect"
    assert rows["0005.HK"]["isin_status"] == "suspect"
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


def test_legal_name_query_preserves_corporate_suffix_but_confirmation_normalizes_it() -> None:
    assert legal_name_query_from_listing("ALPHABET INC-CL C") == "Alphabet Inc"
    assert legal_name_query_from_listing("LENNAR CORP-A") == "Lennar Corporation"
    assert legal_name_query_from_listing("HSBC HOLDINGS PLC") == "Hsbc Holdings Plc"
    assert normalize_legal_name_conservative("ALPHABET INC-CL C") == "ALPHABET"
    assert normalize_legal_name_conservative("ALPHABET INC.") == "ALPHABET"


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


def test_lenb_openfigi_not_found_can_still_group_when_name_anchor_is_confirmed() -> None:
    measurement = _fixture_measurement(["LEN", "LENB"])
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["LENB"]["openfigi_status"] == "not_found"
    assert rows["LENB"]["isin"] == ""
    assert rows["LENB"]["entity_attach_method"] == "name_anchor_confirmed"
    assert rows["LENB"]["attachment_provenance"] == "name_anchor_confirmed"
    assert rows["LENB"]["attachment_confidence"] == "medium"
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
    assert measurement.summary["tail_without_anchor_count"] == 1
    assert measurement.summary["tail_without_anchor_examples"][0]["symbol"] == "ZZZ.L"


def test_yfinance_suspect_isins_are_excluded_from_anchors() -> None:
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


def _fixture_measurement(
    symbols: list[str] | None = None,
    *,
    isin_fixtures: dict | None = None,
    gleif_fixtures: dict | None = None,
    gleif_lei_isin_fixtures: dict | None = None,
    gleif_legal_name_fixtures: dict | None = None,
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
    openfigi = OpenFigiClient(fixture_mappings=acceptance_openfigi_fixtures(), request_sleep_seconds=0)
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
    gleif_records = gleif.lookup_isins(
        [record.isin for record in isin_records if record.isin and record.status == "success"]
    )
    direct_lei_by_isin = {record.isin: record.lei for record in gleif_records if record.lei and record.status == "success"}
    openfigi_by_symbol = {mapping.symbol: mapping for mapping in openfigi_mappings}
    direct_lei_symbols = {
        record.symbol
        for record in isin_records
        if record.isin and record.status == "success" and direct_lei_by_isin.get(record.isin)
    }
    legal_name_records = gleif.search_legal_names(
        [
            legal_name_query_from_listing(
                (openfigi_by_symbol.get(candidate.symbol).name if openfigi_by_symbol.get(candidate.symbol) else "")
                or candidate.name
            )
            for candidate in candidates
            if candidate.symbol not in direct_lei_symbols
        ]
    )
    gleif_lei_isin_records = gleif.lookup_lei_isins(
        [record.lei for record in gleif_records if record.lei and record.status == "success"]
        + [
            candidate["lei"]
            for record in legal_name_records
            for candidate in record.candidates
            if candidate.get("lei")
        ]
    )
    selected_symbols = [candidate.symbol for candidate in candidates]
    return measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        gleif_lei_isin_records=gleif_lei_isin_records,
        gleif_legal_name_records=legal_name_records,
        pairs=pairs if pairs is not None else acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=openfigi.batch_split_retries,
    )


class _FakeGleifSession:
    def __init__(self, payload: object, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
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
        self._call_count += 1
        return _FakeGleifResponse(payload, self.status_code)


class _FakeGleifResponse:
    def __init__(self, payload: object, status_code: int) -> None:
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> object:
        return self.payload
