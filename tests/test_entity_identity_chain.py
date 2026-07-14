from __future__ import annotations

from finance_data_ops.identity.chain import (
    acceptance_fixture_candidates,
    acceptance_gleif_fixtures,
    acceptance_isin_fixtures,
    acceptance_openfigi_fixtures,
    acceptance_pairs_for_symbols,
    measure_entity_identity_chain,
)
from finance_data_ops.identity.models import ListingCandidate
from finance_data_ops.identity.gleif import GleifIsinLeiClient
from finance_data_ops.identity.isin import YFinanceIsinClient
from finance_data_ops.identity.openfigi import OpenFigiClient
from finance_data_ops.identity.publisher import publish_entity_identity_raw_caches
from finance_data_ops.publish.client import RecordingPublisher


def test_acceptance_fixture_chain_reports_symbol_pair_outcomes() -> None:
    measurement = _fixture_measurement()
    summary = measurement.summary

    assert summary["candidate_count"] == 14
    assert summary["isin_found_count"] == 14
    assert summary["lei_found_count"] == 13
    assert summary["entity_groups_formed"] == 5
    assert summary["adr_home_pairs_grouped"] == 2
    assert summary["share_class_pairs_grouped"] == 2
    assert summary["adrs_mapping_to_depositary_or_ambiguous_lei_count"] == 1
    assert summary["unresolved_no_isin_count"] == 0
    assert summary["unresolved_no_lei_count"] == 1

    by_pair = {tuple(row["pair"]): row for row in measurement.pair_rows}
    assert by_pair[("SAP", "SAP.DE")]["grouped"] is False
    assert by_pair[("SAP", "SAP.DE")]["reason"] == "different_lei_depositary_or_ambiguous"
    assert by_pair[("ASML", "ASML.AS")]["grouped"] is True
    assert by_pair[("NVO", "NOVO-B.CO")]["grouped"] is False
    assert by_pair[("NVO", "NOVO-B.CO")]["reason"] == "missing_lei"
    assert by_pair[("RIO", "RIO.L")]["grouped"] is True
    assert by_pair[("0005.HK", "HSBA.L")]["grouped"] is True
    assert by_pair[("GOOG", "GOOGL")]["grouped"] is True
    assert by_pair[("LEN", "LENB")]["grouped"] is True


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
    assert sap["lei"] == "SAPADRDEPOSITARYLEI1"
    assert sap["legal_name"] == "DEPOSITARY BANK FOR SAP ADR"


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


def test_adr_isin_can_map_to_underlying_lei() -> None:
    measurement = _fixture_measurement(["ASML", "ASML.AS"])
    pair = measurement.pair_rows[0]

    assert pair["grouped"] is True
    assert pair["reason"] == "shared_lei"


def test_adr_isin_can_map_to_depositary_or_ambiguous_lei() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    pair = measurement.pair_rows[0]

    assert pair["grouped"] is False
    assert pair["reason"] == "different_lei_depositary_or_ambiguous"
    assert measurement.summary["adrs_mapping_to_depositary_or_ambiguous_lei_count"] == 1


def test_share_classes_with_different_isins_group_by_same_lei() -> None:
    measurement = _fixture_measurement(["GOOG", "GOOGL", "LEN", "LENB"])
    by_pair = {tuple(row["pair"]): row for row in measurement.pair_rows}

    assert by_pair[("GOOG", "GOOGL")]["left_isin"] != by_pair[("GOOG", "GOOGL")]["right_isin"]
    assert by_pair[("GOOG", "GOOGL")]["grouped"] is True
    assert by_pair[("LEN", "LENB")]["left_isin"] != by_pair[("LEN", "LENB")]["right_isin"]
    assert by_pair[("LEN", "LENB")]["grouped"] is True


def test_same_fuzzy_name_with_different_lei_does_not_group() -> None:
    measurement = _fixture_measurement(
        ["AAA", "AAA.L"],
        isin_fixtures={
            "AAA": {"isin": "US0000000001", "source": "fixture_yfinance"},
            "AAA.L": {"isin": "GB0000000002", "source": "fixture_yfinance"},
        },
        gleif_fixtures={
            "US0000000001": {"lei": "LEIUSDIFFERENT1", "legal_name": "EXAMPLE PLC"},
            "GB0000000002": {"lei": "LEIGBDIFFERENT2", "legal_name": "EXAMPLE PLC"},
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


def test_lenb_openfigi_not_found_can_still_group_when_isin_provider_supplies_data() -> None:
    measurement = _fixture_measurement(["LEN", "LENB"])
    rows = {row["symbol"]: row for row in measurement.symbol_rows}

    assert rows["LENB"]["openfigi_status"] == "not_found"
    assert rows["LENB"]["isin"] == "US5260573028"
    assert measurement.pair_rows[0]["grouped"] is True


def test_cache_apply_publishes_only_raw_cache_tables() -> None:
    measurement = _fixture_measurement(["SAP", "SAP.DE"])
    publisher = RecordingPublisher()

    publish_entity_identity_raw_caches(publisher=publisher, measurement=measurement)

    assert [call["table"] for call in publisher.upserts] == [
        "source_cache.openfigi_mapping_raw",
        "source_cache.listing_isin_raw",
        "source_cache.gleif_isin_lei_raw",
    ]
    assert all(not call["table"].startswith("feature_store.entity_") for call in publisher.upserts)
    assert publisher.inserts == []


def _fixture_measurement(
    symbols: list[str] | None = None,
    *,
    isin_fixtures: dict | None = None,
    gleif_fixtures: dict | None = None,
    pairs: list[tuple[str, str, str]] | None = None,
):
    candidates = acceptance_fixture_candidates(symbols=symbols)
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
    gleif = GleifIsinLeiClient(fixture_mappings=gleif_fixtures or acceptance_gleif_fixtures())
    gleif_records = gleif.lookup_isins([record.isin for record in isin_records if record.isin])
    selected_symbols = [candidate.symbol for candidate in candidates]
    return measure_entity_identity_chain(
        candidates=candidates,
        openfigi_mappings=openfigi_mappings,
        isin_records=isin_records,
        gleif_records=gleif_records,
        pairs=pairs if pairs is not None else acceptance_pairs_for_symbols(selected_symbols),
        batch_split_retries=openfigi.batch_split_retries,
    )
