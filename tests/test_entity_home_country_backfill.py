from __future__ import annotations

import sys
from types import SimpleNamespace

from finance_data_ops.identity.home_country_backfill import (
    build_gleif_home_country_index,
    build_home_country_backfill_plan,
)
from scripts.backfill_entity_home_country import apply_home_country_updates


BATCH_ID = "tracked-675-first-publish"


def test_home_country_backfill_plan_uses_gleif_cache_for_null_resolved_entities() -> None:
    entity_rows = [
        _entity("lei:ASMLLEI", "ASMLLEI", "ASML HOLDING N.V.", None),
        _entity("lei:AAPLLEI", "AAPLLEI", "APPLE INC.", None),
        _entity("lei:SAPLEI", "SAPLEI", "SAP SE", "DE"),
        _entity("lei:NVOLEI", "NVOLEI", "NOVO NORDISK A/S", "DK"),
        _entity("lei:MISSING", "MISSING", "MISSING INC", None),
        _entity("provisional:ONE", "", "ONE", None, resolution_status="provisional"),
    ]
    gleif_entity_rows = [
        {
            "normalized_query_name": "ASML HOLDING",
            "candidates_payload": [
                {
                    "lei": "ASMLLEI",
                    "legal_name": "ASML HOLDING N.V.",
                    "legal_country": "NL",
                    "headquarters_country": "NL",
                }
            ],
            "status": "success",
        },
        {
            "normalized_query_name": "SAP",
            "candidates_payload": [
                {
                    "lei": "SAPLEI",
                    "legal_name": "SAP SE",
                    "legal_country": "DE",
                    "headquarters_country": "DE",
                }
            ],
            "status": "success",
        },
    ]
    gleif_isin_lei_rows = [
        {
            "isin": "US0378331005",
            "lei": "AAPLLEI",
            "legal_name": "APPLE INC.",
            "status": "success",
            "response_payload": _gleif_response_payload(
                lei="AAPLLEI",
                legal_name="APPLE INC.",
                legal_country="US",
                jurisdiction="US-CA",
            ),
        },
        {
            "isin": "US6701002056",
            "lei": "NVOLEI",
            "legal_name": "NOVO NORDISK A/S",
            "status": "success",
            "response_payload": _gleif_response_payload(
                lei="NVOLEI",
                legal_name="NOVO NORDISK A/S",
                legal_country="DK",
                jurisdiction="DK",
            ),
        },
    ]

    plan = build_home_country_backfill_plan(
        entity_rows=entity_rows,
        gleif_entity_rows=gleif_entity_rows,
        gleif_isin_lei_rows=gleif_isin_lei_rows,
        batch_id=BATCH_ID,
    )

    changes = {row["entity_id"]: row for row in plan["changes"]}
    assert plan["total_entities"] == 6
    assert plan["resolved_entities"] == 5
    assert plan["home_country_null_before"] == 3
    assert plan["home_country_null_after"] == 1
    assert plan["backfilled_count"] == 2
    assert plan["unchanged_count"] == 2
    assert plan["missing_cache_count"] == 1
    assert plan["unresolved_no_lei_count"] == 0
    assert changes["lei:ASMLLEI"]["new_home_country"] == "NL"
    assert changes["lei:AAPLLEI"]["new_home_country"] == "US"
    assert "lei:SAPLEI" not in changes
    assert "lei:NVOLEI" not in changes


def test_home_country_backfill_is_idempotent_after_country_is_set() -> None:
    entity_rows = [_entity("lei:ASMLLEI", "ASMLLEI", "ASML HOLDING N.V.", "NL")]
    plan = build_home_country_backfill_plan(
        entity_rows=entity_rows,
        gleif_entity_rows=[
            {
                "candidates_payload": [
                    {"lei": "ASMLLEI", "legal_name": "ASML HOLDING N.V.", "legal_country": "NL"}
                ],
                "status": "success",
            }
        ],
        gleif_isin_lei_rows=[],
        batch_id=BATCH_ID,
    )

    assert plan["backfilled_count"] == 0
    assert plan["home_country_null_before"] == 0
    assert plan["home_country_null_after"] == 0
    assert plan["unchanged_count"] == 1


def test_home_country_backfill_does_not_overwrite_conflicting_non_empty_country() -> None:
    plan = build_home_country_backfill_plan(
        entity_rows=[_entity("lei:SHOPLEI", "SHOPLEI", "SHOPIFY INC.", "US")],
        gleif_entity_rows=[
            {"candidates_payload": [{"lei": "SHOPLEI", "legal_name": "SHOPIFY INC.", "legal_country": "CA"}]}
        ],
        gleif_isin_lei_rows=[],
        batch_id=BATCH_ID,
    )

    assert plan["backfilled_count"] == 0
    assert plan["conflict_count"] == 1
    assert plan["conflict_examples"][0]["current_home_country"] == "US"
    assert plan["conflict_examples"][0]["gleif_home_country"] == "CA"


def test_gleif_home_country_index_prefers_legal_country_then_jurisdiction_then_headquarters() -> None:
    index = build_gleif_home_country_index(
        gleif_entity_rows=[
            {
                "candidates_payload": [
                    {"lei": "LEGAL", "legal_country": "NL", "jurisdiction": "US-DE", "headquarters_country": "US"},
                    {"lei": "JURIS", "jurisdiction": "DK", "headquarters_country": "US"},
                    {"lei": "HQ", "headquarters_country": "GB"},
                ]
            }
        ],
        gleif_isin_lei_rows=[],
    )

    assert index["LEGAL"].home_country == "NL"
    assert index["LEGAL"].source_field == "legal_country"
    assert index["JURIS"].home_country == "DK"
    assert index["JURIS"].source_field == "jurisdiction"
    assert index["HQ"].home_country == "GB"
    assert index["HQ"].source_field == "headquarters_country"


def test_apply_home_country_updates_only_updates_null_rows_and_metadata(monkeypatch) -> None:
    fake_psycopg = SimpleNamespace(
        connect=lambda *args, **kwargs: _FakeConnection(),
        types=SimpleNamespace(json=SimpleNamespace(Jsonb=lambda value: value)),
    )
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)
    monkeypatch.setitem(sys.modules, "psycopg.types", SimpleNamespace(json=SimpleNamespace(Jsonb=lambda value: value)))
    monkeypatch.setitem(sys.modules, "psycopg.types.json", SimpleNamespace(Jsonb=lambda value: value))

    result = apply_home_country_updates(
        database_dsn="postgresql://example.invalid/db",
        batch_id=BATCH_ID,
        changes=[
            {
                "entity_id": "lei:ASMLLEI",
                "lei": "ASMLLEI",
                "new_home_country": "NL",
                "source_table": "source_cache.gleif_entity_raw",
                "source_field": "legal_country",
            }
        ],
    )

    assert result == {"status": "ok", "updated_rows": 1}
    statement, params = _FakeCursor.executed[0]
    assert "update feature_store.entity_master" in statement
    assert "feature_store.entity_listing" not in statement
    assert "entity_identity_publication_current" not in statement
    assert params[0] == "NL"
    assert params[1]["home_country"] == "NL"
    assert params[1]["source_table"] == "source_cache.gleif_entity_raw"
    assert params[2] == "lei:ASMLLEI"
    assert params[3] == BATCH_ID
    assert params[4] == "ASMLLEI"


def _entity(
    entity_id: str,
    lei: str,
    legal_name: str,
    home_country: str | None,
    *,
    resolution_status: str = "resolved",
) -> dict:
    return {
        "entity_id": entity_id,
        "lei": lei,
        "legal_name": legal_name,
        "home_country": home_country,
        "resolution_status": resolution_status,
        "publication_batch_id": BATCH_ID,
        "metadata": {},
    }


def _gleif_response_payload(*, lei: str, legal_name: str, legal_country: str, jurisdiction: str) -> dict:
    return {
        "data": [
            {
                "id": lei,
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": legal_name},
                        "legalAddress": {"country": legal_country},
                        "headquartersAddress": {"country": legal_country},
                        "jurisdiction": jurisdiction,
                    },
                },
            }
        ]
    }


class _FakeConnection:
    def __enter__(self):
        _FakeCursor.executed = []
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        return None


class _FakeCursor:
    executed: list[tuple[str, tuple]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement: str, params: tuple | None = None) -> None:
        self.executed.append((statement, params or ()))
        self.rowcount = 1
