#!/usr/bin/env python3
"""Backfill published Entity Layer entity_master.home_country from raw GLEIF cache.

Dry-run is the default. `--apply` updates only feature_store.entity_master
home_country/metadata for the selected publication batch. No provider APIs are
called, no entity mappings are changed, and no current pointer is modified.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from finance_data_ops.identity.home_country_backfill import build_home_country_backfill_plan
from finance_data_ops.settings import load_settings


DEFAULT_BATCH_ID = "tracked-675-first-publish"


def main() -> None:
    args = _parser().parse_args()
    settings = load_settings(cache_root=args.cache_root)
    settings.require_database()
    entity_rows, gleif_entity_rows, gleif_isin_lei_rows = read_postgres_inputs(
        database_dsn=settings.database_dsn,
        batch_id=args.batch_id,
    )
    plan = build_home_country_backfill_plan(
        entity_rows=entity_rows,
        gleif_entity_rows=gleif_entity_rows,
        gleif_isin_lei_rows=gleif_isin_lei_rows,
        batch_id=args.batch_id,
    )
    output: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry_run",
        "batch_id": args.batch_id,
        "plan": _public_plan(plan),
        "apply_result": None,
    }
    if args.apply:
        output["apply_result"] = apply_home_country_updates(
            database_dsn=settings.database_dsn,
            changes=plan["changes"],
            batch_id=args.batch_id,
        )
        entity_rows_after, gleif_entity_rows_after, gleif_isin_lei_rows_after = read_postgres_inputs(
            database_dsn=settings.database_dsn,
            batch_id=args.batch_id,
        )
        output["post_apply_verification"] = _public_plan(
            build_home_country_backfill_plan(
                entity_rows=entity_rows_after,
                gleif_entity_rows=gleif_entity_rows_after,
                gleif_isin_lei_rows=gleif_isin_lei_rows_after,
                batch_id=args.batch_id,
            )
        )
    print(json.dumps(output, indent=2, sort_keys=True, default=str))


def read_postgres_inputs(*, database_dsn: str, batch_id: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Entity Layer home-country backfill.") from exc

    with psycopg.connect(database_dsn, connect_timeout=30, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select entity_id, lei, legal_name, display_name, home_country,
                       resolution_status, publication_batch_id, metadata
                from feature_store.entity_master
                where publication_batch_id = %s
                order by entity_id
                """,
                (batch_id,),
            )
            entity_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                select normalized_query_name, query_name, candidates_payload,
                       response_payload, status, error_message
                from source_cache.gleif_entity_raw
                where status in ('success', 'ambiguous')
                """
            )
            gleif_entity_rows = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """
                select isin, lei, legal_name, response_payload, status, error_message
                from source_cache.gleif_isin_lei_raw
                where status = 'success'
                """
            )
            gleif_isin_lei_rows = [dict(row) for row in cur.fetchall()]
    return entity_rows, gleif_entity_rows, gleif_isin_lei_rows


def apply_home_country_updates(*, database_dsn: str, changes: list[dict[str, Any]], batch_id: str) -> dict[str, Any]:
    if not changes:
        return {"status": "skipped", "updated_rows": 0}
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required for Entity Layer home-country backfill.") from exc

    updated = 0
    with psycopg.connect(database_dsn, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            for change in changes:
                evidence = {
                    "home_country": change["new_home_country"],
                    "source": "entity_home_country_backfill",
                    "source_table": change["source_table"],
                    "source_field": change["source_field"],
                    "lei": change["lei"],
                    "batch_id": batch_id,
                }
                cur.execute(
                    """
                    update feature_store.entity_master
                    set home_country = %s,
                        metadata = coalesce(metadata, '{}'::jsonb)
                          || jsonb_build_object('home_country_backfill', %s::jsonb),
                        updated_at = now()
                    where entity_id = %s
                      and publication_batch_id = %s
                      and resolution_status = 'resolved'
                      and lei = %s
                      and (home_country is null or btrim(home_country) = '')
                    """,
                    (
                        change["new_home_country"],
                        Jsonb(evidence),
                        change["entity_id"],
                        batch_id,
                        change["lei"],
                    ),
                )
                updated += int(cur.rowcount or 0)
        conn.commit()
    return {"status": "ok", "updated_rows": updated}


def _public_plan(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in plan.items()
        if key
        in {
            "batch_id",
            "total_entities",
            "resolved_entities",
            "home_country_null_before",
            "home_country_null_after",
            "backfilled_count",
            "unchanged_count",
            "missing_cache_count",
            "unresolved_no_lei_count",
            "conflict_count",
            "examples_changed",
            "missing_cache_examples",
            "conflict_examples",
            "blockers",
        }
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cache-only backfill of feature_store.entity_master.home_country for a published Entity Layer batch."
    )
    parser.add_argument("--batch-id", default=DEFAULT_BATCH_ID)
    parser.add_argument("--apply", action="store_true", help="Apply home_country updates. Dry-run is default.")
    parser.add_argument("--cache-root", default=None)
    return parser


if __name__ == "__main__":
    main()
