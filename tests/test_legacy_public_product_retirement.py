from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = REPO_ROOT / "sql"

RETIRED_PUBLIC_OBJECTS = (
    "public.market_price_daily",
    "public.market_quotes",
    "public.market_quotes_history",
    "public.ticker_market_stats_snapshot",
    "public.market_fundamentals_v2",
    "public.ticker_fundamental_summary",
    "public.market_earnings_events",
    "public.market_earnings_history",
    "public.mv_latest_prices",
    "public.mv_latest_fundamentals",
    "public.mv_next_earnings",
    "public.refresh_mv_latest_prices",
    "public.refresh_mv_latest_fundamentals",
    "public.refresh_mv_next_earnings",
    "public.ticker_fundamental_point_in_time",
)

CANONICAL_RUNTIME_OBJECTS = (
    "source_cache.market_price_daily",
    "source_cache.fundamentals",
    "source_cache.earnings",
    "feature_store.technical_features_daily",
    "feature_store.scorecard_daily",
    "feature_store.ticker_page_summary",
    "feature_store.ticker_readiness",
    "public.ticker_registry",
    "public.data_source_runs",
    "public.data_asset_status",
    "public.symbol_data_coverage",
)


def test_final_runtime_schema_excludes_retired_public_product_objects() -> None:
    for schema_path in (
        SQL_ROOT / "000_definitive_runtime_schema.sql",
        SQL_ROOT / "000_runtime_schema.sql",
    ):
        sql = schema_path.read_text(encoding="utf-8").lower()
        for object_name in RETIRED_PUBLIC_OBJECTS:
            assert object_name not in sql, f"{schema_path} still mentions {object_name}"
        for object_name in CANONICAL_RUNTIME_OBJECTS:
            assert object_name in sql, f"{schema_path} is missing {object_name}"


def test_active_sql_excludes_retired_public_product_objects() -> None:
    active_sql_files = [
        path
        for path in SQL_ROOT.glob("*.sql")
        if path.name != "019_retire_legacy_public_product_surfaces.sql"
    ]
    assert active_sql_files

    for path in active_sql_files:
        sql = path.read_text(encoding="utf-8").lower()
        for object_name in RETIRED_PUBLIC_OBJECTS:
            assert object_name not in sql, f"{path} still mentions {object_name}"


def test_publish_package_has_no_retired_public_product_targets() -> None:
    publish_root = REPO_ROOT / "src" / "finance_data_ops" / "publish"
    text = "\n".join(path.read_text(encoding="utf-8").lower() for path in publish_root.glob("*.py"))

    forbidden_publish_tokens = RETIRED_PUBLIC_OBJECTS + (
        '"market_quotes"',
        '"market_quotes_history"',
        '"ticker_market_stats_snapshot"',
        '"market_fundamentals_v2"',
        '"ticker_fundamental_summary"',
        '"market_earnings_events"',
        '"market_earnings_history"',
    )
    for token in forbidden_publish_tokens:
        assert token not in text
