from __future__ import annotations

import pandas as pd

from scripts import reconcile_source_refresh_universe
from finance_data_ops.validation.source_universe_reconciliation import build_source_universe_reconciliation_plan


def _tracked(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{"symbol": symbol, "is_tracked": True} for symbol in symbols])


def _prices(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{"symbol": symbol, "price_date": "2026-07-10", "close": 100.0} for symbol in symbols])


def _technicals(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{"symbol": symbol, "as_of_date": "2026-07-10", "features": {}} for symbol in symbols])


def _summary(symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{"symbol": symbol, "as_of_date": "2026-07-10"} for symbol in symbols])


def _registry_row(
    symbol: str,
    *,
    input_symbol: str | None = None,
    normalized_symbol: str | None = None,
    status: str = "active",
    promotion_status: str = "validated_full",
    market_supported: bool = True,
    region: str = "us",
    exchange: str | None = None,
) -> dict[str, object]:
    return {
        "registry_key": f"{input_symbol or symbol}|{region}|{exchange or 'default'}",
        "input_symbol": input_symbol or symbol,
        "normalized_symbol": normalized_symbol if normalized_symbol is not None else symbol,
        "region": region,
        "exchange": exchange,
        "exchange_mic": None,
        "currency": None,
        "instrument_type": "equity",
        "status": status,
        "market_supported": market_supported,
        "fundamentals_supported": True,
        "earnings_supported": True,
        "validation_status": promotion_status,
        "validation_reason": "test",
        "promotion_status": promotion_status,
        "last_validated_at": "2026-07-10T00:00:00+00:00",
        "notes": {},
        "updated_at": "2026-07-10T00:00:00+00:00",
    }


def test_reconciliation_creates_row_for_tracked_materialized_symbol_without_registry() -> None:
    plan = build_source_universe_reconciliation_plan(
        registry_frame=pd.DataFrame(),
        readiness_frame=_tracked(["TTWO"]),
        prices_frame=_prices(["TTWO"]),
        technicals_frame=_technicals(["TTWO"]),
        ticker_page_summary_frame=_summary(["TTWO"]),
    )

    assert plan.issue_counts == {"no_registry_row": 1}
    assert len(plan.upsert_rows) == 1
    row = plan.upsert_rows[0]
    assert row["input_symbol"] == "TTWO"
    assert row["normalized_symbol"] == "TTWO"
    assert row["status"] == "active"
    assert row["market_supported"] is True
    assert row["promotion_status"] == "validated_market_only"


def test_reconciliation_promotes_pending_non_promoted_international_rows() -> None:
    registry = pd.DataFrame(
        [
            _registry_row("SAP.DE", status="pending_validation", promotion_status="pending_validation", region="de"),
            _registry_row("ASML.AS", status="pending_validation", promotion_status="pending_validation", region="nl"),
            _registry_row("BHP.AX", status="pending_validation", promotion_status="pending_validation", region="au"),
        ]
    )
    plan = build_source_universe_reconciliation_plan(
        registry_frame=registry,
        readiness_frame=_tracked(["SAP.DE", "ASML.AS", "BHP.AX"]),
        prices_frame=_prices(["SAP.DE", "ASML.AS", "BHP.AX"]),
        technicals_frame=_technicals(["SAP.DE", "ASML.AS", "BHP.AX"]),
        ticker_page_summary_frame=_summary(["SAP.DE", "ASML.AS", "BHP.AX"]),
    )

    assert plan.issue_counts == {"pending_validation": 3}
    by_symbol = {row["normalized_symbol"]: row for row in plan.upsert_rows}
    assert by_symbol["SAP.DE"]["region"] == "eu"
    assert by_symbol["ASML.AS"]["region"] == "eu"
    assert by_symbol["BHP.AX"]["region"] == "apac"
    assert {row["status"] for row in plan.upsert_rows} == {"active"}
    assert {row["market_supported"] for row in plan.upsert_rows} == {True}


def test_reconciliation_groups_pending_missing_normalized_separately() -> None:
    row = _registry_row(
        "0700.HK",
        status="pending_validation",
        promotion_status="pending_validation",
        market_supported=True,
        region="hk",
    )
    row["normalized_symbol"] = None
    registry = pd.DataFrame(
        [
            row,
        ]
    )
    plan = build_source_universe_reconciliation_plan(
        registry_frame=registry,
        readiness_frame=_tracked(["0700.HK"]),
        prices_frame=_prices(["0700.HK"]),
        technicals_frame=_technicals(["0700.HK"]),
        ticker_page_summary_frame=_summary(["0700.HK"]),
    )

    assert plan.issue_counts == {"missing_normalized_symbol": 1}
    assert len(plan.upsert_rows) == 1
    assert plan.upsert_rows[0]["normalized_symbol"] == "0700.HK"
    assert plan.upsert_rows[0]["region"] == "apac"


def test_reconciliation_keeps_rejected_shadow_and_promotes_canonical_pending_row() -> None:
    registry = pd.DataFrame(
        [
            _registry_row("LLY", input_symbol="LLY", normalized_symbol="LLY", status="rejected", promotion_status="rejected"),
            _registry_row(
                "LLY",
                input_symbol="LLY",
                normalized_symbol=None,
                status="pending_validation",
                promotion_status="pending_validation",
                exchange="XNYS",
            ),
        ]
    )
    plan = build_source_universe_reconciliation_plan(
        registry_frame=registry,
        readiness_frame=_tracked(["LLY"]),
        prices_frame=_prices(["LLY"]),
        technicals_frame=_technicals(["LLY"]),
        ticker_page_summary_frame=_summary(["LLY"]),
    )

    assert plan.issue_counts == {"rejected_or_superseded_without_active_canonical": 1}
    assert len(plan.upsert_rows) == 1
    assert plan.upsert_rows[0]["registry_key"] == "LLY|us|XNYS"
    assert plan.upsert_rows[0]["normalized_symbol"] == "LLY"
    assert plan.upsert_rows[0]["status"] == "active"


def test_active_short_history_symbols_are_selected_not_reconciled() -> None:
    symbols = ["HON", "JBIO", "SHOP", "SLB"]
    registry = pd.DataFrame([_registry_row(symbol, promotion_status="validated_market_only") for symbol in symbols])
    plan = build_source_universe_reconciliation_plan(
        registry_frame=registry,
        readiness_frame=_tracked(symbols),
        prices_frame=_prices(symbols),
        technicals_frame=_technicals(symbols),
        ticker_page_summary_frame=_summary(symbols),
    )

    assert plan.issues == []
    assert plan.upsert_rows == []


def test_price_only_materialized_residue_is_reported_not_promoted() -> None:
    plan = build_source_universe_reconciliation_plan(
        registry_frame=pd.DataFrame(),
        readiness_frame=pd.DataFrame(),
        prices_frame=_prices(["OLD"]),
        technicals_frame=pd.DataFrame(),
        ticker_page_summary_frame=pd.DataFrame(),
    )

    assert plan.issues == []
    assert plan.upsert_rows == []
    assert plan.materialized_residue == [
        {
            "symbol": "OLD",
            "sources": ["source_cache.market_price_daily"],
            "reason": "price_only_materialized_not_tracked",
        }
    ]


def test_postgres_loader_queries_are_symbol_level_not_full_table_fetches() -> None:
    queries = [
        reconcile_source_refresh_universe.READINESS_TRACKED_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.READINESS_TRACKED_SEARCH_READY_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.READINESS_STATUS_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.PRICE_SYMBOL_STATS_QUERY,
        reconcile_source_refresh_universe.TECHNICAL_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.SUMMARY_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.FUNDAMENTAL_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.EARNINGS_SYMBOLS_QUERY,
        reconcile_source_refresh_universe.SELECTED_REGISTRY_QUERY,
        reconcile_source_refresh_universe.RELATED_REGISTRY_QUERY,
        reconcile_source_refresh_universe.ENTITY_ATTRIBUTES_QUERY,
    ]

    for query in queries:
        normalized = " ".join(query.lower().split())
        assert "select *" not in normalized
        if "source_cache.market_price_daily" in normalized:
            assert "group by upper(symbol)" in normalized
            assert "count(*)" in normalized
        if "source_cache.fundamentals" in normalized or "source_cache.earnings" in normalized:
            assert "select distinct upper(symbol)" in normalized
        if "feature_store.technical_features_daily" in normalized:
            assert "select distinct upper(symbol)" in normalized


def test_readiness_query_uses_available_tracking_column() -> None:
    assert reconcile_source_refresh_universe._readiness_symbols_query({"symbol", "is_tracked"}) == (
        reconcile_source_refresh_universe.READINESS_TRACKED_SYMBOLS_QUERY
    )
    assert reconcile_source_refresh_universe._readiness_symbols_query({"symbol", "tracked_search_ready"}) == (
        reconcile_source_refresh_universe.READINESS_TRACKED_SEARCH_READY_SYMBOLS_QUERY
    )
    assert reconcile_source_refresh_universe._readiness_symbols_query({"symbol", "readiness_status"}) == (
        reconcile_source_refresh_universe.READINESS_STATUS_SYMBOLS_QUERY
    )
