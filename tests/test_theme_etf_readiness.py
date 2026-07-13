from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import re

import pandas as pd

from finance_data_ops.theme_etfs.readiness import (
    build_etf_theme_readiness,
    classify_relationship_map_theme_health,
)


def test_relationship_map_eligibility_is_derived_from_current_technical_coverage() -> None:
    holdings = _holdings("COVR", 20)
    themes = _themes([("COVR", "covered_theme", 20)])
    prices = _symbols_frame("symbol", [f"COVR{i:02d}" for i in range(1, 21)])

    low_coverage = build_etf_theme_readiness(
        etf_holdings=holdings,
        etf_themes=themes,
        market_price_daily=prices,
        technical_features_daily=_symbols_frame("symbol", [f"COVR{i:02d}" for i in range(1, 5)]),
        computed_at=datetime(2026, 7, 13, tzinfo=UTC),
    )
    assert bool(low_coverage.iloc[0]["relationship_map_eligible"]) is False
    assert low_coverage.iloc[0]["relationship_map_ineligible_reason"] == "insufficient_constituent_coverage"

    covered = build_etf_theme_readiness(
        etf_holdings=holdings,
        etf_themes=themes,
        market_price_daily=prices,
        technical_features_daily=_symbols_frame("symbol", [f"COVR{i:02d}" for i in range(1, 6)]),
        computed_at=datetime(2026, 7, 13, tzinfo=UTC),
    )
    assert bool(covered.iloc[0]["relationship_map_eligible"]) is True
    assert covered.iloc[0]["relationship_map_ineligible_reason"] is None
    assert covered.iloc[0]["technical_constituent_count"] == 5
    assert covered.iloc[0]["coverage_ratio"] == 0.25

    after_drop = build_etf_theme_readiness(
        etf_holdings=holdings,
        etf_themes=themes,
        market_price_daily=prices,
        technical_features_daily=_symbols_frame("symbol", [f"COVR{i:02d}" for i in range(1, 4)]),
        computed_at=datetime(2026, 7, 13, tzinfo=UTC),
    )
    assert bool(after_drop.iloc[0]["relationship_map_eligible"]) is False


def test_pending_coverage_does_not_become_red_no_edges() -> None:
    holdings = pd.concat([_holdings("PEND", 30), _holdings("ELIG", 30)], ignore_index=True)
    themes = _themes([("PEND", "pending_theme", 30), ("ELIG", "eligible_theme", 30)])
    readiness = build_etf_theme_readiness(
        etf_holdings=holdings,
        etf_themes=themes,
        market_price_daily=_symbols_frame("symbol", [f"ELIG{i:02d}" for i in range(1, 31)]),
        technical_features_daily=_symbols_frame("symbol", [f"ELIG{i:02d}" for i in range(1, 7)]),
        computed_at=datetime(2026, 7, 13, tzinfo=UTC),
    )
    health = classify_relationship_map_theme_health(readiness, edge_counts={"PEND": 0, "ELIG": 0})
    by_etf = {row["etf_symbol"]: row for row in health.to_dict(orient="records")}

    assert by_etf["PEND"]["relationship_map_health_bucket"] == "pendingCoverage"
    assert by_etf["PEND"]["relationship_map_health_state"] == "pending_coverage"
    assert by_etf["PEND"]["relationship_map_health_severity"] == "ok"
    assert by_etf["ELIG"]["relationship_map_health_bucket"] == "noEdges"
    assert by_etf["ELIG"]["relationship_map_health_severity"] == "red"


def test_known_low_coverage_wave2_etfs_are_pending_coverage_not_no_edges() -> None:
    counts = {
        "AQWA": 40,
        "BETZ": 23,
        "JETS": 10,
        "KRE": 159,
        "MOO": 49,
        "REMX": 30,
        "URA": 52,
        "USRT": 126,
        "YOLO": 17,
    }
    holdings = pd.concat([_holdings(etf, count) for etf, count in counts.items()], ignore_index=True)
    themes = _themes([(etf, f"{etf.lower()}_theme", count) for etf, count in counts.items()])
    identity = _identity_for_holdings(holdings)
    technicals = pd.DataFrame(
        [
            {"symbol": "JETS01"},
            {"symbol": "MOO01"},
            {"symbol": "URA01"},
        ]
    )
    readiness = build_etf_theme_readiness(
        etf_holdings=holdings,
        etf_themes=themes,
        market_price_daily=technicals,
        technical_features_daily=technicals,
        etf_holding_onboarding_identity=identity,
        computed_at=datetime(2026, 7, 13, tzinfo=UTC),
    )
    health = classify_relationship_map_theme_health(readiness, edge_counts={etf: 0 for etf in counts})

    assert set(readiness["etf_symbol"]) == set(counts)
    assert readiness["relationship_map_eligible"].eq(False).all()
    assert set(readiness["relationship_map_ineligible_reason"]) == {"insufficient_constituent_coverage"}
    assert health["relationship_map_health_state"].eq("pending_coverage").all()
    assert health["relationship_map_health_bucket"].eq("pendingCoverage").all()
    assert not health["relationship_map_health_bucket"].eq("noEdges").any()
    assert not health["relationship_map_health_severity"].eq("red").any()


def test_active_data_ops_paths_do_not_write_bare_public_etf_theme_tables() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    checked_files = [
        *repo_root.joinpath("src").rglob("*.py"),
        *repo_root.joinpath("flows").rglob("*.py"),
        *repo_root.joinpath("scripts").rglob("*.py"),
    ]
    bare_upsert = re.compile(r"\.upsert\(\s*['\"](?:etf_holdings|etf_themes)['\"]")
    public_sql = re.compile(r"public\.(?:etf_holdings|etf_themes)")

    offenders: list[str] = []
    for path in checked_files:
        text = path.read_text()
        if bare_upsert.search(text) or public_sql.search(text):
            offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def _holdings(etf: str, count: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "etf_ticker": etf,
                "holding_symbol": f"{etf}{idx:02d}",
                "holding_name": f"{etf} Holding {idx}",
                "weight": 1 / count,
                "as_of": "2026-07-02",
            }
            for idx in range(1, count + 1)
        ]
    )


def _themes(rows: list[tuple[str, str, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "etf_ticker": etf,
                "theme": theme,
                "wave": 2,
                "holdings_count": count,
                "holdings_as_of": "2026-07-02",
                "holdings_shallow": False,
                "active": True,
            }
            for etf, theme, count in rows
        ]
    )


def _symbols_frame(column: str, symbols: list[str]) -> pd.DataFrame:
    return pd.DataFrame([{column: symbol} for symbol in symbols])


def _identity_for_holdings(holdings: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "etf_ticker": row["etf_ticker"],
                "source_symbol": row["holding_symbol"],
                "onboard_symbol": row["holding_symbol"],
                "source_country": "US",
                "is_onboardable": True,
            }
            for row in holdings.to_dict(orient="records")
        ]
    )
