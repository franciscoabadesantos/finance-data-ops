from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from finance_data_ops.analysis.windows import build_data_window_items


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def build_ticker_snapshot_report(
    *,
    ticker: str,
    region: str | None,
    exchange: str | None,
    analysis_type: str,
    market_snapshot: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    asset_status_by_key: dict[str, dict[str, Any]] | None,
    registry_row: dict[str, Any] | None,
    market_price_rows: list[dict[str, Any]] | None = None,
    fundamentals_rows: list[dict[str, Any]] | None = None,
    earnings_rows: list[dict[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = str(ticker).strip().upper()
    normalized_region = str(region or "").strip().lower() or "us"
    normalized_exchange = (str(exchange).strip().upper() if exchange else None)
    assets = asset_status_by_key or {}

    sections: list[dict[str, Any]] = [
        {
            "title": "Canonical Availability",
            "items": [
                {"key": "market_data_available", "value": bool((coverage or {}).get("market_data_available", False))},
                {"key": "fundamentals_available", "value": bool((coverage or {}).get("fundamentals_available", False))},
                {"key": "earnings_available", "value": bool((coverage or {}).get("earnings_available", False))},
                {"key": "coverage_status", "value": str((coverage or {}).get("coverage_status") or "unknown")},
                {"key": "market_price_rows_present", "value": bool(market_price_rows)},
                {"key": "fundamentals_rows_present", "value": bool(fundamentals_rows)},
                {"key": "earnings_rows_present", "value": bool(earnings_rows)},
            ],
        },
        {
            "title": "Freshness",
            "items": [
                {"key": "market_price_daily", "value": str((assets.get("market_price_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "market_quotes", "value": str((assets.get("market_quotes") or {}).get("freshness_status") or "unknown")},
                {"key": "fundamentals_daily", "value": str((assets.get("fundamentals_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "earnings_daily", "value": str((assets.get("earnings_daily") or {}).get("freshness_status") or "unknown")},
            ],
        },
        {
            "title": "Snapshot",
            "items": [
                {"key": "last_price", "value": (market_snapshot or {}).get("last_price")},
                {"key": "return_1d_pct", "value": (market_snapshot or {}).get("return_1d_pct")},
                {"key": "return_1m_pct", "value": (market_snapshot or {}).get("return_1m_pct")},
                {"key": "return_1y_pct", "value": (market_snapshot or {}).get("return_1y_pct")},
                {"key": "as_of_date", "value": (market_snapshot or {}).get("as_of_date")},
            ],
        },
        {
            "title": "Data Window / Completeness",
            "items": build_data_window_items(
                market_price_rows=market_price_rows,
                fundamentals_rows=fundamentals_rows,
                earnings_rows=earnings_rows,
            ),
        },
        {
            "title": "Registry / Onboarding",
            "items": [
                {"key": "registry_row_present", "value": registry_row is not None},
                {"key": "registry_key", "value": (registry_row or {}).get("registry_key")},
                {"key": "registry_status", "value": str((registry_row or {}).get("status") or "unknown")},
                {"key": "validation_status", "value": str((registry_row or {}).get("validation_status") or "unknown")},
                {"key": "promotion_status", "value": str((registry_row or {}).get("promotion_status") or "unknown")},
            ],
        },
    ]
    warnings: list[str] = []
    if coverage is None:
        warnings.append("No symbol_data_coverage row found for ticker.")
    if market_snapshot is None:
        warnings.append("No ticker_market_stats_snapshot row found for ticker.")
    if registry_row is None:
        warnings.append(
            "No ticker_registry onboarding row found for requested scope; canonical data availability is assessed independently."
        )
    if not market_price_rows:
        warnings.append("No market_price_daily rows found for ticker.")
    if not fundamentals_rows:
        warnings.append("No market_fundamentals_v2 rows found for ticker.")
    if not earnings_rows:
        warnings.append("No market_earnings_history rows found for ticker.")

    coverage_status = str((coverage or {}).get("coverage_status") or "unknown")
    summary = (
        f"{normalized_ticker} snapshot generated with coverage_status={coverage_status}; "
        f"market_data_available={bool((coverage or {}).get('market_data_available', False))}, "
        f"fundamentals_available={bool((coverage or {}).get('fundamentals_available', False))}, "
        f"earnings_available={bool((coverage or {}).get('earnings_available', False))}; "
        f"registry_row_present={registry_row is not None}."
    )

    return {
        "summary": summary,
        "sections": sections,
        "warnings": warnings,
        "metadata": {
            "ticker": normalized_ticker,
            "region": normalized_region,
            "exchange": normalized_exchange,
            "analysis_type": str(analysis_type).strip(),
            "generated_at": generated_at or now_iso(),
        },
    }
