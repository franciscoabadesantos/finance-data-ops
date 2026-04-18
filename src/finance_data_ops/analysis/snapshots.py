from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from finance_data_ops.analysis.windows import (
    build_completeness_summary_lines,
    build_data_window_items,
    build_data_window_stats,
    coverage_summary_text,
    registry_summary_text,
)


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
    coverage_row = coverage or {}
    stats_by_domain = build_data_window_stats(
        market_price_rows=market_price_rows,
        fundamentals_rows=fundamentals_rows,
        earnings_rows=earnings_rows,
    )
    coverage_summary = coverage_summary_text(ticker=normalized_ticker, coverage=coverage_row)
    registry_summary = registry_summary_text(ticker=normalized_ticker, registry_row=registry_row)
    completeness_lines = build_completeness_summary_lines(stats_by_domain=stats_by_domain)

    sections: list[dict[str, Any]] = [
        {
            "title": "Coverage Summary",
            "items": [
                {"key": "Summary", "value": coverage_summary},
            ],
        },
        {
            "title": "Completeness Summary",
            "items": completeness_lines,
        },
        {
            "title": "Registry / Onboarding Summary",
            "items": [
                {"key": "Summary", "value": registry_summary},
            ],
        },
        {
            "title": "Canonical Availability",
            "items": [
                {"key": "Market data available", "value": bool(coverage_row.get("market_data_available", False))},
                {"key": "Fundamentals available", "value": bool(coverage_row.get("fundamentals_available", False))},
                {"key": "Earnings available", "value": bool(coverage_row.get("earnings_available", False))},
                {"key": "Coverage status", "value": str(coverage_row.get("coverage_status") or "unknown")},
                {"key": "Market price history rows present", "value": bool(market_price_rows)},
                {"key": "Fundamentals rows present", "value": bool(fundamentals_rows)},
                {"key": "Earnings history rows present", "value": bool(earnings_rows)},
            ],
        },
        {
            "title": "Freshness",
            "items": [
                {"key": "Market price history", "value": str((assets.get("market_price_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "Market quotes", "value": str((assets.get("market_quotes") or {}).get("freshness_status") or "unknown")},
                {"key": "Fundamentals", "value": str((assets.get("fundamentals_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "Earnings history", "value": str((assets.get("earnings_daily") or {}).get("freshness_status") or "unknown")},
            ],
        },
        {
            "title": "Snapshot",
            "items": [
                {"key": "Last price", "value": (market_snapshot or {}).get("last_price")},
                {"key": "Return 1D %", "value": (market_snapshot or {}).get("return_1d_pct")},
                {"key": "Return 1M %", "value": (market_snapshot or {}).get("return_1m_pct")},
                {"key": "Return 1Y %", "value": (market_snapshot or {}).get("return_1y_pct")},
                {"key": "As-of date", "value": (market_snapshot or {}).get("as_of_date")},
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
                {"key": "Registry row present", "value": registry_row is not None},
                {"key": "Registry key", "value": (registry_row or {}).get("registry_key")},
                {"key": "Registry status", "value": str((registry_row or {}).get("status") or "unknown")},
                {"key": "Validation status", "value": str((registry_row or {}).get("validation_status") or "unknown")},
                {"key": "Promotion status", "value": str((registry_row or {}).get("promotion_status") or "unknown")},
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

    coverage_status = str(coverage_row.get("coverage_status") or "unknown")
    summary = (
        f"{coverage_summary} {completeness_lines[0]['value']} {registry_summary} "
        f"(coverage_status={coverage_status})."
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
