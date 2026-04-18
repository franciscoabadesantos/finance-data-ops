from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


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
    generated_at: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = str(ticker).strip().upper()
    normalized_region = str(region or "").strip().lower() or "us"
    normalized_exchange = (str(exchange).strip().upper() if exchange else None)
    assets = asset_status_by_key or {}

    sections: list[dict[str, Any]] = [
        {
            "title": "Coverage",
            "items": [
                {"key": "market_data_available", "value": bool((coverage or {}).get("market_data_available", False))},
                {"key": "fundamentals_available", "value": bool((coverage or {}).get("fundamentals_available", False))},
                {"key": "earnings_available", "value": bool((coverage or {}).get("earnings_available", False))},
                {"key": "coverage_status", "value": str((coverage or {}).get("coverage_status") or "unknown")},
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
    ]
    warnings: list[str] = []
    if coverage is None:
        warnings.append("No symbol_data_coverage row found for ticker.")
    if market_snapshot is None:
        warnings.append("No ticker_market_stats_snapshot row found for ticker.")
    if registry_row is None:
        warnings.append("No ticker_registry row found for requested scope.")

    coverage_status = str((coverage or {}).get("coverage_status") or "unknown")
    summary = (
        f"{normalized_ticker} snapshot generated with coverage_status={coverage_status}; "
        f"market_data_available={bool((coverage or {}).get('market_data_available', False))}, "
        f"fundamentals_available={bool((coverage or {}).get('fundamentals_available', False))}, "
        f"earnings_available={bool((coverage or {}).get('earnings_available', False))}."
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
