from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def build_coverage_report(
    *,
    ticker: str,
    region: str | None,
    exchange: str | None,
    analysis_type: str,
    coverage: dict[str, Any] | None,
    asset_status_by_key: dict[str, dict[str, Any]] | None,
    registry_row: dict[str, Any] | None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = str(ticker).strip().upper()
    normalized_region = str(region or "").strip().lower() or "us"
    normalized_exchange = (str(exchange).strip().upper() if exchange else None)
    assets = asset_status_by_key or {}
    coverage_row = coverage or {}
    registry = registry_row or {}

    sections: list[dict[str, Any]] = [
        {
            "title": "Domain Coverage",
            "items": [
                {"key": "coverage_status", "value": str(coverage_row.get("coverage_status") or "unknown")},
                {"key": "market_data_available", "value": bool(coverage_row.get("market_data_available", False))},
                {"key": "fundamentals_available", "value": bool(coverage_row.get("fundamentals_available", False))},
                {"key": "earnings_available", "value": bool(coverage_row.get("earnings_available", False))},
                {"key": "coverage_ratio", "value": coverage_row.get("coverage_ratio")},
            ],
        },
        {
            "title": "Asset Freshness",
            "items": [
                {"key": "market_price_daily", "value": str((assets.get("market_price_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "market_quotes", "value": str((assets.get("market_quotes") or {}).get("freshness_status") or "unknown")},
                {"key": "fundamentals_daily", "value": str((assets.get("fundamentals_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "earnings_daily", "value": str((assets.get("earnings_daily") or {}).get("freshness_status") or "unknown")},
                {"key": "macro_observations", "value": str((assets.get("macro_observations") or {}).get("freshness_status") or "unknown")},
                {
                    "key": "economic_release_calendar",
                    "value": str((assets.get("economic_release_calendar") or {}).get("freshness_status") or "unknown"),
                },
            ],
        },
        {
            "title": "Onboarding Context",
            "items": [
                {"key": "registry_key", "value": registry.get("registry_key")},
                {"key": "registry_status", "value": str(registry.get("status") or "unknown")},
                {"key": "validation_status", "value": str(registry.get("validation_status") or "unknown")},
                {"key": "promotion_status", "value": str(registry.get("promotion_status") or "unknown")},
                {"key": "validation_reason", "value": registry.get("validation_reason")},
            ],
        },
    ]

    warnings: list[str] = []
    if coverage is None:
        warnings.append("No symbol_data_coverage row found for ticker.")
    if registry_row is None:
        warnings.append("No ticker_registry row found for requested scope.")

    freshness_alerts = []
    for asset_key in ("market_price_daily", "market_quotes", "fundamentals_daily", "earnings_daily"):
        freshness = str((assets.get(asset_key) or {}).get("freshness_status") or "unknown").strip().lower()
        if freshness in {"stale", "failed_hard", "partial"}:
            freshness_alerts.append(f"{asset_key} freshness={freshness}")
    if freshness_alerts:
        warnings.append("Freshness alerts: " + ", ".join(freshness_alerts))

    coverage_status = str(coverage_row.get("coverage_status") or "unknown")
    summary = (
        f"{normalized_ticker} coverage report generated with coverage_status={coverage_status}; "
        f"market_data_available={bool(coverage_row.get('market_data_available', False))}, "
        f"fundamentals_available={bool(coverage_row.get('fundamentals_available', False))}, "
        f"earnings_available={bool(coverage_row.get('earnings_available', False))}."
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
