"""Policy definitions for safe historical rebuild execution.

Notes:
- `full_rebuild_start_date` is an operational safety boundary, not a claim about
  the true earliest availability of a dataset. Some upstream sources go much
  further back, but the default fallback stays conservative to keep rebuild
  scope bounded on managed Postgres. If a specific series should rebuild from an
  earlier date, set `required_from_date` explicitly in the canonical catalog
  instead of widening the domain fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

DomainName = Literal["market", "fundamentals", "earnings", "macro", "release-calendar"]


@dataclass(frozen=True, slots=True)
class DomainPolicy:
    domain: DomainName
    chunk_size: int
    chunk_granularity: Literal["ticker", "ticker_window", "series_window"]
    sleep_seconds: float
    preflight_probe_table: str
    inter_batch_health_check_every_n_batches: int
    max_consecutive_db_failures: int
    max_latency_ms: int
    refresh_materialized_views: tuple[str, ...]
    rebuild_status_assets: tuple[str, ...]
    rebuild_coverage: bool
    wipe_tables: tuple[str, ...]
    allow_region_scope: bool
    allow_ticker_scope: bool
    default_window_days: int
    full_rebuild_start_date: date | None = None
    defer_index_eligible: bool = False


_POLICIES: dict[DomainName, DomainPolicy] = {
    "market": DomainPolicy(
        domain="market",
        chunk_size=10,
        chunk_granularity="ticker_window",
        sleep_seconds=0.1,
        preflight_probe_table="market_price_daily",
        inter_batch_health_check_every_n_batches=5,
        max_consecutive_db_failures=3,
        max_latency_ms=2500,
        refresh_materialized_views=("refresh_mv_latest_prices",),
        rebuild_status_assets=("market_price_daily", "market_quotes", "ticker_market_stats_snapshot"),
        rebuild_coverage=True,
        wipe_tables=("ticker_market_stats_snapshot", "market_quotes_history", "market_quotes", "market_price_daily"),
        allow_region_scope=True,
        allow_ticker_scope=True,
        default_window_days=90,
        full_rebuild_start_date=date(2000, 1, 1),
        defer_index_eligible=False,
    ),
    "fundamentals": DomainPolicy(
        domain="fundamentals",
        chunk_size=10,
        chunk_granularity="ticker",
        sleep_seconds=0.1,
        preflight_probe_table="market_fundamentals_v2",
        inter_batch_health_check_every_n_batches=5,
        max_consecutive_db_failures=3,
        max_latency_ms=2500,
        refresh_materialized_views=("refresh_mv_latest_fundamentals",),
        rebuild_status_assets=("market_fundamentals_v2", "ticker_fundamental_summary", "mv_latest_fundamentals"),
        rebuild_coverage=True,
        wipe_tables=("ticker_fundamental_summary", "market_fundamentals_v2"),
        allow_region_scope=True,
        allow_ticker_scope=True,
        default_window_days=365,
        full_rebuild_start_date=None,
        defer_index_eligible=False,
    ),
    "earnings": DomainPolicy(
        domain="earnings",
        chunk_size=10,
        chunk_granularity="ticker",
        sleep_seconds=0.1,
        preflight_probe_table="market_earnings_history",
        inter_batch_health_check_every_n_batches=5,
        max_consecutive_db_failures=3,
        max_latency_ms=2500,
        refresh_materialized_views=("refresh_mv_next_earnings",),
        rebuild_status_assets=("market_earnings_events", "market_earnings_history", "mv_next_earnings"),
        rebuild_coverage=True,
        wipe_tables=("market_earnings_events", "market_earnings_history"),
        allow_region_scope=True,
        allow_ticker_scope=True,
        default_window_days=365,
        full_rebuild_start_date=None,
        defer_index_eligible=False,
    ),
    "macro": DomainPolicy(
        domain="macro",
        chunk_size=4,
        chunk_granularity="series_window",
        sleep_seconds=0.1,
        preflight_probe_table="macro_observations",
        inter_batch_health_check_every_n_batches=3,
        max_consecutive_db_failures=3,
        max_latency_ms=2500,
        refresh_materialized_views=("refresh_mv_latest_macro_observations",),
        rebuild_status_assets=("macro_observations", "macro_daily", "mv_latest_macro_observations"),
        rebuild_coverage=False,
        wipe_tables=("macro_daily", "macro_observations"),
        allow_region_scope=False,
        allow_ticker_scope=False,
        default_window_days=90,
        # Conservative fallback boundary for catalog rows without an explicit
        # required_from_date. Per-series overrides belong in macro_series_catalog.
        full_rebuild_start_date=date(2000, 1, 1),
        defer_index_eligible=False,
    ),
    "release-calendar": DomainPolicy(
        domain="release-calendar",
        chunk_size=4,
        chunk_granularity="series_window",
        sleep_seconds=0.1,
        preflight_probe_table="economic_release_calendar",
        inter_batch_health_check_every_n_batches=3,
        max_consecutive_db_failures=3,
        max_latency_ms=2500,
        refresh_materialized_views=("refresh_mv_latest_economic_release_calendar",),
        rebuild_status_assets=("economic_release_calendar", "mv_latest_economic_release_calendar"),
        rebuild_coverage=False,
        wipe_tables=("economic_release_calendar",),
        allow_region_scope=False,
        allow_ticker_scope=False,
        default_window_days=90,
        # Conservative fallback boundary for release series that do not define a
        # required_from_date explicitly in macro_series_catalog.
        full_rebuild_start_date=date(2000, 1, 1),
        defer_index_eligible=False,
    ),
}


def get_domain_policy(domain: str) -> DomainPolicy:
    token = str(domain).strip().lower()
    try:
        return _POLICIES[token]  # type: ignore[index]
    except KeyError as exc:
        raise ValueError(f"Unsupported rebuild domain: {domain!r}") from exc
