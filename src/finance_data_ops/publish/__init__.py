"""Postgres publication adapters for Data Ops surfaces."""

from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.publish.earnings import publish_earnings_surfaces
from finance_data_ops.publish.fundamentals import publish_fundamentals_surfaces
from finance_data_ops.publish.macro import publish_macro_surfaces
from finance_data_ops.publish.prices import publish_prices_surfaces
from finance_data_ops.publish.release_calendar import publish_release_calendar_surfaces
from finance_data_ops.publish.status import publish_status_surfaces
from finance_data_ops.publish.ticker_registry import publish_ticker_registry
from finance_data_ops.publish.trading_calendar import publish_trading_calendar_surfaces

__all__ = [
    "PostgresPublisher",
    "RecordingPublisher",
    "publish_prices_surfaces",
    "publish_fundamentals_surfaces",
    "publish_earnings_surfaces",
    "publish_macro_surfaces",
    "publish_release_calendar_surfaces",
    "publish_status_surfaces",
    "publish_ticker_registry",
    "publish_trading_calendar_surfaces",
]
