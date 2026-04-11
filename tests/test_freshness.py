from __future__ import annotations

from datetime import UTC, datetime, timedelta

from finance_data_ops.validation.freshness import FreshnessState, classify_freshness


def test_classify_freshness_fresh() -> None:
    now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    observed = now - timedelta(hours=2)
    state = classify_freshness(last_observed_at=observed, now=now)
    assert state == FreshnessState.FRESH


def test_classify_freshness_stale_within_tolerance() -> None:
    now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    observed = now - timedelta(hours=40)
    state = classify_freshness(last_observed_at=observed, now=now)
    assert state == FreshnessState.STALE_WITHIN_TOLERANCE


def test_classify_freshness_delayed_expected() -> None:
    now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    observed = now - timedelta(days=4)
    state = classify_freshness(last_observed_at=observed, now=now, delayed_expected=True)
    assert state == FreshnessState.DELAYED_EXPECTED


def test_classify_freshness_failure_priority() -> None:
    now = datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    state = classify_freshness(last_observed_at=None, now=now, failure_state="failed_retrying")
    assert state == FreshnessState.FAILED_RETRYING
