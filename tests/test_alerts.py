from __future__ import annotations

from finance_data_ops.ops.alerts import emit_alert_webhook


def test_emit_alert_webhook_ignores_placeholder_url() -> None:
    assert emit_alert_webhook({"message": "test"}, webhook_url="placeholder") is False
