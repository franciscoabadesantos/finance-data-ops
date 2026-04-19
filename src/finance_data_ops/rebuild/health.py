"""Database health gates for rebuild execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import time
from typing import Any

from finance_data_ops.rebuild.policies import DomainPolicy


@dataclass(frozen=True, slots=True)
class HealthCheckResult:
    ok: bool
    checked_at: str
    latency_ms: int
    probe_table: str
    failure_count: int
    reason: str | None = None


class RebuildHealthGate:
    def __init__(self, *, client: Any, policy: DomainPolicy) -> None:
        self.client = client
        self.policy = policy
        self._consecutive_failures = 0

    def check(self) -> HealthCheckResult:
        started = time.perf_counter()
        checked_at = datetime.now(UTC).isoformat()
        try:
            _ = self.client.table(self.policy.preflight_probe_table).select("*").limit(1).execute()
            latency_ms = int(round((time.perf_counter() - started) * 1000.0))
            if latency_ms > int(self.policy.max_latency_ms):
                self._consecutive_failures += 1
                return HealthCheckResult(
                    ok=False,
                    checked_at=checked_at,
                    latency_ms=latency_ms,
                    probe_table=self.policy.preflight_probe_table,
                    failure_count=self._consecutive_failures,
                    reason=f"latency_exceeded:{latency_ms}ms>{self.policy.max_latency_ms}ms",
                )
            self._consecutive_failures = 0
            return HealthCheckResult(
                ok=True,
                checked_at=checked_at,
                latency_ms=latency_ms,
                probe_table=self.policy.preflight_probe_table,
                failure_count=0,
            )
        except Exception as exc:
            self._consecutive_failures += 1
            latency_ms = int(round((time.perf_counter() - started) * 1000.0))
            return HealthCheckResult(
                ok=False,
                checked_at=checked_at,
                latency_ms=latency_ms,
                probe_table=self.policy.preflight_probe_table,
                failure_count=self._consecutive_failures,
                reason=f"probe_failed:{exc}",
            )

    def should_abort(self, result: HealthCheckResult) -> bool:
        return bool(result.failure_count >= int(self.policy.max_consecutive_db_failures))

