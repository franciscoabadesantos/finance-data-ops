"""Failure classification and retry primitives for refresh jobs."""

from __future__ import annotations

import socket
import time
import urllib.error
from json import JSONDecodeError
from dataclasses import dataclass
from typing import Callable, TypeVar

import requests

T = TypeVar("T")

_RETRYABLE_HTTP_CODES = {408, 409, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True, slots=True)
class FailureClassification:
    retryable: bool
    code: str
    message: str


def classify_failure(exc: Exception) -> FailureClassification:
    if isinstance(exc, urllib.error.HTTPError):
        if int(exc.code) in _RETRYABLE_HTTP_CODES:
            return FailureClassification(
                retryable=True,
                code="failed_retrying",
                message=f"HTTP {exc.code}",
            )
        return FailureClassification(
            retryable=False,
            code="failed_hard",
            message=f"HTTP {exc.code}",
        )

    if isinstance(exc, (TimeoutError, ConnectionError, socket.timeout, urllib.error.URLError, requests.RequestException)):
        return FailureClassification(retryable=True, code="failed_retrying", message=str(exc))

    if isinstance(exc, JSONDecodeError):
        return FailureClassification(retryable=True, code="failed_retrying", message=str(exc))

    if isinstance(exc, RuntimeError):
        message = str(exc).lower()
        if "curl fetch failed" in message or "timed out" in message or "timeout" in message:
            return FailureClassification(retryable=True, code="failed_retrying", message=str(exc))

    if isinstance(exc, ValueError):
        return FailureClassification(retryable=False, code="failed_hard", message=str(exc))

    return FailureClassification(retryable=False, code="failed_hard", message=repr(exc))


def run_with_retry(
    operation: Callable[[], T],
    *,
    max_attempts: int = 3,
    sleep_seconds: float = 0.0,
) -> tuple[T | None, Exception | None, int, bool]:
    attempts = max(int(max_attempts), 1)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation(), None, attempt, False
        except Exception as exc:  # pragma: no cover - behavior tested via callers
            last_error = exc
            classification = classify_failure(exc)
            should_retry = classification.retryable and attempt < attempts
            if not should_retry:
                return None, exc, attempt, classification.retryable
            if sleep_seconds > 0:
                time.sleep(float(sleep_seconds))
    return None, last_error, attempts, True
