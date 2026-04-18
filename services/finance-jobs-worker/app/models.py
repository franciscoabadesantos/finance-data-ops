from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")
JobType = Literal["ticker_validation", "ticker_backfill"]


class ExecuteJobRequest(BaseModel):
    job_type: JobType
    registry_key: str = Field(..., min_length=3)
    ticker: str = Field(..., min_length=1)
    region: str = Field(default="us")
    exchange: str | None = None
    instrument_type_hint: str | None = None
    history_limit: int | None = 24
    start: str | None = None
    end: str | None = None
    requested_at: str | None = None
    idempotency_key: str | None = None

    @field_validator("ticker")
    @classmethod
    def normalize_ticker(cls, value: str) -> str:
        ticker = str(value).strip().upper()
        if not ticker or not TICKER_PATTERN.fullmatch(ticker):
            raise ValueError("Invalid ticker format.")
        return ticker

    @field_validator("region")
    @classmethod
    def normalize_region(cls, value: str) -> str:
        return str(value).strip().lower() or "us"

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str | None) -> str | None:
        if value is None:
            return None
        token = str(value).strip().upper()
        return token or None

    def resolved_idempotency_key(self) -> str:
        if self.idempotency_key and str(self.idempotency_key).strip():
            return str(self.idempotency_key).strip()
        prefix = "validate" if self.job_type == "ticker_validation" else "backfill"
        return f"{prefix}:{self.registry_key}"

