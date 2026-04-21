from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")
JobType = Literal["ticker_validation", "ticker_backfill", "analysis_job"]
AnalysisType = Literal[
    "ticker_snapshot",
    "coverage_report",
    "ticker_signal_v1",
    "data_ops_rebuild",
    "data_ops_series_upsert",
]


class ExecuteJobRequest(BaseModel):
    job_type: JobType
    registry_key: str | None = Field(default=None, min_length=3)
    job_id: str | None = Field(default=None, min_length=3)
    analysis_type: AnalysisType | None = None
    job_params: dict[str, Any] | None = None
    ticker: str = Field(..., min_length=1)
    region: str | None = Field(default="us")
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
    def normalize_region(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return str(value).strip().lower() or None

    @field_validator("exchange")
    @classmethod
    def normalize_exchange(cls, value: str | None) -> str | None:
        if value is None:
            return None
        token = str(value).strip().upper()
        return token or None

    @model_validator(mode="after")
    def validate_shape(self) -> "ExecuteJobRequest":
        if self.job_type in {"ticker_validation", "ticker_backfill"}:
            if not self.registry_key or not str(self.registry_key).strip():
                raise ValueError("registry_key is required for ticker jobs")
            if not self.region or not str(self.region).strip():
                self.region = "us"
        if self.job_type == "analysis_job":
            if not self.job_id or not str(self.job_id).strip():
                raise ValueError("job_id is required for analysis jobs")
            if self.analysis_type not in {
                "ticker_snapshot",
                "coverage_report",
                "ticker_signal_v1",
                "data_ops_rebuild",
                "data_ops_series_upsert",
            }:
                raise ValueError(
                    "analysis_type must be one of: ticker_snapshot, coverage_report, ticker_signal_v1, data_ops_rebuild, data_ops_series_upsert"
                )
        return self

    def resolved_idempotency_key(self) -> str:
        if self.idempotency_key and str(self.idempotency_key).strip():
            return str(self.idempotency_key).strip()
        if self.job_type == "ticker_validation":
            return f"validate:{self.registry_key}"
        if self.job_type == "ticker_backfill":
            return f"backfill:{self.registry_key}"
        return f"analysis:{self.job_id}"
