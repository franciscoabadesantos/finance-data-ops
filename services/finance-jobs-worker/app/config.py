from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import sys

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    database_url: str
    worker_shared_token: str | None = None

    finance_data_ops_root: str = "../.."

    default_history_limit: int = 24
    default_backfill_years: int = 5

    @field_validator(
        "database_url",
        "worker_shared_token",
        "finance_data_ops_root",
        mode="before",
    )
    @classmethod
    def strip_text_values(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip()
        return value


def bootstrap_data_ops_path(root_hint: str) -> Path:
    root = Path(root_hint).expanduser().resolve()
    src = root / "src"
    for token in (str(root), str(src)):
        if token not in sys.path:
            sys.path.insert(0, token)
    return root


@lru_cache(maxsize=1)
def get_settings() -> WorkerSettings:
    settings = WorkerSettings()
    bootstrap_data_ops_path(settings.finance_data_ops_root)
    return settings
