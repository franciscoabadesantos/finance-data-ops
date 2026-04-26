"""Runtime settings and local path resolution for Data Ops."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def discover_repo_root(start: Path | None = None) -> Path:
    current = (start or Path(__file__)).resolve()
    if current.is_file():
        current = current.parent
    while True:
        if (current / "pyproject.toml").exists():
            return current
        if current.parent == current:
            return Path.cwd().resolve()
        current = current.parent


@dataclass(frozen=True, slots=True)
class DataOpsSettings:
    repo_root: Path
    cache_root: Path
    supabase_url: str
    supabase_secret_key: str
    default_symbols: list[str]
    default_lookback_days: int
    default_max_attempts: int
    symbol_batch_size: int
    alert_webhook_url: str

    def require_supabase(self) -> None:
        if not self.supabase_url:
            raise ValueError("SUPABASE_URL (or NEXT_PUBLIC_SUPABASE_URL) is required when publish is enabled.")
        if not self.supabase_secret_key:
            raise ValueError("SUPABASE_SECRET_KEY (or SUPABASE_SERVICE_ROLE_KEY) is required when publish is enabled.")


def load_settings(
    *,
    cache_root: str | Path | None = None,
    env: dict[str, str] | None = None,
) -> DataOpsSettings:
    env_map = dict(os.environ) if env is None else dict(env)
    repo_root = discover_repo_root()

    if cache_root is None:
        raw_cache_root = str(env_map.get("DATA_OPS_CACHE_ROOT") or "").strip()
        resolved_cache_root = (repo_root / "data_cache") if not raw_cache_root else Path(raw_cache_root)
    else:
        resolved_cache_root = Path(cache_root)

    if not resolved_cache_root.is_absolute():
        resolved_cache_root = (repo_root / resolved_cache_root).resolve()
    resolved_cache_root.mkdir(parents=True, exist_ok=True)

    supabase_url = str(env_map.get("SUPABASE_URL") or env_map.get("NEXT_PUBLIC_SUPABASE_URL") or "").strip()
    supabase_secret_key = str(env_map.get("SUPABASE_SECRET_KEY") or env_map.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    default_symbols = _parse_symbols_env(env_map.get("DATA_OPS_SYMBOLS"))
    default_lookback_days = _parse_positive_int(env_map.get("DATA_OPS_LOOKBACK_DAYS"), fallback=400)
    default_max_attempts = _parse_positive_int(env_map.get("DATA_OPS_MAX_ATTEMPTS"), fallback=3)
    symbol_batch_size = _parse_positive_int(env_map.get("DATA_OPS_SYMBOL_BATCH_SIZE"), fallback=100)
    alert_webhook_url = str(env_map.get("DATA_OPS_ALERT_WEBHOOK_URL") or "").strip()

    return DataOpsSettings(
        repo_root=repo_root,
        cache_root=resolved_cache_root,
        supabase_url=supabase_url,
        supabase_secret_key=supabase_secret_key,
        default_symbols=default_symbols,
        default_lookback_days=default_lookback_days,
        default_max_attempts=default_max_attempts,
        symbol_batch_size=symbol_batch_size,
        alert_webhook_url=alert_webhook_url,
    )


def _parse_symbols_env(raw_symbols: str | None) -> list[str]:
    if raw_symbols is None:
        return []
    return [str(v).strip().upper() for v in str(raw_symbols).split(",") if str(v).strip()]


def _parse_positive_int(raw: str | int | None, *, fallback: int) -> int:
    try:
        parsed = int(raw)  # type: ignore[arg-type]
        if parsed > 0:
            return parsed
    except (TypeError, ValueError):
        pass
    return int(fallback)
