"""Symbol resolution order for production flows with rollout-safe fallbacks."""

from __future__ import annotations

from typing import Mapping

from finance_data_ops.settings import DataOpsSettings
from finance_data_ops.validation.universe_builder import load_validated_symbols

REGION_SYMBOL_ENV = {
    "us": "DATA_OPS_SYMBOLS_US",
    "eu": "DATA_OPS_SYMBOLS_EU",
    "apac": "DATA_OPS_SYMBOLS_APAC",
}


def parse_symbols(raw: str | list[str] | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(v).strip().upper() for v in raw if str(v).strip()]
    return [str(v).strip().upper() for v in str(raw).split(",") if str(v).strip()]


def resolve_symbols(
    *,
    symbols: str | list[str] | None,
    region: str | None,
    settings: DataOpsSettings,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    parsed_symbols = parse_symbols(symbols)
    if parsed_symbols:
        return parsed_symbols

    env_map = dict(env or {})
    normalized_region = str(region or "").strip().lower()
    if normalized_region:
        registry_symbols = load_validated_symbols(
            normalized_region,
            require_market=True,
            cache_root=settings.cache_root,
            supabase_url=settings.supabase_url,
            service_role_key=settings.supabase_secret_key,
        )
        if registry_symbols:
            return registry_symbols

        region_env_key = REGION_SYMBOL_ENV.get(normalized_region, f"DATA_OPS_SYMBOLS_{normalized_region.upper()}")
        region_symbols = parse_symbols(env_map.get(region_env_key))
        if region_symbols:
            return region_symbols

    return list(settings.default_symbols)
