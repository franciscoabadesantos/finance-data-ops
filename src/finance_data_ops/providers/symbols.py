"""Symbol normalization helpers for provider-compatible ticker candidates."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from finance_data_ops.settings import discover_repo_root

try:  # pragma: no cover - import boundary
    import yaml
except Exception:  # pragma: no cover - optional dependency boundary
    yaml = None


DEFAULT_NORMALIZATION_CONFIG: dict[str, Any] = {
    "suffix_by_exchange": {
        "ASX": ".AX",
        "HKEX": ".HK",
        "TSE": ".T",
        "NSE": ".NS",
        "BSE": ".BO",
        "LSE": ".L",
        "TSX": ".TO",
        "ETR": ".DE",
        "EPA": ".PA",
        "BIT": ".MI",
    },
    "explicit_overrides": {
        "ANZ": ["ANZ.AX", "ANZ"],
        "WBC": ["WBC.AX", "WBC"],
        "WOW": ["WOW.AX", "WOW"],
    },
    "instrument_type_overrides": {
        "SPY": "index_proxy",
        "QQQ": "index_proxy",
        "DIA": "index_proxy",
        "IWM": "index_proxy",
        "VOO": "index_proxy",
        "VTI": "index_proxy",
        "VGK": "country_fund",
        "EZU": "country_fund",
        "FEZ": "country_fund",
        "EWJ": "country_fund",
        "EWT": "country_fund",
        "MCHI": "country_fund",
        "KWEB": "country_fund",
        "FXI": "country_fund",
        "ILF": "country_fund",
        "EWZ": "country_fund",
        "EWW": "country_fund",
        "EZA": "country_fund",
        "AFK": "country_fund",
        "EIS": "country_fund",
        "EPHE": "country_fund",
        "THD": "country_fund",
        "EIDO": "country_fund",
        "EWY": "country_fund",
        "EWA": "country_fund",
        "EWS": "country_fund",
        "EWM": "country_fund",
        "ANZ": "equity",
        "ANZ.AX": "equity",
        "WBC": "equity",
        "WBC.AX": "equity",
        "WOW": "equity",
        "WOW.AX": "equity",
    },
    "region_defaults": {
        "us": {"exchanges": []},
        "eu": {"exchanges": ["LSE", "ETR", "EPA", "BIT"]},
        "apac": {"exchanges": ["ASX", "HKEX", "TSE"]},
    },
}


def normalize_input_symbol(raw_symbol: str) -> str:
    return str(raw_symbol).strip().upper()


def normalize_symbol_for_provider(raw_symbol: str, region: str | None, exchange: str | None = None) -> list[str]:
    """Return ordered provider symbol candidates for one input symbol.

    The caller keeps the original raw symbol as its own field (for registry/audit).
    This function only produces normalized candidate symbols to try.
    """

    normalized = normalize_input_symbol(raw_symbol)
    if not normalized:
        return []

    cfg = load_symbol_normalization_config()
    suffix_by_exchange = {
        str(key).strip().upper(): str(value).strip()
        for key, value in dict(cfg.get("suffix_by_exchange") or {}).items()
        if str(key).strip() and str(value).strip()
    }
    explicit_overrides = {
        str(key).strip().upper(): value
        for key, value in dict(cfg.get("explicit_overrides") or {}).items()
        if str(key).strip()
    }

    candidates: list[str] = []
    _extend_candidates(candidates, explicit_overrides.get(normalized))
    if "." in normalized:
        if normalized not in candidates:
            candidates.append(normalized)
        return _dedupe_candidates(candidates)

    region_key = str(region or "").strip().lower()
    region_defaults = dict(cfg.get("region_defaults") or {})
    default_exchanges = []
    if region_key and isinstance(region_defaults.get(region_key), dict):
        default_exchanges = list(region_defaults[region_key].get("exchanges") or [])

    preferred_exchanges = []
    if exchange:
        preferred_exchanges.append(str(exchange).strip().upper())
    preferred_exchanges.extend([str(item).strip().upper() for item in default_exchanges if str(item).strip()])

    for exchange_code in preferred_exchanges:
        suffix = str(suffix_by_exchange.get(exchange_code) or "").strip()
        if not suffix:
            continue
        candidate = f"{normalized}{suffix.upper()}"
        if candidate not in candidates:
            candidates.append(candidate)

    if normalized not in candidates:
        candidates.append(normalized)

    return _dedupe_candidates(candidates)


@lru_cache(maxsize=1)
def load_symbol_normalization_config() -> dict[str, Any]:
    repo_root = discover_repo_root()
    config_path = repo_root / "config" / "symbol_normalization.yml"

    loaded = _read_yaml_dict(config_path)
    return _deep_merge(DEFAULT_NORMALIZATION_CONFIG, loaded)


def _extend_candidates(target: list[str], raw_value: Any) -> None:
    if raw_value is None:
        return
    if isinstance(raw_value, (list, tuple)):
        for item in raw_value:
            token = normalize_input_symbol(str(item))
            if token and token not in target:
                target.append(token)
        return
    token = normalize_input_symbol(str(raw_value))
    if token and token not in target:
        target.append(token)


def _dedupe_candidates(candidates: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        token = normalize_input_symbol(item)
        if not token or token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _read_yaml_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load symbol normalization config. Install dependency: PyYAML>=6.0."
        )
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))  # type: ignore[union-attr]
    if isinstance(parsed, dict):
        return parsed
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(dict(merged[key]), value)
        else:
            merged[key] = value
    return merged
