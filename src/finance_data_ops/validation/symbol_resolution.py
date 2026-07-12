"""Source refresh universe resolution for scheduled and manual Data Ops runs."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from finance_data_ops.geography import REGION_AMER, REGION_APAC, REGION_EU, REGION_US, normalize_country, region_for_country
from finance_data_ops.settings import DataOpsSettings

CANONICAL_REFRESH_REGIONS = ("us", "eu", "apac")
ALLOWED_PROMOTION_STATUSES = {"validated_market_only", "validated_full"}
REGION_SYMBOL_OVERRIDE_ENV = {
    "us": "DATA_OPS_SYMBOLS_OVERRIDE_US",
    "eu": "DATA_OPS_SYMBOLS_OVERRIDE_EU",
    "apac": "DATA_OPS_SYMBOLS_OVERRIDE_APAC",
}


@dataclass(frozen=True, slots=True)
class SourceRefreshUniverse:
    symbols: list[str]
    region: str
    source: str
    selection_reason: str
    counts: dict[str, int]
    excluded_count_by_reason: dict[str, int]

    @property
    def scope(self) -> str:
        if self.source == "ticker_registry" and self.region in CANONICAL_REFRESH_REGIONS:
            return f"region:{self.region}"
        if self.source == "ticker_registry" and self.region in {"all", "global"}:
            return "global"
        return "run_subset"

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["scope"] = self.scope
        return data


def parse_symbols(raw: str | Sequence[str] | None) -> list[str]:
    if raw is None:
        return []
    values = raw if isinstance(raw, (list, tuple, set)) else str(raw).split(",")
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def resolve_source_refresh_universe(
    *,
    symbols: str | Sequence[str] | None,
    region: str | None,
    settings: DataOpsSettings,
    env: Mapping[str, str] | None = None,
    registry_rows: Sequence[Mapping[str, Any]] | None = None,
) -> SourceRefreshUniverse:
    normalized_region = normalize_refresh_region(region)
    explicit_symbols = parse_symbols(symbols)
    if explicit_symbols:
        return SourceRefreshUniverse(
            symbols=explicit_symbols,
            region=normalized_region,
            source="explicit_symbols",
            selection_reason="flow symbols parameter supplied",
            counts={"selected": len(explicit_symbols)},
            excluded_count_by_reason={},
        )

    env_map = dict(env or {})
    override_symbols = _resolve_env_override_symbols(region=normalized_region, env=env_map)
    if override_symbols:
        return SourceRefreshUniverse(
            symbols=override_symbols,
            region=normalized_region,
            source="env_override",
            selection_reason="DATA_OPS_SYMBOLS_OVERRIDE provided",
            counts={"selected": len(override_symbols)},
            excluded_count_by_reason={},
        )

    rows = list(registry_rows) if registry_rows is not None else _load_registry_rows_from_postgres(settings=settings)
    selected, counts, excluded = _select_registry_symbols(rows=rows, region=normalized_region)
    if not selected:
        raise ValueError(
            "ticker_registry source refresh universe is empty after applying active/promoted/market-supported filters "
            f"(region={normalized_region})."
        )
    return SourceRefreshUniverse(
        symbols=selected,
        region=normalized_region,
        source="ticker_registry",
        selection_reason="active promoted market-supported ticker_registry rows",
        counts=counts,
        excluded_count_by_reason=dict(excluded),
    )


def resolve_symbols(
    *,
    symbols: str | Sequence[str] | None,
    region: str | None,
    settings: DataOpsSettings,
    env: Mapping[str, str] | None = None,
) -> list[str]:
    """Compatibility wrapper for callers that only need the symbol list."""

    return resolve_source_refresh_universe(
        symbols=symbols,
        region=region,
        settings=settings,
        env=env,
    ).symbols


def normalize_refresh_region(region: str | None) -> str:
    token = str(region or "all").strip().lower()
    if not token or token in {"all", "global"}:
        return "all"
    if token in CANONICAL_REFRESH_REGIONS:
        return token
    if token in {
        "usa",
        "united_states",
        "united states",
        "amer",
        "americas",
        "north_america",
        "north america",
        "latin_america",
        "latin america",
        "south_america",
        "south america",
        "nyse",
        "nasdaq",
        "nms",
        "nyq",
        "ase",
        "tor",
        "tsx",
        "xtse",
        "sao",
        "bvmf",
    }:
        return "us"
    if token in {"europe", "emea", "eur", "lis", "cph", "xetr", "etr", "lse", "ams", "epa"}:
        return "eu"
    if token in {"asia", "asia_pacific", "asia pacific", "pacific", "il", "israel", "tlv", "tase", "xtae", "hkg", "asx"}:
        return "apac"

    country = normalize_country(token)
    product_region = region_for_country(country)
    if product_region == REGION_US:
        return "us"
    if product_region == REGION_AMER:
        return "us"
    if product_region == REGION_EU:
        return "eu"
    if product_region == REGION_APAC:
        return "apac"
    return "global"


def _resolve_env_override_symbols(*, region: str, env: Mapping[str, str]) -> list[str]:
    keys: list[str] = []
    if region in REGION_SYMBOL_OVERRIDE_ENV:
        keys.append(REGION_SYMBOL_OVERRIDE_ENV[region])
    keys.append("DATA_OPS_SYMBOLS_OVERRIDE")
    for key in keys:
        symbols = parse_symbols(env.get(key))
        if symbols:
            return symbols
    return []


def _load_registry_rows_from_postgres(*, settings: DataOpsSettings, timeout_seconds: int = 30) -> list[dict[str, Any]]:
    dsn = str(settings.database_dsn or "").strip()
    if not dsn:
        raise RuntimeError(
            "DATA_OPS_DATABASE_URL is required to resolve the production source refresh universe from ticker_registry."
        )
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("psycopg[binary] is required to resolve ticker_registry universe from Postgres.") from exc

    try:
        with psycopg.connect(dsn, connect_timeout=int(timeout_seconds), row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select input_symbol, normalized_symbol, region, status, promotion_status, market_supported
                    from public.ticker_registry
                    where normalized_symbol is not null
                    """
                )
                return [dict(row) for row in cur.fetchall()]
    except Exception as exc:  # noqa: BLE001 - surface clear production failure
        raise RuntimeError("Unable to read ticker_registry source refresh universe from Postgres.") from exc


def _select_registry_symbols(
    *,
    rows: Sequence[Mapping[str, Any]],
    region: str,
) -> tuple[list[str], dict[str, int], Counter[str]]:
    selected: list[str] = []
    seen: set[str] = set()
    excluded: Counter[str] = Counter()
    count_by_region: Counter[str] = Counter()
    selected_by_region: Counter[str] = Counter()

    for row in rows:
        schedule_region = normalize_refresh_region(str(row.get("region") or ""))
        count_by_region[schedule_region] += 1
        if region not in {"all", "global"} and schedule_region != region:
            excluded["region_mismatch"] += 1
            continue
        if str(row.get("status") or "").strip().lower() != "active":
            excluded["inactive"] += 1
            continue
        promotion = str(row.get("promotion_status") or "").strip().lower()
        if promotion not in ALLOWED_PROMOTION_STATUSES:
            excluded["not_promoted"] += 1
            continue
        if not _coerce_bool(row.get("market_supported")):
            excluded["market_unsupported"] += 1
            continue
        symbol = str(row.get("normalized_symbol") or "").strip().upper()
        if not symbol or symbol in {"NAN", "NONE", "NULL"}:
            excluded["missing_normalized_symbol"] += 1
            continue
        if symbol in seen:
            excluded["duplicate_symbol"] += 1
            continue
        seen.add(symbol)
        selected.append(symbol)
        selected_by_region[schedule_region] += 1

    counts = {
        "registry_rows": len(rows),
        "selected": len(selected),
        "selected_us": int(selected_by_region["us"]),
        "selected_eu": int(selected_by_region["eu"]),
        "selected_apac": int(selected_by_region["apac"]),
        "rows_us": int(count_by_region["us"]),
        "rows_eu": int(count_by_region["eu"]),
        "rows_apac": int(count_by_region["apac"]),
        "rows_global": int(count_by_region["global"]),
    }
    return selected, counts, excluded


def _coerce_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    token = str(value).strip().lower()
    return token in {"true", "1", "yes", "y", "on"}
