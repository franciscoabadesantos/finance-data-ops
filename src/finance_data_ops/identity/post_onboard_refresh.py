"""Post-onboarding Entity Layer refresh orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Callable

from finance_data_ops.identity.publisher import publish_entity_identity_controlled
from finance_data_ops.publish.client import PostgresPublisher, RecordingPublisher
from finance_data_ops.settings import DataOpsSettings, load_settings
from scripts.measure_entity_identity_chain import build_entity_identity_measurement


DEFAULT_POST_ONBOARD_ENTITY_SCOPE_KEY = "tracked"


@dataclass(frozen=True, slots=True)
class PostOnboardEntityIdentityRefreshOptions:
    source: str = "postgres"
    scope_key: str = DEFAULT_POST_ONBOARD_ENTITY_SCOPE_KEY
    batch_id: str | None = None
    apply_caches: bool = False
    apply_entities: bool = False
    refresh_live: bool = False
    refresh_cache_misses: bool = False
    cache_root: str | None = None
    curated_identity_file: str | None = None
    batch_size: int | None = None
    request_sleep_seconds: float = 6.5
    gleif_page_size: int = 200
    gleif_request_sleep_seconds: float = 0.5
    gleif_lei_isin_max_retries: int = 3
    gleif_retry_backoff_seconds: float = 1.0
    gleif_retry_jitter_seconds: float = 0.25


def build_post_onboard_entity_refresh_batch_id(now: datetime | None = None) -> str:
    resolved_now = now or datetime.now(UTC)
    if resolved_now.tzinfo is None:
        resolved_now = resolved_now.replace(tzinfo=UTC)
    return f"tracked-entity-refresh-{resolved_now.astimezone(UTC).strftime('%Y%m%d-%H%M%S')}"


def run_post_onboard_entity_identity_refresh(
    *,
    options: PostOnboardEntityIdentityRefreshOptions,
    settings: DataOpsSettings | None = None,
    measurement_builder: Callable[..., Any] = build_entity_identity_measurement,
    publisher_factory: Callable[[bool, DataOpsSettings], Any] | None = None,
    previous_batch_fetcher: Callable[..., str | None] | None = None,
) -> dict[str, Any]:
    """Run the current tracked-universe Entity Layer refresh.

    The command is cache-first and dry-run by default. For Postgres sources it
    always targets `feature_store.ticker_readiness.is_tracked = true`.
    """

    if options.refresh_cache_misses and not options.refresh_live:
        raise ValueError("refresh_cache_misses requires refresh_live.")
    settings = settings or load_settings(cache_root=options.cache_root)
    batch_id = options.batch_id or build_post_onboard_entity_refresh_batch_id()
    measurement_args = _measurement_args_from_options(options)
    measurement = measurement_builder(args=measurement_args, settings=settings)
    writes_enabled = bool(options.apply_caches or options.apply_entities)
    publisher_factory = publisher_factory or _default_publisher_factory
    publisher = publisher_factory(writes_enabled, settings)
    previous_batch_id = None
    if previous_batch_fetcher is not None:
        previous_batch_id = previous_batch_fetcher(
            database_dsn=settings.database_dsn,
            scope_key=options.scope_key,
        )
    elif options.source == "postgres" and settings.database_dsn:
        previous_batch_id = fetch_current_entity_publication_batch_id(
            database_dsn=settings.database_dsn,
            scope_key=options.scope_key,
        )

    publish_result = publish_entity_identity_controlled(
        publisher=publisher,
        measurement=measurement,
        apply_caches=bool(options.apply_caches),
        apply_entities=bool(options.apply_entities),
        batch_id=batch_id,
        scope_key=options.scope_key,
    )
    summary = build_post_onboard_entity_refresh_summary(
        options=options,
        measurement=measurement,
        publish_result=publish_result,
        batch_id=batch_id,
        previous_batch_id=previous_batch_id,
    )
    return {
        "status": publish_result.get("status"),
        "mode": publish_result.get("mode"),
        "scope_key": options.scope_key,
        "batch_id": batch_id,
        "summary": summary,
        "publication_gate": publish_result.get("publication_gate") or {},
        "publication_blockers": publish_result.get("publication_blockers") or [],
        "publish_result": publish_result,
    }


def build_post_onboard_entity_refresh_summary(
    *,
    options: PostOnboardEntityIdentityRefreshOptions,
    measurement: Any,
    publish_result: dict[str, Any],
    batch_id: str,
    previous_batch_id: str | None,
) -> dict[str, Any]:
    planned_counts = dict(publish_result.get("planned_counts") or {})
    verification = dict(publish_result.get("verification_summary") or {})
    gate = dict(publish_result.get("publication_gate") or {})
    measurement_summary = dict(getattr(measurement, "summary", {}) or {})
    pointer_advanced = bool(options.apply_entities and publish_result.get("status") == "published_side_by_side")
    return {
        "scope_key": options.scope_key,
        "batch_id": batch_id,
        "previous_batch_id": previous_batch_id,
        "pointer_advanced": pointer_advanced,
        "current_batch_id": batch_id if pointer_advanced else previous_batch_id,
        "tracked_count": int(measurement_summary.get("tracked_candidate_count") or measurement_summary.get("candidate_count") or 0),
        "entity_master_planned_count": int(planned_counts.get("feature_store.entity_master") or 0),
        "entity_listing_planned_count": int(planned_counts.get("feature_store.entity_listing") or 0),
        "entity_review_planned_count": int(planned_counts.get("feature_store.entity_identity_review") or 0),
        "resolved_count": int((verification.get("listing_rows_by_lifecycle_state") or {}).get("resolved") or 0),
        "provisional_count": int((verification.get("listing_rows_by_lifecycle_state") or {}).get("provisional") or 0),
        "review_required_count": int(gate.get("review_required_count") or verification.get("review_required_count") or 0),
        "unresolved_multi_listing_entities_count": int(
            verification.get("unresolved_multi_listing_entities_count")
            or measurement_summary.get("unresolved_multi_listing_entities_count")
            or 0
        ),
        "publication_gate_status": str(gate.get("status") or ""),
        "cache_refresh": {
            "refresh_live": bool(options.refresh_live),
            "refresh_cache_misses": bool(options.refresh_cache_misses),
            "raw_cache_enabled": bool(measurement_summary.get("raw_cache_enabled", options.source == "postgres")),
            "raw_cache_refresh_misses_enabled": bool(measurement_summary.get("raw_cache_refresh_misses_enabled", False)),
            "openfigi_cache_miss_count": int(measurement_summary.get("openfigi_cache_miss_count") or 0),
            "listing_isin_cache_miss_count": int(measurement_summary.get("listing_isin_cache_miss_count") or 0),
            "legal_name_cache_miss_count": int(measurement_summary.get("legal_name_cache_miss_count") or 0),
            "gleif_isin_lei_cache_miss_count": int(measurement_summary.get("gleif_isin_lei_cache_miss_count") or 0),
            "gleif_lei_isin_cache_miss_count": int(measurement_summary.get("gleif_lei_isin_cache_miss_count") or 0),
        },
    }


def fetch_current_entity_publication_batch_id(*, database_dsn: str | None, scope_key: str) -> str | None:
    if not database_dsn:
        return None
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment environment coverage
        raise RuntimeError("psycopg[binary] is required for Postgres current-pointer lookup.") from exc
    with psycopg.connect(database_dsn, autocommit=True, application_name="finance-data-ops-entity-refresh") as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select batch_id
                from feature_store.entity_identity_publication_current
                where scope_key = %s
                """,
                (scope_key,),
            )
            row = cur.fetchone()
    return str(row[0]) if row else None


def _measurement_args_from_options(options: PostOnboardEntityIdentityRefreshOptions) -> SimpleNamespace:
    return SimpleNamespace(
        symbols=[],
        source=options.source,
        offline=not bool(options.refresh_live),
        use_raw_cache=options.source == "postgres",
        refresh_cache_misses=bool(options.refresh_cache_misses),
        tracked_only=options.source == "postgres",
        apply_cache=False,
        cache_root=options.cache_root,
        curated_identity_file=options.curated_identity_file,
        batch_size=options.batch_size,
        request_sleep_seconds=options.request_sleep_seconds,
        gleif_page_size=options.gleif_page_size,
        gleif_request_sleep_seconds=options.gleif_request_sleep_seconds,
        gleif_lei_isin_max_retries=options.gleif_lei_isin_max_retries,
        gleif_retry_backoff_seconds=options.gleif_retry_backoff_seconds,
        gleif_retry_jitter_seconds=options.gleif_retry_jitter_seconds,
    )


def _default_publisher_factory(writes_enabled: bool, settings: DataOpsSettings):
    if not writes_enabled:
        return RecordingPublisher()
    settings.require_database()
    return PostgresPublisher(database_dsn=settings.database_dsn)
