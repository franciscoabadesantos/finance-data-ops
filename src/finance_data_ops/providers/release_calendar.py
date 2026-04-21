"""Economic release-calendar providers (monthly + weekly canonical builders)."""

from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as dtime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# Use the FRED host for ALFRED-mode endpoints; in this runtime environment it is
# materially more reliable than direct alfred.stlouisfed.org while preserving payload semantics.
ALFRED_BASE_URL = "https://fred.stlouisfed.org"
ALFRED_GRAPH_API_SERIES_URL = f"{ALFRED_BASE_URL}/graph/api/series/"
ALFRED_GRAPH_CSV_URL = f"{ALFRED_BASE_URL}/graph/fredgraph.csv"
FRED_RELEASE_CALENDAR_URL = "https://fred.stlouisfed.org/releases/calendar"

DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_SLEEP_SECONDS = 0.5

RELEASE_TIMEZONE = "America/New_York"
RELEASE_TIME_LOCAL = dtime(hour=8, minute=30)
WEEKLY_RELEASE_WINDOW_MAX_DAYS = 14
RELEASE_AVAILABILITY_GRACE_SECONDS = 4 * 60 * 60

ICSA_RELEASE_SOURCE = "fred_icsa_release_calendar_v2"
ICSA_LAPSE_BACKFILL_SOURCE = "dol_eta20251120_lapse_notice_backfill_v1"
ICSA_LAPSE_BACKFILL_RELEASE_DATE = date(2025, 11, 20)
ICSA_LAPSE_BACKFILL_OBSERVATIONS: tuple[date, ...] = (
    date(2025, 9, 27),
    date(2025, 10, 4),
    date(2025, 10, 11),
    date(2025, 10, 18),
    date(2025, 10, 25),
    date(2025, 11, 1),
)

_FRED_CALENDAR_DATE_PATTERN = re.compile(
    r'<span style="font-weight: bold;">([A-Za-z]+ [A-Za-z]+ \d{2}, \d{4})</span>'
)

# Explicit CPI release-timestamp corrections required for parity with legacy official
# calendar semantics on specific Jan observation periods.
_CPI_RELEASE_DATE_OVERRIDES: dict[str, str] = {
    "2020-01": "2020-02-13",
    "2021-01": "2021-02-10",
    "2022-01": "2022-02-10",
    "2023-01": "2023-02-14",
    "2024-01": "2024-02-13",
}

_CPI_RELEASE_OVERRIDE_SOURCE = "bls_cpi_manual_override_v1"

AVAILABILITY_STATUS_OBSERVED = "observed_available"
AVAILABILITY_STATUS_PROVISIONAL = "scheduled_provisional"
AVAILABILITY_STATUS_LATE = "late_missing_observation"
AVAILABILITY_STATUS_UNSUPPORTED = "schedule_only_unsupported_history"


@dataclass(frozen=True)
class AnchorGroup:
    anchor_series_id: str
    release_calendar_source: str
    series_keys: tuple[str, ...]
    generation_method: str = "monthly_first_release"
    source_label: str = "alfred_graph_revision_dates_v1"


ANCHOR_GROUPS: tuple[AnchorGroup, ...] = (
    AnchorGroup(
        anchor_series_id="CPIAUCSL",
        release_calendar_source="bls_cpi_release_calendar_v1",
        series_keys=("CPI_Headline", "CPI_Core"),
        # Reliability path: avoid high-volume vintage snapshots and derive monthly first release
        # from available revision dates + latest observation presence.
        generation_method="monthly_first_release",
        source_label="alfred_graph_revision_dates_v1",
    ),
    AnchorGroup(
        anchor_series_id="UNRATE",
        release_calendar_source="bls_unrate_release_calendar_v1",
        series_keys=("UNRATE", "U6RATE", "CIVPART"),
        generation_method="monthly_first_release",
        source_label="alfred_graph_revision_dates_v1",
    ),
)


class ReleaseCalendarProviderError(RuntimeError):
    """Raised when release-calendar provider output is invalid."""


class EconomicReleaseCalendarProvider:
    """Build canonical economic release calendar rows from external sources."""

    def build_release_calendar(
        self,
        *,
        start_date: date,
        end_date: date,
        sleep_seconds: float = 0.0,
        official_start_year: int | None = None,
        official_end_year: int | None = None,
    ) -> pd.DataFrame:
        if end_date < start_date:
            raise ValueError("end_date must be on/after start_date")

        monthly = self.build_monthly_release_calendar(
            min_observation_date=start_date,
            sleep_seconds=sleep_seconds,
            release_window_start_date=start_date,
            release_window_end_date=end_date,
        )
        weekly = self.build_weekly_release_calendar(
            min_observation_date=start_date,
            official_start_year=official_start_year or start_date.year,
            official_end_year=official_end_year or end_date.year,
        )

        out = pd.concat([monthly, weekly], ignore_index=True)
        out["observation_date"] = pd.to_datetime(out["observation_date"], errors="coerce").dt.date
        release_window_dates = pd.to_datetime(
            out.get("scheduled_release_timestamp_utc"),
            utc=True,
            errors="coerce",
        ).dt.date
        out = out[(release_window_dates >= start_date) & (release_window_dates <= end_date)].copy()
        sort_col = "scheduled_release_timestamp_utc" if "scheduled_release_timestamp_utc" in out.columns else "release_timestamp_utc"
        out = out.sort_values(["series_key", "observation_period", sort_col]).drop_duplicates(
            subset=["series_key", "observation_period"],
            keep="last",
        )
        out = out.reset_index(drop=True)
        return out

    def build_monthly_release_calendar(
        self,
        *,
        min_observation_date: date,
        sleep_seconds: float = 0.0,
        release_window_start_date: date | None = None,
        release_window_end_date: date | None = None,
    ) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        min_obs_iso = min_observation_date.isoformat()
        now_utc = pd.Timestamp(datetime.now(UTC))

        for group in ANCHOR_GROUPS:
            if group.generation_method == "observation_first_appearance":
                observed_release_by_obs = _derive_first_release_mapping_from_first_appearance(
                    anchor_series_id=group.anchor_series_id,
                    min_observation_date=min_obs_iso,
                    sleep_seconds=sleep_seconds,
                )
                schedule_release_by_obs = dict(observed_release_by_obs)
            else:
                present_observation_dates = _load_latest_observation_presence(group.anchor_series_id)
                schedule_release_by_obs, observed_release_by_obs = _derive_monthly_schedule_and_observed(
                    anchor_series_id=group.anchor_series_id,
                    min_observation_date=min_obs_iso,
                    sleep_seconds=sleep_seconds,
                    present_observation_dates=present_observation_dates,
                    release_window_start_date=(
                        release_window_start_date.isoformat()
                        if release_window_start_date is not None
                        else None
                    ),
                    release_window_end_date=(
                        release_window_end_date.isoformat()
                        if release_window_end_date is not None
                        else None
                    ),
                )

            for observation_date, scheduled_release_date_local in sorted(schedule_release_by_obs.items()):
                observation_period = observation_date[:7]
                source_label = group.source_label
                observed_release_date_local = observed_release_by_obs.get(observation_date)
                if group.anchor_series_id == "CPIAUCSL":
                    override_release_date = _CPI_RELEASE_DATE_OVERRIDES.get(observation_period)
                    if override_release_date:
                        scheduled_release_date_local = override_release_date
                        if observed_release_date_local is not None:
                            observed_release_date_local = override_release_date
                        source_label = _CPI_RELEASE_OVERRIDE_SOURCE
                scheduled_release_timestamp_utc = _release_timestamp_utc(date.fromisoformat(scheduled_release_date_local))
                observed_first_available_at_utc = (
                    _release_timestamp_utc(date.fromisoformat(observed_release_date_local))
                    if observed_release_date_local
                    else None
                )
                availability_source = (
                    source_label
                    if observed_first_available_at_utc is not None
                    else f"{group.release_calendar_source}_scheduled_calendar_v1"
                )
                availability = _derive_availability_fields(
                    scheduled_release_timestamp_utc=scheduled_release_timestamp_utc,
                    observed_first_available_at_utc=observed_first_available_at_utc,
                    availability_source=availability_source,
                    now_utc=now_utc,
                    authoritative_supported=True,
                )
                provenance_class = _classify_provenance(source_label)
                for series_key in group.series_keys:
                    rows.append(
                        {
                            "series_key": series_key,
                            "observation_period": observation_period,
                            "observation_date": observation_date,
                            # Backward-compatible alias retained for existing readers.
                            "release_timestamp_utc": scheduled_release_timestamp_utc,
                            "scheduled_release_timestamp_utc": scheduled_release_timestamp_utc,
                            "observed_first_available_at_utc": observed_first_available_at_utc,
                            "availability_status": availability["availability_status"],
                            "availability_source": availability["availability_source"],
                            "delay_vs_schedule_seconds": availability["delay_vs_schedule_seconds"],
                            "is_schedule_based_only": availability["is_schedule_based_only"],
                            "release_timezone": RELEASE_TIMEZONE,
                            "release_date_local": scheduled_release_date_local,
                            "release_calendar_source": group.release_calendar_source,
                            "source": source_label,
                            "provenance_class": provenance_class,
                            "ingested_at": datetime.now(UTC).isoformat(),
                        }
                    )

        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(
                columns=[
                    "series_key",
                    "observation_period",
                    "observation_date",
                    "release_timestamp_utc",
                    "scheduled_release_timestamp_utc",
                    "observed_first_available_at_utc",
                    "availability_status",
                    "availability_source",
                    "delay_vs_schedule_seconds",
                    "is_schedule_based_only",
                    "release_timezone",
                    "release_date_local",
                    "release_calendar_source",
                    "source",
                    "provenance_class",
                    "ingested_at",
                ]
            )
        out = out.sort_values(["series_key", "observation_period", "scheduled_release_timestamp_utc"]).drop_duplicates(
            subset=["series_key", "observation_period"],
            keep="last",
        )
        return out.reset_index(drop=True)

    def build_weekly_release_calendar(
        self,
        *,
        min_observation_date: date,
        official_start_year: int,
        official_end_year: int,
    ) -> pd.DataFrame:
        observation_dates = _load_icsa_observation_weeks_from_fred(min_observation_date=min_observation_date)
        official_release_dates = _load_fred_release_calendar_dates(
            start_year=int(official_start_year),
            end_year=int(official_end_year),
        )
        official_obs_dates = {
            _previous_saturday(release_date)
            for release_date in official_release_dates
            if _previous_saturday(release_date) >= min_observation_date
        }
        observation_dates = sorted(set(observation_dates).union(official_obs_dates))
        latest_obs_raw: set[str] = set()
        # Union with current ICSA observation presence so frontier weeks still get
        # official release timestamps even if the latest full-history snapshot lags.
        try:
            latest_obs_raw = _load_latest_observation_presence("ICSA")
            latest_obs_dates = [
                date.fromisoformat(token)
                for token in sorted(latest_obs_raw)
                if token and date.fromisoformat(token) >= min_observation_date
            ]
            observation_dates = sorted(set(observation_dates).union(set(latest_obs_dates)))
        except Exception:
            # Keep the FRED-history + official-calendar baseline when latest presence
            # probing is transiently unavailable.
            observation_dates = sorted(set(observation_dates))
        available_revision_dates = _load_icsa_available_revision_dates()

        if not available_revision_dates:
            raise ReleaseCalendarProviderError("No ICSA ALFRED revision dates loaded.")

        first_vendor_supported_obs = _previous_saturday(min(available_revision_dates))
        release_by_observation: dict[date, tuple[date, str]] = {}
        observed_by_observation: dict[date, tuple[date, str]] = {}

        for obs in observation_dates:
            if obs < first_vendor_supported_obs:
                release_by_observation[obs] = (_next_thursday(obs), "inferred_pre_alfred_revision_window_v1")

        vendor_dates = sorted(available_revision_dates)
        vendor_idx = 0
        for obs in observation_dates:
            if obs < first_vendor_supported_obs:
                continue
            while vendor_idx < len(vendor_dates) and _previous_saturday(vendor_dates[vendor_idx]) < obs:
                vendor_idx += 1
            if vendor_idx >= len(vendor_dates):
                break
            candidate_release = vendor_dates[vendor_idx]
            lag_days = (candidate_release - obs).days
            if lag_days > WEEKLY_RELEASE_WINDOW_MAX_DAYS:
                continue
            release_by_observation[obs] = (candidate_release, "alfred_available_revision_dates_v1")
            if obs.isoformat() in latest_obs_raw:
                observed_by_observation[obs] = (candidate_release, "alfred_available_revision_dates_v1")

        for official_release in sorted(official_release_dates):
            obs = _previous_saturday(official_release)
            if obs in observation_dates:
                release_by_observation[obs] = (official_release, "fred_release_calendar_rid180_v1")

        for obs in ICSA_LAPSE_BACKFILL_OBSERVATIONS:
            if obs not in observation_dates:
                continue
            release_by_observation[obs] = (ICSA_LAPSE_BACKFILL_RELEASE_DATE, ICSA_LAPSE_BACKFILL_SOURCE)
            observed_by_observation[obs] = (ICSA_LAPSE_BACKFILL_RELEASE_DATE, ICSA_LAPSE_BACKFILL_SOURCE)

        rows: list[dict[str, object]] = []
        now_utc = pd.Timestamp(datetime.now(UTC))
        for obs in sorted(release_by_observation.keys()):
            release_date_local, source = release_by_observation[obs]
            scheduled_release_timestamp_utc = _release_timestamp_utc(release_date_local)
            observed_tuple = observed_by_observation.get(obs)
            observed_first_available_at_utc = (
                _release_timestamp_utc(observed_tuple[0]) if observed_tuple is not None else None
            )
            authoritative_supported = obs >= first_vendor_supported_obs or source == ICSA_LAPSE_BACKFILL_SOURCE
            availability = _derive_availability_fields(
                scheduled_release_timestamp_utc=scheduled_release_timestamp_utc,
                observed_first_available_at_utc=observed_first_available_at_utc,
                availability_source=(
                    observed_tuple[1]
                    if observed_tuple is not None
                    else f"{ICSA_RELEASE_SOURCE}_scheduled_calendar_v1"
                ),
                now_utc=now_utc,
                authoritative_supported=authoritative_supported,
            )
            rows.append(
                {
                    "series_key": "ICSA",
                    "observation_period": obs.isoformat(),
                    "observation_date": obs.isoformat(),
                    # Backward-compatible alias retained for existing readers.
                    "release_timestamp_utc": scheduled_release_timestamp_utc,
                    "scheduled_release_timestamp_utc": scheduled_release_timestamp_utc,
                    "observed_first_available_at_utc": observed_first_available_at_utc,
                    "availability_status": availability["availability_status"],
                    "availability_source": availability["availability_source"],
                    "delay_vs_schedule_seconds": availability["delay_vs_schedule_seconds"],
                    "is_schedule_based_only": availability["is_schedule_based_only"],
                    "release_timezone": RELEASE_TIMEZONE,
                    "release_date_local": release_date_local.isoformat(),
                    "release_calendar_source": ICSA_RELEASE_SOURCE,
                    "source": source,
                    "provenance_class": _classify_provenance(source),
                    "ingested_at": datetime.now(UTC).isoformat(),
                }
            )

        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(
                columns=[
                    "series_key",
                    "observation_period",
                    "observation_date",
                    "release_timestamp_utc",
                    "scheduled_release_timestamp_utc",
                    "observed_first_available_at_utc",
                    "availability_status",
                    "availability_source",
                    "delay_vs_schedule_seconds",
                    "is_schedule_based_only",
                    "release_timezone",
                    "release_date_local",
                    "release_calendar_source",
                    "source",
                    "provenance_class",
                    "ingested_at",
                ]
            )
        out = out.sort_values(["series_key", "observation_period", "scheduled_release_timestamp_utc"]).drop_duplicates(
            subset=["series_key", "observation_period"],
            keep="last",
        )
        return out.reset_index(drop=True)


def _fetch_text(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_sleep_seconds: float = DEFAULT_RETRY_SLEEP_SECONDS,
) -> str:
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "finance-data-ops/0.1 (+https://github.com/franciscoabadesantos/finance-data-ops)"},
            )
            response.raise_for_status()
            return str(response.text or "")
        except Exception as exc:
            last_error = exc
            if attempt >= max_retries:
                break
            time.sleep(retry_sleep_seconds * float(2 ** (attempt - 1)))

    if last_error is not None:
        raise last_error
    raise RuntimeError("unreachable")


def _fetch_json(url: str, *, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, DEFAULT_MAX_RETRIES + 1):
        try:
            raw = _fetch_text(url, timeout=timeout)
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            last_error = exc
            if attempt >= DEFAULT_MAX_RETRIES:
                break
            time.sleep(DEFAULT_RETRY_SLEEP_SECONDS * float(2 ** (attempt - 1)))
    if last_error is not None:
        raise last_error
    raise RuntimeError("unreachable")


def _parse_date(token: str, fmt: str) -> date | None:
    text = str(token).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, fmt).date()
    except ValueError:
        return None


def _release_timestamp_utc(release_date_local: date) -> str:
    tz = ZoneInfo(RELEASE_TIMEZONE)
    release_dt_local = datetime.combine(release_date_local, RELEASE_TIME_LOCAL, tzinfo=tz)
    return release_dt_local.astimezone(ZoneInfo("UTC")).isoformat()


def _load_available_revision_dates(series_id: str) -> list[str]:
    params = {
        "id": series_id,
        "mode": "alfred",
        "firstRequest": "seriesPage",
        "width": "1200",
    }
    payload = _fetch_json(f"{ALFRED_GRAPH_API_SERIES_URL}?{urlencode(params)}")
    chart_series = payload.get("chart_series") or []
    if not chart_series:
        raise ReleaseCalendarProviderError(f"No chart_series payload for {series_id}.")
    series_obj = ((chart_series[0].get("series_objects") or {}).get("a") or {})
    release_dates = series_obj.get("available_revision_dates") or []
    if not release_dates:
        raise ReleaseCalendarProviderError(f"No available_revision_dates for {series_id}.")
    return sorted({str(v).strip() for v in release_dates if str(v).strip()})


def _load_vintage_snapshot(series_id: str, vintage_date: str) -> dict[str, str]:
    raw = _fetch_text(f"{ALFRED_GRAPH_CSV_URL}?id={series_id}&vintage_date={vintage_date}")
    reader = csv.DictReader(raw.splitlines())
    fieldnames = list(reader.fieldnames or [])
    date_col = "observation_date" if "observation_date" in fieldnames else ("DATE" if "DATE" in fieldnames else None)
    if date_col is None:
        date_col = fieldnames[0] if fieldnames else None
    value_col = f"{series_id}_{vintage_date.replace('-', '')}"
    if value_col not in fieldnames:
        if series_id in fieldnames:
            value_col = series_id
        else:
            fallback_cols = [name for name in fieldnames if name != date_col]
            value_col = fallback_cols[0] if fallback_cols else value_col
    out: dict[str, str] = {}
    for row in reader:
        if not isinstance(row, dict):
            continue
        obs = str(row.get(str(date_col), "")).strip() if date_col is not None else ""
        if not obs:
            continue
        out[obs] = str(row.get(value_col, "")).strip()
    return out


def _load_latest_observation_presence(series_id: str) -> set[str]:
    raw = _fetch_text(f"{ALFRED_GRAPH_CSV_URL}?id={series_id}")
    reader = csv.DictReader(raw.splitlines())
    fieldnames = list(reader.fieldnames or [])
    date_col = "observation_date" if "observation_date" in fieldnames else ("DATE" if "DATE" in fieldnames else None)
    if date_col is None:
        date_col = fieldnames[0] if fieldnames else None

    value_col: str | None = None
    if series_id in fieldnames:
        value_col = series_id
    else:
        prefixed = sorted([name for name in fieldnames if str(name).startswith(f"{series_id}_")])
        if prefixed:
            value_col = prefixed[-1]
    if value_col is None:
        fallback_cols = [name for name in fieldnames if name != "observation_date"]
        value_col = fallback_cols[0] if fallback_cols else None
    if value_col is None:
        raise ReleaseCalendarProviderError(f"Could not resolve value column for latest snapshot {series_id}.")

    present: set[str] = set()
    for row in reader:
        if not isinstance(row, dict):
            continue
        obs = str(row.get(str(date_col), "")).strip() if date_col is not None else ""
        if not obs:
            continue
        val = str(row.get(value_col, "")).strip()
        if not val or val == ".":
            continue
        present.add(obs)
    return present


def _add_month(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year + 1, 1, 1)
    return date(month_start.year, month_start.month + 1, 1)


def _derive_monthly_schedule_and_observed(
    *,
    anchor_series_id: str,
    min_observation_date: str,
    sleep_seconds: float,
    present_observation_dates: set[str],
    release_window_start_date: str | None = None,
    release_window_end_date: str | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    revision_dates = _load_available_revision_dates(anchor_series_id)

    monthly_first_release: dict[str, str] = {}
    for release_date in revision_dates:
        release_month = release_date[:7]
        current = monthly_first_release.get(release_month)
        if current is None or release_date < current:
            monthly_first_release[release_month] = release_date

    release_dates = [monthly_first_release[k] for k in sorted(monthly_first_release.keys())]
    release_window_start = (
        date.fromisoformat(release_window_start_date)
        if release_window_start_date
        else None
    )
    release_window_end = (
        date.fromisoformat(release_window_end_date)
        if release_window_end_date
        else None
    )
    if release_window_start is not None:
        release_dates = [d for d in release_dates if date.fromisoformat(d) >= release_window_start]
    else:
        release_dates = [d for d in release_dates if d >= min_observation_date]
    if release_window_end is not None:
        release_dates = [d for d in release_dates if date.fromisoformat(d) <= release_window_end]

    schedule_by_observation: dict[str, str] = {}
    observed_by_observation: dict[str, str] = {}
    min_obs_date = date.fromisoformat(min_observation_date)
    latest_seen_obs_month: date | None = None

    for release_date in release_dates:
        release_month_start = date.fromisoformat(f"{release_date[:7]}-01")
        max_obs_month_for_release = date(
            (release_month_start.year - 1) if release_month_start.month == 1 else release_month_start.year,
            12 if release_month_start.month == 1 else release_month_start.month - 1,
            1,
        )
        if release_window_start is None and max_obs_month_for_release < min_obs_date:
            continue

        first_obs_month = max_obs_month_for_release if latest_seen_obs_month is None else _add_month(latest_seen_obs_month)
        current = first_obs_month
        while current <= max_obs_month_for_release:
            obs_iso = current.isoformat()
            if release_window_start is not None or current >= min_obs_date:
                schedule_by_observation[obs_iso] = release_date
                if obs_iso in present_observation_dates:
                    observed_by_observation[obs_iso] = release_date
            current = _add_month(current)

        if max_obs_month_for_release >= first_obs_month:
            latest_seen_obs_month = max_obs_month_for_release

        if sleep_seconds > 0:
            time.sleep(float(sleep_seconds))

    return schedule_by_observation, observed_by_observation


def _derive_first_release_mapping_from_first_appearance(
    *,
    anchor_series_id: str,
    min_observation_date: str,
    sleep_seconds: float,
) -> dict[str, str]:
    revision_dates = _load_available_revision_dates(anchor_series_id)
    revision_dates = [d for d in revision_dates if d >= min_observation_date]
    min_obs = date.fromisoformat(min_observation_date)

    first_release_by_observation: dict[str, str] = {}
    for vintage_date in revision_dates:
        snapshot = _load_vintage_snapshot(anchor_series_id, vintage_date)
        for observation_date, raw_value in snapshot.items():
            if not raw_value or raw_value == ".":
                continue
            obs_date = date.fromisoformat(observation_date)
            if obs_date < min_obs:
                continue
            first_release_by_observation.setdefault(observation_date, vintage_date)
        if sleep_seconds > 0:
            time.sleep(float(sleep_seconds))

    return first_release_by_observation


def _load_icsa_available_revision_dates() -> list[date]:
    params = {
        "id": "ICSA",
        "mode": "alfred",
        "firstRequest": "seriesPage",
        "width": "1200",
    }
    payload = _fetch_json(f"{ALFRED_GRAPH_API_SERIES_URL}?{urlencode(params)}")
    chart_series = payload.get("chart_series") or []
    if not chart_series:
        raise ReleaseCalendarProviderError("No chart_series payload for ICSA.")
    series_obj = ((chart_series[0].get("series_objects") or {}).get("a") or {})
    revision_dates = series_obj.get("available_revision_dates") or []
    out: list[date] = []
    for raw in revision_dates:
        parsed = _parse_date(str(raw), "%Y-%m-%d")
        if parsed is not None:
            out.append(parsed)
    if not out:
        raise ReleaseCalendarProviderError("No available_revision_dates for ICSA.")
    return sorted(set(out))


def _load_icsa_observation_weeks_from_fred(*, min_observation_date: date) -> list[date]:
    raw = _fetch_text(f"{ALFRED_GRAPH_CSV_URL}?id=ICSA")
    reader = csv.DictReader(raw.splitlines())
    fieldnames = list(reader.fieldnames or [])
    date_col = "observation_date" if "observation_date" in fieldnames else ("DATE" if "DATE" in fieldnames else None)
    if date_col is None:
        date_col = fieldnames[0] if fieldnames else None
    if date_col is None:
        raise ReleaseCalendarProviderError("Could not resolve DATE column for FRED ICSA history.")

    value_col: str | None = None
    if "ICSA" in fieldnames:
        value_col = "ICSA"
    else:
        prefixed = sorted([name for name in fieldnames if str(name).startswith("ICSA_")])
        if prefixed:
            value_col = prefixed[-1]
    if value_col is None:
        fallback_cols = [name for name in fieldnames if name != date_col]
        value_col = fallback_cols[0] if fallback_cols else None
    if value_col is None:
        raise ReleaseCalendarProviderError("Could not resolve value column for FRED ICSA history.")

    obs: set[date] = set()
    for row in reader:
        if not isinstance(row, dict):
            continue
        obs_raw = str(row.get(str(date_col), "")).strip()
        if not obs_raw:
            continue
        value_raw = str(row.get(value_col, "")).strip()
        if not value_raw or value_raw == ".":
            continue
        parsed = _parse_date(obs_raw, "%Y-%m-%d")
        if parsed is None or parsed < min_observation_date:
            continue
        obs.add(parsed)
    return sorted(obs)


def _load_fred_release_calendar_dates(*, start_year: int, end_year: int) -> set[date]:
    out: set[date] = set()
    for year in range(int(start_year), int(end_year) + 1):
        url = (
            f"{FRED_RELEASE_CALENDAR_URL}"
            f"?ob=rd&od=&y={year}&m=&rid=180&view=year&vs={year}-01-01&ve={year}-12-31"
        )
        html = _fetch_text(url, timeout=30, max_retries=4)
        for token in _FRED_CALENDAR_DATE_PATTERN.findall(html):
            parsed = _parse_date(token, "%A %B %d, %Y")
            if parsed is not None:
                out.add(parsed)
    return out


def _previous_saturday(d: date) -> date:
    delta = (d.weekday() - 5) % 7
    return d - timedelta(days=delta)


def _next_thursday(d: date) -> date:
    delta = (3 - d.weekday()) % 7
    return d + timedelta(days=delta)


def _classify_provenance(source: str) -> str:
    token = str(source or "").strip().lower()
    if token.startswith("alfred"):
        return "ALFRED-native"
    if token.startswith("fred_release_calendar") or "official" in token:
        return "official release calendar"
    if "inferred" in token:
        return "inferred"
    if "manual" in token or "fixture" in token or "lapse" in token:
        return "fixture/manual exception"
    if "missing" in token or "canceled" in token or "cancelled" in token:
        return "canceled/missing"
    return "unknown"


def _derive_availability_fields(
    *,
    scheduled_release_timestamp_utc: str,
    observed_first_available_at_utc: str | None,
    availability_source: str,
    now_utc: pd.Timestamp,
    authoritative_supported: bool,
) -> dict[str, object]:
    scheduled_ts = pd.to_datetime(scheduled_release_timestamp_utc, utc=True, errors="coerce")
    observed_ts = pd.to_datetime(observed_first_available_at_utc, utc=True, errors="coerce")
    if pd.isna(scheduled_ts):
        raise ReleaseCalendarProviderError(
            f"Invalid scheduled_release_timestamp_utc={scheduled_release_timestamp_utc!r}."
        )

    if pd.notna(observed_ts):
        delay_seconds = int((observed_ts - scheduled_ts).total_seconds())
        return {
            "availability_status": AVAILABILITY_STATUS_OBSERVED,
            "availability_source": str(availability_source or "").strip() or "unknown",
            "delay_vs_schedule_seconds": delay_seconds,
            "is_schedule_based_only": False,
        }

    if not authoritative_supported:
        return {
            "availability_status": AVAILABILITY_STATUS_UNSUPPORTED,
            "availability_source": "historical_schedule_inference_v1",
            "delay_vs_schedule_seconds": None,
            "is_schedule_based_only": True,
        }

    late_threshold = scheduled_ts + pd.Timedelta(seconds=RELEASE_AVAILABILITY_GRACE_SECONDS)
    is_late = pd.Timestamp(now_utc).tz_convert("UTC") > late_threshold
    return {
        "availability_status": AVAILABILITY_STATUS_LATE if is_late else AVAILABILITY_STATUS_PROVISIONAL,
        "availability_source": str(availability_source or "").strip() or "scheduled_calendar",
        "delay_vs_schedule_seconds": None,
        "is_schedule_based_only": True,
    }
