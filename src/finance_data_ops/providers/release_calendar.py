"""Economic release-calendar providers (monthly + weekly canonical builders)."""

from __future__ import annotations

import csv
import json
import re
import socket
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as dtime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd

ALFRED_BASE_URL = "https://alfred.stlouisfed.org"
ALFRED_GRAPH_API_SERIES_URL = f"{ALFRED_BASE_URL}/graph/api/series/"
ALFRED_GRAPH_CSV_URL = f"{ALFRED_BASE_URL}/graph/alfredgraph.csv"
DOL_AR539_URL = "https://oui.doleta.gov/unemploy/csv/ar539.csv"
FRED_RELEASE_CALENDAR_URL = "https://fred.stlouisfed.org/releases/calendar"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

DEFAULT_TIMEOUT_SECONDS = 90
DEFAULT_MAX_RETRIES = 6
DEFAULT_RETRY_SLEEP_SECONDS = 0.75

RELEASE_TIMEZONE = "America/New_York"
RELEASE_TIME_LOCAL = dtime(hour=8, minute=30)
WEEKLY_RELEASE_WINDOW_MAX_DAYS = 14

ICSA_RELEASE_SOURCE = "dol_icsa_release_calendar_v1"
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
        generation_method="observation_first_appearance",
        source_label="alfred_observation_first_appearance_v1",
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
        )
        weekly = self.build_weekly_release_calendar(
            min_observation_date=start_date,
            official_start_year=official_start_year or start_date.year,
            official_end_year=official_end_year or end_date.year,
        )

        out = pd.concat([monthly, weekly], ignore_index=True)
        out["observation_date"] = pd.to_datetime(out["observation_date"], errors="coerce").dt.date
        out = out[(out["observation_date"] >= start_date) & (out["observation_date"] <= end_date)].copy()
        out = out.sort_values(["series_key", "observation_period", "release_timestamp_utc"]).drop_duplicates(
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
    ) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        min_obs_iso = min_observation_date.isoformat()

        for group in ANCHOR_GROUPS:
            if group.generation_method == "observation_first_appearance":
                obs_to_release = _derive_first_release_mapping_from_first_appearance(
                    anchor_series_id=group.anchor_series_id,
                    min_observation_date=min_obs_iso,
                    sleep_seconds=sleep_seconds,
                )
            else:
                present_observation_dates = _load_latest_observation_presence(group.anchor_series_id)
                obs_to_release = _derive_first_release_mapping(
                    anchor_series_id=group.anchor_series_id,
                    min_observation_date=min_obs_iso,
                    sleep_seconds=sleep_seconds,
                    present_observation_dates=present_observation_dates,
                )

            for observation_date, release_date_local in sorted(obs_to_release.items()):
                observation_period = observation_date[:7]
                release_timestamp_utc = _release_timestamp_utc(date.fromisoformat(release_date_local))
                provenance_class = _classify_provenance(group.source_label)
                for series_key in group.series_keys:
                    rows.append(
                        {
                            "series_key": series_key,
                            "observation_period": observation_period,
                            "observation_date": observation_date,
                            "release_timestamp_utc": release_timestamp_utc,
                            "release_timezone": RELEASE_TIMEZONE,
                            "release_date_local": release_date_local,
                            "release_calendar_source": group.release_calendar_source,
                            "source": group.source_label,
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
                    "release_timezone",
                    "release_date_local",
                    "release_calendar_source",
                    "source",
                    "provenance_class",
                    "ingested_at",
                ]
            )
        out = out.sort_values(["series_key", "observation_period", "release_timestamp_utc"]).drop_duplicates(
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
        observation_dates = _load_icsa_observation_weeks_from_dol(min_observation_date=min_observation_date)
        available_revision_dates = _load_icsa_available_revision_dates()
        official_release_dates = _load_fred_release_calendar_dates(
            start_year=int(official_start_year),
            end_year=int(official_end_year),
        )

        if not available_revision_dates:
            raise ReleaseCalendarProviderError("No ICSA ALFRED revision dates loaded.")

        first_vendor_supported_obs = _previous_saturday(min(available_revision_dates))
        release_by_observation: dict[date, tuple[date, str]] = {}

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

        for official_release in sorted(official_release_dates):
            obs = _previous_saturday(official_release)
            if obs in release_by_observation:
                release_by_observation[obs] = (official_release, "fred_release_calendar_rid180_v1")

        for obs in ICSA_LAPSE_BACKFILL_OBSERVATIONS:
            if obs not in observation_dates:
                continue
            release_by_observation[obs] = (ICSA_LAPSE_BACKFILL_RELEASE_DATE, ICSA_LAPSE_BACKFILL_SOURCE)

        rows: list[dict[str, object]] = []
        for obs in sorted(release_by_observation.keys()):
            release_date_local, source = release_by_observation[obs]
            rows.append(
                {
                    "series_key": "ICSA",
                    "observation_period": obs.isoformat(),
                    "observation_date": obs.isoformat(),
                    "release_timestamp_utc": _release_timestamp_utc(release_date_local),
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
                    "release_timezone",
                    "release_date_local",
                    "release_calendar_source",
                    "source",
                    "provenance_class",
                    "ingested_at",
                ]
            )
        out = out.sort_values(["series_key", "observation_period", "release_timestamp_utc"]).drop_duplicates(
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
    req = Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, max_retries + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except HTTPError:
            raise
        except (TimeoutError, socket.timeout, URLError, OSError):
            if attempt >= max_retries:
                raise
            time.sleep(retry_sleep_seconds * float(2 ** (attempt - 1)))
    raise RuntimeError("unreachable")


def _fetch_json(url: str, *, timeout: int = DEFAULT_TIMEOUT_SECONDS) -> dict:
    raw = _fetch_text(url, timeout=timeout)
    return json.loads(raw)


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
    value_col = f"{series_id}_{vintage_date.replace('-', '')}"
    out: dict[str, str] = {}
    for row in reader:
        if not isinstance(row, dict):
            continue
        obs = str(row.get("observation_date", "")).strip()
        if not obs:
            continue
        out[obs] = str(row.get(value_col, "")).strip()
    return out


def _load_latest_observation_presence(series_id: str) -> set[str]:
    raw = _fetch_text(f"{ALFRED_GRAPH_CSV_URL}?id={series_id}")
    reader = csv.DictReader(raw.splitlines())
    fieldnames = list(reader.fieldnames or [])

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
        obs = str(row.get("observation_date", "")).strip()
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


def _derive_first_release_mapping(
    *,
    anchor_series_id: str,
    min_observation_date: str,
    sleep_seconds: float,
    present_observation_dates: set[str],
) -> dict[str, str]:
    revision_dates = _load_available_revision_dates(anchor_series_id)

    monthly_first_release: dict[str, str] = {}
    for release_date in revision_dates:
        release_month = release_date[:7]
        current = monthly_first_release.get(release_month)
        if current is None or release_date < current:
            monthly_first_release[release_month] = release_date

    release_dates = [monthly_first_release[k] for k in sorted(monthly_first_release.keys())]
    release_dates = [d for d in release_dates if d >= min_observation_date]

    first_release_by_observation: dict[str, str] = {}
    min_obs_date = date.fromisoformat(min_observation_date)
    latest_seen_obs_month: date | None = None

    for release_date in release_dates:
        release_month_start = date.fromisoformat(f"{release_date[:7]}-01")
        max_obs_month_for_release = date(
            (release_month_start.year - 1) if release_month_start.month == 1 else release_month_start.year,
            12 if release_month_start.month == 1 else release_month_start.month - 1,
            1,
        )
        if max_obs_month_for_release < min_obs_date:
            continue

        first_obs_month = max_obs_month_for_release if latest_seen_obs_month is None else _add_month(latest_seen_obs_month)
        current = first_obs_month
        while current <= max_obs_month_for_release:
            obs_iso = current.isoformat()
            if current >= min_obs_date and obs_iso in present_observation_dates:
                first_release_by_observation[obs_iso] = release_date
            current = _add_month(current)

        if max_obs_month_for_release >= first_obs_month:
            latest_seen_obs_month = max_obs_month_for_release

        if sleep_seconds > 0:
            time.sleep(float(sleep_seconds))

    return first_release_by_observation


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


def _load_icsa_observation_weeks_from_dol(*, min_observation_date: date) -> list[date]:
    raw = _fetch_text(DOL_AR539_URL, timeout=60, max_retries=4)
    reader = csv.DictReader(raw.splitlines())
    obs: set[date] = set()
    for row in reader:
        if not isinstance(row, dict):
            continue
        parsed = _parse_date(str(row.get("c2", "")).strip(), "%m/%d/%Y")
        if parsed is None:
            continue
        if parsed < min_observation_date:
            continue
        obs.add(parsed)
    if not obs:
        raise ReleaseCalendarProviderError("No weekly observation dates extracted from DOL ar539.csv column c2.")
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
