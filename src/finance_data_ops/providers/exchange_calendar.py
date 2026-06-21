"""Exchange trading calendar helpers."""

from __future__ import annotations

from datetime import date

import exchange_calendars as ec

_SUFFIX_TO_MIC: dict[str, str] = {
    ".AX": "XASX",
    ".L": "XLON",
    ".DE": "XETR",
    ".HK": "XHKG",
    # exchange_calendars 4.13.2 has no XNSE calendar; XBOM is the available India calendar.
    ".NS": "XBOM",
    ".BO": "XBOM",
    ".T": "XTKS",
    ".AS": "XAMS",
    ".PA": "XPAR",
    ".LS": "XLIS",
    ".CO": "XCSE",
    ".SS": "XSHG",
    # exchange_calendars 4.13.2 has no XSHE calendar; Shenzhen uses XSHG as the China-market proxy.
    ".SZ": "XSHG",
}

SUPPORTED_MICS: tuple[str, ...] = (
    "XNYS",
    "XASX",
    "XLON",
    "XETR",
    "XHKG",
    "XBOM",
    "XTKS",
    "XAMS",
    "XPAR",
    "XLIS",
    "XCSE",
    "XSHG",
)
DEFAULT_MIC = "XNYS"


def mic_for_symbol(symbol: str) -> str:
    token = str(symbol or "").strip().upper()
    for suffix, mic in _SUFFIX_TO_MIC.items():
        if token.endswith(suffix.upper()):
            return mic
    return DEFAULT_MIC


def trading_sessions(mic: str, *, start: date, end: date) -> list[date]:
    cal = ec.get_calendar(str(mic).strip().upper())
    bounded_start, bounded_end = _bounded_calendar_window(cal, start=start, end=end)
    if bounded_start is None or bounded_end is None:
        return []
    sessions = cal.sessions_in_range(bounded_start.isoformat(), bounded_end.isoformat())
    return [session.date() for session in sessions]


def trading_session_rows(mic: str, *, start: date, end: date) -> list[dict[str, object]]:
    normalized_mic = str(mic).strip().upper()
    sessions = trading_sessions(normalized_mic, start=start, end=end)
    half_days = _half_day_sessions(normalized_mic, start=start, end=end)
    return [
        {
            "exchange_mic": normalized_mic,
            "session_date": session,
            "is_half_day": session in half_days,
        }
        for session in sessions
    ]


def _half_day_sessions(mic: str, *, start: date, end: date) -> set[date]:
    cal = ec.get_calendar(mic)
    bounded_start, bounded_end = _bounded_calendar_window(cal, start=start, end=end)
    if bounded_start is None or bounded_end is None:
        return set()
    early_closes = getattr(cal, "early_closes", None)
    if early_closes is None:
        return set()
    try:
        frame = early_closes.loc[bounded_start.isoformat() : bounded_end.isoformat()]
    except Exception:
        return set()
    try:
        index = frame.index
    except AttributeError:
        index = early_closes
    return {session.date() for session in index}


def _bounded_calendar_window(cal: object, *, start: date, end: date) -> tuple[date | None, date | None]:
    first_session = getattr(cal, "first_session", None)
    last_session = getattr(cal, "last_session", None)
    first_date = first_session.date() if first_session is not None else start
    last_date = last_session.date() if last_session is not None else end
    bounded_start = max(start, first_date)
    bounded_end = min(end, last_date)
    if bounded_start > bounded_end:
        return None, None
    return bounded_start, bounded_end
