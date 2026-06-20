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
}

SUPPORTED_MICS: tuple[str, ...] = ("XNYS", "XASX", "XLON", "XETR", "XHKG", "XBOM", "XTKS")
DEFAULT_MIC = "XNYS"


def mic_for_symbol(symbol: str) -> str:
    token = str(symbol or "").strip().upper()
    for suffix, mic in _SUFFIX_TO_MIC.items():
        if token.endswith(suffix.upper()):
            return mic
    return DEFAULT_MIC


def trading_sessions(mic: str, *, start: date, end: date) -> list[date]:
    cal = ec.get_calendar(str(mic).strip().upper())
    sessions = cal.sessions_in_range(start.isoformat(), end.isoformat())
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
    early_closes = getattr(cal, "early_closes", None)
    if early_closes is None:
        return set()
    try:
        frame = early_closes.loc[start.isoformat() : end.isoformat()]
    except Exception:
        return set()
    try:
        index = frame.index
    except AttributeError:
        index = early_closes
    return {session.date() for session in index}
