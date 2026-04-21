from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

NON_EARNINGS_INSTRUMENT_TYPES = {"etf", "index_proxy", "country_fund"}
MARKET_REQUIRED_ASSETS = (
    "market_price_daily",
    "market_quotes",
    "market_earnings_history",
    "mv_next_earnings",
)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def build_ticker_signal_v1_report(
    *,
    ticker: str,
    region: str | None,
    exchange: str | None,
    analysis_type: str,
    market_snapshot: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    asset_status_by_key: dict[str, dict[str, Any]] | None,
    registry_row: dict[str, Any] | None,
    market_price_rows: list[dict[str, Any]] | None = None,
    earnings_event_rows: list[dict[str, Any]] | None = None,
    earnings_rows: list[dict[str, Any]] | None = None,
    next_earnings_row: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    normalized_ticker = str(ticker).strip().upper()
    normalized_region = str(region or "").strip().lower() or "us"
    normalized_exchange = (str(exchange).strip().upper() if exchange else None)
    generated_at_value = generated_at or now_iso()

    freshness_ok, freshness_warnings, freshness_map = _evaluate_signal_freshness(
        asset_status_by_key=asset_status_by_key,
    )
    market_frame = _prepare_market_frame(market_price_rows)
    market_features, market_warnings = _compute_market_features(
        market_frame=market_frame,
        market_snapshot=market_snapshot,
    )
    earnings_history_frame = _prepare_earnings_history_frame(earnings_rows)
    resolved_next_earnings = _prepare_next_earnings_row(
        next_earnings_row=next_earnings_row,
        earnings_event_rows=earnings_event_rows,
    )
    earnings_features, earnings_warnings = _compute_earnings_features(
        earnings_history_frame=earnings_history_frame,
        next_earnings_row=resolved_next_earnings,
        registry_row=registry_row,
        generated_at=generated_at_value,
    )

    market_score, market_drivers, market_risks = _score_market_block(market_features)
    earnings_score, earnings_drivers, earnings_risks, earnings_mode = _score_earnings_block(
        earnings_features=earnings_features,
        registry_row=registry_row,
    )
    base_score = (0.65 * market_score) + (0.35 * earnings_score)
    adjusted_score, event_risk_notes = _apply_event_risk_adjustment(
        base_score=base_score,
        next_event=earnings_features.get("next_event"),
    )
    confidence = _compute_confidence(
        market_features=market_features,
        earnings_features=earnings_features,
        freshness_ok=freshness_ok,
        freshness_warnings=freshness_warnings,
        earnings_mode=earnings_mode,
    )

    warnings = freshness_warnings + market_warnings + earnings_warnings
    if not freshness_ok:
        warnings.append("required upstream assets are not fully fresh; signal degraded to neutral")
        signal_score: float | None = None
        signal_label = "neutral"
        stance = "insufficient_data"
    else:
        signal_score = round(_clamp(adjusted_score), 4)
        signal_label = _label_from_score(signal_score, confidence)
        stance = _stance_from_components(
            label=signal_label,
            market_score=market_score,
            earnings_score=earnings_score,
            earnings_mode=earnings_mode,
            next_event=earnings_features.get("next_event"),
            confidence=confidence,
        )

    drivers = market_drivers + earnings_drivers
    risks = market_risks + earnings_risks + event_risk_notes
    summary_text = _build_summary_text(
        ticker=normalized_ticker,
        label=signal_label,
        score=signal_score,
        confidence=confidence,
        stance=stance,
        drivers=drivers,
        risks=risks,
    )

    summary = {
        "ticker": normalized_ticker,
        "region": normalized_region,
        "analysis_type": str(analysis_type).strip(),
        "generated_at": generated_at_value,
        "signal_label": signal_label,
        "signal_score": signal_score,
        "confidence": round(confidence, 4),
        "as_of_date": market_features.get("as_of_date"),
        "next_earnings_date": ((earnings_features.get("next_event") or {}).get("earnings_date")),
        "freshness_ok": freshness_ok,
    }

    return {
        "summary": summary,
        "summary_text": summary_text,
        "market": market_features,
        "earnings": earnings_features,
        "signal": {
            "label": signal_label,
            "score": signal_score,
            "confidence": round(confidence, 4),
            "stance": stance,
            "drivers": drivers,
            "risks": risks,
        },
        "warnings": warnings,
        "metadata": {
            "ticker": normalized_ticker,
            "region": normalized_region,
            "exchange": normalized_exchange,
            "analysis_type": str(analysis_type).strip(),
            "generated_at": generated_at_value,
            "source_assets": [
                "market_price_daily",
                "market_quotes",
                "ticker_market_stats_snapshot",
                "market_earnings_events",
                "market_earnings_history",
                "mv_next_earnings",
            ],
            "freshness": freshness_map,
            "version": 1,
        },
    }


def _prepare_market_frame(market_price_rows: list[dict[str, Any]] | None) -> pd.DataFrame:
    if not market_price_rows:
        return pd.DataFrame(columns=["date", "close", "volume"])
    frame = pd.DataFrame(list(market_price_rows))
    if frame.empty:
        return pd.DataFrame(columns=["date", "close", "volume"])
    ticker_col = "ticker" if "ticker" in frame.columns else ("symbol" if "symbol" in frame.columns else None)
    if ticker_col and ticker_col != "ticker":
        frame["ticker"] = frame[ticker_col]
    frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")
    frame["close"] = pd.to_numeric(frame.get("close"), errors="coerce")
    frame["volume"] = pd.to_numeric(frame.get("volume"), errors="coerce")
    frame = frame.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return frame.reset_index(drop=True)


def _compute_market_features(
    *,
    market_frame: pd.DataFrame,
    market_snapshot: dict[str, Any] | None,
) -> tuple[dict[str, Any], list[str]]:
    snapshot = dict(market_snapshot or {})
    warnings: list[str] = []
    features: dict[str, Any] = {
        "last_price": _to_float(snapshot.get("last_price")),
        "as_of_date": snapshot.get("as_of_date"),
        "return_1d_pct": _to_float(snapshot.get("return_1d_pct")),
        "return_1m_pct": _to_float(snapshot.get("return_1m_pct")),
        "return_3m_pct": _to_float(snapshot.get("return_3m_pct")),
        "return_1y_pct": _to_float(snapshot.get("return_1y_pct")),
        "distance_from_20d_ma_pct": None,
        "distance_from_50d_ma_pct": None,
        "distance_from_200d_ma_pct": None,
        "drawdown_from_252d_high_pct": _to_float(snapshot.get("drawdown_1y_pct")),
        "volatility_20d_pct": None,
        "volume_vs_20d_avg": None,
    }
    if market_frame.empty:
        warnings.append("no market_price_daily rows found for ticker")
        return features, warnings

    close = market_frame["close"].astype(float).reset_index(drop=True)
    dates = pd.to_datetime(market_frame["date"], errors="coerce")
    features["last_price"] = features["last_price"] if features["last_price"] is not None else float(close.iloc[-1])
    features["as_of_date"] = features["as_of_date"] or pd.Timestamp(dates.iloc[-1]).date().isoformat()
    if features["return_1d_pct"] is None:
        features["return_1d_pct"] = _window_return(close, 1)
    if features["return_1m_pct"] is None:
        features["return_1m_pct"] = _window_return(close, 21)
    if features["return_3m_pct"] is None:
        features["return_3m_pct"] = _window_return(close, 63)
    if features["return_1y_pct"] is None:
        features["return_1y_pct"] = _window_return(close, 252)

    for periods, key in ((20, "distance_from_20d_ma_pct"), (50, "distance_from_50d_ma_pct"), (200, "distance_from_200d_ma_pct")):
        if len(close) < periods:
            warnings.append(f"insufficient price history for {periods}d moving average")
            continue
        ma_value = float(close.tail(periods).mean())
        if ma_value != 0.0:
            features[key] = (float(close.iloc[-1]) / ma_value) - 1.0

    if features["drawdown_from_252d_high_pct"] is None and len(close) >= 2:
        trailing = close.tail(min(len(close), 252))
        trailing_max = float(trailing.max())
        if trailing_max != 0.0:
            features["drawdown_from_252d_high_pct"] = (float(close.iloc[-1]) / trailing_max) - 1.0

    returns_20d = close.pct_change().tail(20).dropna()
    if len(returns_20d) >= 2:
        features["volatility_20d_pct"] = float(returns_20d.std(ddof=1) * (252.0 ** 0.5))
    else:
        warnings.append("insufficient price history for 20d volatility")

    if "volume" in market_frame.columns and market_frame["volume"].notna().sum() >= 20:
        latest_volume = _to_float(market_frame["volume"].iloc[-1])
        avg_volume = _to_float(market_frame["volume"].tail(20).mean())
        if latest_volume is not None and avg_volume not in {None, 0.0}:
            features["volume_vs_20d_avg"] = latest_volume / avg_volume
    else:
        warnings.append("insufficient volume history for 20d volume ratio")

    return features, warnings


def _prepare_earnings_history_frame(earnings_rows: list[dict[str, Any]] | None) -> pd.DataFrame:
    if not earnings_rows:
        return pd.DataFrame(columns=["ticker", "earnings_date", "surprise_eps"])
    frame = pd.DataFrame(list(earnings_rows))
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "earnings_date", "surprise_eps"])
    ticker_col = "ticker" if "ticker" in frame.columns else ("symbol" if "symbol" in frame.columns else None)
    if ticker_col and ticker_col != "ticker":
        frame["ticker"] = frame[ticker_col]
    frame["earnings_date"] = pd.to_datetime(frame.get("earnings_date"), errors="coerce")
    frame["surprise_eps"] = pd.to_numeric(frame.get("surprise_eps"), errors="coerce")
    frame = frame.dropna(subset=["earnings_date"]).sort_values("earnings_date").drop_duplicates(subset=["earnings_date"], keep="last")
    return frame.reset_index(drop=True)


def _prepare_next_earnings_row(
    next_earnings_row: dict[str, Any] | None,
    earnings_event_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if next_earnings_row:
        return dict(next_earnings_row)
    if not earnings_event_rows:
        return None
    frame = pd.DataFrame(list(earnings_event_rows))
    if frame.empty or "earnings_date" not in frame.columns:
        return None
    frame["earnings_date"] = pd.to_datetime(frame["earnings_date"], errors="coerce")
    frame = frame.dropna(subset=["earnings_date"]).sort_values("earnings_date")
    if frame.empty:
        return None
    return dict(frame.iloc[0].to_dict())


def _instrument_supports_earnings(registry_row: dict[str, Any] | None) -> bool | None:
    instrument_type = str((registry_row or {}).get("instrument_type") or "").strip().lower()
    if not instrument_type:
        return None
    if instrument_type in NON_EARNINGS_INSTRUMENT_TYPES:
        return False
    if instrument_type in {"equity", "adr"}:
        return True
    return None


def _compute_earnings_features(
    *,
    earnings_history_frame: pd.DataFrame,
    next_earnings_row: dict[str, Any] | None,
    registry_row: dict[str, Any] | None,
    generated_at: str,
) -> tuple[dict[str, Any], list[str]]:
    generated_date = pd.Timestamp(generated_at).date()
    warnings: list[str] = []
    next_event = None
    if next_earnings_row:
        earnings_date = pd.to_datetime(next_earnings_row.get("earnings_date"), errors="coerce")
        if not pd.isna(earnings_date):
            next_event = {
                "earnings_date": pd.Timestamp(earnings_date).date().isoformat(),
                "session": next_earnings_row.get("earnings_time"),
                "days_until": int((pd.Timestamp(earnings_date).date() - generated_date).days),
            }
    else:
        warnings.append("no next earnings event found")

    supports_earnings = _instrument_supports_earnings(registry_row)
    if supports_earnings is False:
        warnings.append("instrument_type_not_supported_for_earnings")
        return {
            "next_event": next_event,
            "history": {
                "recent_quarters": 0,
                "beat_count_4q": 0,
                "miss_count_4q": 0,
                "avg_surprise_pct_4q": None,
                "last_surprise_pct": None,
                "trend": "market_only",
            },
        }, warnings

    recent = earnings_history_frame.tail(4).copy()
    if recent.empty:
        warnings.append("no_recent_earnings_history")
        return {
            "next_event": next_event,
            "history": {
                "recent_quarters": 0,
                "beat_count_4q": 0,
                "miss_count_4q": 0,
                "avg_surprise_pct_4q": None,
                "last_surprise_pct": None,
                "trend": "insufficient_history",
            },
        }, warnings

    surprise_values = pd.to_numeric(recent.get("surprise_eps"), errors="coerce")
    beat_count = int((surprise_values > 0).sum())
    miss_count = int((surprise_values < 0).sum())
    avg_surprise = _to_float(surprise_values.mean())
    last_surprise = _to_float(surprise_values.iloc[-1]) if len(surprise_values.index) else None
    trend = _surprise_trend(surprise_values.tolist())
    return {
        "next_event": next_event,
        "history": {
            "recent_quarters": int(len(recent.index)),
            "beat_count_4q": beat_count,
            "miss_count_4q": miss_count,
            "avg_surprise_pct_4q": avg_surprise,
            "last_surprise_pct": last_surprise,
            "trend": trend,
        },
    }, warnings


def _evaluate_signal_freshness(
    *,
    asset_status_by_key: dict[str, dict[str, Any]] | None,
) -> tuple[bool, list[str], dict[str, str]]:
    warnings: list[str] = []
    freshness_map: dict[str, str] = {}
    assets = asset_status_by_key or {}
    freshness_ok = True
    for asset_key in MARKET_REQUIRED_ASSETS:
        freshness = str((assets.get(asset_key) or {}).get("freshness_status") or "unknown").strip().lower()
        freshness_map[asset_key] = freshness or "unknown"
        if freshness != "fresh":
            freshness_ok = False
            warnings.append(f"{asset_key} freshness={freshness or 'unknown'}")
    return freshness_ok, warnings, freshness_map


def _score_market_block(features: dict[str, Any]) -> tuple[float, list[str], list[str]]:
    score = 0.5
    drivers: list[str] = []
    risks: list[str] = []

    r1m = _to_float(features.get("return_1m_pct"))
    r3m = _to_float(features.get("return_3m_pct"))
    r1y = _to_float(features.get("return_1y_pct"))
    positives = sum(1 for value in (r1m, r3m, r1y) if value is not None and value > 0)
    if positives == 3:
        score += 0.18
        drivers.append("price trend positive across 1m, 3m, and 1y windows")
    elif positives == 2:
        score += 0.10
        drivers.append("price trend positive across multiple windows")
    elif positives == 0 and any(value is not None for value in (r1m, r3m)):
        score -= 0.14
        risks.append("price trend negative across short and medium windows")

    ma_scores = []
    for key, label in (
        ("distance_from_20d_ma_pct", "20d"),
        ("distance_from_50d_ma_pct", "50d"),
        ("distance_from_200d_ma_pct", "200d"),
    ):
        value = _to_float(features.get(key))
        if value is None:
            continue
        ma_scores.append(value > 0)
        if value > 0:
            drivers.append(f"price above {label} moving average")
        else:
            risks.append(f"price below {label} moving average")
    if ma_scores:
        score += 0.02 * sum(1 for item in ma_scores if item)
        score -= 0.02 * sum(1 for item in ma_scores if not item)

    drawdown = _to_float(features.get("drawdown_from_252d_high_pct"))
    if drawdown is not None:
        if drawdown > -0.10:
            score += 0.05
            drivers.append("modest drawdown from trailing high")
        elif drawdown < -0.25:
            score -= 0.08
            risks.append("deep drawdown from trailing high")

    volatility = _to_float(features.get("volatility_20d_pct"))
    if volatility is not None:
        if volatility > 0.45:
            score -= 0.05
            risks.append("elevated short-term volatility")
        elif volatility < 0.25:
            score += 0.02

    volume_ratio = _to_float(features.get("volume_vs_20d_avg"))
    if volume_ratio is not None and volume_ratio > 1.5:
        drivers.append("volume running above 20d average")

    return _clamp(score), _dedupe(drivers), _dedupe(risks)


def _score_earnings_block(
    *,
    earnings_features: dict[str, Any],
    registry_row: dict[str, Any] | None,
) -> tuple[float, list[str], list[str], str]:
    supports_earnings = _instrument_supports_earnings(registry_row)
    history = dict((earnings_features or {}).get("history") or {})
    if supports_earnings is False:
        return 0.5, [], ["earnings not applicable for instrument type"], "market_only"

    recent_quarters = int(history.get("recent_quarters") or 0)
    if recent_quarters <= 0:
        return 0.45, [], ["recent earnings history unavailable"], "insufficient_history"

    score = 0.5
    drivers: list[str] = []
    risks: list[str] = []
    beat_count = int(history.get("beat_count_4q") or 0)
    avg_surprise = _to_float(history.get("avg_surprise_pct_4q"))
    last_surprise = _to_float(history.get("last_surprise_pct"))
    trend = str(history.get("trend") or "mixed")

    if beat_count >= 3:
        score += 0.10
        drivers.append("earnings beats outnumber misses over the last 4 quarters")
    elif beat_count <= 1:
        score -= 0.08
        risks.append("earnings misses dominate recent quarters")

    if avg_surprise is not None:
        if avg_surprise > 0:
            score += 0.07
            drivers.append("average earnings surprise is positive")
        elif avg_surprise < 0:
            score -= 0.07
            risks.append("average earnings surprise is negative")

    if last_surprise is not None:
        if last_surprise > 0:
            score += 0.05
            drivers.append("last reported earnings surprise was positive")
        elif last_surprise < 0:
            score -= 0.05
            risks.append("last reported earnings surprise was negative")

    if trend == "improving":
        score += 0.04
        drivers.append("earnings surprise trend is improving")
    elif trend == "deteriorating":
        score -= 0.04
        risks.append("earnings surprise trend is deteriorating")

    return _clamp(score), _dedupe(drivers), _dedupe(risks), "full"


def _apply_event_risk_adjustment(
    *,
    base_score: float,
    next_event: dict[str, Any] | None,
) -> tuple[float, list[str]]:
    if not next_event:
        return base_score, []
    days_until = next_event.get("days_until")
    try:
        horizon = int(days_until)
    except (TypeError, ValueError):
        return base_score, []
    if horizon <= 7:
        return base_score - 0.12, ["elevated pre-earnings uncertainty"]
    if horizon <= 14:
        return base_score - 0.07, ["near-term earnings event risk"]
    if horizon <= 21:
        return base_score - 0.03, ["earnings event approaching"]
    return base_score, []


def _compute_confidence(
    *,
    market_features: dict[str, Any],
    earnings_features: dict[str, Any],
    freshness_ok: bool,
    freshness_warnings: list[str],
    earnings_mode: str,
) -> float:
    confidence = 1.0
    if market_features.get("distance_from_200d_ma_pct") is None:
        confidence -= 0.10
    if market_features.get("distance_from_50d_ma_pct") is None:
        confidence -= 0.05
    if market_features.get("return_3m_pct") is None:
        confidence -= 0.05
    if earnings_mode == "insufficient_history":
        confidence -= 0.15
    elif earnings_mode == "market_only":
        confidence -= 0.08
    if not freshness_ok:
        confidence -= 0.15
    if freshness_warnings:
        confidence -= min(0.10, 0.02 * len(freshness_warnings))
    if not (earnings_features.get("next_event") or {}).get("earnings_date"):
        confidence -= 0.05
    return round(_clamp(confidence), 4)


def _label_from_score(score: float, confidence: float) -> str:
    if score >= 0.75:
        label = "positive"
    elif score >= 0.60:
        label = "watch"
    elif score >= 0.40:
        label = "neutral"
    else:
        label = "caution"
    if confidence < 0.50 and label == "positive":
        return "watch"
    return label


def _stance_from_components(
    *,
    label: str,
    market_score: float,
    earnings_score: float,
    earnings_mode: str,
    next_event: dict[str, Any] | None,
    confidence: float,
) -> str:
    if confidence < 0.40:
        return "insufficient_data"
    if earnings_mode == "market_only":
        return "market_only"
    if next_event and isinstance(next_event.get("days_until"), int) and int(next_event["days_until"]) <= 14 and label in {"positive", "watch"}:
        return "positive_but_event_risk_near"
    if market_score >= 0.60 and earnings_score >= 0.60:
        return "trend_and_earnings_aligned"
    if market_score < 0.45 and earnings_score < 0.45:
        return "weak_trend_and_weak_earnings"
    return "mixed_signal"


def _build_summary_text(
    *,
    ticker: str,
    label: str,
    score: float | None,
    confidence: float,
    stance: str,
    drivers: list[str],
    risks: list[str],
) -> str:
    if score is None:
        return f"{ticker} is neutral because required upstream inputs are not fresh enough for a confident signal."
    driver_text = drivers[0] if drivers else "market and earnings inputs are mixed"
    risk_text = risks[0] if risks else "no immediate risk factors dominate"
    return (
        f"{ticker} scores {label} ({score:.2f}) with {driver_text}, "
        f"while {risk_text} (confidence={confidence:.2f}, stance={stance})."
    )


def _window_return(close: pd.Series, periods: int) -> float | None:
    if len(close.index) <= periods:
        return None
    start = _to_float(close.iloc[-(periods + 1)])
    end = _to_float(close.iloc[-1])
    if start in {None, 0.0} or end is None:
        return None
    return (end / start) - 1.0


def _surprise_trend(values: list[Any]) -> str:
    cleaned = [_to_float(value) for value in values]
    cleaned = [value for value in cleaned if value is not None]
    if len(cleaned) < 2:
        return "insufficient_history"
    recent_half = cleaned[len(cleaned) // 2 :]
    early_half = cleaned[: len(cleaned) // 2]
    if not early_half or not recent_half:
        return "mixed"
    if sum(recent_half) > sum(early_half):
        return "improving"
    if sum(recent_half) < sum(early_half):
        return "deteriorating"
    return "mixed"


def _to_float(value: Any) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        token = str(item).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out
