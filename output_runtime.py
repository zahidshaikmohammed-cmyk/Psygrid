from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from indicators import enrich
from output import market_live_json as _market_live_json, normalize_candle

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
NATIVE_HIGHER_TIMEFRAMES = ("5m", "15m", "1h")
MAX_SIGNAL_LIVE_AGE_SECONDS = 30


def _enrich_native(seed: list[dict], rows: list[dict], settings) -> list[dict]:
    """Enrich genuine Dhan candles without constructing new candles."""
    combined = [dict(c) for c in seed] + [dict(c) for c in rows]
    combined.sort(key=lambda c: int(c["timestamp"]))
    seed_count = len(seed)
    enriched = enrich(
        combined,
        settings.ma_period,
        settings.ema_period,
        settings.rsi_period,
        day_key=lambda candle: datetime.fromtimestamp(
            int(candle["timestamp"]), PUBLIC_TZ
        ).date(),
    )
    return enriched[seed_count:]


def _native_timeframe_payload(state, security_id: str, key: str) -> list[dict]:
    today = datetime.now(PUBLIC_TZ).date()
    with state.lock:
        history = state.historical.get(security_id, {})
        seed = [dict(c) for c in history.get(f"{key}_seed", []) if c.get("complete", True)]
        rows = [dict(c) for c in history.get(key, []) if c.get("complete", True)]
    rows = [
        row for row in rows
        if datetime.fromtimestamp(int(row["timestamp"]), PUBLIC_TZ).date() == today
    ]
    enriched = _enrich_native(seed, rows, state.settings)
    return [normalize_candle(row) for row in enriched]


def _stock_fixups(state, payload: dict) -> None:
    """Attach native HTFs and receipt-time freshness without changing source data."""
    for stock in payload.get("stocks", {}).values():
        security_id = str(stock.get("security_id", ""))
        received = state.last_tick_received_by_security.get(security_id)
        stock["last_tick_timestamp"] = (
            datetime.fromtimestamp(received, PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
            if received is not None else None
        )
        for key in NATIVE_HIGHER_TIMEFRAMES:
            stock.setdefault("timeframes", {})[key] = _native_timeframe_payload(
                state, security_id, key
            )


def _higher_timeframes_ready(stock: dict) -> bool:
    """Require real native 5m/15m/1h candles and calculated indicators."""
    timeframes = stock.get("timeframes", {})
    for key in NATIVE_HIGHER_TIMEFRAMES:
        rows = timeframes.get(key) or []
        if not rows:
            return False
        latest = rows[-1]
        if any(latest.get(field) is None for field in ("ma9", "ema20", "rsi14")):
            return False
    return True


def market_live_json(state, stock_range=None) -> dict:
    payload = _market_live_json(state, stock_range)
    session = payload.get("session", {})
    session["max_live_age_seconds"] = MAX_SIGNAL_LIVE_AGE_SECONDS
    payload["session"] = session

    rules = payload.setdefault("rules", {})
    rules["live_freshness_policy"] = "MAX_30_SECONDS"
    rules["synthetic_candles"] = False
    rules["native_higher_timeframes"] = True
    rules["higher_timeframe_source"] = "DHAN_HISTORICAL_API"
    rules["higher_timeframe_policy"] = "NATIVE_DHAN_ONLY_NO_1M_AGGREGATION"

    if session.get("status") == "LIVE":
        _stock_fixups(state, payload)

    stocks = payload.get("stocks", {})
    fresh_count = sum(
        1 for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
    )
    analysis_ready_count = sum(
        1 for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
        and _higher_timeframes_ready(stock)
    )
    stock_count = int(payload.get("stock_count", len(stocks)) or 0)

    signal_valid = bool(
        session.get("status") == "LIVE"
        and session.get("feed_status") == "CONNECTED"
        and stock_count > 0
        and analysis_ready_count == stock_count
    )
    if signal_valid:
        reason = None
    elif session.get("status") != "LIVE":
        reason = "MARKET_SESSION_NOT_LIVE"
    elif session.get("feed_status") != "CONNECTED":
        reason = f"FEED_STATUS_{session.get('feed_status', 'UNKNOWN')}"
    elif fresh_count < stock_count:
        reason = "INCOMPLETE_FRESH_LIVE_COVERAGE"
    else:
        reason = "NATIVE_HIGHER_TIMEFRAME_HISTORY_NOT_READY"

    payload["signal_input"] = {
        "valid": signal_valid,
        "status": "LIVE" if signal_valid else "BLOCKED",
        "stock_count": stock_count,
        "fresh_stock_count": fresh_count,
        "analysis_ready_stock_count": analysis_ready_count,
        "max_live_age_seconds": MAX_SIGNAL_LIVE_AGE_SECONDS,
        "block_reason": reason,
        "per_stock_freshness_required": True,
        "native_higher_timeframes_required": True,
        "synthetic_candles_allowed": False,
        "all_stocks_must_be_analysis_ready": True,
    }
    return payload
