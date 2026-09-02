from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap
from output import market_live_json as _market_live_json, normalize_candle

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
NATIVE_HIGHER_TIMEFRAMES = ("5m", "15m", "1h")
MAX_SIGNAL_LIVE_AGE_SECONDS = 30


def _enrich_native(seed: list[dict], rows: list[dict], settings) -> list[dict]:
    """Enrich genuine Dhan candles without constructing new candles."""
    combined = [dict(c) for c in seed] + [dict(c) for c in rows]
    combined.sort(key=lambda c: int(c["timestamp"]))
    closes = [float(c["close"]) for c in combined]
    out: list[dict] = []
    day_rows: list[dict] = []
    current_day = None
    seed_count = len(seed)
    for idx, row in enumerate(combined):
        day = datetime.fromtimestamp(int(row["timestamp"]), PUBLIC_TZ).date()
        if day != current_day:
            day_rows = []
            current_day = day
        day_rows.append(row)
        if idx < seed_count:
            continue
        item = dict(row)
        prefix = closes[: idx + 1]
        item["vwap"] = vwap(day_rows)
        item["ma9"] = sma(prefix, settings.ma_period)
        item["ema20"] = ema(prefix, settings.ema_period)
        item["rsi14"] = rsi(prefix, settings.rsi_period)
        item["complete"] = bool(row.get("complete", True))
        out.append(item)
    return out


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
    for stock in payload.get("stocks", {}).values():
        security_id = str(stock.get("security_id", ""))
        received = state.last_tick_received_by_security.get(security_id)
        stock["last_tick_timestamp"] = (
            datetime.fromtimestamp(received, PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
            if received is not None else None
        )
        # 5m/15m/1h remain native Dhan candles. No 1m aggregation, filling,
        # interpolation, or synthetic candle construction is permitted.
        for key in NATIVE_HIGHER_TIMEFRAMES:
            stock.setdefault("timeframes", {})[key] = _native_timeframe_payload(
                state, security_id, key
            )


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
        1
        for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
    )

    # Signal eligibility is global only for session/feed state. Quote
    # freshness remains per stock; stale symbols are individually rejected by
    # the downstream scanner rather than poisoning the whole universe.
    signal_valid = bool(
        session.get("status") == "LIVE"
        and session.get("feed_status") == "CONNECTED"
        and fresh_count > 0
    )
    if signal_valid:
        reason = None
    elif session.get("status") != "LIVE":
        reason = "MARKET_SESSION_NOT_LIVE"
    elif session.get("feed_status") != "CONNECTED":
        reason = f"FEED_STATUS_{session.get('feed_status', 'UNKNOWN')}"
    else:
        reason = "NO_FRESH_LIVE_STOCKS"

    payload["signal_input"] = {
        "valid": signal_valid,
        "status": "LIVE" if signal_valid else "BLOCKED",
        "stock_count": int(payload.get("stock_count", len(stocks)) or 0),
        "fresh_stock_count": fresh_count,
        "max_live_age_seconds": MAX_SIGNAL_LIVE_AGE_SECONDS,
        "block_reason": reason,
        "per_stock_freshness_required": True,
        "native_higher_timeframes_required": True,
    }
    return payload
