from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

PUBLIC_TIMEZONE = ZoneInfo("Asia/Kolkata")
PUBLIC_TIMEZONE_NAME = "Asia/Kolkata"


def _price(value):
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return value


def _ist_timestamp(value) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), timezone.utc).astimezone(PUBLIC_TIMEZONE)
        else:
            text = str(value).strip()
            try:
                dt = datetime.fromtimestamp(float(text), timezone.utc).astimezone(PUBLIC_TIMEZONE)
            except ValueError:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                dt = parsed.astimezone(PUBLIC_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M:%S IST")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _clean_candle(candle: dict) -> dict:
    return {
        "timestamp": _ist_timestamp(candle.get("timestamp", candle.get("epoch"))),
        "open": _price(candle.get("open")),
        "high": _price(candle.get("high")),
        "low": _price(candle.get("low")),
        "close": _price(candle.get("close")),
        "volume": int(candle.get("volume", 0) or 0),
    }


def _stock_payload(state, security_id: str, meta: dict) -> dict:
    with state.lock:
        candles = [dict(c) for c in state.live_candles.get(security_id, []) if c.get("complete", True)]
        reference = dict(state.market_reference.get(security_id, {}))
        ltp = state.last_ltp_by_security.get(security_id)
        ltt = state.last_ltt_by_security.get(security_id)
    return {
        "symbol": meta["symbol"],
        "security_id": security_id,
        "previous_close": _price(reference.get("previous_close")),
        "today_open": _price(reference.get("today_open")),
        "candles_1m": [_clean_candle(c) for c in sorted(candles, key=lambda c: int(c.get("epoch", c.get("timestamp", 0))))],
        "ltp": _price(ltp),
        "ltp_timestamp": _ist_timestamp(ltt),
    }


def market_live_json(state, stock_range: Optional[tuple[int, int]] = None, preserve_instrument_order: bool = False) -> dict:
    with state.lock:
        items = list(state.instruments.items())
        if not preserve_instrument_order:
            items.sort(key=lambda item: item[1]["symbol"])
        if stock_range is not None:
            items = items[stock_range[0]:stock_range[1]]
    stocks = {meta["symbol"]: _stock_payload(state, security_id, meta) for security_id, meta in items}
    snap = state.snapshot()
    return {
        "service": "PSYGRID",
        "schema_version": "4.0",
        "status": "OK" if state.session_status == "LIVE" else state.session_status,
        "session": {
            "status": state.session_status,
            "date": state.session_date,
            "timezone": PUBLIC_TIMEZONE_NAME,
            "current_time_ist": datetime.now(PUBLIC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST"),
        },
        "universe_size": 450,
        "stock_count": len(stocks),
        "data_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN",
        "synthetic_candles": False,
        "stocks": stocks,
    }


def stock_json(state, symbol: str) -> dict:
    symbol = symbol.upper()
    with state.lock:
        found = next(((sid, meta) for sid, meta in state.instruments.items() if meta["symbol"] == symbol), None)
    if found is None:
        return {"service": "PSYGRID", "symbol": symbol, "status": "NOT_FOUND"}
    security_id, meta = found
    payload = _stock_payload(state, security_id, meta)
    return {
        "service": "PSYGRID",
        "schema_version": "4.0",
        "status": "OK",
        **payload,
    }


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str) -> str:
    return dumps_json(stock_json(state, symbol))
