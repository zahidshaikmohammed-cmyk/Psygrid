from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config import UNIVERSE_SIZE

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


def _minute_key(row: dict) -> Optional[int]:
    value = row.get("epoch")
    if value is None:
        value = row.get("timestamp")
    try:
        return int(value) // 60
    except (TypeError, ValueError):
        return None


def _prefer_candle(existing: dict, incoming: dict) -> dict:
    existing_source = str(existing.get("source", ""))
    incoming_source = str(incoming.get("source", ""))
    if existing_source == "DHAN_HISTORICAL_API" and incoming_source != "DHAN_HISTORICAL_API":
        return existing
    return incoming


def _dedupe_candles(rows: list[dict]) -> list[dict]:
    by_minute: dict[int, dict] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("complete", True) is False:
            continue
        if any(row.get(key) is None for key in ("open", "high", "low", "close")):
            continue
        key = _minute_key(row)
        if key is None:
            continue
        by_minute[key] = _prefer_candle(by_minute[key], row) if key in by_minute else dict(row)
    return [by_minute[key] for key in sorted(by_minute)]


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
        candles = [dict(c) for c in state.live_candles.get(security_id, [])]
        current = state.current_1m.get(security_id)
        if current is not None:
            candles.append(dict(current))
        reference = dict(state.market_reference.get(security_id, {}))
    candles = _dedupe_candles(candles)
    return {
        "symbol": meta["symbol"],
        "security_id": security_id,
        "previous_close": _price(reference.get("previous_close")),
        "today_open": _price(reference.get("today_open")),
        "candles_1m": [_clean_candle(c) for c in candles],
    }


def market_live_json(state, stock_range: Optional[tuple[int, int]] = None, preserve_instrument_order: bool = False) -> dict:
    with state.lock:
        items = list(state.instruments.items())
        if not preserve_instrument_order:
            items.sort(key=lambda item: item[1]["symbol"])
        if stock_range is not None:
            items = items[stock_range[0]:stock_range[1]]
    stocks = {meta["symbol"]: _stock_payload(state, security_id, meta) for security_id, meta in items}
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
        "universe_size": UNIVERSE_SIZE,
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
    return {"service": "PSYGRID", "schema_version": "4.0", "status": "OK", **_stock_payload(state, security_id, meta)}


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str) -> str:
    return dumps_json(stock_json(state, symbol))
