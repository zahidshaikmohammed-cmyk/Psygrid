from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo


PUBLIC_TIMEZONE = ZoneInfo("Asia/Kolkata")
PUBLIC_TIMEZONE_NAME = "Asia/Kolkata"
LIVE_TIMEFRAMES = ("1m", "5m", "15m", "1h")


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
                    parsed = parsed.replace(tzinfo=PUBLIC_TIMEZONE)
                dt = parsed.astimezone(PUBLIC_TIMEZONE)
        return dt.strftime("%Y-%m-%d %H:%M:%S IST")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _normalize_ohlcv(row: dict) -> dict:
    return {
        "timestamp": _ist_timestamp(row.get("timestamp") or row.get("epoch")),
        "open": _price(row.get("open")),
        "high": _price(row.get("high")),
        "low": _price(row.get("low")),
        "close": _price(row.get("close")),
        "volume": row.get("volume"),
    }


def _completed_rows(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("complete", True) is False:
            continue
        if row.get("open") is None or row.get("high") is None or row.get("low") is None or row.get("close") is None:
            continue
        out.append(row)
    out.sort(key=lambda row: int(row.get("timestamp", row.get("epoch", 0))))
    return out


def _timeframe_rows(state, security_id: str, timeframe: str) -> list[dict]:
    if timeframe == "1m":
        # Only finalized WebSocket-built 1m candles are public. The active minute
        # remains internal and is deliberately never exposed.
        with state.lock:
            rows = [dict(row) for row in state.live_candles.get(security_id, [])]
        return _completed_rows(rows)

    with state.lock:
        rows = [dict(row) for row in state.historical.get(security_id, {}).get(timeframe, [])]
    return _completed_rows(rows)


def _stock_payload(state, security_id: str, meta: dict) -> dict:
    with state.lock:
        ltp = state.last_ltp_by_security.get(security_id)
        ltt = state.last_ltt_by_security.get(security_id)

    return {
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "instrument": meta["instrument"],
        "ltp": _price(ltp),
        "ltp_timestamp": _ist_timestamp(ltt),
        "1m": [_normalize_ohlcv(row) for row in _timeframe_rows(state, security_id, "1m")],
        "5m": [_normalize_ohlcv(row) for row in _timeframe_rows(state, security_id, "5m")],
        "15m": [_normalize_ohlcv(row) for row in _timeframe_rows(state, security_id, "15m")],
        "1h": [_normalize_ohlcv(row) for row in _timeframe_rows(state, security_id, "1h")],
    }


def _session_payload(state) -> dict:
    snap = state.snapshot()
    return {
        "status": snap["session_status"],
        "date": snap["session_date"],
        "timezone": PUBLIC_TIMEZONE_NAME,
        "current_time_ist": datetime.now(PUBLIC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST"),
    }


def market_live_json(state, stock_range: Optional[tuple[int, int]] = None) -> dict:
    with state.lock:
        items = sorted(state.instruments.items(), key=lambda item: item[1]["symbol"])
        if stock_range is not None:
            start, end = stock_range
            items = items[start:end]
        stocks = {
            meta["symbol"]: _stock_payload(state, security_id, meta)
            for security_id, meta in items
        }

    return {
        "service": "PSYGRID",
        "schema_version": "3.0",
        "session": _session_payload(state),
        "stock_count": len(stocks),
        "timeframes": list(LIVE_TIMEFRAMES),
        "candle_source": {
            "1m": "DHAN_WEBSOCKET_FULL",
            "5m": "DHAN_NATIVE_HISTORICAL",
            "15m": "DHAN_NATIVE_HISTORICAL",
            "1h": "DHAN_NATIVE_HISTORICAL",
        },
        "synthetic_candles": False,
        "stocks": stocks,
    }


def stock_json(state, symbol: str, timeframe: Optional[str] = None) -> dict:
    symbol = symbol.upper()
    with state.lock:
        found = next(
            ((sid, meta) for sid, meta in state.instruments.items() if meta["symbol"] == symbol),
            None,
        )
    if found is None:
        return {"service": "PSYGRID", "symbol": symbol, "status": "NOT_FOUND"}

    security_id, meta = found
    full = _stock_payload(state, security_id, meta)
    payload = {
        "service": "PSYGRID",
        "schema_version": "3.0",
        "symbol": symbol,
        "security_id": security_id,
        "session": _session_payload(state),
        "candle_source": {
            "1m": "DHAN_WEBSOCKET_FULL",
            "5m": "DHAN_NATIVE_HISTORICAL",
            "15m": "DHAN_NATIVE_HISTORICAL",
            "1h": "DHAN_NATIVE_HISTORICAL",
        },
        "synthetic_candles": False,
    }

    if timeframe is None:
        payload.update(full)
        return payload

    if timeframe not in LIVE_TIMEFRAMES:
        return {"service": "PSYGRID", "symbol": symbol, "status": "INVALID_TIMEFRAME"}

    payload.update({
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "instrument": meta["instrument"],
        "ltp": full["ltp"],
        "ltp_timestamp": full["ltp_timestamp"],
        timeframe: full[timeframe],
    })
    return payload


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str, timeframe: Optional[str] = None) -> str:
    return dumps_json(stock_json(state, symbol, timeframe))
