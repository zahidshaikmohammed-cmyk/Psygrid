from __future__ import annotations

import json
from typing import Optional

from indicators import ema, rsi, sma, vwap


TIMEFRAMES = ("1m", "5m", "15m", "1h", "1d", "1w")


def enrich_history(candles: list, settings) -> list:
    rows = [dict(c) for c in candles]
    closes = [float(c["close"]) for c in rows]
    for idx, row in enumerate(rows):
        window = rows[: idx + 1]
        close_window = closes[: idx + 1]
        row["vwap"] = vwap(window)
        row["ma9"] = sma(close_window, settings.ma_period)
        row["ema20"] = ema(close_window, settings.ema_period)
        row["rsi14"] = rsi(close_window, settings.rsi_period)
        row["complete"] = True
    return rows


def normalize_candle(row: dict) -> dict:
    return {
        "timestamp": row.get("timestamp"),
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "volume": row.get("volume"),
        "vwap": row.get("vwap"),
        "ma9": row.get("ma9"),
        "ema20": row.get("ema20"),
        "rsi14": row.get("rsi14"),
        "complete": bool(row.get("complete", True)),
        "source": row.get("source"),
    }


def _historical_payload(state, security_id: str, key: str):
    if key == "1w":
        return {
            "status": "UNAVAILABLE_NATIVE_DHAN_WEEKLY_CANDLE",
            "synthetic_candles": False,
            "source": "DHAN",
            "note": "Dhan v2 documented historical candle endpoints provide daily and minute intervals, not a native weekly equity candle. Psygrid never aggregates daily candles into weekly candles."
        }
    rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
    return [normalize_candle(row) for row in rows]


def market_live_json(state) -> dict:
    snap = state.snapshot()
    payload = {
        "service": "PSYGRID",
        "schema_version": "1.1",
        "session": {
            "status": snap["session_status"],
            "date": snap["session_date"],
            "timezone": state.settings.timezone,
            "market_start": state.settings.market_start,
            "market_end": state.settings.market_end,
            "feed_status": snap["feed_status"],
            "last_tick_utc": snap["last_tick_at"],
            "last_feed_error": snap["last_feed_error"] or None,
        },
        "rules": {
            "storage": "RAM_ONLY",
            "synthetic_candles": False,
            "source": "DHAN",
            "live_candle_source": "DHAN_WEBSOCKET_QUOTE",
            "historical_candle_source": "DHAN_HISTORICAL_API",
        },
        "stock_count": snap["stock_count"],
        "stocks": {},
    }
    if snap["session_status"] != "LIVE":
        return payload

    with state.lock:
        for security_id, meta in sorted(state.instruments.items(), key=lambda x: x[1]["symbol"]):
            live_rows = state.live_enriched(security_id)
            payload["stocks"][meta["symbol"]] = {
                "security_id": security_id,
                "exchange_segment": meta["exchange_segment"],
                "instrument": meta["instrument"],
                "timeframes": {
                    "1m": [normalize_candle(row) for row in live_rows],
                    "5m": _historical_payload(state, security_id, "5m"),
                    "15m": _historical_payload(state, security_id, "15m"),
                    "1h": _historical_payload(state, security_id, "1h"),
                    "1d": _historical_payload(state, security_id, "1d"),
                    "1w": _historical_payload(state, security_id, "1w"),
                },
            }
    return payload


def stock_json(state, symbol: str, timeframe: Optional[str] = None) -> dict:
    snap = state.snapshot()
    symbol = symbol.upper()
    with state.lock:
        found = next(((sid, meta) for sid, meta in state.instruments.items() if meta["symbol"] == symbol), None)
        if found is None:
            return {"service": "PSYGRID", "symbol": symbol, "status": "NOT_FOUND"}

        security_id, meta = found
        payload = {
            "service": "PSYGRID",
            "schema_version": "1.1",
            "symbol": symbol,
            "security_id": security_id,
            "exchange_segment": meta["exchange_segment"],
            "instrument": meta["instrument"],
            "session": {
                "status": snap["session_status"],
                "date": snap["session_date"],
                "timezone": state.settings.timezone,
                "feed_status": snap["feed_status"],
                "last_tick_utc": snap["last_tick_at"],
                "last_feed_error": snap["last_feed_error"] or None,
            },
            "rules": {"storage": "RAM_ONLY", "synthetic_candles": False, "source": "DHAN"},
            "timeframes": {},
        }
        if snap["session_status"] != "LIVE":
            return payload

        requested = [timeframe] if timeframe else list(TIMEFRAMES)
        for key in requested:
            if key == "1m":
                payload["timeframes"][key] = [normalize_candle(row) for row in state.live_enriched(security_id)]
            else:
                payload["timeframes"][key] = _historical_payload(state, security_id, key)
        return payload


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str, timeframe: Optional[str] = None) -> str:
    return dumps_json(stock_json(state, symbol, timeframe))
