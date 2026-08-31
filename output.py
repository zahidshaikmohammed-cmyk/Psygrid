from __future__ import annotations

import json
from typing import Optional

from indicators import ema, rsi, sma, vwap


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
    return rows


def json_value(value):
    return None if value is None else value


def normalize_candle(row: dict) -> dict:
    return {
        "timestamp": row.get("timestamp"),
        "open": json_value(row.get("open")),
        "high": json_value(row.get("high")),
        "low": json_value(row.get("low")),
        "close": json_value(row.get("close")),
        "volume": json_value(row.get("volume")),
        "vwap": json_value(row.get("vwap")),
        "ma9": json_value(row.get("ma9")),
        "ema20": json_value(row.get("ema20")),
        "rsi14": json_value(row.get("rsi14")),
        "source": row.get("source"),
    }


def market_live_json(state) -> dict:
    snap = state.snapshot()
    payload = {
        "service": "PSYGRID",
        "schema_version": "1.0",
        "session": {
            "status": snap["session_status"],
            "date": snap["session_date"],
            "feed_status": snap["feed_status"],
            "last_tick_utc": snap["last_tick_at"],
        },
        "rules": {
            "storage": "RAM_ONLY",
            "synthetic_candles": False,
            "source": "DHAN",
        },
        "stock_count": snap["stock_count"],
        "stocks": {},
    }
    if snap["session_status"] != "LIVE":
        return payload

    with state.lock:
        for security_id, meta in sorted(state.instruments.items(), key=lambda x: x[1]["symbol"]):
            rows = state.live_enriched(security_id)
            if not rows:
                continue
            payload["stocks"][meta["symbol"]] = {
                "security_id": security_id,
                "exchange_segment": meta["exchange_segment"],
                "instrument": meta["instrument"],
                "live_1m": normalize_candle(rows[-1]),
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
            "schema_version": "1.0",
            "symbol": symbol,
            "security_id": security_id,
            "exchange_segment": meta["exchange_segment"],
            "instrument": meta["instrument"],
            "session": {
                "status": snap["session_status"],
                "date": snap["session_date"],
                "feed_status": snap["feed_status"],
                "last_tick_utc": snap["last_tick_at"],
            },
            "rules": {"storage": "RAM_ONLY", "synthetic_candles": False, "source": "DHAN"},
            "timeframes": {},
        }
        if snap["session_status"] != "LIVE":
            return payload

        requested = [timeframe] if timeframe else ["1m", "5m", "15m", "1h", "1d", "1w"]
        for key in requested:
            if key == "1m":
                rows = state.live_enriched(security_id)
                payload["timeframes"][key] = [normalize_candle(r) for r in rows]
            elif key == "1w":
                payload["timeframes"][key] = {
                    "status": "UNAVAILABLE_NATIVE_DHAN_WEEKLY_CANDLE",
                    "synthetic_candles": False,
                    "note": "Weekly candles are never synthesized from daily candles."
                }
            else:
                rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
                payload["timeframes"][key] = [normalize_candle(r) for r in rows]
        return payload


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str, timeframe: Optional[str] = None) -> str:
    return dumps_json(stock_json(state, symbol, timeframe))
