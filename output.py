from __future__ import annotations

import json
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap

TIMEFRAMES = ("1m", "5m", "15m", "1h", "1d", "1w")


def enrich_history(candles: list, settings) -> list:
    """Enrich genuine Dhan candles without altering their OHLCV."""
    rows = [dict(c) for c in candles]
    rows.sort(key=lambda c: int(c["timestamp"]))
    closes = [float(c["close"]) for c in rows]
    day_window = []
    current_day = None
    for idx, row in enumerate(rows):
        try:
            day = datetime.fromtimestamp(int(row["timestamp"]), ZoneInfo(settings.timezone)).date()
        except (KeyError, TypeError, ValueError, OSError):
            day = None
        if day != current_day:
            day_window = []
            current_day = day
        day_window.append(row)
        close_window = closes[: idx + 1]
        row["vwap"] = vwap(day_window)
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
        "dhan_day_vwap": row.get("dhan_day_vwap"),
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
            "note": "Weekly candles are never synthesized from daily candles."
        }
    rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
    if key == "1d":
        rows = rows[-state.settings.daily_lookback:]
    return [normalize_candle(row) for row in rows]


def _stock_payload(state, security_id: str, meta: dict) -> dict:
    live_rows = state.live_enriched(security_id)
    current = live_rows[-1] if live_rows else None
    return {
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "instrument": meta["instrument"],
        "freshness": state.freshness(security_id),
        "current": {
            "ltp": current.get("close") if current else None,
            "timestamp": current.get("timestamp") if current else None,
            "candle_complete": bool(current.get("complete")) if current else False,
            "dhan_day_vwap": state.dhan_day_average_price.get(security_id),
        },
        "timeframes": {
            "1m": [normalize_candle(row) for row in live_rows],
            "5m": _historical_payload(state, security_id, "5m"),
            "15m": _historical_payload(state, security_id, "15m"),
            "1h": _historical_payload(state, security_id, "1h"),
            "1d": _historical_payload(state, security_id, "1d"),
            "1w": _historical_payload(state, security_id, "1w"),
        },
    }


def market_live_json(state) -> dict:
    snap = state.snapshot()
    payload = {
        "service": "PSYGRID",
        "schema_version": "1.7",
        "session": {
            "status": snap["session_status"],
            "date": snap["session_date"],
            "timezone": state.settings.timezone,
            "market_start": state.settings.market_start,
            "market_end": state.settings.market_end,
            "feed_status": snap["feed_status"],
            "last_tick_utc": snap["last_tick_at"],
            "last_tick_age_seconds": snap["last_tick_age_seconds"],
            "max_live_age_seconds": snap["max_live_age_seconds"],
            "last_feed_error": snap["last_feed_error"] or None,
        },
        "dhan": {
            "data_plan": snap["data_plan_status"],
            "data_validity": snap["data_validity"],
            "token_validity": snap["token_validity"],
        },
        "rules": {
            "storage": "RAM_ONLY",
            "synthetic_candles": False,
            "source": "DHAN",
            "live_candle_source": "DHAN_WEBSOCKET_QUOTE",
            "historical_candle_source": "DHAN_HISTORICAL_API",
            "calculated_indicator_source": "PSYGRID_FROM_GENUINE_DHAN_OHLCV",
            "candle_vwap_method": "TYPICAL_PRICE_VOLUME_WEIGHTED_DAILY_RESET",
            "dhan_day_vwap_source": "DHAN_MARKET_QUOTE_AVERAGE_PRICE",
            "intraday_history_limit": "DHAN_DOCUMENTED_LAST_5_TRADING_DAYS",
            "weekly_candles": "NATIVE_DHAN_ONLY",
            "live_freshness_policy": "MAX_60_SECONDS",
            "live_acquisition": "PERSISTENT_WEBSOCKET_NO_POLLING",
        },
        "stock_count": snap["stock_count"],
        "stocks": {},
    }
    if snap["session_status"] != "LIVE":
        return payload
    with state.lock:
        for security_id, meta in sorted(state.instruments.items(), key=lambda x: x[1]["symbol"]):
            payload["stocks"][meta["symbol"]] = _stock_payload(state, security_id, meta)
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
            "schema_version": "1.7",
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
                "max_live_age_seconds": snap["max_live_age_seconds"],
                "last_feed_error": snap["last_feed_error"] or None,
            },
            "dhan": {
                "data_plan": snap["data_plan_status"],
                "data_validity": snap["data_validity"],
                "token_validity": snap["token_validity"],
            },
            "rules": {
                "storage": "RAM_ONLY",
                "synthetic_candles": False,
                "source": "DHAN",
                "candle_vwap_method": "TYPICAL_PRICE_VOLUME_WEIGHTED_DAILY_RESET",
                "dhan_day_vwap_source": "DHAN_MARKET_QUOTE_AVERAGE_PRICE",
                "live_freshness_policy": "MAX_60_SECONDS",
                "live_acquisition": "PERSISTENT_WEBSOCKET_NO_POLLING",
            },
            "timeframes": {},
        }
        if snap["session_status"] != "LIVE":
            return payload
        full = _stock_payload(state, security_id, meta)
        payload["freshness"] = full["freshness"]
        if timeframe is None:
            payload["current"] = full["current"]
            payload["timeframes"] = full["timeframes"]
            return payload
        if timeframe == "1m":
            payload["current"] = full["current"]
            payload["timeframes"]["1m"] = [normalize_candle(row) for row in state.live_enriched(security_id)]
        else:
            payload["timeframes"][timeframe] = _historical_payload(state, security_id, timeframe)
        return payload


def dumps_json(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n"


def market_live_text(state) -> str:
    return dumps_json(market_live_json(state))


def stock_text(state, symbol: str, timeframe: Optional[str] = None) -> str:
    return dumps_json(stock_json(state, symbol, timeframe))
