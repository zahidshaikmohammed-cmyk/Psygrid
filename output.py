from __future__ import annotations

import json
from datetime import datetime, timezone
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
            "requested_count": state.settings.weekly_lookback,
            "source": "DHAN",
            "note": "Dhan's documented historical candle API exposes daily and minute intervals, not a native weekly equity candle endpoint. Psygrid never synthesizes weekly candles.",
        }
    rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
    if key == "1d":
        rows = rows[-state.settings.daily_lookback:]
    return [normalize_candle(row) for row in rows]


def _stock_payload(state, security_id: str, meta: dict) -> dict:
    live_rows = state.live_enriched(security_id)
    freshness = state.freshness(security_id)
    live_valid = freshness["live_data_valid"]
    live_ltp = state.last_ltp_by_security.get(security_id) if live_valid else None
    live_ltt_epoch = state.last_ltt_by_security.get(security_id) if live_valid else None
    live_ltt_utc = (
        datetime.fromtimestamp(live_ltt_epoch, timezone.utc).isoformat()
        if live_ltt_epoch else None
    )
    current_candle = state.current_1m.get(security_id)
    return {
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "instrument": meta["instrument"],
        "freshness": freshness,
        "current": {
            "ltp": live_ltp,
            "timestamp": current_candle.get("timestamp") if current_candle else None,
            "last_trade_time_utc": live_ltt_utc,
            "candle_complete": bool(current_candle.get("complete")) if current_candle else False,
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


def market_live_json(state, stock_range: Optional[tuple[int, int]] = None) -> dict:
    snap = state.snapshot()
    ordered = sorted(state.instruments.items(), key=lambda x: x[1]["symbol"])
    total_stock_count = len(ordered)
    if stock_range is not None:
        start, end = stock_range
        ordered = ordered[start:end]
    selected_count = len(ordered)
    payload = {
        "service": "PSYGRID",
        "schema_version": "2.1",
        "view": "MASTER" if stock_range is None else ("A" if stock_range == (0, 45) else "B"),
        "universe_stock_count": total_stock_count,
        "returned_stock_count": selected_count,
        "session": {
            "status": snap["session_status"],
            "date": snap["session_date"],
            "timezone": state.settings.timezone,
            "market_start": state.settings.market_start,
            "market_end": state.settings.market_end,
            "feed_status": snap["feed_status"],
            "stream_health": snap["stream_health"],
            "last_tick_utc": snap["last_tick_at"],
            "last_tick_age_seconds": snap["last_tick_age_seconds"],
            "max_live_age_seconds": snap["max_live_age_seconds"],
            "live_stock_count": snap["live_stock_count"],
            "last_feed_error": snap["last_feed_error"] or None,
            "websocket_connected_at": snap["websocket_connected_at"],
            "last_message_at": snap["last_message_at"],
            "last_message_type": snap["last_message_type"],
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
            "intraday_history_days": state.settings.intraday_history_days,
            "daily_history_candles": state.settings.daily_lookback,
            "weekly_candles": "NATIVE_DHAN_ONLY",
            "weekly_synthetic_policy": "FORBIDDEN",
            "live_freshness_policy": f"MAX_{state.settings.max_live_age_seconds}_SECONDS",
            "live_acquisition": "PERSISTENT_WEBSOCKET_NO_POLLING",
        },
        "feed_diagnostics": {
            "stock_count": snap["stock_count"],
            "subscribed_count": snap["subscribed_count"],
            "feed_messages": snap["feed_messages"],
            "quote_packets": snap["quote_packets"],
            "live_quotes": snap["live_quotes"],
            "websocket_reconnects": snap["websocket_reconnects"],
        },
        "stocks": {},
    }
    if snap["session_status"] != "LIVE":
        return payload
    with state.lock:
        for security_id, meta in ordered:
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
            "schema_version": "2.0",
            "symbol": symbol,
            "security_id": security_id,
            "exchange_segment": meta["exchange_segment"],
            "instrument": meta["instrument"],
            "session": {
                "status": snap["session_status"],
                "date": snap["session_date"],
                "timezone": state.settings.timezone,
                "feed_status": snap["feed_status"],
                "stream_health": snap["stream_health"],
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
                "candle_vwap_method": "TYPICAL_PRICE_VOLUME_WEIGHTED_DAILY_RESET",
                "dhan_day_vwap_source": "DHAN_MARKET_QUOTE_AVERAGE_PRICE",
                "intraday_history_days": state.settings.intraday_history_days,
                "weekly_candles": "NATIVE_DHAN_ONLY",
                "weekly_synthetic_policy": "FORBIDDEN",
                "live_freshness_policy": f"MAX_{state.settings.max_live_age_seconds}_SECONDS",
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
