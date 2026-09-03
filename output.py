from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap

TIMEFRAMES = ("1m", "5m", "15m", "1h", "1d", "1w")
PUBLIC_PRICE_DECIMALS = 4
PUBLIC_TIMEZONE = ZoneInfo("Asia/Kolkata")
PUBLIC_TIMEZONE_NAME = "Asia/Kolkata"


def _price(value):
    if value is None:
        return None
    try:
        return round(float(value), PUBLIC_PRICE_DECIMALS)
    except (TypeError, ValueError):
        return value


def _ist_timestamp(value) -> Optional[str]:
    """Return every public timestamp in explicit Indian Standard Time."""
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(float(value), timezone.utc).astimezone(PUBLIC_TIMEZONE)
        elif isinstance(value, str):
            text = value.strip()
            try:
                numeric = float(text)
                dt = datetime.fromtimestamp(numeric, timezone.utc).astimezone(PUBLIC_TIMEZONE)
            except ValueError:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=PUBLIC_TIMEZONE)
                dt = parsed.astimezone(PUBLIC_TIMEZONE)
        else:
            return None
        return dt.strftime("%Y-%m-%d %H:%M:%S IST")
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _age_display(age_seconds) -> str:
    if age_seconds is None:
        return "NO LIVE QUOTE"
    seconds = max(0, int(round(float(age_seconds))))
    if seconds < 60:
        return f"{seconds}s OLD"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s OLD"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m OLD"


def _public_freshness(freshness: dict, last_trade_epoch=None) -> dict:
    age = freshness.get("data_age_seconds")
    status = freshness.get("status", "NO_LIVE_QUOTE")
    return {
        "status": status,
        "data_age_seconds": age,
        "age_display": _age_display(age),
        "last_trade_time_ist": _ist_timestamp(last_trade_epoch),
        "live_data_valid": bool(freshness.get("live_data_valid", False)),
        "source": freshness.get("source"),
    }


def enrich_history(candles: list, settings) -> list:
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
        "timestamp": _ist_timestamp(row.get("timestamp") or row.get("epoch")),
        "open": _price(row.get("open")),
        "high": _price(row.get("high")),
        "low": _price(row.get("low")),
        "close": _price(row.get("close")),
        "volume": row.get("volume"),
        "vwap": _price(row.get("vwap")),
        "dhan_day_vwap": _price(row.get("dhan_day_vwap")),
        "ma9": _price(row.get("ma9")),
        "ema20": _price(row.get("ema20")),
        "rsi14": _price(row.get("rsi14")),
    }


def _historical_payload(state, security_id: str, key: str):
    if key == "1w":
        return {
            "status": "UNAVAILABLE_NATIVE_DHAN_WEEKLY_CANDLE",
            "synthetic_candles": False,
            "requested_count": state.settings.weekly_lookback,
            "source": "DHAN",
            "note": "Native Dhan weekly equity candles are not available through the configured historical API; Psygrid never synthesizes weekly candles.",
        }
    rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
    if key == "1d":
        rows = rows[-state.settings.daily_lookback:]
    return [normalize_candle(row) for row in rows]


def _stock_payload(state, security_id: str, meta: dict) -> dict:
    live_rows = state.live_enriched(security_id)
    raw_freshness = state.freshness(security_id)
    freshness = _public_freshness(raw_freshness, state.last_ltt_by_security.get(security_id))
    live_valid = freshness["live_data_valid"]
    ws_ltp = state.last_ltp_by_security.get(security_id) if live_valid and freshness.get("source") == "DHAN_WEBSOCKET_QUOTE" else None
    with state.lock:
        recovery = state.rest_fallback_by_security.get(security_id, {})
    recovery_ltp = recovery.get("ltp") if live_valid else None
    live_ltp = ws_ltp if ws_ltp is not None else recovery_ltp
    current_candle = state.current_1m.get(security_id)
    return {
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "instrument": meta["instrument"],
        "freshness": freshness,
        "current": {
            "ltp": _price(live_ltp),
            "timestamp": _ist_timestamp(current_candle.get("timestamp")) if current_candle else None,
            "last_trade_time_ist": freshness["last_trade_time_ist"],
            "candle_complete": bool(current_candle.get("complete")) if current_candle else False,
            "dhan_day_vwap": _price(state.dhan_day_average_price.get(security_id)),
            "quote_source": freshness.get("source"),
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


def _rules(state) -> dict:
    return {
        "storage": "RAM_ONLY",
        "synthetic_candles": False,
        "source": "DHAN",
        "live_candle_source": "DHAN_WEBSOCKET_QUOTE",
        "historical_candle_source": "DHAN_HISTORICAL_API",
        "calculated_indicator_source": "PSYGRID_FROM_GENUINE_DHAN_OHLCV",
        "candle_vwap_method": "TYPICAL_PRICE_VOLUME_WEIGHTED_DAILY_RESET",
        "dhan_day_vwap_source": "DHAN_MARKET_QUOTE_AVERAGE_PRICE",
        "public_timeframe_retention": {
            "1m": "CURRENT_SESSION_DAY_ONLY",
            "5m": "CURRENT_SESSION_DAY_ONLY",
            "15m": "CURRENT_SESSION_DAY_ONLY",
            "1h": "CURRENT_SESSION_DAY_ONLY",
            "1d": f"PREVIOUS_{state.settings.daily_lookback}_TRADING_DAYS",
            "1w": f"PREVIOUS_{state.settings.weekly_lookback}_NATIVE_DHAN_CANDLES_ONLY",
        },
        "public_candle_fields": "timestamp,open,high,low,close,volume,vwap,dhan_day_vwap,ma9,ema20,rsi14",
        "public_timestamp_timezone": PUBLIC_TIMEZONE_NAME,
        "public_timestamp_format": "YYYY-MM-DD HH:MM:SS IST",
        "public_freshness_display": "data_age_seconds + age_display + last_trade_time_ist + source",
        "public_candle_repeated_metadata": "OMITTED_FROM_EACH_CANDLE; PRESERVED_AT_RULES/SOURCE LEVEL",
        "public_numeric_precision": f"PRICES_AND_INDICATORS_ROUNDED_TO_{PUBLIC_PRICE_DECIMALS}_DECIMALS_FOR_TRANSPORT_ONLY",
        "indicator_seed_policy": {
            "1m": f"UP_TO_{state.settings.intraday_history_days}_PRIOR_DAYS_INTERNAL_ONLY",
            "public_seed_exposure": False,
        },
        "weekly_synthetic_policy": "FORBIDDEN",
        "live_freshness_policy": f"MAX_{state.settings.max_live_age_seconds}_SECONDS",
        "live_acquisition": "WEBSOCKET_PRIMARY_DHAN_QUOTE_RECOVERY",
    }


def market_live_json(state, stock_range: Optional[tuple[int, int]] = None) -> dict:
    snap = state.snapshot()
    payload = {
        "service": "PSYGRID",
        "schema_version": "2.2",
        "session": {
            "status": snap["session_status"],
            "date": snap["session_date"],
            "timezone": PUBLIC_TIMEZONE_NAME,
            "current_time_ist": datetime.now(PUBLIC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST"),
            "market_start": state.settings.market_start,
            "market_end": state.settings.market_end,
            "feed_status": snap["feed_status"],
            "stream_health": snap["stream_health"],
            "last_tick_ist": _ist_timestamp(snap["last_tick_at"]),
            "last_tick_age_seconds": snap["last_tick_age_seconds"],
            "last_tick_age_display": _age_display(snap["last_tick_age_seconds"]),
            "max_live_age_seconds": snap["max_live_age_seconds"],
            "live_stock_count": snap["live_stock_count"],
            "last_feed_error": snap["last_feed_error"] or None,
            "websocket_connected_ist": _ist_timestamp(snap["websocket_connected_at"]),
            "last_message_ist": _ist_timestamp(snap["last_message_at"]),
            "last_message_type": snap["last_message_type"],
        },
        "dhan": {
            "data_plan": snap["data_plan_status"],
            "data_validity": snap["data_validity"],
            "token_validity": snap["token_validity"],
        },
        "rules": _rules(state),
        "feed_diagnostics": {
            "stock_count": snap["stock_count"],
            "subscribed_count": snap["subscribed_count"],
            "feed_messages": snap["feed_messages"],
            "quote_packets": snap["quote_packets"],
            "live_quotes": snap["live_quotes"],
            "websocket_reconnects": snap["websocket_reconnects"],
        },
        "stock_count": 0,
        "stocks": {},
    }
    if snap["session_status"] != "LIVE":
        return payload
    with state.lock:
        items = sorted(state.instruments.items(), key=lambda x: x[1]["symbol"])
        if stock_range is not None:
            start, end = stock_range
            items = items[start:end]
        payload["stock_count"] = len(items)
        payload["stocks"] = {meta["symbol"]: _stock_payload(state, security_id, meta) for security_id, meta in items}
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
            "schema_version": "2.2",
            "symbol": symbol,
            "security_id": security_id,
            "exchange_segment": meta["exchange_segment"],
            "instrument": meta["instrument"],
            "session": {
                "status": snap["session_status"],
                "date": snap["session_date"],
                "timezone": PUBLIC_TIMEZONE_NAME,
                "current_time_ist": datetime.now(PUBLIC_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S IST"),
                "market_start": state.settings.market_start,
                "market_end": state.settings.market_end,
                "feed_status": snap["feed_status"],
                "stream_health": snap["stream_health"],
                "last_tick_ist": _ist_timestamp(snap["last_tick_at"]),
                "last_tick_age_seconds": snap["last_tick_age_seconds"],
                "last_tick_age_display": _age_display(snap["last_tick_age_seconds"]),
                "max_live_age_seconds": snap["max_live_age_seconds"],
                "last_feed_error": snap["last_feed_error"] or None,
            },
            "dhan": {
                "data_plan": snap["data_plan_status"],
                "data_validity": snap["data_validity"],
                "token_validity": snap["token_validity"],
            },
            "rules": _rules(state),
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
