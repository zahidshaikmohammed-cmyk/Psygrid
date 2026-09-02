from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap
from output import market_live_json as _market_live_json, normalize_candle

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
TIMEFRAME_MINUTES = {"5m": 5, "15m": 15, "1h": 60}


def _aggregate_1m(candles: list[dict], interval_minutes: int, timezone: ZoneInfo) -> list[dict]:
    """Aggregate finalized 1m OHLCV into NSE session-aligned bars for warmup."""
    buckets: dict[tuple[str, int], dict] = {}
    anchor_seconds = 9 * 3600 + 15 * 60
    width = interval_minutes * 60
    for source in candles:
        try:
            epoch = int(source.get("epoch", source["timestamp"]))
            dt = datetime.fromtimestamp(epoch, timezone)
            day = dt.date().isoformat()
            seconds = dt.hour * 3600 + dt.minute * 60 + dt.second
            bucket = (seconds - anchor_seconds) // width
            if bucket < 0:
                continue
            key = (day, bucket)
            row = buckets.get(key)
            if row is None:
                start_local_seconds = anchor_seconds + bucket * width
                hour, rem = divmod(start_local_seconds, 3600)
                minute, second = divmod(rem, 60)
                start_dt = dt.replace(hour=hour, minute=minute, second=second, microsecond=0)
                start_epoch = int(start_dt.timestamp())
                row = {
                    "timestamp": start_epoch,
                    "epoch": start_epoch,
                    "open": float(source["open"]),
                    "high": float(source["high"]),
                    "low": float(source["low"]),
                    "close": float(source["close"]),
                    "volume": int(source.get("volume", 0) or 0),
                    "complete": True,
                    "source": "PSYGRID_1M_WARMUP_AGGREGATION",
                }
                buckets[key] = row
            else:
                row["high"] = max(row["high"], float(source["high"]))
                row["low"] = min(row["low"], float(source["low"]))
                row["close"] = float(source["close"])
                row["volume"] += int(source.get("volume", 0) or 0)
        except (KeyError, TypeError, ValueError, OSError):
            continue
    return [buckets[key] for key in sorted(buckets)]


def _enrich(rows: list[dict], settings) -> list[dict]:
    rows = [dict(row) for row in rows]
    rows.sort(key=lambda r: int(r["timestamp"]))
    closes = [float(r["close"]) for r in rows]
    out = []
    day_rows: list[dict] = []
    current_day = None
    for idx, row in enumerate(rows):
        day = datetime.fromtimestamp(int(row["timestamp"]), PUBLIC_TZ).date()
        if day != current_day:
            day_rows = []
            current_day = day
        day_rows.append(row)
        prefix = closes[: idx + 1]
        item = dict(row)
        item["vwap"] = vwap(day_rows)
        item["ma9"] = sma(prefix, settings.ma_period)
        item["ema20"] = ema(prefix, settings.ema_period)
        item["rsi14"] = rsi(prefix, settings.rsi_period)
        item["complete"] = True
        out.append(item)
    return out


def _timeframe_payload(state, security_id: str, key: str) -> list[dict]:
    if key not in TIMEFRAME_MINUTES:
        return None
    interval = TIMEFRAME_MINUTES[key]
    today = datetime.now(PUBLIC_TZ).date()
    seed = list(state.indicator_seed_1m.get(security_id, []))
    warmup = _aggregate_1m(seed, interval, PUBLIC_TZ)
    native_today = [dict(c) for c in state.historical.get(security_id, {}).get(key, [])]
    combined = warmup + native_today
    enriched = _enrich(combined, state.settings)
    current = [r for r in enriched if datetime.fromtimestamp(int(r["timestamp"]), PUBLIC_TZ).date() == today]
    return [normalize_candle(r) for r in current]


def _stock_fixups(state, payload: dict) -> None:
    for symbol, stock in payload.get("stocks", {}).items():
        security_id = str(stock.get("security_id", ""))
        received = state.last_tick_received_epoch.get(security_id)
        if received is not None:
            stock["last_tick_timestamp"] = datetime.fromtimestamp(received, PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
        else:
            stock["last_tick_timestamp"] = None
        if stock.get("timeframes"):
            for key in TIMEFRAME_MINUTES:
                stock["timeframes"][key] = _timeframe_payload(state, security_id, key)


def market_live_json(state, stock_range=None) -> dict:
    payload = _market_live_json(state, stock_range)
    if payload.get("session", {}).get("status") == "LIVE":
        _stock_fixups(state, payload)
    return payload
