from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap


def fmt(value) -> str:
    if value is None:
        return "NA"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


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


def candle_line(row: dict) -> str:
    return "|".join([
        str(row.get("timestamp", "")),
        fmt(row.get("open")),
        fmt(row.get("high")),
        fmt(row.get("low")),
        fmt(row.get("close")),
        fmt(row.get("volume")),
        fmt(row.get("vwap")),
        fmt(row.get("ma9")),
        fmt(row.get("ema20")),
        fmt(row.get("rsi14")),
        str(row.get("source", "")),
    ])


HEADER = "TIME|OPEN|HIGH|LOW|CLOSE|VOLUME|VWAP|MA9|EMA20|RSI14|SOURCE"


def market_live_text(state) -> str:
    snap = state.snapshot()
    lines = [
        "PSYGRID=LIVE_MARKET_DATA",
        f"SESSION_STATUS={snap['session_status']}",
        f"SESSION_DATE={snap['session_date'] or 'NA'}",
        f"FEED_STATUS={snap['feed_status']}",
        f"STOCK_COUNT={snap['stock_count']}",
        f"LAST_TICK_UTC={snap['last_tick_at'] or 'NA'}",
        "SYNTHETIC_CANDLES=FALSE",
        "STORAGE=RAM_ONLY",
        "",
        "SYMBOL|SECURITY_ID|TIME|OPEN|HIGH|LOW|CLOSE|VOLUME|VWAP|MA9|EMA20|RSI14|SOURCE",
    ]
    if snap["session_status"] != "LIVE":
        lines.append("NO_ACTIVE_MARKET_SESSION")
        return "\n".join(lines) + "\n"

    with state.lock:
        for security_id, meta in sorted(state.instruments.items(), key=lambda x: x[1]["symbol"]):
            rows = state.live_enriched(security_id)
            if not rows:
                continue
            row = rows[-1]
            lines.append("|".join([
                meta["symbol"],
                security_id,
                str(row.get("timestamp", "")),
                fmt(row.get("open")),
                fmt(row.get("high")),
                fmt(row.get("low")),
                fmt(row.get("close")),
                fmt(row.get("volume")),
                fmt(row.get("vwap")),
                fmt(row.get("ma9")),
                fmt(row.get("ema20")),
                fmt(row.get("rsi14")),
                str(row.get("source", "")),
            ]))
    return "\n".join(lines) + "\n"


def stock_text(state, symbol: str, timeframe: Optional[str] = None) -> str:
    snap = state.snapshot()
    symbol = symbol.upper()
    with state.lock:
        found = None
        for security_id, meta in state.instruments.items():
            if meta["symbol"] == symbol:
                found = (security_id, meta)
                break
        if found is None:
            return f"PSYGRID=STOCK\nSYMBOL={symbol}\nSTATUS=NOT_FOUND\n"

        security_id, meta = found
        lines = [
            "PSYGRID=STOCK",
            f"SYMBOL={symbol}",
            f"SECURITY_ID={security_id}",
            f"SESSION_STATUS={snap['session_status']}",
            "SYNTHETIC_CANDLES=FALSE",
            "STORAGE=RAM_ONLY",
        ]
        if snap["session_status"] != "LIVE":
            lines.append("NO_ACTIVE_MARKET_SESSION")
            return "\n".join(lines) + "\n"

        if timeframe in (None, "1m"):
            lines += ["", "TIMEFRAME=1m", HEADER]
            rows = state.live_enriched(security_id)
            lines.extend(candle_line(row) for row in rows)
            if timeframe == "1m" or timeframe is None:
                if timeframe == "1m":
                    return "\n".join(lines) + "\n"

        if timeframe in (None, "5m", "15m", "1h", "1d", "1w"):
            requested = [timeframe] if timeframe else ["5m", "15m", "1h", "1d", "1w"]
            for key in requested:
                lines += ["", f"TIMEFRAME={key}", HEADER]
                if key == "1w":
                    # Dhan's documented historical candle APIs expose daily and intraday intervals,
                    # but no native weekly equity candle endpoint. Never aggregate daily candles here:
                    # that would violate Psygrid's zero-synthetic-candle rule.
                    lines.append("STATUS=UNAVAILABLE_NATIVE_DHAN_WEEKLY_CANDLE")
                    lines.append("SYNTHETIC_WEEKLY_CANDLE=FORBIDDEN")
                    continue
                rows = enrich_history(state.historical.get(security_id, {}).get(key, []), state.settings)
                lines.extend(candle_line(row) for row in rows)
        return "\n".join(lines) + "\n"
