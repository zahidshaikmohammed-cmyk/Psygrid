from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma, vwap


class PsygridState:
    """RAM-only market state. Nothing is persisted to disk or a database."""

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.session_date: Optional[str] = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        self.last_tick_at: Optional[str] = None
        self.instruments: Dict[str, dict] = {}
        self.live_candles: Dict[str, List[dict]] = defaultdict(list)
        self.current_1m: Dict[str, Optional[dict]] = {}
        self.historical: Dict[str, Dict[str, List[dict]]] = defaultdict(dict)
        self.prev_cumulative_volume: Dict[str, int] = {}
        self.last_trade_key: Dict[str, tuple] = {}

    def reset(self) -> None:
        with self.lock:
            self.session_date = None
            self.session_status = "CLOSED"
            self.feed_status = "STOPPED"
            self.last_feed_error = ""
            self.last_tick_at = None
            self.instruments.clear()
            self.live_candles.clear()
            self.current_1m.clear()
            self.historical.clear()
            self.prev_cumulative_volume.clear()
            self.last_trade_key.clear()

    def begin(self, session_date: str, instruments: list) -> None:
        with self.lock:
            self.session_date = session_date
            self.session_status = "LIVE"
            self.feed_status = "STARTING"
            self.last_feed_error = ""
            self.instruments = {
                i.security_id: {
                    "symbol": i.symbol,
                    "security_id": i.security_id,
                    "exchange_segment": i.exchange_segment,
                    "instrument": i.instrument,
                }
                for i in instruments
            }
            for item in instruments:
                self.current_1m.setdefault(item.security_id, None)

    def set_historical(self, security_id: str, timeframe: str, candles: List[dict]) -> None:
        with self.lock:
            self.historical[security_id][timeframe] = [dict(c) for c in candles]

    def merge_today_1m_history(self, security_id: str, candles: List[dict]) -> None:
        """Restore only genuine Dhan 1m candles from today's historical endpoint."""
        with self.lock:
            existing = {int(c["timestamp"]) // 60: dict(c) for c in self.live_candles.get(security_id, [])}
            for candle in candles:
                existing[int(candle["timestamp"]) // 60] = dict(candle)
            self.live_candles[security_id] = [existing[k] for k in sorted(existing)]

    def seed_cumulative_volume(self, security_id: str, cumulative_volume: int) -> None:
        with self.lock:
            self.prev_cumulative_volume[security_id] = max(0, int(cumulative_volume))

    def update_quote(self, security_id: str, quote: dict) -> None:
        """Accept only genuine Dhan Quote data; never invent a candle or volume."""
        with self.lock:
            meta = self.instruments.get(security_id)
            if not meta or self.session_status != "LIVE":
                return

            try:
                ltp = float(quote["LTP"])
                ltt_epoch = int(quote["LTT_EPOCH"])
                cumulative_volume = int(quote.get("volume", 0) or 0)
                ltq = int(quote.get("LTQ", quote.get("ltq", 0)) or 0)
            except (KeyError, TypeError, ValueError):
                return

            if ltp <= 0 or cumulative_volume < 0:
                return

            now = datetime.now(timezone.utc).isoformat()
            self.last_tick_at = now

            previous_volume = self.prev_cumulative_volume.get(security_id)
            if previous_volume is None:
                # First packet establishes the genuine cumulative-volume baseline.
                self.prev_cumulative_volume[security_id] = cumulative_volume
                return

            delta_volume = cumulative_volume - previous_volume
            if delta_volume < 0:
                # Exchange/session/feed reset. Never invent the missing volume.
                self.prev_cumulative_volume[security_id] = cumulative_volume
                return
            self.prev_cumulative_volume[security_id] = cumulative_volume

            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            if trade_key == self.last_trade_key.get(security_id):
                return
            self.last_trade_key[security_id] = trade_key

            # A quote packet is a genuine Dhan market event. A candle is created
            # only for the minute represented by that event.
            minute_key = ltt_epoch - (ltt_epoch % 60)
            candle = self.current_1m.get(security_id)

            if candle is None or candle["epoch"] != minute_key:
                if candle is not None:
                    self.live_candles[security_id].append(candle)
                candle = {
                    "timestamp": datetime.fromtimestamp(minute_key, timezone.utc).astimezone(self.tz).strftime("%Y-%m-%d %H:%M:00"),
                    "epoch": minute_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": max(0, delta_volume),
                    "source": "DHAN_WEBSOCKET_QUOTE",
                }
                self.current_1m[security_id] = candle
            else:
                candle["high"] = max(float(candle["high"]), ltp)
                candle["low"] = min(float(candle["low"]), ltp)
                candle["close"] = ltp
                candle["volume"] = int(candle["volume"]) + max(0, delta_volume)

    def finalize_current(self) -> None:
        with self.lock:
            for security_id, candle in list(self.current_1m.items()):
                if candle is not None:
                    self.live_candles[security_id].append(dict(candle))
                    self.current_1m[security_id] = None

    def live_enriched(self, security_id: str) -> List[dict]:
        with self.lock:
            candles = [dict(c) for c in self.live_candles.get(security_id, [])]
            current = self.current_1m.get(security_id)
            if current is not None:
                candles.append(dict(current))

            closes = [float(c["close"]) for c in candles]
            for idx, candle in enumerate(candles):
                window = candles[: idx + 1]
                close_window = closes[: idx + 1]
                candle["vwap"] = vwap(window)
                candle["ma9"] = sma(close_window, self.settings.ma_period)
                candle["ema20"] = ema(close_window, self.settings.ema_period)
                candle["rsi14"] = rsi(close_window, self.settings.rsi_period)
                candle["complete"] = not (current is not None and idx == len(candles) - 1)
                candle.pop("epoch", None)
            return candles

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "session_date": self.session_date,
                "session_status": self.session_status,
                "feed_status": self.feed_status,
                "last_feed_error": self.last_feed_error,
                "last_tick_at": self.last_tick_at,
                "stock_count": len(self.instruments),
            }
