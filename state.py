from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from indicators import ema, rsi, sma


class PsygridState:
    """RAM-only state. Nothing in this class is persisted to disk or a database."""

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
        self.session_trade_value: Dict[str, float] = defaultdict(float)
        self.session_trade_volume: Dict[str, int] = defaultdict(int)
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
            self.session_trade_value.clear()
            self.session_trade_volume.clear()
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

    def set_historical(self, security_id: str, timeframe: str, candles: List[dict]) -> None:
        with self.lock:
            self.historical[security_id][timeframe] = candles

    def merge_today_1m_history(self, security_id: str, candles: List[dict]) -> None:
        with self.lock:
            existing = {int(c["timestamp"]) // 60: c for c in self.live_candles.get(security_id, [])}
            for candle in candles:
                existing[int(candle["timestamp"]) // 60] = candle
            self.live_candles[security_id] = [existing[k] for k in sorted(existing)]

    def seed_cumulative_volume(self, security_id: str, cumulative_volume: int) -> None:
        with self.lock:
            self.prev_cumulative_volume[security_id] = max(0, int(cumulative_volume))

    def update_quote(self, security_id: str, quote: dict) -> None:
        """Accept only a genuine Dhan Quote packet. No candle is invented."""
        now = datetime.now(timezone.utc).isoformat()
        with self.lock:
            meta = self.instruments.get(security_id)
            if not meta or self.session_status != "LIVE":
                return

            ltp = float(quote["LTP"])
            ltt_epoch = int(quote["LTT_EPOCH"])
            cumulative_volume = int(quote["volume"])
            ltq = int(quote.get("LTQ", 0) or 0)
            avg_price = float(quote.get("avg_price", 0) or 0)
            minute_key = ltt_epoch - (ltt_epoch % 60)

            previous_volume = self.prev_cumulative_volume.get(security_id, cumulative_volume)
            delta_volume = cumulative_volume - previous_volume
            if delta_volume < 0:
                # Never fabricate volume after a reset/reconnect. Wait for a fresh baseline.
                self.prev_cumulative_volume[security_id] = cumulative_volume
                return
            self.prev_cumulative_volume[security_id] = cumulative_volume

            # De-duplicate repeated quote packets carrying the same cumulative volume/time.
            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            if trade_key != self.last_trade_key.get(security_id):
                self.last_trade_key[security_id] = trade_key
                if ltq > 0:
                    self.session_trade_value[security_id] += ltp * ltq
                    self.session_trade_volume[security_id] += ltq

            if delta_volume <= 0:
                self.last_tick_at = now
                return

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
                    "volume": delta_volume,
                    "source": "DHAN_WEBSOCKET_QUOTE",
                }
                self.current_1m[security_id] = candle
            else:
                candle["high"] = max(candle["high"], ltp)
                candle["low"] = min(candle["low"], ltp)
                candle["close"] = ltp
                candle["volume"] += delta_volume

            # Dhan's Quote packet exposes the day's volume-weighted average price.
            candle["vwap"] = avg_price if avg_price > 0 else None
            self.last_tick_at = now

    def finalize_current(self) -> None:
        with self.lock:
            for security_id, candle in list(self.current_1m.items()):
                if candle is not None:
                    self.live_candles[security_id].append(candle)
                    self.current_1m[security_id] = None

    def live_enriched(self, security_id: str) -> List[dict]:
        with self.lock:
            candles = list(self.live_candles.get(security_id, []))
            current = self.current_1m.get(security_id)
            if current is not None:
                candles.append(dict(current))
            closes = [float(c["close"]) for c in candles]
            for idx, candle in enumerate(candles):
                window = closes[: idx + 1]
                candle["ma9"] = sma(window, self.settings.ma_period)
                candle["ema20"] = ema(window, self.settings.ema_period)
                candle["rsi14"] = rsi(window, self.settings.rsi_period)
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
