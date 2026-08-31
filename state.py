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
        self.data_plan_status = "UNKNOWN"
        self.data_validity = None
        self.token_validity = None
        self.instruments: Dict[str, dict] = {}
        self.live_candles: Dict[str, List[dict]] = defaultdict(list)
        self.current_1m: Dict[str, Optional[dict]] = {}
        self.indicator_seed_1m: Dict[str, List[dict]] = defaultdict(list)
        self.historical: Dict[str, Dict[str, List[dict]]] = defaultdict(dict)
        self.prev_cumulative_volume: Dict[str, int] = {}
        self.last_trade_key: Dict[str, tuple] = {}
        self.dhan_day_average_price: Dict[str, Optional[float]] = {}

    def reset(self) -> None:
        with self.lock:
            self.session_date = None
            self.session_status = "CLOSED"
            self.feed_status = "STOPPED"
            self.last_feed_error = ""
            self.last_tick_at = None
            self.data_plan_status = "UNKNOWN"
            self.data_validity = None
            self.token_validity = None
            self.instruments.clear()
            self.live_candles.clear()
            self.current_1m.clear()
            self.indicator_seed_1m.clear()
            self.historical.clear()
            self.prev_cumulative_volume.clear()
            self.last_trade_key.clear()
            self.dhan_day_average_price.clear()

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
            self.current_1m = {i.security_id: None for i in instruments}
            self.dhan_day_average_price = {i.security_id: None for i in instruments}

    def set_profile(self, profile: dict) -> None:
        with self.lock:
            self.data_plan_status = str(profile.get("dataPlan", "UNKNOWN"))
            self.data_validity = profile.get("dataValidity")
            self.token_validity = profile.get("tokenValidity")

    def set_historical(self, security_id: str, timeframe: str, candles: List[dict]) -> None:
        with self.lock:
            ordered = sorted((dict(c) for c in candles), key=lambda c: int(c["timestamp"]))
            self.historical[security_id][timeframe] = ordered

    def set_indicator_seed_1m(self, security_id: str, candles: List[dict]) -> None:
        with self.lock:
            self.indicator_seed_1m[security_id] = sorted(
                (dict(c) for c in candles if c.get("complete", True)),
                key=lambda c: int(c["timestamp"]),
            )

    def merge_today_1m_history(self, security_id: str, candles: List[dict]) -> None:
        with self.lock:
            existing = {
                int(c["timestamp"]) // 60: dict(c)
                for c in self.live_candles.get(security_id, [])
                if c.get("complete", True)
            }
            for candle in candles:
                if candle.get("complete", True):
                    existing[int(candle["timestamp"]) // 60] = dict(candle)
            self.live_candles[security_id] = [existing[k] for k in sorted(existing)]

    def seed_cumulative_volume(self, security_id: str, cumulative_volume: int) -> None:
        with self.lock:
            self.prev_cumulative_volume[security_id] = max(0, int(cumulative_volume))

    def update_quote(self, security_id: str, quote: dict) -> None:
        with self.lock:
            meta = self.instruments.get(security_id)
            if not meta or self.session_status != "LIVE":
                return
            try:
                ltp = float(quote["LTP"])
                ltt_epoch = int(quote["LTT_EPOCH"])
                cumulative_volume = int(quote.get("volume", 0) or 0)
                ltq = int(quote.get("LTQ", quote.get("ltq", 0)) or 0)
                atp_raw = quote.get("ATP", quote.get("atp", quote.get("average_price")))
                atp = float(atp_raw) if atp_raw not in (None, "") else None
            except (KeyError, TypeError, ValueError):
                return
            if ltt_epoch <= 0 or ltp <= 0 or cumulative_volume < 0:
                return
            if atp is not None and atp > 0:
                self.dhan_day_average_price[security_id] = atp

            self.last_tick_at = datetime.now(timezone.utc).isoformat()
            previous_volume = self.prev_cumulative_volume.get(security_id)
            if previous_volume is None:
                self.prev_cumulative_volume[security_id] = cumulative_volume
                return

            delta_volume = cumulative_volume - previous_volume
            if delta_volume < 0:
                self.prev_cumulative_volume[security_id] = cumulative_volume
                return
            self.prev_cumulative_volume[security_id] = cumulative_volume

            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            if trade_key == self.last_trade_key.get(security_id):
                return
            self.last_trade_key[security_id] = trade_key

            minute_key = ltt_epoch - (ltt_epoch % 60)
            candle = self.current_1m.get(security_id)
            if candle is None or candle["epoch"] != minute_key:
                if candle is not None:
                    candle["complete"] = True
                    self.live_candles[security_id].append(dict(candle))
                candle = {
                    "timestamp": datetime.fromtimestamp(minute_key, timezone.utc)
                    .astimezone(self.tz)
                    .strftime("%Y-%m-%d %H:%M:00"),
                    "epoch": minute_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": max(0, delta_volume),
                    "source": "DHAN_WEBSOCKET_QUOTE",
                    "complete": False,
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
                    candle = dict(candle)
                    candle["complete"] = True
                    self.live_candles[security_id].append(candle)
                    self.current_1m[security_id] = None

    def _day_key(self, candle: dict) -> str:
        timestamp = int(candle.get("epoch", candle["timestamp"]))
        return datetime.fromtimestamp(timestamp, self.tz).date().isoformat()

    def _enrich_live(self, seed: List[dict], candles: List[dict]) -> List[dict]:
        """Calculate today's 1m indicators using genuine prior-day warmup + today."""
        ordered_seed = sorted((dict(c) for c in seed), key=lambda c: int(c["timestamp"]))
        ordered_today = sorted((dict(c) for c in candles), key=lambda c: int(c["epoch"]))
        combined = ordered_seed + ordered_today
        closes = [float(c["close"]) for c in combined]

        out: List[dict] = []
        day_candles: List[dict] = []
        current_day: Optional[str] = None
        seed_count = len(ordered_seed)
        for idx, candle in enumerate(combined):
            day = self._day_key(candle)
            if day != current_day:
                day_candles = []
                current_day = day
            day_candles.append(candle)
            prefix_closes = closes[: idx + 1]
            if idx < seed_count:
                continue
            item = dict(candle)
            item["vwap"] = vwap(day_candles)
            item["ma9"] = sma(prefix_closes, self.settings.ma_period)
            item["ema20"] = ema(prefix_closes, self.settings.ema_period)
            item["rsi14"] = rsi(prefix_closes, self.settings.rsi_period)
            out.append(item)
        return out

    def live_enriched(self, security_id: str) -> List[dict]:
        with self.lock:
            candles = [dict(c) for c in self.live_candles.get(security_id, [])]
            current = self.current_1m.get(security_id)
            if current is not None:
                candles.append(dict(current))
            seed = [dict(c) for c in self.indicator_seed_1m.get(security_id, [])]
            enriched = self._enrich_live(seed, candles)
            for candle in enriched:
                candle.pop("epoch", None)
            return enriched

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "session_date": self.session_date,
                "session_status": self.session_status,
                "feed_status": self.feed_status,
                "last_feed_error": self.last_feed_error,
                "last_tick_at": self.last_tick_at,
                "data_plan_status": self.data_plan_status,
                "data_validity": self.data_validity,
                "token_validity": self.token_validity,
                "stock_count": len(self.instruments),
            }
