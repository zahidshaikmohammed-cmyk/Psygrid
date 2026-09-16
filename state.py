from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo


class PsygridState:
    """RAM-only market state for the canonical 1-minute OHLCV feed.

    Runtime stores no higher timeframes, depth, indicators, signals, or
    execution metadata. WebSocket quotes build native 1m candles; the Dhan
    Quote API supplies current reference values and recovery.
    """

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.session_date: Optional[str] = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        self.last_tick_at: Optional[str] = None
        self.last_tick_epoch: Optional[float] = None
        self.last_tick_received_epoch: Optional[float] = None
        self.last_tick_by_security: Dict[str, float] = {}
        self.last_ltp_by_security: Dict[str, float] = {}
        self.last_ltt_by_security: Dict[str, int] = {}
        self.data_plan_status = "UNKNOWN"
        self.data_validity = None
        self.token_validity = None
        self.instruments: Dict[str, dict] = {}
        self.live_candles: Dict[str, List[dict]] = defaultdict(list)
        self.current_1m: Dict[str, Optional[dict]] = {}
        self.prev_cumulative_volume: Dict[str, int] = {}
        self.last_trade_key: Dict[str, tuple] = {}
        self.market_reference: Dict[str, dict] = {}
        self.feed_messages = 0
        self.quote_packets = 0
        self.live_quotes = 0
        self.websocket_reconnects = 0
        self.last_message_type: Optional[str] = None
        self.last_message_at: Optional[str] = None
        self.websocket_connected_at: Optional[str] = None
        self.subscribed_count = 0

    def reset(self) -> None:
        with self.lock:
            self.__init_runtime()
            self.instruments.clear()

    def __init_runtime(self) -> None:
        self.session_date = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        self.last_tick_at = None
        self.last_tick_epoch = None
        self.last_tick_received_epoch = None
        self.last_tick_by_security.clear()
        self.last_ltp_by_security.clear()
        self.last_ltt_by_security.clear()
        self.data_plan_status = "UNKNOWN"
        self.data_validity = None
        self.token_validity = None
        self.live_candles.clear()
        self.current_1m.clear()
        self.prev_cumulative_volume.clear()
        self.last_trade_key.clear()
        self.market_reference.clear()
        self.feed_messages = 0
        self.quote_packets = 0
        self.live_quotes = 0
        self.websocket_reconnects = 0
        self.last_message_type = None
        self.last_message_at = None
        self.websocket_connected_at = None
        self.subscribed_count = 0

    def begin(self, session_date: str, instruments: list) -> None:
        with self.lock:
            self.live_candles.clear()
            self.current_1m.clear()
            self.prev_cumulative_volume.clear()
            self.last_trade_key.clear()
            self.market_reference.clear()
            self.session_date = session_date
            self.session_status = "LIVE"
            self.feed_status = "STARTING"
            self.last_feed_error = ""
            self.last_tick_at = None
            self.last_tick_epoch = None
            self.last_tick_received_epoch = None
            self.last_tick_by_security = {}
            self.last_ltp_by_security = {}
            self.last_ltt_by_security = {}
            self.instruments = {
                str(i.security_id): {
                    "symbol": i.symbol,
                    "security_id": str(i.security_id),
                    "exchange_segment": i.exchange_segment,
                    "instrument": i.instrument,
                }
                for i in instruments
            }
            self.current_1m = {str(i.security_id): None for i in instruments}
            self.feed_messages = 0
            self.quote_packets = 0
            self.live_quotes = 0
            self.websocket_reconnects = 0
            self.last_message_type = None
            self.last_message_at = None
            self.websocket_connected_at = None
            self.subscribed_count = len(instruments)

    def set_profile(self, profile: dict) -> None:
        with self.lock:
            self.data_plan_status = str(profile.get("dataPlan", "UNKNOWN"))
            self.data_validity = profile.get("dataValidity")
            self.token_validity = profile.get("tokenValidity")

    def set_feed_status(self, status: str, error: str = "") -> None:
        with self.lock:
            self.feed_status = status
            if error:
                self.last_feed_error = error

    def mark_websocket_connected(self, subscribed_count: int) -> None:
        with self.lock:
            self.feed_status = "CONNECTED"
            self.websocket_connected_at = datetime.now(timezone.utc).isoformat()
            self.last_feed_error = ""
            self.subscribed_count = int(subscribed_count)

    def mark_websocket_reconnecting(self, reason: str) -> None:
        with self.lock:
            self.feed_status = "RECONNECTING"
            self.websocket_reconnects += 1
            self.last_feed_error = reason

    def mark_websocket_error(self, error: str) -> None:
        with self.lock:
            self.feed_status = "ERROR"
            self.last_feed_error = error

    def record_feed_message(self, packet_type: str) -> None:
        with self.lock:
            if self.session_status != "LIVE":
                return
            self.feed_messages += 1
            self.last_message_type = packet_type
            self.last_message_at = datetime.now(timezone.utc).isoformat()
            if packet_type.lower() in {"quote data", "quote", "full data", "full"}:
                self.quote_packets += 1

    def set_market_reference(self, security_id: str, *, previous_close=None, today_open=None) -> None:
        with self.lock:
            security_id = str(security_id)
            if security_id not in self.instruments:
                return
            row = dict(self.market_reference.get(security_id, {}))
            for key, value in (("previous_close", previous_close), ("today_open", today_open)):
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                if value > 0:
                    row[key] = value
            self.market_reference[security_id] = row

    def apply_quote_snapshot(self, snapshot: dict) -> None:
        """Persist only previous-close, today's-open and current LTP."""
        with self.lock:
            for security_id, row in snapshot.items():
                security_id = str(security_id)
                if security_id not in self.instruments or not isinstance(row, dict):
                    continue
                ohlc = row.get("ohlc") if isinstance(row.get("ohlc"), dict) else {}
                previous_close = row.get("previous_close", row.get("prev_close", ohlc.get("close")))
                today_open = row.get("open", ohlc.get("open"))
                self.set_market_reference(security_id, previous_close=previous_close, today_open=today_open)
                ltp = row.get("last_price", row.get("LTP", row.get("ltp")))
                try:
                    ltp = float(ltp)
                except (TypeError, ValueError):
                    ltp = None
                if ltp is not None and ltp > 0:
                    self.last_ltp_by_security[security_id] = ltp

    def record_live_quote(self, security_id: str, ltt_epoch: int) -> None:
        with self.lock:
            security_id = str(security_id)
            if self.session_status != "LIVE" or security_id not in self.instruments:
                return
            received_now = datetime.now(timezone.utc).timestamp()
            self.live_quotes += 1
            self.last_tick_epoch = float(ltt_epoch)
            self.last_tick_at = datetime.fromtimestamp(ltt_epoch, timezone.utc).isoformat()
            self.last_tick_received_epoch = received_now
            self.last_tick_by_security[security_id] = received_now
            self.last_ltt_by_security[security_id] = int(ltt_epoch)

    def merge_today_1m_history(self, security_id: str, candles: List[dict]) -> None:
        security_id = str(security_id)
        with self.lock:
            existing = {}
            for candle in self.live_candles.get(security_id, []):
                if candle.get("complete", True):
                    item = dict(candle)
                    epoch = int(item.get("epoch", item["timestamp"]))
                    item["epoch"] = epoch
                    existing[epoch // 60] = item
            for candle in candles:
                if not isinstance(candle, dict) or not candle.get("complete", True):
                    continue
                try:
                    item = dict(candle)
                    epoch = int(item.get("epoch", item["timestamp"]))
                except (KeyError, TypeError, ValueError):
                    continue
                item["epoch"] = epoch
                item["complete"] = True
                existing[epoch // 60] = item
            self.live_candles[security_id] = [existing[k] for k in sorted(existing)]

    def seed_cumulative_volume(self, security_id: str, cumulative_volume: int) -> None:
        with self.lock:
            security_id = str(security_id)
            if security_id in self.instruments:
                self.prev_cumulative_volume[security_id] = max(0, int(cumulative_volume))

    def update_quote(self, security_id: str, quote: dict) -> None:
        security_id = str(security_id)
        with self.lock:
            if security_id not in self.instruments or self.session_status != "LIVE":
                return
            try:
                ltp = float(quote["LTP"])
                ltt_epoch = int(quote["LTT_EPOCH"])
                cumulative_volume = int(quote.get("volume", 0) or 0)
                ltq = int(quote.get("LTQ", quote.get("ltq", 0)) or 0)
            except (KeyError, TypeError, ValueError):
                return
            if ltt_epoch <= 0 or ltp <= 0 or cumulative_volume < 0:
                return

            self.last_ltp_by_security[security_id] = ltp
            self.last_ltt_by_security[security_id] = ltt_epoch
            minute_key = ltt_epoch - (ltt_epoch % 60)
            current = self.current_1m.get(security_id)
            if current is not None:
                current_minute = int(current["epoch"])
                if minute_key < current_minute:
                    return
                if minute_key > current_minute:
                    current["complete"] = True
                    self.live_candles[security_id].append(dict(current))
                    self.current_1m[security_id] = None

            previous_volume = self.prev_cumulative_volume.get(security_id)
            if previous_volume is None:
                previous_volume = cumulative_volume
            delta_volume = max(0, cumulative_volume - previous_volume)
            self.prev_cumulative_volume[security_id] = cumulative_volume
            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            if trade_key == self.last_trade_key.get(security_id):
                return
            self.last_trade_key[security_id] = trade_key

            candle = self.current_1m.get(security_id)
            if candle is None:
                self.current_1m[security_id] = {
                    "timestamp": minute_key,
                    "epoch": minute_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": delta_volume,
                    "source": "DHAN_WEBSOCKET_QUOTE",
                    "complete": False,
                }
            else:
                candle["high"] = max(float(candle["high"]), ltp)
                candle["low"] = min(float(candle["low"]), ltp)
                candle["close"] = ltp
                candle["volume"] = int(candle["volume"]) + delta_volume

    def finalize_current(self) -> None:
        with self.lock:
            for security_id, candle in list(self.current_1m.items()):
                if candle is not None:
                    item = dict(candle)
                    item["complete"] = True
                    self.live_candles[security_id].append(item)
                    self.current_1m[security_id] = None

    def freshness(self, security_id: str, now_epoch: Optional[float] = None) -> dict:
        with self.lock:
            now_epoch = now_epoch or datetime.now(timezone.utc).timestamp()
            last = self.last_tick_by_security.get(str(security_id))
            if last is None:
                return {"status": "NO_LIVE_QUOTE", "data_age_seconds": None, "live_data_valid": False, "source": None}
            age = max(0.0, now_epoch - last)
            valid = age <= self.settings.max_live_age_seconds
            return {"status": "LIVE" if valid else "STALE", "data_age_seconds": round(age, 3), "live_data_valid": valid, "source": "DHAN_WEBSOCKET_FULL"}

    def snapshot(self) -> dict:
        with self.lock:
            now_epoch = datetime.now(timezone.utc).timestamp()
            live_count = sum(
                1 for security_id in self.instruments
                if security_id in self.last_tick_by_security
                and now_epoch - self.last_tick_by_security[security_id] <= self.settings.max_live_age_seconds
            )
            if self.session_status == "LIVE" and live_count == 0:
                stream_health = "CONNECTED_NO_LIVE_QUOTES" if self.feed_status == "CONNECTED" else self.feed_status
            elif self.session_status == "LIVE" and live_count == len(self.instruments) and self.instruments:
                stream_health = "FULL_LIVE"
            elif self.session_status == "LIVE":
                stream_health = "PARTIAL_LIVE"
            else:
                stream_health = self.feed_status
            return {
                "session_date": self.session_date,
                "session_status": self.session_status,
                "feed_status": self.feed_status,
                "stream_health": stream_health,
                "last_feed_error": self.last_feed_error,
                "last_tick_at": self.last_tick_at,
                "last_tick_age_seconds": round(now_epoch - self.last_tick_received_epoch, 3) if self.last_tick_received_epoch else None,
                "max_live_age_seconds": self.settings.max_live_age_seconds,
                "live_stock_count": live_count,
                "subscribed_count": self.subscribed_count,
                "feed_messages": self.feed_messages,
                "quote_packets": self.quote_packets,
                "live_quotes": self.live_quotes,
                "websocket_reconnects": self.websocket_reconnects,
                "last_message_type": self.last_message_type,
                "last_message_at": self.last_message_at,
                "websocket_connected_at": self.websocket_connected_at,
                "data_plan_status": self.data_plan_status,
                "data_validity": self.data_validity,
                "token_validity": self.token_validity,
                "stock_count": len(self.instruments),
                "one_minute_candles_only": True,
                "depth_enabled": False,
                "higher_timeframes_enabled": False,
                "indicators_enabled": False,
            }
