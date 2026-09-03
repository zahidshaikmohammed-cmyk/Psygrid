from __future__ import annotations

import time
from typing import Optional

from state import PsygridState


class RuntimeFreshnessState(PsygridState):
    """Runtime state with WebSocket-first freshness and current Dhan quote recovery."""

    def __init__(self, settings):
        super().__init__(settings)
        self.last_tick_received_by_security: dict[str, float] = {}
        self.rest_fallback_by_security: dict[str, dict] = {}

    def reset(self) -> None:
        super().reset()
        with self.lock:
            self.last_tick_received_by_security.clear()
            self.rest_fallback_by_security.clear()

    def begin(self, session_date: str, instruments: list) -> None:
        super().begin(session_date, instruments)
        with self.lock:
            self.last_tick_received_by_security = {}
            self.rest_fallback_by_security = {}

    def record_live_quote(self, security_id: str, ltt_epoch: int) -> None:
        received_now = time.time()
        super().record_live_quote(security_id, ltt_epoch)
        with self.lock:
            if self.session_status == "LIVE" and security_id in self.instruments:
                self.last_tick_received_by_security[security_id] = received_now
                self.last_tick_received_epoch = received_now
                self.rest_fallback_by_security.pop(security_id, None)

    def apply_rest_snapshot(self, snapshot: dict) -> None:
        """Use the current Dhan quote snapshot only to recover missing/stale quotes."""
        now = time.time()
        with self.lock:
            for security_id, row in snapshot.items():
                if security_id not in self.instruments or not isinstance(row, dict):
                    continue
                ltp = row.get("last_price", row.get("LTP", row.get("ltp")))
                try:
                    ltp = float(ltp)
                except (TypeError, ValueError):
                    continue
                if ltp <= 0:
                    continue
                self.rest_fallback_by_security[security_id] = {
                    "ltp": ltp,
                    "timestamp_epoch": now,
                    "source": "DHAN_REST_QUOTE_RECOVERY",
                }

    def freshness(self, security_id: str, now_epoch: Optional[float] = None) -> dict:
        with self.lock:
            now_epoch = now_epoch or time.time()
            ws_received = self.last_tick_received_by_security.get(security_id)
            rest = self.rest_fallback_by_security.get(security_id)
            rest_received = rest.get("timestamp_epoch") if rest else None
            candidates = [(ws_received, "DHAN_WEBSOCKET_FULL"), (rest_received, "DHAN_REST_QUOTE_RECOVERY")]
            received, source = max(
                ((value, src) for value, src in candidates if value is not None),
                key=lambda pair: pair[0],
                default=(None, None),
            )
            if received is None:
                return {"status": "NO_LIVE_QUOTE", "data_age_seconds": None, "live_data_valid": False, "source": None}
            age = max(0.0, now_epoch - received)
            valid = age <= self.settings.max_live_age_seconds
            return {
                "status": "LIVE" if valid else "STALE",
                "data_age_seconds": round(age, 3),
                "live_data_valid": valid,
                "source": source,
            }

    def all_live_stale(self, now_epoch: Optional[float] = None) -> bool:
        with self.lock:
            if self.session_status != "LIVE" or not self.instruments:
                return False
            now_epoch = now_epoch or time.time()
            for security_id in self.instruments:
                ws = self.last_tick_received_by_security.get(security_id, 0.0)
                rest = self.rest_fallback_by_security.get(security_id, {})
                received = max(ws, float(rest.get("timestamp_epoch", 0.0)))
                if now_epoch - received <= self.settings.max_live_age_seconds:
                    return False
            return True

    def snapshot(self) -> dict:
        with self.lock:
            now_epoch = time.time()
            live_count = 0
            for security_id in self.instruments:
                ws = self.last_tick_received_by_security.get(security_id, 0.0)
                rest = self.rest_fallback_by_security.get(security_id, {})
                received = max(ws, float(rest.get("timestamp_epoch", 0.0)))
                if received and now_epoch - received <= self.settings.max_live_age_seconds:
                    live_count += 1
            if self.session_status == "LIVE" and live_count == 0:
                stream_health = "CONNECTED_NO_LIVE_QUOTES"
            elif self.session_status == "LIVE" and live_count == len(self.instruments) and self.instruments:
                stream_health = "FULL_LIVE"
            elif self.session_status == "LIVE":
                stream_health = "RECOVERY_PENDING"
            else:
                stream_health = self.feed_status
            last_received = self.last_tick_received_epoch
            return {
                "session_date": self.session_date,
                "session_status": self.session_status,
                "feed_status": self.feed_status,
                "stream_health": stream_health,
                "last_feed_error": self.last_feed_error,
                "last_tick_at": self.last_tick_at,
                "last_tick_age_seconds": round(now_epoch - last_received, 3) if last_received else None,
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
            }
