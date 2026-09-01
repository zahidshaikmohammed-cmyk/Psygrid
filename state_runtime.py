from __future__ import annotations

import time
from typing import Optional

from state import PsygridState


class RuntimeFreshnessState(PsygridState):
    """PsygridState with freshness measured from actual packet receipt time.

    Dhan LTT is market/trade time. It is useful for reporting last trade time,
    but it is not a valid WebSocket-health clock. A quiet symbol can have an
    older LTT while the WebSocket is still delivering packets. Runtime health
    therefore uses the application's receive timestamp for each accepted quote.
    """

    def __init__(self, settings):
        super().__init__(settings)
        self.last_tick_received_by_security: dict[str, float] = {}

    def reset(self) -> None:
        super().reset()
        with self.lock:
            self.last_tick_received_by_security.clear()

    def begin(self, session_date: str, instruments: list) -> None:
        super().begin(session_date, instruments)
        with self.lock:
            self.last_tick_received_by_security = {}

    def record_live_quote(self, security_id: str, ltt_epoch: int) -> None:
        received_now = time.time()
        super().record_live_quote(security_id, ltt_epoch)
        with self.lock:
            if self.session_status == "LIVE" and security_id in self.instruments:
                self.last_tick_received_by_security[security_id] = received_now
                self.last_tick_received_epoch = received_now

    def freshness(self, security_id: str, now_epoch: Optional[float] = None) -> dict:
        with self.lock:
            now_epoch = now_epoch or time.time()
            received = self.last_tick_received_by_security.get(security_id)
            if received is None:
                return {
                    "status": "NO_TICK_YET",
                    "data_age_seconds": None,
                    "live_data_valid": False,
                }
            age = max(0.0, now_epoch - received)
            valid = age <= self.settings.max_live_age_seconds
            return {
                "status": "LIVE" if valid else "STALE",
                "data_age_seconds": round(age, 3),
                "live_data_valid": valid,
            }

    def all_live_stale(self, now_epoch: Optional[float] = None) -> bool:
        with self.lock:
            if self.session_status != "LIVE" or not self.instruments:
                return False
            now_epoch = now_epoch or time.time()
            if not self.last_tick_received_by_security:
                return True
            return all(
                now_epoch - self.last_tick_received_by_security.get(security_id, 0.0)
                > self.settings.max_live_age_seconds
                for security_id in self.instruments
            )

    def snapshot(self) -> dict:
        with self.lock:
            now_epoch = time.time()
            ages = [
                max(0.0, now_epoch - value)
                for value in self.last_tick_received_by_security.values()
            ]
            live_count = sum(
                1 for age in ages if age <= self.settings.max_live_age_seconds
            )
            if self.session_status == "LIVE" and live_count == 0:
                stream_health = "CONNECTED_NO_TICKS"
            elif self.session_status == "LIVE" and live_count < len(self.instruments):
                stream_health = "PARTIAL_LIVE"
            elif self.session_status == "LIVE" and live_count == len(self.instruments) and self.instruments:
                stream_health = "FULL_LIVE"
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
