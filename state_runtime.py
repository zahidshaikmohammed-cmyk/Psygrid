from __future__ import annotations

import time
from typing import Optional

from state import PsygridState


class RuntimeFreshnessState(PsygridState):
    """1m-only state with bounded current-quote recovery freshness."""

    def __init__(self, settings):
        super().__init__(settings)
        self.rest_quote_received: dict[str, float] = {}

    def reset(self) -> None:
        super().reset()
        with self.lock:
            self.rest_quote_received.clear()

    def begin(self, session_date: str, instruments: list) -> None:
        super().begin(session_date, instruments)
        with self.lock:
            self.rest_quote_received = {}

    def record_live_quote(self, security_id: str, ltt_epoch: int) -> None:
        super().record_live_quote(security_id, ltt_epoch)
        with self.lock:
            self.rest_quote_received.pop(str(security_id), None)

    def apply_rest_snapshot(self, snapshot: dict) -> None:
        now = time.time()
        self.apply_quote_snapshot(snapshot)
        with self.lock:
            for security_id, row in snapshot.items():
                security_id = str(security_id)
                if security_id not in self.instruments or not isinstance(row, dict):
                    continue
                ltp = row.get("last_price", row.get("LTP", row.get("ltp")))
                try:
                    ltp = float(ltp)
                except (TypeError, ValueError):
                    continue
                if ltp > 0:
                    self.rest_quote_received[security_id] = now

    def freshness(self, security_id: str, now_epoch: Optional[float] = None) -> dict:
        with self.lock:
            now_epoch = now_epoch or time.time()
            security_id = str(security_id)
            ws = self.last_tick_by_security.get(security_id)
            rest = self.rest_quote_received.get(security_id)
            received, source = max(
                ((value, src) for value, src in ((ws, "DHAN_WEBSOCKET_FULL"), (rest, "DHAN_REST_QUOTE_RECOVERY")) if value is not None),
                key=lambda pair: pair[0],
                default=(None, None),
            )
            if received is None:
                return {"status": "NO_LIVE_QUOTE", "data_age_seconds": None, "live_data_valid": False, "source": None}
            age = max(0.0, now_epoch - received)
            valid = age <= self.settings.max_live_age_seconds
            return {"status": "LIVE" if valid else "STALE", "data_age_seconds": round(age, 3), "live_data_valid": valid, "source": source}

    def snapshot(self) -> dict:
        snap = super().snapshot()
        with self.lock:
            now = time.time()
            live_count = 0
            for security_id in self.instruments:
                ws = self.last_tick_by_security.get(security_id, 0.0)
                rest = self.rest_quote_received.get(security_id, 0.0)
                received = max(ws, rest)
                if received and now - received <= self.settings.max_live_age_seconds:
                    live_count += 1
            snap["live_stock_count"] = live_count
            if self.session_status == "LIVE" and live_count == len(self.instruments) and self.instruments:
                snap["stream_health"] = "FULL_LIVE"
            elif self.session_status == "LIVE" and live_count == 0:
                snap["stream_health"] = "CONNECTED_NO_LIVE_QUOTES"
            elif self.session_status == "LIVE":
                snap["stream_health"] = "PARTIAL_LIVE"
            return snap
