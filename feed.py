from __future__ import annotations

import threading
import time
from typing import Optional

from dhanhq import DhanContext, MarketFeed


class LiveFeed:
    """Dhan v2 Quote WebSocket feeding only genuine quote events into RAM."""

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        subscriptions = [
            (MarketFeed.NSE, item.security_id, MarketFeed.Quote)
            for item in self.instruments
        ]
        return MarketFeed(context, subscriptions, version="v2")

    def _run(self) -> None:
        backoff = 2.0
        while not self._stop_requested.is_set():
            feed = None
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.feed_status = "CONNECTING"

                # DhanHQ-py documents run_forever() as the event-loop starter.
                # get_data() must be called after it returns from each loop turn;
                # do not call run_forever once and then expect get_data to run.
                while not self._stop_requested.is_set():
                    feed.run_forever()
                    if self._stop_requested.is_set():
                        break
                    data = feed.get_data()
                    if data:
                        self._handle_packet(data)
                    else:
                        time.sleep(0.01)

                self._disconnect(feed)
                break
            except Exception as exc:
                if self._stop_requested.is_set():
                    break
                self.state.feed_status = "RECONNECTING"
                self.state.last_feed_error = f"websocket:{exc}"
                self._disconnect(feed)
                time.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            finally:
                with self._lock:
                    if self._feed is feed:
                        self._feed = None

    @staticmethod
    def _disconnect(feed) -> None:
        if feed is None:
            return
        for method_name in ("disconnect", "close_connection"):
            try:
                method = getattr(feed, method_name, None)
                if method:
                    method()
                    return
            except Exception:
                continue

    def _handle_packet(self, data) -> None:
        if not isinstance(data, dict):
            return

        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        if packet_type and packet_type not in {"quote data", "quote"}:
            return

        security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
        if not security_id or security_id not in self.state.instruments:
            return

        # LTT comes from Dhan's packet and determines the genuine candle minute.
        ltt = data.get("LTT", data.get("ltt", data.get("last_trade_time")))
        try:
            ltt_epoch = int(ltt)
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
        except (TypeError, ValueError):
            return

        if ltt_epoch <= 0 or ltp <= 0 or volume < 0:
            return

        quote = dict(data)
        quote["LTT_EPOCH"] = ltt_epoch
        quote["LTP"] = ltp
        quote["volume"] = volume
        self.state.update_quote(security_id, quote)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-dhan-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.state.feed_status = "STOPPING"
        with self._lock:
            feed = self._feed
        self._disconnect(feed)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None
        with self._lock:
            self._feed = None
        self.state.feed_status = "STOPPED"
