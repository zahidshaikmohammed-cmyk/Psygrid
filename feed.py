from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

from dhanhq import DhanContext, MarketFeed


class LiveFeed:
    """Persistent Dhan v2 Quote WebSocket; live data flows continuously into RAM."""

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()
        self._backoff = 2.0

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        subscriptions = [
            (MarketFeed.NSE, item.security_id, MarketFeed.Quote)
            for item in self.instruments
        ]
        return MarketFeed(
            context,
            subscriptions,
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

    def _on_connect(self, _feed) -> None:
        self._backoff = 2.0
        self.state.mark_websocket_connected(len(self.instruments))

    def _on_close(self, _feed) -> None:
        if not self._stop_requested.is_set():
            self.state.mark_websocket_reconnecting("websocket closed")

    def _on_error(self, _feed, error) -> None:
        if not self._stop_requested.is_set():
            self.state.mark_websocket_error(str(error))

    @staticmethod
    def _parse_ltt(value) -> int | None:
        """Normalize DhanHQ-py's LTT into a real UTC epoch.

        Dhan's wire protocol defines LTT as epoch, while the current Python
        client converts it to an HH:MM:SS UTC string before invoking callbacks.
        Accept both forms so the endpoint stays resilient across SDK versions.
        """
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            epoch = int(value)
            return epoch if epoch > 0 else None
        text = str(value).strip()
        if text.isdigit():
            epoch = int(text)
            return epoch if epoch > 0 else None
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt)
                now_utc = datetime.now(timezone.utc)
                parsed = parsed.replace(
                    year=now_utc.year,
                    month=now_utc.month,
                    day=now_utc.day,
                    tzinfo=timezone.utc,
                )
                return int(parsed.timestamp())
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            return None

    def _on_message(self, _feed, data) -> None:
        if not isinstance(data, dict):
            return
        self.state.record_feed_message(str(data.get("type", "UNKNOWN")))
        self._handle_packet(data)

    def _handle_packet(self, data) -> None:
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return

        security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
        if not security_id or security_id not in self.state.instruments:
            return

        ltt_epoch = self._parse_ltt(data.get("LTT", data.get("ltt", data.get("last_trade_time"))))
        try:
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
            avg_price = data.get("avg_price", data.get("ATP", data.get("atp", data.get("average_price"))))
            avg_price = float(avg_price) if avg_price not in (None, "") else None
        except (TypeError, ValueError):
            return

        if ltt_epoch is None or ltp <= 0 or volume < 0:
            return

        quote = dict(data)
        quote["LTT_EPOCH"] = ltt_epoch
        quote["LTP"] = ltp
        quote["volume"] = volume
        quote["LTQ"] = ltq
        if avg_price is not None:
            quote["average_price"] = avg_price
        self.state.update_quote(security_id, quote)
        self.state.record_live_quote(security_id, ltt_epoch)

    def _run(self) -> None:
        while not self._stop_requested.is_set():
            feed = None
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.set_feed_status("CONNECTING")

                # DhanHQ-py v2.2 provides a callback-driven blocking run() loop.
                # It keeps receiving binary websocket packets and internally
                # reconnects when the socket closes. This avoids the fragile
                # connect/get_data handoff used by older examples.
                feed.run()

                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_reconnecting("feed loop ended")
            except Exception as exc:
                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_error(f"websocket:{exc}")
            finally:
                with self._lock:
                    if self._feed is feed:
                        self._feed = None

            if not self._stop_requested.is_set():
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2.0, 30.0)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-dhan-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.state.set_feed_status("STOPPING")
        with self._lock:
            feed = self._feed
        if feed is not None:
            try:
                feed.close_connection()
            except Exception:
                try:
                    feed.disconnect()
                except Exception:
                    pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        self._thread = None
        with self._lock:
            self._feed = None
        self.state.set_feed_status("STOPPED")
