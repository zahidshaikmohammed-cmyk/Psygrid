from __future__ import annotations

import threading
from datetime import datetime, timezone
from time import sleep
from typing import Optional

from dhanhq import DhanContext, MarketFeed


class LiveFeed:
    """Dhan v2 Quote websocket. Only genuine Dhan Quote packets become live candles."""

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()

    def _on_connect(self, _feed):
        self.state.feed_status = "CONNECTED"

    def _on_error(self, _feed, error):
        self.state.feed_status = "ERROR"
        self.state.last_feed_error = str(error)

    def _on_close(self, _feed):
        if not self._stop_requested.is_set():
            self.state.feed_status = "RECONNECTING"

    def _on_message(self, _feed, data):
        if not isinstance(data, dict):
            return
        if data.get("type") != "Quote Data":
            return
        security_id = str(data.get("security_id", ""))
        ltt = data.get("LTT", "")
        try:
            # Dhan SDK returns LTT as UTC HH:MM:SS. NSE session date is the same UTC date.
            parsed = datetime.strptime(ltt, "%H:%M:%S").replace(
                year=datetime.now(timezone.utc).year,
                month=datetime.now(timezone.utc).month,
                day=datetime.now(timezone.utc).day,
                tzinfo=timezone.utc,
            )
            epoch = int(parsed.timestamp())
        except Exception:
            return

        quote = dict(data)
        quote["LTT_EPOCH"] = epoch
        self.state.update_quote(security_id, quote)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        feed_instruments = []
        for item in self.instruments:
            feed_instruments.append((MarketFeed.NSE, item.security_id, MarketFeed.Quote))

        self._feed = MarketFeed(
            context,
            feed_instruments,
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

        self._thread = self._feed.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.state.feed_status = "STOPPING"
        if self._feed is not None:
            try:
                self._feed.close_connection()
            except Exception:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.state.feed_status = "STOPPED"
