from __future__ import annotations

import asyncio
import time

from feed import LiveFeed as BaseLiveFeed


class LiveFeed(BaseLiveFeed):
    """LiveFeed with a hard receive-time watchdog around the SDK read loop."""

    SOCKET_READ_TIMEOUT_SECONDS = 20.0

    def _run_connected_session(self, feed) -> None:
        # Keep ownership of reconnect/backoff in the parent class. The only
        # change here is that a blocked/dead SDK read cannot leave the service
        # reporting CONNECTED forever.
        feed.loop.run_until_complete(feed.connect())

        while not self._stop_requested.is_set():
            try:
                data = feed.loop.run_until_complete(
                    asyncio.wait_for(
                        feed.get_instrument_data(),
                        timeout=self.SOCKET_READ_TIMEOUT_SECONDS,
                    )
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    "LIVE_FEED_WATCHDOG: no WebSocket packet received for "
                    f"{int(self.SOCKET_READ_TIMEOUT_SECONDS)}s"
                ) from exc

            self._on_message(feed, data)

            # Freshness is based on packet receipt, never on Dhan LTT. This
            # catches a connected socket that is delivering no usable quotes.
            now = time.time()
            received = getattr(self.state, "last_tick_received_by_security", {})
            if self.state.session_status == "LIVE" and self.state.instruments:
                if not received:
                    raise RuntimeError(
                        "LIVE_FEED_WATCHDOG: connected but no valid live quotes received"
                    )
                stale = [
                    sid
                    for sid in self.state.instruments
                    if now - received.get(sid, 0.0) > self.settings.max_live_age_seconds
                ]
                if len(stale) == len(self.state.instruments):
                    raise RuntimeError(
                        "LIVE_FEED_WATCHDOG: all subscribed instruments are stale "
                        f"> {self.settings.max_live_age_seconds}s"
                    )
