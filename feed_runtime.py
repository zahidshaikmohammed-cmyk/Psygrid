from __future__ import annotations

import asyncio
import time

from feed import LiveFeed as BaseLiveFeed


class LiveFeed(BaseLiveFeed):
    """LiveFeed with a hard receive-time watchdog around the SDK read loop.

    The base feed owns connection lifecycle and deliberate reconnect/backoff.
    This runtime layer only decides when a connected socket has stopped
    delivering usable live quotes. It never changes candle or indicator logic.
    """

    SOCKET_READ_TIMEOUT_SECONDS = 20.0

    def _run_connected_session(self, feed) -> None:
        feed.loop.run_until_complete(feed.connect())
        connected_at = time.monotonic()
        last_valid_quote_count = self.state.live_quotes
        last_valid_quote_monotonic = connected_at

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

            current_live_quotes = self.state.live_quotes
            if current_live_quotes > last_valid_quote_count:
                last_valid_quote_count = current_live_quotes
                last_valid_quote_monotonic = time.monotonic()

            # A connected socket is not considered healthy merely because it
            # returns packets. During market hours it must continue delivering
            # accepted Dhan Quote packets. This catches sockets that remain
            # technically connected while the data stream has stopped.
            if (
                self.state.session_status == "LIVE"
                and time.monotonic() - connected_at >= self.settings.max_live_age_seconds
                and time.monotonic() - last_valid_quote_monotonic > self.settings.max_live_age_seconds
            ):
                raise RuntimeError(
                    "LIVE_FEED_WATCHDOG: connected WebSocket delivered no valid "
                    f"live quote for > {self.settings.max_live_age_seconds}s"
                )

            # Once the connection has had enough time to initialise, a total
            # loss of fresh instruments is also a hard reconnect condition.
            all_stale = getattr(self.state, "all_live_stale", None)
            if (
                callable(all_stale)
                and time.monotonic() - connected_at >= self.settings.max_live_age_seconds
                and all_stale()
            ):
                raise RuntimeError(
                    "LIVE_FEED_WATCHDOG: all subscribed instruments have exceeded "
                    f"{self.settings.max_live_age_seconds}s without a fresh quote"
                )
