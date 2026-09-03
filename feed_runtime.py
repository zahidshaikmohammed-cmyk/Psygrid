from __future__ import annotations

import asyncio
import json
import random
import threading
import time

from dhan_api import DhanAPI
from feed import DhanConnectionLimited, LiveFeed as BaseLiveFeed
from self_keepalive import SelfKeepAlive


class LiveFeed(BaseLiveFeed):
    """Runtime WebSocket feed with continuous Dhan quote recovery."""

    SOCKET_READ_CHECK_SECONDS = 5.0
    PING_INTERVAL_SECONDS = 15.0
    PONG_TIMEOUT_SECONDS = 30.0
    REST_FALLBACK_AFTER_SECONDS = 1.0
    RESUBSCRIBE_COOLDOWN_SECONDS = 30.0
    BACKOFF_INITIAL_SECONDS = 1.0
    BACKOFF_MAX_SECONDS = 15.0
    NORMAL_RETRY_JITTER_SECONDS = 0.75
    RATE_LIMIT_COOLDOWN = 120.0

    def __init__(self, settings, state, instruments, dhan_api=None):
        super().__init__(settings, state, instruments)
        self.dhan_api = dhan_api or DhanAPI(settings)
        self._last_resubscribe: dict[str, float] = {}
        self._last_rest_fallback = 0.0
        keepalive_url = "https://psygrid.onrender.com/public/live-a.json"
        self.self_keepalive = SelfKeepAlive(keepalive_url)

    def _market_hours(self) -> bool:
        return self.state.session_status == "LIVE"

    async def _heartbeat_and_health(self, feed) -> None:
        """Ping every 15s and keep every configured quote current within 30s."""
        last_ping = 0.0
        while not self._stop_requested.is_set():
            now_mono = time.monotonic()
            if now_mono - last_ping >= self.PING_INTERVAL_SECONDS:
                ws = getattr(feed, "ws", None)
                if ws is None:
                    raise RuntimeError("LIVE_HEARTBEAT: websocket object missing")
                try:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.PONG_TIMEOUT_SECONDS)
                except Exception as exc:
                    raise RuntimeError("LIVE_HEARTBEAT: PONG not received within 30s") from exc
                last_ping = time.monotonic()

            if self._market_hours():
                await self._health_pass(feed)
            await asyncio.sleep(1.0)

    async def _health_pass(self, feed) -> None:
        now = time.time()
        stale = []
        for item in self.instruments:
            health = self.state.freshness(item.security_id, now)
            age = health.get("data_age_seconds")
            if age is None or age > self.state.settings.max_live_age_seconds:
                stale.append(item)

        # Dhan v2 Quote subscription is RequestCode 17. Re-subscribe only the
        # affected stock and never more often than once per 30 seconds.
        for item in stale:
            key = str(item.security_id)
            if now - self._last_resubscribe.get(key, 0.0) < self.RESUBSCRIBE_COOLDOWN_SECONDS:
                continue
            try:
                await feed.ws.send(json.dumps({
                    "RequestCode": 17,
                    "InstrumentCount": 1,
                    "InstrumentList": [{
                        "ExchangeSegment": item.exchange_segment,
                        "SecurityId": str(item.security_id),
                    }],
                }))
                self._last_resubscribe[key] = now
            except Exception as exc:
                raise RuntimeError(f"LIVE_HEALTH: failed to re-subscribe {item.symbol}") from exc

        # Dhan Quote API supports the full configured universe in one request
        # and is limited to 1 request/sec. Use it continuously only while any
        # symbol lacks a fresh WebSocket quote, making the REST path recovery,
        # not the primary acquisition source.
        if stale and now - self._last_rest_fallback >= self.REST_FALLBACK_AFTER_SECONDS:
            try:
                snapshot = self.dhan_api.quote_snapshot(self.instruments)
                self.state.apply_rest_snapshot(snapshot)
                self._last_rest_fallback = time.time()
            except Exception as exc:
                self.state.set_feed_status(self.state.feed_status, f"REST_RECOVERY:{exc}")

    def _run_connected_session(self, feed) -> None:
        feed.loop.run_until_complete(feed.connect())
        heartbeat = feed.loop.create_task(self._heartbeat_and_health(feed))
        try:
            while not self._stop_requested.is_set():
                if heartbeat.done():
                    error = heartbeat.exception()
                    if error is not None:
                        raise error
                    raise RuntimeError("LIVE_HEARTBEAT: watchdog stopped unexpectedly")
                try:
                    data = feed.loop.run_until_complete(
                        asyncio.wait_for(feed.get_instrument_data(), timeout=self.SOCKET_READ_CHECK_SECONDS)
                    )
                except asyncio.TimeoutError:
                    continue
                self._on_message(feed, data)
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
                try:
                    feed.loop.run_until_complete(heartbeat)
                except BaseException:
                    pass

    def _run(self) -> None:
        """Reconnect with 1/2/4/8/15s exponential backoff plus jitter."""
        backoff = self.BACKOFF_INITIAL_SECONDS
        while not self._stop_requested.is_set():
            feed = None
            rate_limited = False
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.set_feed_status("CONNECTING")
                self._run_connected_session(feed)
                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_reconnecting("feed connection ended")
            except DhanConnectionLimited as exc:
                rate_limited = True
                self.state.mark_websocket_error(str(exc))
                self.state.mark_websocket_reconnecting(
                    f"Dhan rate/connection limit; retrying in {int(self.RATE_LIMIT_COOLDOWN)}s"
                )
            except Exception as exc:
                message = str(exc)
                self.state.mark_websocket_error(f"websocket:{message}")
                self.state.mark_websocket_reconnecting(f"websocket reconnect scheduled: {message}")
            finally:
                self._close_feed(feed)
                with self._lock:
                    if self._feed is feed:
                        self._feed = None

            if self._stop_requested.is_set():
                break
            if rate_limited or "805" in str(getattr(self.state, "last_feed_error", "")):
                delay = self.RATE_LIMIT_COOLDOWN
                backoff = self.BACKOFF_INITIAL_SECONDS
            else:
                delay = min(backoff, self.BACKOFF_MAX_SECONDS) + random.uniform(0.0, self.NORMAL_RETRY_JITTER_SECONDS)
                backoff = min(backoff * 2.0, self.BACKOFF_MAX_SECONDS)
            self._stop_requested.wait(delay)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self.self_keepalive.start()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-dhan-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.self_keepalive.stop()
        super().stop()
