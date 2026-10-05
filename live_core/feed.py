"""The Live Core's Dhan feed: the production ``feed.LiveFeed`` lifecycle plus leak accounting.

All connection handling is inherited unchanged from ``feed.LiveFeed``: one Dhan Full-mode
WebSocket per process, exponential reconnect back-off, a 300 s cool-down on Dhan connection
limits, a no-quote watchdog, and - on every connection cycle, in a ``finally`` - a call to
``runtime_guard.close_market_feed`` which disconnects, cancels the loop's pending tasks, shuts
down async generators and closes the private asyncio loop dhanhq's ``MarketFeed`` creates.

This subclass counts those cycles and verifies after each one that the loop really is closed,
so ``/health`` can prove that reconnects are not leaking event loops or descriptors.

It also replaces ``stop()``. The inherited one calls ``MarketFeed.close_connection()`` from the
stopping thread; when that lands after a connection is built but before its loop runs, dhanhq
runs the feed's loop on the stopping thread, so the feed thread's own ``run()`` can collide with
it, ``close_market_feed`` can find the loop "running" and skip closing it, or the connection can
start afterwards and stay open. Here the loop is only ever run by the feed thread: ``stop()``
signals it thread-safely until the feed thread has exited through its own cleanup.

Three more protections run per connection:

* packet isolation - an exception while handling one packet is counted and dropped. Inside
  dhanhq it would otherwise reach ``on_error`` and pause the whole socket for a second.
* silence watchdog - if a connected socket delivers no message for ``SILENCE_RECONNECT_SECONDS``
  (a half-open connection, or dhanhq's own once-a-second internal reconnect loop failing), the
  connection is ended so ``LiveFeed`` reconnects cleanly with its exponential back-off.
* stale resubscription - stocks with no valid data for longer than the 120 s staleness limit are
  resubscribed in batches (at most once per ``RESUBSCRIBE_COOLDOWN_SECONDS`` each), like the full
  PSYGRID's resubscribe pass; a failed resubscribe is recorded but never marks the feed ERROR.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import threading
import time

from feed import LiveFeed
from live_core.state import STALE_AFTER_SECONDS


class LiveCoreFeed(LiveFeed):
    MONITOR_INTERVAL_SECONDS = 5.0
    # Connection health, not data freshness: ~495 stocks in Full mode never fall silent together.
    SILENCE_RECONNECT_SECONDS = 45.0
    RESUBSCRIBE_AFTER_SECONDS = STALE_AFTER_SECONDS
    RESUBSCRIBE_COOLDOWN_SECONDS = 300.0
    RESUBSCRIBE_BATCH = 100
    RESUBSCRIBE_MAX_BATCHES_PER_PASS = 5

    def __init__(self, settings, state, instruments):
        super().__init__(settings, state, instruments)
        self._counter_lock = threading.Lock()
        self.connection_cycles = 0
        self.feeds_closed = 0
        self.event_loops_closed = 0
        self.event_loops_leaked = 0
        self.packet_errors = 0
        self.silence_reconnects = 0
        self.resubscribed = 0
        self.resubscribe_failures = 0
        self._segments = {str(item.security_id): getattr(item, "exchange_segment", "NSE_EQ") for item in instruments}
        self._last_resubscribe: dict[str, float] = {}
        self._connected_at: float | None = None
        self._connects_this_cycle = 0
        self.internal_reconnects = 0

    # ------------------------------------------------------------------ per-packet isolation

    def _on_message(self, feed, data) -> None:
        try:
            super()._on_message(feed, data)
        except Exception as exc:  # one bad packet must never stall the socket for every stock
            with self._counter_lock:
                self.packet_errors += 1
                count = self.packet_errors
            if count <= 3 or count % 1000 == 0:
                self.state.record_error(f"packet rejected ({count} so far): {type(exc).__name__}: {exc}")

    def _on_connect(self, _feed) -> None:
        super()._on_connect(_feed)
        self._connected_at = self.state.clock()
        self._connects_this_cycle += 1
        if self._connects_this_cycle > 1:
            # dhanhq re-opened the socket inside its own loop after a drop. LiveFeed never sees that,
            # so record it as a reconnect: health counts it and the runtime refills the gap from Dhan.
            with self._counter_lock:
                self.internal_reconnects += 1
            self.state.note_reconnect("dhanhq reconnected the market-feed socket after a drop")

    # ------------------------------------------------------------------ connection monitor

    def _run_connected_session(self, feed) -> None:
        self._connected_at = None
        self._connects_this_cycle = 0
        stop = threading.Event()
        monitor = threading.Thread(target=self._monitor, args=(feed, stop), daemon=True, name="live-core-feed-monitor")
        monitor.start()
        try:
            super()._run_connected_session(feed)
        finally:
            stop.set()
            if monitor is not threading.current_thread():
                monitor.join(timeout=2)

    def _monitor(self, feed, stop: threading.Event) -> None:
        while not stop.wait(self.MONITOR_INTERVAL_SECONDS) and not self._stop_requested.is_set():
            try:
                if self.monitor_tick(feed) == "reconnect":
                    return
            except Exception as exc:  # the monitor must never take the feed down
                self.state.record_error(f"feed monitor: {type(exc).__name__}: {exc}")

    def monitor_tick(self, feed, now: float | None = None) -> str:
        """One monitor pass. Returns "reconnect" when the connection was ended for silence."""
        if self.state.session_status != "LIVE" or self._connected_at is None:
            return "idle"
        now = self.state.clock() if now is None else now
        with self.state.lock:
            last_message = self.state.last_message_epoch
        last_activity = max(last_message or 0.0, self._connected_at)
        if now - last_activity > self.SILENCE_RECONNECT_SECONDS:
            with self._counter_lock:
                self.silence_reconnects += 1
            self.state.mark_websocket_error(
                f"websocket delivered nothing for {int(now - last_activity)}s; forcing a clean reconnect"
            )
            feed._running = False
            loop = getattr(feed, "loop", None)
            thread = self._thread
            if loop is not None and loop.is_running() and thread is not None:
                self._request_disconnect(feed, loop, thread)
            return "reconnect"
        if now - self._connected_at > self.RESUBSCRIBE_AFTER_SECONDS:
            self.resubscribe_stale(feed, now)
        return "ok"

    def resubscribe_stale(self, feed, now: float) -> int:
        """Resubscribe stocks with no valid data for longer than the staleness limit, in batches."""
        stale = [
            security_id
            for security_id in self.state.stale_security_ids(now, self.RESUBSCRIBE_AFTER_SECONDS)
            if now - self._last_resubscribe.get(security_id, float("-inf")) >= self.RESUBSCRIBE_COOLDOWN_SECONDS
        ]
        loop, ws = getattr(feed, "loop", None), getattr(feed, "ws", None)
        if not stale or loop is None or ws is None or loop.is_closed() or not loop.is_running():
            return 0
        sent = 0
        limit = self.RESUBSCRIBE_BATCH * self.RESUBSCRIBE_MAX_BATCHES_PER_PASS
        for start in range(0, min(len(stale), limit), self.RESUBSCRIBE_BATCH):
            batch = stale[start : start + self.RESUBSCRIBE_BATCH]
            message = json.dumps(
                {
                    "RequestCode": 21,
                    "InstrumentCount": len(batch),
                    "InstrumentList": [
                        {"ExchangeSegment": self._segments.get(security_id, "NSE_EQ"), "SecurityId": security_id}
                        for security_id in batch
                    ],
                }
            )
            try:
                asyncio.run_coroutine_threadsafe(ws.send(message), loop).result(timeout=3.0)
            except Exception as exc:
                # One failed resubscribe is not a feed failure: the socket keeps streaming every other
                # stock. Record it without changing the feed status and retry on a later pass.
                with self._counter_lock:
                    self.resubscribe_failures += 1
                self.state.record_error(f"resubscribe: {type(exc).__name__}: {exc}")
                break
            for security_id in batch:
                self._last_resubscribe[security_id] = now
            sent += len(batch)
        with self._counter_lock:
            self.resubscribed += sent
        return sent

    def _build_feed(self):
        feed = super()._build_feed()
        with self._counter_lock:
            self.connection_cycles += 1
        return feed

    def _close_feed(self, feed) -> None:
        if feed is None:
            return
        super()._close_feed(feed)
        loop = getattr(feed, "loop", None)
        closed = loop is None or loop.is_closed()
        with self._counter_lock:
            self.feeds_closed += 1
            if closed:
                self.event_loops_closed += 1
            else:
                self.event_loops_leaked += 1
        if not closed:
            self.state.record_error("feed lifecycle: MarketFeed event loop was not closed after its connection ended")

    STOP_TIMEOUT_SECONDS = 10.0

    def stop(self) -> None:
        self._stop_requested.set()
        self._connection_stop.set()
        self.state.set_feed_status("STOPPING")
        thread = self._thread
        disconnected: set[int] = set()
        deadline = time.monotonic() + self.STOP_TIMEOUT_SECONDS
        while thread is not None and thread is not threading.current_thread() and thread.is_alive():
            if time.monotonic() >= deadline:
                self.state.record_error("feed stop: Dhan feed thread did not exit within the stop timeout")
                break
            with self._lock:
                feed = self._feed
            if feed is not None:
                # dhanhq's run() loop exits once _running is false; run() sets it true when it starts,
                # so it is cleared again on every pass until the thread is gone.
                feed._running = False
                loop = getattr(feed, "loop", None)
                if id(feed) not in disconnected and loop is not None and loop.is_running() and not loop.is_closed():
                    disconnected.add(id(feed))
                    self._request_disconnect(feed, loop, thread)
            thread.join(0.25)
        self._thread = None
        with self._lock:
            self._feed = None
        self.state.set_feed_status("STOPPED")

    @staticmethod
    def _request_disconnect(feed, loop, thread: threading.Thread) -> None:
        """Ask the feed thread's loop to disconnect; never runs the loop on this thread."""
        coroutine = feed.disconnect()
        try:
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        except Exception:
            coroutine.close()
            return
        deadline = time.monotonic() + 3.0
        while not future.done() and thread.is_alive() and time.monotonic() < deadline:
            thread.join(0.05)
        if not future.done():
            future.cancel()
            # The loop finished before it picked the request up; it will never run now.
            if not thread.is_alive() and inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED:
                coroutine.close()

    def thread_alive(self) -> bool:
        thread = self._thread
        return bool(thread is not None and thread.is_alive())

    def lifecycle(self) -> dict:
        with self._counter_lock:
            return {
                "feed_thread_alive": self.thread_alive(),
                "connection_cycles": self.connection_cycles,
                "feeds_closed": self.feeds_closed,
                "event_loops_closed": self.event_loops_closed,
                "event_loops_leaked": self.event_loops_leaked,
                "packet_errors": self.packet_errors,
                "internal_reconnects": self.internal_reconnects,
                "silence_reconnects": self.silence_reconnects,
                "resubscribed_instruments": self.resubscribed,
                "resubscribe_failures": self.resubscribe_failures,
            }
