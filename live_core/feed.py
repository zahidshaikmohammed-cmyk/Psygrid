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
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import time

from feed import LiveFeed


class LiveCoreFeed(LiveFeed):
    def __init__(self, settings, state, instruments):
        super().__init__(settings, state, instruments)
        self._counter_lock = threading.Lock()
        self.connection_cycles = 0
        self.feeds_closed = 0
        self.event_loops_closed = 0
        self.event_loops_leaked = 0

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
            }
