"""One rate-limited worker that fills today's genuine Dhan 1m bars after a restart or a feed gap.

It requests today's completed 1-minute bars (``DhanAPI.load_today_completed_intraday``) for one
stock at a time, at most one request per ``interval`` seconds, and merges them into RAM. It is a
single thread with a de-duplicated queue: never a burst, never a second copy of the universe.
Bars come only from Dhan; nothing is interpolated or synthesized, and nothing is written to disk.
"""

from __future__ import annotations

import contextlib
import threading
from collections import OrderedDict

from auth_retry import AuthRetryGuard
from live_core.redact import redact


class HistoryWorker:
    def __init__(self, state, dhan_api, settings, instruments, interval_seconds: float = 0.5):
        self.state = state
        self.dhan_api = dhan_api
        self.instruments = {str(item.security_id): item for item in instruments}
        # Bars are only ever merged into the session they were requested for.
        self.session_date = state.session_date
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.guard = AuthRetryGuard(settings, dhan_api)
        self._queue: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.requests = 0
        self.failures = 0
        self.candles_merged = 0
        self.last_error = ""

    def enqueue(self, security_ids) -> int:
        added = 0
        with self._lock:
            for security_id in security_ids:
                security_id = str(security_id)
                if security_id in self.instruments and security_id not in self._queue:
                    self._queue[security_id] = None
                    added += 1
        if added:
            self._wake.set()
        return added

    def enqueue_all(self) -> int:
        return self.enqueue(self.instruments)

    def _next(self) -> str | None:
        with self._lock:
            if not self._queue:
                self._wake.clear()
                return None
            security_id, _ = self._queue.popitem(last=False)
            return security_id

    def _fetch_one(self, security_id: str) -> None:
        item = self.instruments[security_id]
        self.requests += 1
        try:
            rows = self.guard.call(lambda: self.dhan_api.load_today_completed_intraday(item, 1))
        except Exception as exc:
            self.failures += 1
            self.last_error = redact(f"{item.symbol}: {type(exc).__name__}: {exc}")[:300]
            self.state.record_error(f"history:{self.last_error}")
            return
        self.candles_merged += self.state.merge_history(security_id, rows or [], session_date=self.session_date)

    def _run(self) -> None:
        while not self._stop.is_set():
            security_id = self._next()
            if security_id is None:
                self._wake.wait(1.0)
                continue
            self._fetch_one(security_id)
            if self._stop.wait(self.interval_seconds):
                break

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="live-core-history")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        with self._lock:
            self._queue.clear()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            with contextlib.suppress(RuntimeError):
                thread.join(timeout=5)
        self._thread = None

    def status(self) -> dict:
        with self._lock:
            queued = len(self._queue)
        thread = self._thread
        return {
            "running": bool(thread is not None and thread.is_alive()),
            "queued": queued,
            "requests": self.requests,
            "failures": self.failures,
            "candles_merged": self.candles_merged,
            "interval_seconds": self.interval_seconds,
            "last_error": self.last_error,
        }
