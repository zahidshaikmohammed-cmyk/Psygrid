from __future__ import annotations

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional


class HistoricalBackfill:
    """Bounded, rate-limited historical backfill that never blocks the live feed."""

    REQUEST_INTERVAL_SECONDS = 0.21  # <= 4.8 requests/sec, below Dhan's 5 rps ceiling.
    WORKERS = 4
    MAX_RETRIES = 5

    def __init__(self, settings, state, dhan_api, instruments):
        self.settings = settings
        self.state = state
        self.dhan_api = dhan_api
        self.instruments = instruments
        self._queue: queue.Queue = queue.Queue()
        self._queued: set[str] = set()
        self._lock = threading.Lock()
        self._rate_lock = threading.Lock()
        self._last_request = 0.0
        self._executor = ThreadPoolExecutor(max_workers=self.WORKERS, thread_name_prefix="psygrid-backfill")
        self._closed = False

    def _rate_limit(self) -> None:
        with self._rate_lock:
            now = time.monotonic()
            wait = self.REQUEST_INTERVAL_SECONDS - (now - self._last_request)
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def _enqueue(self, item, from_dt: datetime, to_dt: datetime) -> None:
        key = str(item.security_id)
        with self._lock:
            if self._closed or key in self._queued:
                return
            self._queued.add(key)
        self._queue.put((item, from_dt, to_dt))

    def enqueue_gap(self, now: Optional[datetime] = None) -> int:
        """Queue only the missing 1m interval for every stock after a feed interruption."""
        if self._closed or not self.instruments:
            return 0
        now = now or datetime.now(self.settings.tz)
        to_dt = now.replace(second=0, microsecond=0)
        queued = 0
        for item in self.instruments:
            start_epoch = self.state.last_completed_candle_epoch(item.security_id)
            if start_epoch is None:
                continue
            # Start strictly after the last completed candle. No duplicate minute.
            from_epoch = int(start_epoch) + 60
            if from_epoch >= int(to_dt.timestamp()):
                continue
            from_dt = datetime.fromtimestamp(from_epoch, self.settings.tz)
            self._enqueue(item, from_dt, to_dt)
            queued += 1
        for _ in range(self.WORKERS):
            self._executor.submit(self._worker)
        return queued

    def _worker(self) -> None:
        while True:
            try:
                item, from_dt, to_dt = self._queue.get_nowait()
            except queue.Empty:
                return
            key = str(item.security_id)
            try:
                rows = []
                for attempt in range(self.MAX_RETRIES):
                    try:
                        self._rate_limit()
                        rows = self.dhan_api.intraday(item, 1, from_dt, to_dt)
                        break
                    except Exception as exc:
                        text = str(exc).lower()
                        if "429" not in text and "too many" not in text and attempt >= 1:
                            raise
                        if attempt >= self.MAX_RETRIES - 1:
                            raise
                        time.sleep(min(8.0, 0.5 * (2 ** attempt)))
                if rows:
                    self.state.merge_today_1m_history(item.security_id, rows)
            except Exception as exc:
                self.state.record_backfill_error(item.symbol, str(exc))
            finally:
                with self._lock:
                    self._queued.discard(key)
                self._queue.task_done()

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._executor.shutdown(wait=False, cancel_futures=True)
