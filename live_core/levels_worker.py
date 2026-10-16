"""Support/resistance levels for the stock node: one daily-bar request per stock per session.

At the start of each session a single thread asks Dhan for each stock's daily bars (one request at
a time, at most one per ``interval`` seconds, sharing ``DhanAPI``'s global throttle with the history
worker) and keeps only the computed major levels (``levels.major_levels``), a few dozen numbers per
stock. Stocks whose request failed are retried every ``RETRY_SECONDS``. Intraday levels are
computed on request from the 1-minute candles already in RAM. Nothing is written to disk.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import levels as levels_mod
from live_core.redact import redact

RETRY_SECONDS = 600.0
CACHE_SECONDS = 5.0


class LevelsWorker:
    def __init__(self, dhan_api, instruments, timezone: str, interval_seconds: float = 0.5, now=None):
        self.dhan_api = dhan_api
        self.instruments = list(instruments)
        self.tz = ZoneInfo(timezone)
        self.interval_seconds = max(0.0, float(interval_seconds))
        self._now = now or (lambda: datetime.now(self.tz))
        self.major: dict[str, dict] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.requests = 0
        self.failures = 0
        self.last_error = ""
        self.passes = 0

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True, name="live-core-levels")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=5.0)

    def fetch_one(self, item) -> None:
        now = self._now()
        self.requests += 1
        try:
            rows = self.dhan_api.daily(item, now - timedelta(days=levels_mod.DAILY_LOOKBACK_CALENDAR_DAYS), now)
            prior = levels_mod.daily_rows_before(rows or [], now.date(), self.tz)
        except Exception as exc:
            self.failures += 1
            self.last_error = redact(f"{item.symbol}: {type(exc).__name__}: {exc}")[:300]
            return
        if prior:
            self.major[str(item.symbol)] = levels_mod.major_levels(prior, now.date())

    def run_pass(self) -> None:
        for item in self.instruments:
            if self._stop.is_set():
                return
            if str(item.symbol) in self.major:
                continue
            self.fetch_one(item)
            if self._stop.wait(self.interval_seconds):
                return
        self.passes += 1

    def _run(self) -> None:
        while not self._stop.is_set():
            self.run_pass()
            if len(self.major) >= len(self.instruments):
                return
            self._stop.wait(RETRY_SECONDS)

    def status(self) -> dict:
        return {
            "stocks_with_major_levels": len(self.major),
            "expected": len(self.instruments),
            "requests": self.requests,
            "failures": self.failures,
            "last_error": self.last_error,
        }


def series_levels(series, major: dict | None, tz: ZoneInfo, as_of: str) -> dict:
    """Levels payload for one ``StockSeries`` (call with the state lock held)."""
    candles = [
        (series.epochs[i], series.opens[i], series.highs[i], series.lows[i], series.closes[i], series.volumes[i])
        for i in range(len(series.epochs))
    ]
    ltp = series.current[4] if series.current else (candles[-1][4] if candles else None)
    intraday = levels_mod.intraday_levels(candles, major.get("week_prior_days") if major else None, tz)
    return levels_mod.build_levels(series.symbol, ltp, major, intraday, as_of=as_of)


class LevelsView:
    """``/public/levels.json`` and ``/public/levels/{symbol}.json`` for the stock node, cached briefly."""

    def __init__(self, runtime):
        self.runtime = runtime
        self._cache: tuple[float, dict] | None = None
        self._lock = threading.Lock()

    def _worker_major(self) -> dict[str, dict]:
        worker = getattr(self.runtime, "levels", None)
        return worker.major if worker is not None else {}

    def one(self, symbol: str) -> dict | None:
        state = self.runtime.state
        tz = ZoneInfo(state.timezone)
        as_of = datetime.now(tz).isoformat(timespec="seconds")
        with state.lock:
            series = state.by_symbol.get(symbol.strip().upper())
            if series is None:
                return None
            return series_levels(series, self._worker_major().get(series.symbol), tz, as_of)

    def everything(self) -> dict:
        with self._lock:
            if self._cache is not None and time.monotonic() - self._cache[0] < CACHE_SECONDS:
                return self._cache[1]
            state = self.runtime.state
            tz = ZoneInfo(state.timezone)
            as_of = datetime.now(tz).isoformat(timespec="seconds")
            major = self._worker_major()
            with state.lock:
                out = {s.symbol: series_levels(s, major.get(s.symbol), tz, as_of) for s in state.ordered}
                session_date = state.session_date
            worker = getattr(self.runtime, "levels", None)
            payload = {
                "service": "PSYGRID",
                "session_date": session_date,
                "as_of": as_of,
                "stock_count": len(out),
                "major_levels_status": worker.status() if worker is not None else None,
                "levels": out,
            }
            self._cache = (time.monotonic(), payload)
            return payload
