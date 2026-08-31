from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
from typing import Optional
from zoneinfo import ZoneInfo


class SessionManager:
    def __init__(self, settings, state, dhan_api, feed, instruments):
        self.settings = settings
        self.state = state
        self.dhan_api = dhan_api
        self.feed = feed
        self.instruments = instruments
        self.tz = ZoneInfo(settings.timezone)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.history_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._started_for_date: Optional[str] = None

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def in_market(self, now: Optional[datetime] = None) -> bool:
        now = now or self.now()
        start_h, start_m = map(int, self.settings.market_start.split(":"))
        end_h, end_m = map(int, self.settings.market_end.split(":"))
        return time(start_h, start_m) <= now.time() <= time(end_h, end_m)

    def start(self) -> None:
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-session")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            self.feed.stop()
        except Exception:
            pass
        self.state.reset()
        if self.thread:
            self.thread.join(timeout=3)

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            now = self.now()
            if self.in_market(now):
                if self._started_for_date != now.date().isoformat():
                    self._start_session(now)
            else:
                if self._started_for_date is not None:
                    self._end_session()
            self.stop_event.wait(2.0)

    def _start_session(self, now: datetime) -> None:
        with self._lock:
            if self._started_for_date == now.date().isoformat():
                return
            self._started_for_date = now.date().isoformat()
            self.state.begin(self._started_for_date, self.instruments)
            try:
                # Snapshot seeds cumulative day volume before websocket ticks arrive.
                snapshot = self.dhan_api.quote_snapshot(self.instruments)
                for item in self.instruments:
                    row = snapshot.get(item.security_id, {})
                    self.state.seed_cumulative_volume(item.security_id, int(row.get("volume", 0) or 0))
            except Exception as exc:
                self.state.last_feed_error = f"snapshot: {exc}"

            # Start live acquisition immediately. Historical context loads in the background.
            self.feed.start()
            self.history_thread = threading.Thread(
                target=self._load_history,
                args=(now,),
                daemon=True,
                name="psygrid-history",
            )
            self.history_thread.start()

    def _load_history(self, now: datetime) -> None:
        def load_one(item):
            if self.stop_event.is_set() or not self.in_market():
                return
            try:
                # Native Dhan 5m / 15m / 60m historical candles.
                for interval, key in ((5, "5m"), (15, "15m"), (60, "1h")):
                    rows = self.dhan_api.load_previous_intraday(
                        item, interval, self.settings.intraday_history_days
                    )
                    self.state.set_historical(item.security_id, key, rows)

                # Native Dhan daily candles: keep the previous 7 completed trading days.
                daily = self.dhan_api.load_previous_daily(item, self.settings.daily_lookback)
                self.state.set_historical(item.security_id, "1d", daily)

                # Native 1m data for today is used only for genuine restart recovery / indicator warm-up.
                # We never construct a missing candle from timestamps.
                today_1m = self.dhan_api.load_today_1m(item)
                current_epoch = int(now.timestamp())
                current_minute = current_epoch - (current_epoch % 60)
                prior = [r for r in today_1m if r["timestamp"] < current_minute]
                self.state.live_candles[item.security_id] = prior
            except Exception as exc:
                self.state.last_feed_error = f"history:{item.symbol}:{exc}"

        # Dhan Data APIs currently document 5 requests/sec; keep concurrency modest.
        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="psygrid-hist") as pool:
            futures = [pool.submit(load_one, item) for item in self.instruments]
            for future in futures:
                if self.stop_event.is_set() or not self.in_market():
                    break
                try:
                    future.result()
                except Exception:
                    pass

    def _end_session(self) -> None:
        with self._lock:
            try:
                self.feed.stop()
            except Exception:
                pass
            self.state.finalize_current()
            # The requirement is RAM-only session data and complete disappearance after 15:15.
            self.state.reset()
            self._started_for_date = None
