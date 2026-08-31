from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time
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
        self.history_stop = threading.Event()
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
        return time(start_h, start_m) <= now.time() < time(end_h, end_m)

    def start(self) -> None:
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-session")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.history_stop.set()
        try:
            self.feed.stop()
        except Exception:
            pass
        if self.history_thread:
            self.history_thread.join(timeout=5)
        self.state.reset()
        if self.thread:
            self.thread.join(timeout=3)

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            now = self.now()
            if self.in_market(now):
                if self._started_for_date != now.date().isoformat():
                    self._start_session(now)
            elif self._started_for_date is not None:
                self._end_session()
            self.stop_event.wait(2.0)

    def _start_session(self, now: datetime) -> None:
        with self._lock:
            session_date = now.date().isoformat()
            if self._started_for_date == session_date:
                return
            self._started_for_date = session_date
            self.history_stop.clear()
            self.state.begin(session_date, self.instruments)
            try:
                profile = self.dhan_api.verify_data_access()
                self.state.set_profile(profile)
            except Exception as exc:
                self.state.last_feed_error = f"profile:{exc}"
                self._started_for_date = None
                self.state.reset()
                return

            try:
                snapshot = self.dhan_api.quote_snapshot(self.instruments)
                for item in self.instruments:
                    row = snapshot.get(item.security_id, {})
                    self.state.seed_cumulative_volume(item.security_id, int(row.get("volume", 0) or 0))
            except Exception as exc:
                self.state.last_feed_error = f"snapshot:{exc}"

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
            if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                return
            try:
                for interval, key in ((5, "5m"), (15, "15m"), (60, "1h")):
                    if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                        return
                    rows = self.dhan_api.load_previous_intraday(item, interval, self.settings.intraday_history_days)
                    if not self.history_stop.is_set():
                        self.state.set_historical(item.security_id, key, rows)

                if self.history_stop.is_set() or not self.in_market():
                    return
                daily = self.dhan_api.load_previous_daily(item, self.settings.daily_lookback)
                if not self.history_stop.is_set():
                    self.state.set_historical(item.security_id, "1d", daily)

                if self.history_stop.is_set() or not self.in_market():
                    return
                today_1m = self.dhan_api.load_today_1m(item)
                current_epoch = int(self.now().timestamp())
                current_minute = current_epoch - (current_epoch % 60)
                prior = [r for r in today_1m if r["timestamp"] < current_minute]
                if not self.history_stop.is_set():
                    self.state.merge_today_1m_history(item.security_id, prior)
            except Exception as exc:
                if not self.history_stop.is_set():
                    self.state.last_feed_error = f"history:{item.symbol}:{exc}"

        with ThreadPoolExecutor(max_workers=4, thread_name_prefix="psygrid-hist") as pool:
            futures = [pool.submit(load_one, item) for item in self.instruments]
            for future in futures:
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    break
                try:
                    future.result()
                except Exception:
                    pass

    def _end_session(self) -> None:
        with self._lock:
            self.history_stop.set()
            try:
                self.feed.stop()
            except Exception:
                pass
            if self.history_thread:
                self.history_thread.join(timeout=5)
            self.history_thread = None
            self.state.finalize_current()
            # Hard requirement: after 15:15, all session data disappears from RAM.
            self.state.reset()
            self._started_for_date = None
