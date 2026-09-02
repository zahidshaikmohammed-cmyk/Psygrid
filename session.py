from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo

from backfill import HistoricalBackfill
from config import refresh_access_token
from dhan_auth import DhanTokenRateLimited, generate_access_token


class SessionManager:
    def __init__(self, settings, state, dhan_api, feed, instruments):
        self.settings = settings
        self.state = state
        self.dhan_api = dhan_api
        self.feed = feed
        self.feed.dhan_api = dhan_api
        self.instruments = instruments
        self.tz = ZoneInfo(settings.timezone)
        self.stop_event = threading.Event()
        self.history_stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.history_thread: Optional[threading.Thread] = None
        self.htf_thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()
        self._started_for_date: Optional[str] = None
        self._auth_retry_at = 0.0
        self._last_reconnect_seen = 0
        self._last_htf_refresh_slot: dict[str, int] = {}
        self.backfill = HistoricalBackfill(settings, state, dhan_api, instruments)

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def in_market(self, now: Optional[datetime] = None) -> bool:
        now = now or self.now()
        start_h, start_m = map(int, self.settings.market_start.split(":"))
        end_h, end_m = map(int, self.settings.market_end.split(":"))
        return time(start_h, start_m) <= now.time() < time(end_h, end_m)

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
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
        try:
            self.backfill.close()
        except Exception:
            pass
        if self.history_thread:
            self.history_thread.join(timeout=10)
            self.history_thread = None
        if self.htf_thread:
            self.htf_thread.join(timeout=10)
            self.htf_thread = None
        self.state.reset()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=3)
        self.thread = None

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            now = self.now()
            if self.in_market(now):
                if self._started_for_date != now.date().isoformat():
                    self._start_session(now)
                self._check_for_feed_interruption(now)
            elif self._started_for_date is not None:
                self._end_session()
            self.stop_event.wait(2.0)

    def _check_for_feed_interruption(self, now: datetime) -> None:
        with self.state.lock:
            reconnects = self.state.websocket_reconnects
        if reconnects <= self._last_reconnect_seen:
            return
        self._last_reconnect_seen = reconnects
        if self.state.session_status == "LIVE":
            self.backfill.enqueue_gap(now)

    def _auth_retry_with_totp(self) -> None:
        pin = os.getenv("DHAN_PIN", "").strip()
        totp_secret = os.getenv("DHAN_TOTP_SECRET", "").strip()
        if not pin or not totp_secret:
            raise RuntimeError("Dhan token expired/invalid and TOTP credentials are unavailable")
        token, expiry = generate_access_token(self.settings.client_id, pin, totp_secret)
        self.settings.access_token = token
        self.settings.token_expiry = expiry
        self.settings.token_source = "AUTO_GENERATED_TOTP"

    @staticmethod
    def _looks_like_auth_failure(exc: Exception) -> bool:
        text = str(exc).lower()
        return (
            "401" in text
            or "807" in text
            or "808" in text
            or "809" in text
            or "expired" in text
            or "invalid token" in text
            or "authentication failed" in text
            or "unauthorized" in text
        )

    def _start_session(self, now: datetime) -> None:
        with self._lock:
            session_date = now.date().isoformat()
            if self._started_for_date == session_date:
                return
            if now.timestamp() < self._auth_retry_at:
                remaining = int(self._auth_retry_at - now.timestamp())
                self.state.set_feed_status("AUTH_WAITING", f"Dhan authentication retry in {remaining}s")
                return
            self.history_stop.clear()
            self._last_htf_refresh_slot.clear()
            self.state.begin(session_date, self.instruments)
            self.state.session_status = "AUTHENTICATING"
            self.state.set_feed_status("AUTHENTICATING")
            try:
                refresh_access_token(self.settings)
                self.dhan_api.settings = self.settings
                self.feed.settings = self.settings
                self.feed.dhan_api = self.dhan_api
                try:
                    profile = self.dhan_api.verify_data_access()
                except Exception as first_exc:
                    if not self._looks_like_auth_failure(first_exc):
                        raise
                    self.state.set_feed_status("TOKEN_REFRESHING", "Dhan token expired/invalid; generating one fresh token")
                    self._auth_retry_with_totp()
                    self.dhan_api.settings = self.settings
                    self.feed.settings = self.settings
                    profile = self.dhan_api.verify_data_access()
                self.state.set_profile(profile)
            except DhanTokenRateLimited as exc:
                self._auth_retry_at = now.timestamp() + exc.retry_after
                self.state.session_status = "AUTH_WAITING"
                self.state.set_feed_status("AUTH_WAITING", f"Dhan token generation rate-limited; retrying in {exc.retry_after}s")
                return
            except Exception as exc:
                self._auth_retry_at = now.timestamp() + 30
                self.state.session_status = "AUTH_ERROR"
                self.state.set_feed_status("AUTH_ERROR", f"authentication:{exc}")
                return
            self.state.session_status = "LIVE"
            self._started_for_date = session_date
            self._auth_retry_at = 0.0
            self._last_reconnect_seen = self.state.websocket_reconnects
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
        """Load genuine Dhan history efficiently; never synthesize candles."""
        def load_one(item):
            if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                return

            def set_window(interval: int, key: str) -> None:
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    return
                try:
                    seed, today = self.dhan_api.load_intraday_window(
                        item, interval, self.settings.intraday_history_days
                    )
                    self.state.set_historical(item.security_id, f"{key}_seed", seed)
                    self.state.set_historical(item.security_id, key, today)
                except Exception as exc:
                    self.state.last_feed_error = f"history:{item.symbol}:{key}:{exc}"

            for interval, key in ((1, "1m"), (5, "5m"), (15, "15m"), (60, "1h")):
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    return
                set_window(interval, key)

            if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                return
            try:
                daily = self.dhan_api.load_previous_daily(item, self.settings.daily_lookback)
                self.state.set_historical(item.security_id, "1d", daily)
            except Exception as exc:
                self.state.last_feed_error = f"history:{item.symbol}:1d:{exc}"

        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="psygrid-hist") as pool:
            futures = [pool.submit(load_one, item) for item in self.instruments]
            for future in futures:
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    break
                try:
                    future.result()
                except Exception:
                    pass

        # Initial bootstrap is complete. From here onward, keep native 5m,
        # 15m and 1h candles advancing throughout the session. The WebSocket
        # remains authoritative for the live 1m stream; higher timeframes are
        # refreshed from Dhan's native historical API only.
        if not self.stop_event.is_set() and not self.history_stop.is_set() and self.in_market():
            with self._lock:
                if not self.htf_thread or not self.htf_thread.is_alive():
                    self.htf_thread = threading.Thread(
                        target=self._refresh_native_higher_timeframes,
                        daemon=True,
                        name="psygrid-htf-refresh",
                    )
                    self.htf_thread.start()

    def _refresh_native_interval(self, interval: int, key: str) -> None:
        """Refresh one native timeframe without aggregating 1m candles."""
        def refresh_one(item):
            if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                return
            try:
                rows = self.dhan_api.load_today_completed_intraday(item, interval)
                if rows:
                    self.state.set_historical(item.security_id, key, rows)
            except Exception as exc:
                if self._looks_like_auth_failure(exc):
                    try:
                        with self._lock:
                            self._auth_retry_with_totp()
                            self.dhan_api.settings = self.settings
                            self.feed.settings = self.settings
                        rows = self.dhan_api.load_today_completed_intraday(item, interval)
                        if rows:
                            self.state.set_historical(item.security_id, key, rows)
                        return
                    except Exception as retry_exc:
                        exc = retry_exc
                self.state.last_feed_error = f"history_refresh:{item.symbol}:{key}:{exc}"

        with ThreadPoolExecutor(max_workers=8, thread_name_prefix=f"psygrid-{key}") as pool:
            futures = [pool.submit(refresh_one, item) for item in self.instruments]
            for future in futures:
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    break
                try:
                    future.result()
                except Exception:
                    pass

    def _refresh_native_higher_timeframes(self) -> None:
        """Maintain live native 5m/15m/1h data at completed candle boundaries."""
        schedules = ((5, "5m"), (15, "15m"), (60, "1h"))
        while not self.stop_event.is_set() and not self.history_stop.is_set():
            now = self.now()
            if not self.in_market(now):
                break
            start_h, start_m = map(int, self.settings.market_start.split(":"))
            market_start = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
            elapsed_minutes = int((now - market_start).total_seconds() // 60)
            if elapsed_minutes < 5:
                self.history_stop.wait(2.0)
                continue

            for interval, key in schedules:
                if elapsed_minutes % interval != 0:
                    continue
                # Wait until the boundary has definitely closed so the native
                # historical endpoint is queried only for completed candles.
                slot = elapsed_minutes // interval
                if self._last_htf_refresh_slot.get(key) == slot:
                    continue
                if now.second < 2:
                    continue
                self._last_htf_refresh_slot[key] = slot
                self._refresh_native_interval(interval, key)

            self.history_stop.wait(2.0)

    def _end_session(self) -> None:
        with self._lock:
            self.history_stop.set()
            try:
                self.feed.stop()
            except Exception:
                pass
            if self.history_thread:
                self.history_thread.join(timeout=10)
            if self.htf_thread:
                self.htf_thread.join(timeout=10)
            self.history_thread = None
            self.htf_thread = None
            self.state.finalize_current()
            self.state.reset()
            self._started_for_date = None
            self._auth_retry_at = 0.0
            self._last_reconnect_seen = 0
            self._last_htf_refresh_slot.clear()
