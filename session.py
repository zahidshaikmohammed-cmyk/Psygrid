from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dt_time
from typing import Optional
from zoneinfo import ZoneInfo

from backfill import HistoricalBackfill
from config import refresh_access_token
from dhan_auth import DhanTokenRateLimited, generate_access_token


class SessionManager:
    """Own the market session and bootstrap only the 1m OHLCV dataset."""

    HISTORY_BOOTSTRAP_RETRIES = 3
    HISTORY_RETRY_DELAYS = (0.5, 1.5, 3.0)

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
        self._lock = threading.RLock()
        self._auth_refresh_lock = threading.Lock()
        self._last_auth_refresh_epoch = 0.0
        self._started_for_date: Optional[str] = None
        self._auth_retry_at = 0.0
        self._last_reconnect_seen = 0
        self.backfill = HistoricalBackfill(settings, state, dhan_api, instruments)

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def in_market(self, now: Optional[datetime] = None) -> bool:
        now = now or self.now()
        sh, sm = map(int, self.settings.market_start.split(":"))
        eh, em = map(int, self.settings.market_end.split(":"))
        return dt_time(sh, sm) <= now.time() < dt_time(eh, em)

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
        if self.history_thread and self.history_thread is not threading.current_thread():
            self.history_thread.join(timeout=10)
        self.history_thread = None
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
        secret = os.getenv("DHAN_TOTP_SECRET", "").strip()
        if not pin or not secret:
            raise RuntimeError("Dhan token expired/invalid and TOTP credentials are unavailable")
        token, expiry = generate_access_token(self.settings.client_id, pin, secret)
        self.settings.access_token = token
        self.settings.token_expiry = expiry
        self.settings.token_source = "AUTO_GENERATED_TOTP"

    def _refresh_auth_once(self) -> None:
        now = time.time()
        with self._auth_refresh_lock:
            if now - self._last_auth_refresh_epoch < 60:
                return
            self._auth_retry_with_totp()
            self.dhan_api.settings = self.settings
            self.feed.settings = self.settings
            self.feed.dhan_api = self.dhan_api
            self._last_auth_refresh_epoch = time.time()

    @staticmethod
    def _looks_like_auth_failure(exc: Exception) -> bool:
        text = str(exc).lower()
        return any(x in text for x in ("401", "807", "808", "809", "expired", "invalid token", "authentication failed", "unauthorized"))

    def _start_session(self, now: datetime) -> None:
        with self._lock:
            session_date = now.date().isoformat()
            if self._started_for_date == session_date:
                return
            if now.timestamp() < self._auth_retry_at:
                self.state.set_feed_status("AUTH_WAITING", f"Dhan token generation retry in {int(self._auth_retry_at - now.timestamp())}s")
                return

            self.history_stop.clear()
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
                    self._refresh_auth_once()
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
                self.state.apply_quote_snapshot(snapshot)
                for item in self.instruments:
                    row = snapshot.get(str(item.security_id), snapshot.get(item.security_id, {}))
                    self.state.seed_cumulative_volume(int(item.security_id), int(row.get("volume", 0) or 0) if isinstance(row, dict) else 0)
            except Exception as exc:
                self.state.last_feed_error = f"snapshot:{exc}"

            self.feed.start()
            self.history_thread = threading.Thread(
                target=self._load_1m_history,
                args=(now,),
                daemon=True,
                name="psygrid-live-1m-bootstrap",
            )
            self.history_thread.start()

    def _load_one_1m_history(self, item) -> bool:
        last_exc: Optional[Exception] = None
        for attempt in range(self.HISTORY_BOOTSTRAP_RETRIES):
            if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                return False
            try:
                rows = self.dhan_api.load_today_completed_intraday(item, 1)
                if rows:
                    self.state.merge_today_1m_history(item.security_id, rows)
                    return True
                last_exc = RuntimeError("no_completed_1m_rows_returned")
            except Exception as exc:
                last_exc = exc
                if self._looks_like_auth_failure(exc):
                    try:
                        self._refresh_auth_once()
                        rows = self.dhan_api.load_today_completed_intraday(item, 1)
                        if rows:
                            self.state.merge_today_1m_history(item.security_id, rows)
                            return True
                        last_exc = RuntimeError("no_completed_1m_rows_after_auth_refresh")
                    except Exception as retry_exc:
                        last_exc = retry_exc
            if attempt < self.HISTORY_BOOTSTRAP_RETRIES - 1:
                time.sleep(self.HISTORY_RETRY_DELAYS[attempt])
        with self.state.lock:
            self.state.last_feed_error = f"live_candle_bootstrap:{item.symbol}:1m:{last_exc}"
        return False

    def _load_1m_history(self, now: datetime) -> None:
        # The API throttle is shared across workers, so the pool can remain
        # concurrent without creating request bursts. Every instrument gets
        # multiple attempts; a transient empty/error response is not final.
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix="psygrid-live-1m") as pool:
            futures = {pool.submit(self._load_one_1m_history, item): item for item in self.instruments}
            failed = []
            for future, item in list(futures.items()):
                if self.stop_event.is_set() or self.history_stop.is_set():
                    break
                try:
                    if not future.result():
                        failed.append(item)
                except Exception as exc:
                    failed.append(item)
                    with self.state.lock:
                        self.state.last_feed_error = f"live_candle_bootstrap:{item.symbol}:1m:{exc}"

        # A second bounded pass catches instruments that failed because of a
        # transient provider response while the first 450-request sweep ran.
        if failed and not self.stop_event.is_set() and not self.history_stop.is_set() and self.in_market():
            for item in failed:
                if self.stop_event.is_set() or self.history_stop.is_set() or not self.in_market():
                    break
                self._load_one_1m_history(item)

    def _end_session(self) -> None:
        with self._lock:
            self.history_stop.set()
            try:
                self.feed.stop()
            except Exception:
                pass
            if self.history_thread and self.history_thread is not threading.current_thread():
                self.history_thread.join(timeout=10)
            self.history_thread = None
            self.state.finalize_current()
            self.state.reset()
            self._started_for_date = None
            self._auth_retry_at = 0.0
            self._last_reconnect_seen = 0
