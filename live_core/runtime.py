"""Live Core process: the session lifecycle for one partition and the ``python -m live_core`` entrypoint.

Session (Asia/Kolkata, trading days per ``runtime_guard.is_trading_day``):

* 08:55 - pre-market: load the shared token, resolve and validate the full canonical universe.
* 09:05 (``preopen_connect_minutes`` before the open) - take this node's partition, authenticate
  and verify the Dhan data plan, seed reference prices from one REST quote snapshot and connect
  the WebSocket feed. The session is ``PRE_OPEN``: packets prove the subscription and set volume
  baselines but make no candles. ``/health/node`` reports ``readiness.ready_for_market`` and any
  failure is retried before 09:15.
* 09:15 - the session turns ``LIVE`` (on the first packet after the open, or the next loop pass).
* during the session - publish each minute's candle once it has ended, and after a feed
  reconnect re-fetch today's genuine Dhan 1m bars so the gap is filled from Dhan, not guessed.
* 15:15 - stop the feed (closing its asyncio loop), stop the history worker, wipe all market
  state, collect garbage and return freed heap to the OS. Nothing is archived; nothing carries
  into the next day.

Outside the session no Dhan market feed is held open.

Feed supervision (one supervisor per node: this session loop, under ``_lock``). The supervisor
state is one of CLOSED, STARTING, AUTHENTICATING, CONNECTING, RECOVERING, LIVE, DEGRADED,
RECONNECTING, STOPPING, re-evaluated every second from the feed's state and timestamps. No state
waits forever:

* a feed that is not connected and makes no new connection attempt for ``FEED_STUCK_SECONDS``
  (``FEED_STUCK_LIMIT_COOLDOWN_SECONDS`` after a Dhan limit) is replaced;
* LIVE requires, since the last (re)connect: the socket connected and subscribed, a valid quote
  frame, an accepted packet and fresh data from several stocks. A connected feed that does not
  get there within ``RECOVERY_TIMEOUT_SECONDS`` during market hours is replaced;
* during the session a LIVE feed with no accepted packet for ``ZOMBIE_SECONDS`` is DEGRADED (the
  feed's own watchdog reconnects it) and after ``HARD_RESET_SILENCE_SECONDS`` it is replaced;
* a token renewal by the token authority always replaces the feed: the old LiveFeed is stopped
  (bounded teardown, its private loop closed, its writes detached) and a new one is built with the
  renewed token for the same partition. It is never "half reconnected".

Replacing a feed never touches market state: every published candle stays, the gap is refilled
from Dhan's own 1m bars, and the HTTP server keeps answering throughout (it only reads RAM).
"""

from __future__ import annotations

import contextlib
import ctypes
import gc
import logging
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo

import config as psygrid_config
from auth_retry import looks_like_auth_failure
from dhan_auth import DhanTokenRateLimited
from live_core import SERVICE_NAME
from live_core import auth as live_core_auth
from live_core.aggregate import PeerClient
from live_core.config import LiveCoreConfig, LiveCoreConfigError
from live_core.feed import LiveCoreFeed
from live_core.history import HistoryWorker
from live_core.metrics import MinuteSampler
from live_core.partition import (
    Partition,
    PartitionError,
    Universe,
    build_partition,
    load_universe,
    validate_partitions,
)
from live_core.redact import RedactingFilter, redact, register_secret
from live_core.state import STALE_AFTER_SECONDS, NodeState
from runtime_guard import is_trading_day, process_stats, raise_nofile_limit

log = logging.getLogger("live_core")

GAP_REFILL_DELAY_SECONDS = 90.0
CONFIG_RETRY_SECONDS = 60.0
AUTH_RETRY_SECONDS = 30.0
# A feed that has neither a connection, nor data, nor a new connection attempt for this long is
# stuck (e.g. a reconnect interrupted mid-handshake) and is replaced by a fresh one. Dhan's own
# connection/rate-limit cooldown (300 s) is respected with the longer limit.
FEED_STUCK_SECONDS = 150.0
FEED_STUCK_LIMIT_COOLDOWN_SECONDS = 330.0
# Connection health during the session (separate from the 120 s per-stock freshness rule).
ZOMBIE_SECONDS = 45.0
HARD_RESET_SILENCE_SECONDS = 180.0
RECOVERY_TIMEOUT_SECONDS = 90.0
RECOVERY_MIN_STOCKS = 5
# Minimum spacing between consecutive feed replacements that never reached LIVE (token renewals
# are exempt): bounded retries, never a reconnect storm against Dhan.
RESET_SPACING_SECONDS = (0.0, 30.0, 60.0, 120.0, 300.0)
PREPARE_AHEAD = timedelta(minutes=20)
TRIM_INTERVAL_SECONDS = 60.0
PEER_PROBE_SECONDS = 30.0

SUPERVISOR_STATES = (
    "CLOSED",
    "STARTING",
    "AUTHENTICATING",
    "CONNECTING",
    "RECOVERING",
    "LIVE",
    "DEGRADED",
    "RECONNECTING",
    "STOPPING",
)


def release_memory() -> None:
    """Collect garbage and hand freed heap pages back to the OS (glibc only; a no-op elsewhere)."""
    gc.collect()
    with contextlib.suppress(Exception):
        ctypes.CDLL("libc.so.6").malloc_trim(0)


def _default_api_factory(settings):
    from dhan_api import DhanAPI

    return DhanAPI(settings)


class LiveCoreRuntime:
    def __init__(
        self,
        cfg: LiveCoreConfig,
        universe: Universe,
        partition: Partition,
        *,
        settings_loader=None,
        instrument_loader=None,
        api_factory=None,
        feed_factory=None,
        token_refresher=None,
        now=None,
        clock=time.time,
        peer_get=None,
        token_get=None,
    ):
        self.cfg = cfg
        self.universe = universe
        self.partition = partition
        self.tz = ZoneInfo(psygrid_config.TIMEZONE)
        self.market_start = dt_time(*map(int, psygrid_config.MARKET_START.split(":")))
        self.market_end = dt_time(*map(int, psygrid_config.MARKET_END.split(":")))
        # Data freshness follows the Live Core rule (stale only after 120 s), not the full app's 30 s.
        self.state = NodeState(psygrid_config.TIMEZONE, STALE_AFTER_SECONDS, clock=clock)
        # By default a node consumes the shared Dhan token and never generates one (live_core/auth.py).
        # With LIVE_CORE_TOKEN_SOURCE it takes the token the full PSYGRID (the token authority) holds.
        self.token_source = (
            live_core_auth.TokenSource(cfg.token_source, get=token_get)
            if cfg.token_source and not cfg.token_generation
            else None
        )
        self._last_token_poll = 0.0
        if cfg.token_generation:
            default_loader, default_refresher = psygrid_config.load_settings, psygrid_config.refresh_access_token
        elif self.token_source is not None:
            default_loader, default_refresher = self.token_source.load_settings, self.token_source.refresher
        else:
            default_loader, default_refresher = (
                live_core_auth.load_shared_settings,
                live_core_auth.shared_token_refresher,
            )
        self._settings_loader = settings_loader or default_loader
        self._instrument_loader = instrument_loader or psygrid_config.load_instruments
        self._api_factory = api_factory or _default_api_factory
        self._feed_factory = feed_factory or LiveCoreFeed
        self._token_refresher = token_refresher or default_refresher
        self._now = now or (lambda: datetime.now(self.tz))
        self._clock = clock
        self.peers: dict[int, PeerClient] = {
            peer_id: PeerClient(
                peer_id,
                url,
                expected_partition=build_partition(universe, peer_id, cfg.node_count).describe(),
                timeout_seconds=cfg.peer_timeout_seconds,
                cache_seconds=cfg.peer_cache_seconds,
                stale_seconds=cfg.peer_stale_seconds,
                get=peer_get,
            )
            for peer_id, url in sorted(cfg.peers.items())
        }
        self.settings = None
        self.dhan_api = None
        self.feed: LiveCoreFeed | None = None
        self.history: HistoryWorker | None = None
        self.watchdog = None
        # Called at session end, before memory is released (the API drops its cached bodies here).
        self.session_end_hooks: list = []
        self.config_error = ""
        self.process_started = time.time()
        self.sessions_started = 0
        self.sessions_ended = 0
        self._instruments_date: str | None = None
        self._instruments: list | None = None
        self._started_for_date: str | None = None
        self._retry_at = 0.0
        self._prepare_retry_at = 0.0
        self._reconnects_seen = 0
        self._gap_refill_at: float | None = None
        self._last_trim = 0.0
        self.feed_replacements = 0
        self._feed_watch: tuple[int, int, float] | None = None
        # Feed supervisor (see the module docstring).
        self.supervisor_state = "CLOSED"
        self.supervisor_since = clock()
        self.supervisor_reason = ""
        self.transitions: deque[dict] = deque(maxlen=30)
        self.token_renewals = 0
        self.hard_resets = 0
        self.abandoned_feed_threads = 0
        self.last_hard_reset_reason = ""
        self._recovery_started: float | None = None
        self._resets_since_live = 0
        self._next_reset_allowed = 0.0
        self._auth_verified_epoch: float | None = None
        self._market_opened_date: str | None = None
        self._peer_ready: dict[int, dict] = {}
        self._last_peer_probe = clock()  # first probe one interval after start
        self.http_metrics = None  # set by the API (live_core.metrics.HttpMetrics)
        self.sampler = MinuteSampler(self, opening_window=(psygrid_config.MARKET_START, "09:30"), clock=clock)
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ session window

    def in_market(self, now: datetime | None = None) -> bool:
        now = now or self._now()
        return is_trading_day(now.date()) and self.market_start <= now.time() < self.market_end

    def market_window(self, now: datetime) -> str:
        if self.in_market(now):
            return "OPEN"
        if self.in_session_window(now):
            return "PRE_OPEN"
        return "CLOSED" if is_trading_day(now.date()) else "NON_TRADING_DAY"

    def session_window_start(self, now: datetime) -> datetime:
        opens = datetime.combine(now.date(), self.market_start, tzinfo=now.tzinfo)
        return opens - timedelta(minutes=self.cfg.preopen_connect_minutes)

    def in_session_window(self, now: datetime | None = None) -> bool:
        """The pre-open connect window plus the market session: when a session (and feed) runs."""
        now = now or self._now()
        return is_trading_day(now.date()) and self.session_window_start(now) <= now < datetime.combine(
            now.date(), self.market_end, tzinfo=now.tzinfo
        )

    # ------------------------------------------------------------------ loop

    def tick(self, now: datetime | None = None) -> None:
        now = now or self._now()
        with contextlib.suppress(Exception):  # metrics never take the session loop down
            self.sampler.tick(now)
        if self.in_session_window(now):
            self._watch_token(now)
            if self._started_for_date != now.date().isoformat():
                if self._started_for_date is not None:
                    self._end_session()
                self._start_session(now)
            else:
                self._maintain(now)
        elif self._started_for_date is not None or self.feed is not None or self.state.session_status != "CLOSED":
            # Also clears a session that never went LIVE (auth/config error) once the window closes.
            self._end_session()
        else:
            self._prepare(now)
        self._probe_peers(now)

    # ------------------------------------------------------------------ supervisor state

    def _set_supervisor(self, state: str, reason: str = "", epoch: float | None = None) -> None:
        epoch = self._clock() if epoch is None else epoch
        if state == self.supervisor_state:
            if reason:
                self.supervisor_reason = reason[:300]
            return
        self.transitions.append(
            {
                "at": datetime.fromtimestamp(epoch, self.tz).isoformat(timespec="seconds"),
                "from": self.supervisor_state,
                "to": state,
                "reason": redact(reason)[:300],
            }
        )
        log.info(
            "live core node %s feed supervisor %s -> %s%s",
            self.partition.node_id,
            self.supervisor_state,
            state,
            f": {redact(reason)}" if reason else "",
        )
        self.supervisor_state = state
        self.supervisor_since = epoch
        self.supervisor_reason = redact(reason)[:300]

    def _watch_token(self, now: datetime) -> None:
        """Follow the token authority: when it renews its Dhan token, switch to the new one at once."""
        if self.token_source is None or self.settings is None:
            return
        epoch = now.timestamp()
        if epoch - self._last_token_poll < self.cfg.token_poll_seconds:
            return
        self._last_token_poll = epoch
        try:
            changed = self.token_source.apply(self.settings)
        except Exception as exc:  # the authority being briefly unreachable never stops a running feed
            self.state.record_error(f"token authority: {type(exc).__name__}: {exc}")
            return
        if not changed:
            return
        log.info("Dhan token renewed by the token authority; switching to it")
        self.token_renewals += 1
        register_secret(getattr(self.settings, "access_token", ""))
        if self._started_for_date is None:
            self._retry_at = 0.0  # a session waiting on a rejected token starts now
        else:
            # Never half-reconnect a feed that may be poisoned by the old token: the REST client and
            # history worker share this settings object, and the feed is replaced by a new one.
            self._replace_feed("Dhan token renewed by the token authority", epoch, token=True)

    def _prepare(self, now: datetime) -> None:
        """Pre-market (from 20 minutes before the open): load the token and resolve security ids, so the
        session window only authenticates and connects."""
        session_date = now.date().isoformat()
        if not is_trading_day(now.date()):
            return
        opens = datetime.combine(now.date(), self.market_start, tzinfo=now.tzinfo)
        if not opens - PREPARE_AHEAD <= now < opens or now.timestamp() < self._prepare_retry_at:
            return
        if self.settings is None:
            try:
                self.settings = self._settings_loader()
                self.config_error = ""
            except Exception as exc:  # the session window retries and reports it
                self._prepare_retry_at = now.timestamp() + CONFIG_RETRY_SECONDS
                self.config_error = redact(f"{type(exc).__name__}: {exc}")[:500]
                return
        if self._instruments_date == session_date:
            return
        try:
            self._resolve_instruments(session_date)
        except Exception as exc:  # the session start tries again and reports the error through the session state
            self._prepare_retry_at = now.timestamp() + CONFIG_RETRY_SECONDS
            self.state.record_error(f"pre-open instrument resolution: {type(exc).__name__}: {exc}")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:  # the session loop must outlive any single failure
                self.state.record_error(f"session loop: {type(exc).__name__}: {exc}")
                log.exception("live core session loop error")
            self._stop.wait(1.0)

    def preflight(self) -> None:
        """Load Dhan settings from the environment (no network) so a config error shows before 09:15."""
        if self.settings is not None:
            return
        try:
            self.settings = self._settings_loader()
            self.config_error = ""
        except Exception as exc:
            self.config_error = redact(f"{type(exc).__name__}: {exc}")[:500]
            log.error("live core configuration error: %s", self.config_error)

    def start(self) -> None:
        """Start the one session loop (idempotent: a second call never adds a loop, feed or timer)."""
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self.preflight()
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, daemon=True, name="live-core-session")
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
        self._thread = None
        self._end_session()

    # ------------------------------------------------------------------ session start/stop

    def _resolve_instruments(self, session_date: str) -> list:
        if self._instruments is not None and self._instruments_date == session_date:
            return self._instruments
        resolved = list(self._instrument_loader())
        symbols = tuple(str(item.symbol).upper() for item in resolved)
        if symbols != self.universe.symbols:
            raise PartitionError("resolved instruments do not match the canonical universe order")
        if len({str(item.security_id) for item in resolved}) != len(resolved):
            raise PartitionError("Dhan resolved duplicate security ids")
        self._instruments = resolved[self.partition.start : self.partition.end]
        self._instruments_date = session_date
        del resolved
        release_memory()
        return self._instruments

    def _authenticate(self) -> dict:
        try:
            self._token_refresher(self.settings)
        finally:
            register_secret(getattr(self.settings, "access_token", ""))
        self.dhan_api.settings = self.settings
        try:
            return self.dhan_api.verify_data_access()
        except Exception as first:
            if not looks_like_auth_failure(first):
                raise
            self.state.set_feed_status(
                "TOKEN_REFRESHING",
                "Dhan token expired/invalid; generating one fresh token"
                if self.cfg.token_generation
                else "Dhan rejected the shared token; this node does not generate tokens",
            )
            try:
                self._token_refresher(self.settings, force=True)
            finally:
                register_secret(getattr(self.settings, "access_token", ""))
            self.dhan_api.settings = self.settings
            return self.dhan_api.verify_data_access()

    def _start_session(self, now: datetime) -> None:
        with self._lock:
            session_date = now.date().isoformat()
            epoch = now.timestamp()
            if epoch < self._retry_at:
                return
            try:
                if self.settings is None:
                    self.settings = self._settings_loader()
                instruments = self._resolve_instruments(session_date)
                self.config_error = ""
            except Exception as exc:
                self.config_error = redact(f"{type(exc).__name__}: {exc}")[:500]
                self._retry_at = epoch + CONFIG_RETRY_SECONDS
                self.state.reset()
                self.state.set_session_status("CONFIG_ERROR")
                self.state.set_feed_status("STOPPED", f"configuration: {self.config_error}")
                log.error("live core cannot start the session: %s", self.config_error)
                return

            self._set_supervisor("STARTING", "session window open", epoch)
            self.state.begin(session_date, instruments, self.partition.start, open_time=psygrid_config.MARKET_START)
            self.state.set_session_status("AUTHENTICATING")
            self.state.set_feed_status("AUTHENTICATING")
            self._set_supervisor("AUTHENTICATING", "verifying the Dhan token and data plan", epoch)
            self._auth_verified_epoch = None
            if self.dhan_api is None:
                self.dhan_api = self._api_factory(self.settings)
            try:
                self._authenticate()
            except DhanTokenRateLimited as exc:
                self._retry_at = epoch + exc.retry_after
                self.state.set_session_status("AUTH_WAITING")
                self.state.set_feed_status(
                    "AUTH_WAITING", f"Dhan token generation rate-limited; retry in {exc.retry_after}s"
                )
                self._set_supervisor("AUTHENTICATING", f"token rate-limited; retry in {exc.retry_after}s", epoch)
                return
            except Exception as exc:
                self._retry_at = epoch + AUTH_RETRY_SECONDS
                self.state.set_session_status("AUTH_ERROR")
                self.state.set_feed_status("AUTH_ERROR", f"authentication: {type(exc).__name__}: {exc}"[:500])
                self._set_supervisor(
                    "AUTHENTICATING", f"authentication failed; retry in {int(AUTH_RETRY_SECONDS)}s", epoch
                )
                return
            self._auth_verified_epoch = epoch

            # Before the open the session is PRE_OPEN: connected, validated, no candles yet.
            self.state.set_session_status("LIVE" if self.in_market(now) else "PRE_OPEN")
            try:
                self.state.seed_from_snapshot(self.dhan_api.quote_snapshot(instruments))
            except Exception as exc:
                self.state.record_error(f"quote snapshot: {type(exc).__name__}: {exc}")
            try:
                feed = self._feed_factory(self.settings, self.state, instruments)
                feed.start()
            except Exception as exc:
                # Never report LIVE without a feed: retry the whole start shortly.
                self._retry_at = epoch + AUTH_RETRY_SECONDS
                self.state.set_session_status("FEED_ERROR")
                self.state.set_feed_status("ERROR", f"feed start: {type(exc).__name__}: {exc}"[:500])
                self._set_supervisor("CONNECTING", "feed could not start; retrying", epoch)
                return
            self.feed = feed
            self._recovery_started = epoch
            self._resets_since_live = 0
            self._next_reset_allowed = 0.0
            self._feed_watch = None
            if self.state.session_status == "LIVE":
                self._market_opened_date = session_date
            self._set_supervisor("CONNECTING", "feed started; waiting for the socket and valid packets", epoch)
            self._started_for_date = session_date
            self._retry_at = 0.0
            self._reconnects_seen = 0
            self._gap_refill_at = None
            self.sessions_started += 1
            self.history = HistoryWorker(
                self.state, self.dhan_api, self.settings, instruments, self.cfg.history_interval_seconds
            )
            self.history.start()
            opened = now.replace(hour=self.market_start.hour, minute=self.market_start.minute, second=0, microsecond=0)
            if (
                self.cfg.history_bootstrap
                and self.state.session_status == "LIVE"
                and now - opened >= timedelta(minutes=1)
            ):
                # A start after the open (restart, reboot, late deploy): load today's bars so far.
                self.history.enqueue_all()
            log.info(
                "live core node %s session %s %s: %s instruments [%s, %s)",
                self.partition.node_id,
                session_date,
                self.state.session_status,
                len(instruments),
                self.partition.start,
                self.partition.end,
            )

    def _maintain(self, now: datetime) -> None:
        epoch = now.timestamp()
        session_date = now.date().isoformat()
        if self.in_market(now) and self._market_opened_date != session_date:
            self._open_market(now)
        live = self.state.session_status == "LIVE"
        if live:
            self.state.finalize_due(epoch, self.cfg.finalize_grace_seconds)
        self._supervise(now)
        if epoch - self._last_trim >= TRIM_INTERVAL_SECONDS:
            # Encoded fragments are replaced every minute; hand the freed heap back to the OS so RSS
            # on a 1 GB VM tracks what is actually live (a few milliseconds, glibc only).
            self._last_trim = epoch
            with contextlib.suppress(Exception):
                ctypes.CDLL("libc.so.6").malloc_trim(0)
        with self.state.lock:
            reconnects = self.state.websocket_reconnects
        if reconnects > self._reconnects_seen:
            self._reconnects_seen = reconnects
            # Wait until the interrupted minutes have closed, then take Dhan's bars for them.
            self._gap_refill_at = epoch + GAP_REFILL_DELAY_SECONDS
        if self._gap_refill_at is not None and epoch >= self._gap_refill_at and live:
            self._gap_refill_at = None
            if self.cfg.history_bootstrap and self.history is not None:
                self.history.enqueue_all()

    def _open_market(self, now: datetime) -> None:
        """09:15: the PRE_OPEN session becomes LIVE and the feed must prove itself with real trades."""
        epoch = now.timestamp()
        self._market_opened_date = now.date().isoformat()
        if self.state.session_status == "PRE_OPEN":
            self.state.set_session_status("LIVE")
        self._recovery_started = epoch
        log.info("live core node %s market open: session LIVE", self.partition.node_id)
        if self.supervisor_state == "LIVE":
            self._set_supervisor("RECOVERING", "market open: waiting for accepted trades", epoch)

    def recovery_stages(self, epoch: float | None = None) -> dict:
        """The staged evidence a (re)connected feed must show before it is LIVE again."""
        epoch = self._clock() if epoch is None else epoch
        since = self._recovery_started
        expected = len(self._instruments or ())
        with self.state.lock:
            status = self.state.feed_status
            connected_at = self.state.websocket_connected_epoch
            subscribed = self.state.subscribed_count
            last_frame = self.state.last_quote_packet_epoch or self.state.last_message_epoch
            last_accepted = self.state.last_tick_received_epoch
            fresh_since = (
                sum(
                    1
                    for series in self.state.ordered
                    if series.last_received is not None and (since is None or series.last_received >= since)
                )
                if since is not None
                else None
            )
        floor = since if since is not None else float("-inf")
        need = max(1, min(RECOVERY_MIN_STOCKS, expected or 1))
        pre_open = self.state.session_status == "PRE_OPEN"
        stages = {
            "connected": status == "CONNECTED" and connected_at is not None and subscribed == expected,
            # An accepted packet is necessarily a valid one, whichever arrived first.
            "valid_packet": (last_frame is not None and last_frame >= floor)
            or (last_accepted is not None and last_accepted >= floor),
            # Before the open no trade can be accepted as live data; the open re-checks these.
            "accepted_packet": True if pre_open else (last_accepted is not None and last_accepted >= floor),
            "freshness_recovering": True if pre_open else (fresh_since is None or fresh_since >= need),
        }
        stages["since"] = datetime.fromtimestamp(since, self.tz).isoformat(timespec="seconds") if since else None
        stages["fresh_stocks_since"] = fresh_since
        return stages

    def _supervise(self, now: datetime) -> None:
        """One deterministic supervisor pass (session thread only)."""
        epoch = now.timestamp()
        if not self._instruments or self._started_for_date is None:
            return
        feed = self.feed
        if feed is None:
            # A replacement that could not start: try again, spaced.
            self._replace_feed("no feed is running", epoch)
            return
        in_market = self.in_market(now)
        with self.state.lock:
            status = self.state.feed_status
            last_message = self.state.last_message_epoch
            last_accepted = self.state.last_tick_received_epoch
            connected_at = self.state.websocket_connected_epoch
            last_error = self.state.last_feed_error or ""
        cycles = int(getattr(feed, "connection_cycles", 0) or 0)

        if status != "CONNECTED":
            if self.supervisor_state in ("LIVE", "RECOVERING") and self._recovery_started is None:
                self._recovery_started = epoch
            if self._recovery_started is None:
                self._recovery_started = epoch
            receiving = last_message is not None and epoch - last_message < FEED_STUCK_SECONDS
            self._set_supervisor(
                "DEGRADED" if status == "ERROR" else "RECONNECTING", f"feed {status}: {last_error}"[:300], epoch
            )
            if receiving:
                self._feed_watch = None
                return
            if self._feed_watch is None or self._feed_watch[:2] != (id(feed), cycles):
                self._feed_watch = (id(feed), cycles, epoch)  # a new feed or connection attempt is progress
                return
            limit = FEED_STUCK_LIMIT_COOLDOWN_SECONDS if "limit" in last_error.lower() else FEED_STUCK_SECONDS
            if epoch - self._feed_watch[2] >= limit:
                stuck_for = int(epoch - self._feed_watch[2])
                self._replace_feed(f"feed stuck for {stuck_for}s ({status}, no new connection attempt)", epoch)
            return
        self._feed_watch = None

        if self._recovery_started is not None:
            stages = self.recovery_stages(epoch)
            if all(stages[key] for key in ("connected", "valid_packet", "accepted_packet", "freshness_recovering")):
                self._recovery_started = None
                self._resets_since_live = 0
                self._next_reset_allowed = 0.0
                self._set_supervisor("LIVE", "connected, subscribed and receiving valid market packets", epoch)
                return
            missing = [
                key
                for key in ("connected", "valid_packet", "accepted_packet", "freshness_recovering")
                if not stages[key]
            ]
            self._set_supervisor("RECOVERING", "waiting for: " + ", ".join(missing), epoch)
            if in_market and epoch - self._recovery_started >= RECOVERY_TIMEOUT_SECONDS:
                self._replace_feed(
                    f"connected but no valid market data for {int(epoch - self._recovery_started)}s "
                    f"(missing: {', '.join(missing)})",
                    epoch,
                )
            return

        if in_market and self.state.session_status == "LIVE":
            silent = epoch - max(last_accepted or 0.0, connected_at or 0.0)
            if silent >= HARD_RESET_SILENCE_SECONDS:
                self._replace_feed(f"no accepted market packet for {int(silent)}s", epoch)
            elif silent >= ZOMBIE_SECONDS:
                self._recovery_started = epoch
                self._set_supervisor(
                    "DEGRADED", f"no accepted market packet for {int(silent)}s; the feed watchdog reconnects it", epoch
                )
            return
        self._set_supervisor("LIVE", self.supervisor_reason, epoch)

    def _replace_feed(self, reason: str, epoch: float, *, token: bool = False) -> bool:
        """Stop the current LiveFeed (bounded, detached) and start a completely new one for the same
        partition with the current settings. Market state is never touched."""
        with self._lock:
            if self._started_for_date is None or not self._instruments or self.settings is None:
                return False
            if not token and epoch < self._next_reset_allowed:
                return False
            old, self.feed = self.feed, None
            self._set_supervisor("RECONNECTING", f"replacing the feed: {reason}", epoch)
            log.warning("live core node %s: replacing the feed: %s", self.partition.node_id, redact(reason))
            self.state.record_error(f"feed supervisor: replacing the feed: {reason}")
            if old is not None:
                with contextlib.suppress(Exception):
                    old.stop()
                if getattr(old, "abandoned", False) or (
                    callable(getattr(old, "thread_alive", None)) and old.thread_alive()
                ):
                    self.abandoned_feed_threads += 1
                    self.state.record_error("feed supervisor: the old feed thread did not exit in time (detached)")
            del old
            self.hard_resets += 1
            self.last_hard_reset_reason = redact(reason)[:300]
            if not token:
                spacing = RESET_SPACING_SECONDS[min(self._resets_since_live, len(RESET_SPACING_SECONDS) - 1)]
                self._resets_since_live += 1
                next_spacing = RESET_SPACING_SECONDS[min(self._resets_since_live, len(RESET_SPACING_SECONDS) - 1)]
                self._next_reset_allowed = epoch + max(spacing, next_spacing)
            # Counted as a reconnect: once the interrupted minutes close, Dhan's bars refill the gap.
            self.state.note_reconnect(f"feed replaced: {reason}")
            self._recovery_started = epoch
            self._feed_watch = None
            try:
                replacement = self._feed_factory(self.settings, self.state, self._instruments)
                replacement.start()
            except Exception as exc:
                self.state.set_feed_status("ERROR", f"feed restart: {type(exc).__name__}: {exc}"[:500])
                self._set_supervisor("RECONNECTING", "the replacement feed could not start; retrying", epoch)
                return False
            self.feed = replacement
            self.feed_replacements += 1
            self._set_supervisor(
                "CONNECTING", f"new feed started ({'token renewal' if token else 'hard reset'})", epoch
            )
            return True

    def _probe_peers(self, now: datetime) -> None:
        """Every ``PEER_PROBE_SECONDS`` record each peer's reachability for the readiness check.

        Uses the peer client's own timeout and back-off; the result is cached, so health never waits on a peer.
        """
        epoch = now.timestamp()
        if not self.peers or epoch - self._last_peer_probe < PEER_PROBE_SECONDS:
            return
        self._last_peer_probe = epoch
        for node_id, peer in self.peers.items():
            try:
                answer = peer.probe()
            except Exception as exc:
                answer = {"reachable": False, "health": None, "error": f"{type(exc).__name__}: {exc}"}
            remote = answer.get("health") or {}
            expected = build_partition(self.universe, node_id, self.partition.node_count).describe()
            fingerprint_ok = remote.get("partition", {}).get("fingerprint") == expected["fingerprint"]
            self._peer_ready[node_id] = {
                "reachable": bool(answer.get("reachable")),
                "partition_ok": bool(answer.get("reachable")) and fingerprint_ok,
                "status": remote.get("status"),
                "session_status": (remote.get("session") or {}).get("session_status"),
                "ready_for_market": (remote.get("readiness") or {}).get("ready_for_market"),
                "error": redact(str(answer.get("error") or ""))[:200],
                "checked_at": datetime.fromtimestamp(epoch, self.tz).isoformat(timespec="seconds"),
            }

    def _end_session(self) -> None:
        with self._lock:
            if self._started_for_date is not None or self.feed is not None:
                self._set_supervisor("STOPPING", "session window closed")
            feed, self.feed = self.feed, None
            history, self.history = self.history, None
            if feed is not None:
                with contextlib.suppress(Exception):
                    feed.stop()
            if history is not None:
                with contextlib.suppress(Exception):
                    history.stop()
            api, self.dhan_api = self.dhan_api, None
            session = getattr(api, "session", None)
            if session is not None:
                with contextlib.suppress(Exception):
                    session.close()
            had_session = self._started_for_date is not None
            self._started_for_date = None
            self._gap_refill_at = None
            self._reconnects_seen = 0
            self._recovery_started = None
            self._feed_watch = None
            self._auth_verified_epoch = None
            self._market_opened_date = None
            self._resets_since_live = 0
            self._next_reset_allowed = 0.0
            self.state.reset()
            for peer in self.peers.values():
                peer.forget()
            for hook in list(self.session_end_hooks):
                with contextlib.suppress(Exception):
                    hook()
            if had_session:
                self.sessions_ended += 1
                log.info("live core node %s session ended; market state cleared", self.partition.node_id)
            del feed, history, api
            release_memory()
            self._set_supervisor("CLOSED", "market state cleared")

    # ------------------------------------------------------------------ health

    def readiness(self, now: datetime | None = None) -> dict:
        """READY_FOR_MARKET: every check the open depends on, evaluated from RAM (never waits on a peer)."""
        now = now or self._now()
        expected = self.partition.size
        snap = self.state.snapshot()
        with self.state.lock:
            connected_at = self.state.websocket_connected_epoch
            last_message = self.state.last_message_epoch
        session_live = snap["session_status"] in ("PRE_OPEN", "LIVE")
        peers = {
            str(node_id): dict(self._peer_ready.get(node_id) or {"reachable": None, "partition_ok": None})
            for node_id in self.peers
        }
        checks = {
            "token_available": bool(self.settings is not None and getattr(self.settings, "access_token", "")),
            "data_access_verified": self._auth_verified_epoch is not None and session_live,
            "instruments_loaded": snap["instrument_count"] == expected and session_live,
            "partition_correct": self.partition.size == expected
            and (not self._instruments or len(self._instruments) == expected),
            "websocket_connected": snap["feed_status"] == "CONNECTED",
            "subscriptions_complete": snap["subscribed_instrument_count"] == expected,
            "valid_packets_received": bool(
                connected_at is not None and last_message is not None and last_message >= connected_at
            ),
            "candle_engine_ready": session_live and snap["session_date"] == now.date().isoformat(),
            "http_ready": True,  # this answer is being served
            "peers_ready": all(peer.get("partition_ok") for peer in peers.values()) if peers else True,
        }
        failing = [name for name, ok in checks.items() if not ok]
        return {
            "ready_for_market": not failing,
            "failing_checks": failing,
            "checks": checks,
            "peers": peers,
            "expected_instrument_count": expected,
        }

    def lifecycle_phase(self, now: datetime | None = None) -> str:
        """PRE_MARKET, PRE_OPEN, READY, OPEN, DEGRADED, RECOVERING, CLOSED or NON_TRADING_DAY."""
        now = now or self._now()
        if not is_trading_day(now.date()):
            return "NON_TRADING_DAY"
        status = self.state.session_status
        if self.in_market(now):
            if status != "LIVE" or self.supervisor_state in ("DEGRADED", "RECONNECTING"):
                return "DEGRADED"
            return "RECOVERING" if self.supervisor_state in ("RECOVERING", "CONNECTING") else "OPEN"
        if self.in_session_window(now):
            return "READY" if self.readiness(now)["ready_for_market"] else "PRE_OPEN"
        opens = datetime.combine(now.date(), self.market_start, tzinfo=now.tzinfo)
        if opens - PREPARE_AHEAD <= now < opens:
            return "PRE_MARKET"
        return "CLOSED"

    def supervisor_status(self) -> dict:
        epoch = self._clock()
        return {
            "state": self.supervisor_state,
            "state_age_seconds": round(max(0.0, epoch - self.supervisor_since), 1),
            "reason": self.supervisor_reason,
            "recovery": self.recovery_stages(epoch) if self._recovery_started is not None else None,
            "token_renewals": self.token_renewals,
            "hard_resets": self.hard_resets,
            "last_hard_reset_reason": self.last_hard_reset_reason,
            "abandoned_feed_threads": self.abandoned_feed_threads,
            "next_reset_allowed_in_seconds": round(max(0.0, self._next_reset_allowed - epoch), 1),
            "limits": {
                "zombie_seconds": ZOMBIE_SECONDS,
                "hard_reset_silence_seconds": HARD_RESET_SILENCE_SECONDS,
                "recovery_timeout_seconds": RECOVERY_TIMEOUT_SECONDS,
                "stuck_seconds": FEED_STUCK_SECONDS,
                "stuck_after_dhan_limit_seconds": FEED_STUCK_LIMIT_COOLDOWN_SECONDS,
                "stock_stale_after_seconds": STALE_AFTER_SECONDS,
            },
            "transitions": list(self.transitions),
        }

    def node_health(self) -> dict:
        now = self._now()
        process = process_stats()
        process["uptime_seconds"] = round(time.time() - self.process_started, 1)
        process["max_rss_mb"] = self.cfg.max_rss_mb
        snap = self.state.snapshot()
        freshness = self.state.freshness()
        feed = self.feed
        lifecycle = {
            "feed_thread_alive": False,
            "retired": False,
            "abandoned": False,
            "zombie_reconnects": 0,
            "connection_cycles": 0,
            "feeds_closed": 0,
            "event_loops_closed": 0,
            "event_loops_leaked": 0,
            "packet_errors": 0,
            "internal_reconnects": 0,
            "silence_reconnects": 0,
            "resubscribed_instruments": 0,
            "resubscribe_failures": 0,
        }
        if feed is not None:
            lifecycle.update(feed.lifecycle())
        history = self.history.status() if self.history is not None else None
        in_hours = self.in_market(now)
        expected = self.partition.size
        reasons: list[str] = []
        if self.config_error:
            reasons.append(self.config_error)
        ratio = process.get("fd_usage_ratio")
        if ratio is not None and ratio >= 0.7:
            reasons.append(f"open file descriptors high: {process.get('open_fds')}/{process.get('fd_limit')}")
        rss = process.get("rss_mb")
        if rss is not None and rss >= 0.85 * self.cfg.max_rss_mb:
            reasons.append(f"memory high: rss {rss}MB of {self.cfg.max_rss_mb}MB budget")
        if lifecycle["event_loops_leaked"]:
            reasons.append(f"{lifecycle['event_loops_leaked']} MarketFeed event loop(s) were not closed")
        watchdog = self.watchdog.last_check if self.watchdog is not None else None
        if watchdog and watchdog.get("ok") is False:
            reasons.extend(f"watchdog: {reason}" for reason in watchdog.get("reasons", []))
        readiness = self.readiness(now)
        if not in_hours and self.in_session_window(now) and not readiness["ready_for_market"]:
            # Pre-open: whatever would break the open is visible (and being retried) before 09:15.
            reasons.append("pre-open: not ready for the market: " + ", ".join(readiness["failing_checks"]))
        if self.abandoned_feed_threads:
            reasons.append(f"{self.abandoned_feed_threads} replaced feed thread(s) did not exit in time")
        if in_hours and not self.config_error:
            if self.supervisor_state != "LIVE":
                reasons.append(f"feed supervisor is {self.supervisor_state}: {self.supervisor_reason}"[:300])
            if snap["session_status"] != "LIVE":
                reasons.append(f"market hours but session is {snap['session_status']}")
            if snap["feed_status"] != "CONNECTED":
                reasons.append(f"market hours but Dhan feed is {snap['feed_status']}")
            if not lifecycle["feed_thread_alive"]:
                reasons.append("market hours but Dhan feed thread is not running")
            if snap["subscribed_instrument_count"] != expected:
                reasons.append(f"subscribed {snap['subscribed_instrument_count']} of {expected} instruments")
            if not freshness["fresh"]:
                reasons.append(
                    f"market data stale: last tick {freshness['last_tick_age_seconds']}s ago "
                    f"(max {freshness['max_live_age_seconds']}s)"
                )
        status = "CONFIG_ERROR" if self.config_error else ("DEGRADED" if reasons else "OK")
        data_quality = {
            "rejected_packets": snap["rejected_packets"],
            "duplicate_trades": snap["duplicate_trades"],
            "no_trade_today_packets": snap["no_trade_today_packets"],
            "repeated_last_trade_packets": snap["repeated_last_trade_packets"],
            "render_errors": snap["render_errors"],
            "packet_errors": lifecycle["packet_errors"],
        }
        return {
            "service": "PSYGRID",
            "runtime": SERVICE_NAME,
            "status": status,
            "reasons": reasons,
            "checked_at": now.isoformat(timespec="seconds"),
            "node_id": self.partition.node_id,
            "node_count": self.partition.node_count,
            # Independent dimensions: an answering API, a reconnecting socket and fresh data can coexist.
            "dimensions": _dimensions(snap, freshness, in_hours, expected),
            "partition": self.partition.describe(),
            "lifecycle": {
                "phase": self.lifecycle_phase(now),
                "preopen_connect_at": self.session_window_start(now).strftime("%H:%M"),
                "market_open": psygrid_config.MARKET_START,
                "market_close": psygrid_config.MARKET_END,
            },
            "readiness": readiness,
            "supervisor": self.supervisor_status(),
            "liveness": {
                "last_valid_packet_at": snap["last_quote_packet_at"],
                "last_accepted_packet_at": snap["last_accepted_packet_at"],
                "last_subscription_at": snap["websocket_connected_at"],
                "last_candle_published_at": snap["last_candle_published_at"],
                "preopen_packets": snap["preopen_packets"],
                "preopen_trade_packets": snap["preopen_trade_packets"],
                "zombie_after_seconds": ZOMBIE_SECONDS,
            },
            "http": self.http_metrics.snapshot() if self.http_metrics is not None else None,
            "session": {
                "timezone": psygrid_config.TIMEZONE,
                "window": f"{psygrid_config.MARKET_START}-{psygrid_config.MARKET_END}",
                "trading_day": is_trading_day(now.date()),
                "market_window": self.market_window(now),
                "session_status": snap["session_status"],
                "session_date": snap["session_date"],
                "sessions_started": self.sessions_started,
                "sessions_ended": self.sessions_ended,
            },
            "feed": {
                "feed_status": snap["feed_status"],
                "subscribed_instrument_count": snap["subscribed_instrument_count"],
                "expected_instrument_count": expected,
                "instruments_in_session": snap["instrument_count"],
                "websocket_reconnects": snap["websocket_reconnects"],
                "websocket_connected_at": snap["websocket_connected_at"],
                "last_feed_message_at": snap["last_feed_message_at"],
                "last_feed_message_age_seconds": snap["last_feed_message_age_seconds"],
                "last_message_type": snap["last_message_type"],
                "feed_messages": snap["feed_messages"],
                "quote_packets": snap["quote_packets"],
                "last_feed_error": snap["last_feed_error"],
                **lifecycle,
                "feed_replacements": self.feed_replacements,
            },
            "freshness": freshness,
            "data_quality": data_quality,
            # The full PSYGRID /health blocks, with the same keys, for existing monitors.
            "dhan": {
                "feed_status": snap["feed_status"],
                "feed_thread_alive": lifecycle["feed_thread_alive"],
                "stream_health": freshness["stream_health"],
                "websocket_reconnects": snap["websocket_reconnects"],
                "websocket_connected_at": snap["websocket_connected_at"],
                "last_message_at": snap["last_feed_message_at"],
                "last_feed_error": snap["last_feed_error"],
                "token_validity": None,
            },
            "data": {
                "fresh": freshness["fresh"],
                "last_market_timestamp": freshness["last_market_timestamp"],
                "last_tick_age_seconds": freshness["last_tick_age_seconds"],
                "max_live_age_seconds": int(freshness["max_live_age_seconds"]),
                "stock_count": snap["instrument_count"],
                "subscribed_count": snap["subscribed_instrument_count"],
                "live_stock_count": freshness["live_stock_count"],
                "last_endpoint_generated_at": None,
            },
            "index_layer": {"available": False, "feed_status": {}, "error": "not part of the Live Core"},
            "history": history,
            "process": process,
            "memory": self.state.memory_summary(),
            "watchdog": watchdog,
            "auth": self.token_source.describe()
            if self.token_source is not None
            else live_core_auth.describe(self.cfg.token_generation),
            "storage": {"market_data_on_disk": False, "archive_enabled": False, "microstructure_enabled": False},
            "errors": list(self.state.errors),
        }

    def ready_payload(self) -> tuple[dict, bool]:
        """The full PSYGRID /ready contract, for this node's partition."""
        health = self.node_health()
        snap = self.state.snapshot()
        freshness = health["freshness"]
        expected = self.partition.size
        ready = bool(
            snap["session_status"] == "LIVE"
            and snap["feed_status"] == "CONNECTED"
            and snap["instrument_count"] == expected
            and snap["subscribed_instrument_count"] == expected
            and freshness["live_stock_count"] == expected
            and freshness["stream_health"] == "FULL_LIVE"
        )
        payload = {
            "service": "PSYGRID",
            "ready": ready,
            "node_id": self.partition.node_id,
            "session_date": snap["session_date"],
            "session_status": snap["session_status"],
            "feed_status": snap["feed_status"],
            "stream_health": freshness["stream_health"],
            "last_feed_error": snap["last_feed_error"],
            "last_tick_at": freshness["last_market_timestamp"],
            "last_tick_age_seconds": freshness["last_tick_age_seconds"],
            "max_live_age_seconds": int(freshness["max_live_age_seconds"]),
            "live_stock_count": freshness["live_stock_count"],
            "subscribed_count": snap["subscribed_instrument_count"],
            "feed_messages": snap["feed_messages"],
            "quote_packets": snap["quote_packets"],
            "live_quotes": snap["live_quotes"],
            "websocket_reconnects": snap["websocket_reconnects"],
            "last_message_type": snap["last_message_type"],
            "last_message_at": snap["last_feed_message_at"],
            "websocket_connected_at": snap["websocket_connected_at"],
            "data_plan_status": "UNKNOWN",
            "data_validity": None,
            "token_validity": None,
            "stock_count": snap["instrument_count"],
            "one_minute_candles_only": True,
            "depth_enabled": False,
            "higher_timeframes_enabled": False,
            "indicators_enabled": False,
        }
        return payload, ready

    def public_health(self) -> dict:
        """``/public/health.json`` in the full PSYGRID's schema (components, counts, overall status)."""
        from health_monitor import build_health, component_health

        cluster_view = self.cluster_health()
        nodes = cluster_view["cluster"]["nodes"]
        now = self._now()
        in_hours = self.in_market(now)
        components = []
        message_times = []
        for node_id in sorted(nodes, key=int):
            node = nodes[node_id]
            if node.get("last_feed_message_at"):
                message_times.append(node["last_feed_message_at"])
            live = node.get("session_status") == "LIVE" and node.get("feed_status") == "CONNECTED"
            components.append(
                component_health(
                    name=f"live_core_node_{node_id}",
                    status=("LIVE" if live else (node.get("feed_status") or "ERROR"))
                    if node.get("reachable")
                    else None,
                    updated_at=node.get("last_feed_message_at"),
                    expected_refresh_seconds=2.0,
                    now=now,
                    last_error=node.get("error") or "",
                    record_count=node.get("subscribed_instrument_count") or 0,
                    expected_record_count=node.get("expected_instrument_count")
                    or build_partition(self.universe, int(node_id), self.partition.node_count).size,
                    extra={
                        "session_status": node.get("session_status"),
                        "feed_status": node.get("feed_status"),
                        "websocket_reconnects": node.get("websocket_reconnects") or 0,
                    },
                )
            )
        not_live = [c["source_status"] for c in components if c["source_status"] != "LIVE"]
        if not any(node.get("reachable") for node in nodes.values()):
            equity_status = None
        else:
            equity_status = not_live[0] if not_live else "LIVE"
        components.insert(
            0,
            component_health(
                # Same component name as the full PSYGRID's equity feed, for existing monitors.
                name="equity_990",
                status=equity_status,
                updated_at=max(message_times) if message_times else None,
                expected_refresh_seconds=2.0,
                now=now,
                record_count=cluster_view["cluster"]["covered_instrument_count"],
                expected_record_count=self.universe.size,
                extra={
                    "session_status": self.state.session_status,
                    "feed_status": self.state.feed_status,
                    "websocket_reconnects": sum(n.get("websocket_reconnects") or 0 for n in nodes.values()),
                },
            ),
        )
        out_of_session = frozenset() if in_hours else frozenset(c["name"] for c in components)
        payload = build_health(components, "OPEN" if self.state.session_status == "LIVE" else "CLOSED", out_of_session)
        payload["archive"] = {"status": "DISABLED"}
        payload["live_core"] = cluster_view["cluster"]
        return payload

    def cluster_health(self) -> dict:
        local = self.node_health()
        nodes: dict[str, dict] = {str(self.partition.node_id): _node_summary(local, reachable=True, source="local")}
        for node_id in range(self.partition.node_count):
            if node_id == self.partition.node_id:
                continue
            peer = self.peers.get(node_id)
            if peer is None:
                nodes[str(node_id)] = {"reachable": False, "healthy": False, "error": "peer URL not configured"}
                continue
            answer = peer.health()
            if not answer["reachable"]:
                nodes[str(node_id)] = {"reachable": False, "healthy": False, "error": answer["error"]}
                continue
            remote = answer["health"]
            summary = _node_summary(remote, reachable=True, source="peer")
            expected = build_partition(self.universe, node_id, self.partition.node_count).describe()
            if remote.get("partition", {}).get("fingerprint") != expected["fingerprint"]:
                summary["healthy"] = False
                summary["error"] = "partition/universe fingerprint mismatch"
            nodes[str(node_id)] = summary
        in_hours = self.in_market()
        all_healthy = all(node.get("healthy") for node in nodes.values())
        all_reachable = all(node.get("reachable") for node in nodes.values())
        covered = sum(
            node.get("subscribed_instrument_count") or 0
            for node in nodes.values()
            if node.get("reachable") and node.get("session_status") == "LIVE" and not node.get("error")
        )
        if in_hours:
            partitions_covered = all_healthy and covered == self.universe.size
            coverage = "LIVE_COMPLETE" if partitions_covered else "LIVE_INCOMPLETE"
        else:
            partitions_covered = all_reachable and not any(node.get("error") for node in nodes.values())
            coverage = "IDLE_READY" if partitions_covered else "IDLE_NODE_MISSING"
        cluster = {
            "dimensions": {
                "http": "AVAILABLE",
                "nodes_reachable": sum(1 for node in nodes.values() if node.get("reachable")),
                "coverage": coverage,
            },
            "node_count": self.partition.node_count,
            "expected_instrument_count": self.universe.size,
            "covered_instrument_count": covered,
            "all_nodes_healthy": all_healthy,
            "partitions_covered": partitions_covered,
            "coverage_status": coverage,
            "nodes": nodes,
            "peers": [peer.status() for peer in self.peers.values()],
        }
        return {**local, "cluster": cluster}


def _node_summary(health: dict, *, reachable: bool, source: str) -> dict:
    feed = health.get("feed", {})
    session = health.get("session", {})
    freshness = health.get("freshness", {})
    process = health.get("process", {})
    return {
        "reachable": reachable,
        "source": source,
        "healthy": health.get("status") == "OK",
        "status": health.get("status"),
        "reasons": health.get("reasons", []),
        "session_status": session.get("session_status"),
        "feed_status": feed.get("feed_status"),
        "subscribed_instrument_count": feed.get("subscribed_instrument_count"),
        "expected_instrument_count": feed.get("expected_instrument_count"),
        "websocket_reconnects": feed.get("websocket_reconnects"),
        "fresh": freshness.get("fresh"),
        "last_tick_age_seconds": freshness.get("last_tick_age_seconds"),
        "last_feed_message_at": feed.get("last_feed_message_at"),
        "stale_stock_count": freshness.get("stale_stock_count"),
        "dimensions": health.get("dimensions"),
        "open_fds": process.get("open_fds"),
        "rss_mb": process.get("rss_mb"),
        "partition": health.get("partition", {}).get("start_index"),
        "partition_end": health.get("partition", {}).get("end_index"),
        "supervisor_state": (health.get("supervisor") or {}).get("state"),
        "ready_for_market": (health.get("readiness") or {}).get("ready_for_market"),
        "lifecycle_phase": (health.get("lifecycle") or {}).get("phase"),
    }


def _dimensions(snap: dict, freshness: dict, in_hours: bool, expected: int) -> dict:
    """Feed connection, data freshness and partition coverage as separate, independent states."""
    session = snap["session_status"]
    if session != "LIVE":
        data = "CLOSED" if session == "CLOSED" else "NO_SESSION"
    elif freshness["live_stock_count"] == 0 and freshness["stale_stock_count"] == 0:
        data = "NO_DATA"
    elif not freshness["fresh"]:
        data = "STALE"
    elif freshness["stale_stock_count"] or freshness["no_quote_stock_count"]:
        data = "FRESH_WITH_STALE_STOCKS"
    else:
        data = "FRESH"
    coverage = ("COMPLETE" if snap["instrument_count"] == expected else "INCOMPLETE") if session == "LIVE" else "IDLE"
    return {
        "http": "AVAILABLE",
        "feed": snap["feed_status"] if session == "LIVE" or in_hours else "IDLE",
        "data": data,
        "coverage": coverage,
        "stale_after_seconds": freshness["max_live_age_seconds"],
    }


def build_runtime(environ=None) -> LiveCoreRuntime:
    cfg = LiveCoreConfig.from_environment(environ)
    universe = load_universe()
    validate_partitions(universe, cfg.node_count)
    partition = build_partition(universe, cfg.node_id, cfg.node_count)
    return LiveCoreRuntime(cfg, universe, partition)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    for handler in logging.getLogger().handlers:
        handler.addFilter(RedactingFilter())
    try:
        runtime = build_runtime()
    except (LiveCoreConfigError, PartitionError) as exc:
        # Exit status 2 is a configuration error: systemd's RestartPreventExitStatus=2 stops a restart loop.
        print(f"PSYGRID Live Core refused to start: {exc}", file=sys.stderr)
        return 2
    if os.getenv("PSYGRID_ARCHIVE", "").strip():
        log.warning("PSYGRID_ARCHIVE is set but ignored: the Live Core never stores market data on disk")
    missing = runtime.cfg.missing_peers()
    if missing:
        log.warning("no LIVE_CORE_PEERS URL for node(s) %s; their stocks will be reported missing", missing)
    raise_nofile_limit()
    log.info(
        "PSYGRID Live Core node %s/%s: %s instruments [%s, %s) universe=%s port=%s",
        runtime.partition.node_id,
        runtime.partition.node_count,
        runtime.partition.size,
        runtime.partition.start,
        runtime.partition.end,
        runtime.universe.fingerprint[:12],
        runtime.cfg.port,
    )

    import uvicorn

    from live_core.api import create_app

    uvicorn.run(
        create_app(runtime),
        host=runtime.cfg.host,
        port=runtime.cfg.port,
        log_level="warning",
        access_log=False,
        workers=1,
        timeout_keep_alive=5,
        limit_concurrency=64,
    )
    return 0
