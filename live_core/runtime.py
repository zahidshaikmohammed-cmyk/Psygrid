"""Live Core process: the session lifecycle for one partition and the ``python -m live_core`` entrypoint.

Session (Asia/Kolkata, trading days per ``runtime_guard.is_trading_day``):

* 09:15 - resolve and validate the full canonical universe against Dhan's instrument master,
  take this node's partition, authenticate (explicit token or PIN+TOTP, exactly like the full
  PSYGRID), seed reference prices from one REST quote snapshot, start the WebSocket feed.
* during the session - publish each minute's candle once it has ended, and after a feed
  reconnect re-fetch today's genuine Dhan 1m bars so the gap is filled from Dhan, not guessed.
* 15:15 - stop the feed (closing its asyncio loop), stop the history worker, wipe all market
  state, collect garbage and return freed heap to the OS. Nothing is archived; nothing carries
  into the next day.

Outside the session no Dhan market feed is held open.
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
from datetime import datetime, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo

import config as psygrid_config
from auth_retry import looks_like_auth_failure
from dhan_auth import DhanTokenRateLimited
from live_core import SERVICE_NAME
from live_core.aggregate import PeerClient
from live_core.config import LiveCoreConfig, LiveCoreConfigError
from live_core.feed import LiveCoreFeed
from live_core.history import HistoryWorker
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
PREPARE_AHEAD = timedelta(minutes=15)
TRIM_INTERVAL_SECONDS = 60.0


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
    ):
        self.cfg = cfg
        self.universe = universe
        self.partition = partition
        self.tz = ZoneInfo(psygrid_config.TIMEZONE)
        self.market_start = dt_time(*map(int, psygrid_config.MARKET_START.split(":")))
        self.market_end = dt_time(*map(int, psygrid_config.MARKET_END.split(":")))
        # Data freshness follows the Live Core rule (stale only after 120 s), not the full app's 30 s.
        self.state = NodeState(psygrid_config.TIMEZONE, STALE_AFTER_SECONDS, clock=clock)
        self._settings_loader = settings_loader or psygrid_config.load_settings
        self._instrument_loader = instrument_loader or psygrid_config.load_instruments
        self._api_factory = api_factory or _default_api_factory
        self._feed_factory = feed_factory or LiveCoreFeed
        self._token_refresher = token_refresher or psygrid_config.refresh_access_token
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
        return "CLOSED" if is_trading_day(now.date()) else "NON_TRADING_DAY"

    # ------------------------------------------------------------------ loop

    def tick(self, now: datetime | None = None) -> None:
        now = now or self._now()
        if self.in_market(now):
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

    def _prepare(self, now: datetime) -> None:
        """Resolve security ids during the 15 minutes before the open, so 09:15 only authenticates."""
        session_date = now.date().isoformat()
        if self._instruments_date == session_date or not is_trading_day(now.date()):
            return
        opens = datetime.combine(now.date(), self.market_start, tzinfo=now.tzinfo)
        if not opens - PREPARE_AHEAD <= now < opens or now.timestamp() < self._prepare_retry_at:
            return
        try:
            self._resolve_instruments(session_date)
        except Exception as exc:  # 09:15 tries again and reports the error through the session state
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
            self.state.set_feed_status("TOKEN_REFRESHING", "Dhan token expired/invalid; generating one fresh token")
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

            self.state.begin(session_date, instruments, self.partition.start)
            self.state.set_session_status("AUTHENTICATING")
            self.state.set_feed_status("AUTHENTICATING")
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
                return
            except Exception as exc:
                self._retry_at = epoch + AUTH_RETRY_SECONDS
                self.state.set_session_status("AUTH_ERROR")
                self.state.set_feed_status("AUTH_ERROR", f"authentication: {type(exc).__name__}: {exc}"[:500])
                return

            self.state.set_session_status("LIVE")
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
                return
            self.feed = feed
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
            if self.cfg.history_bootstrap and now - opened >= timedelta(minutes=1):
                # A start after the open (restart, reboot, late deploy): load today's bars so far.
                self.history.enqueue_all()
            log.info(
                "live core node %s session %s LIVE: %s instruments [%s, %s)",
                self.partition.node_id,
                session_date,
                len(instruments),
                self.partition.start,
                self.partition.end,
            )

    def _maintain(self, now: datetime) -> None:
        epoch = now.timestamp()
        self.state.finalize_due(epoch, self.cfg.finalize_grace_seconds)
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
        if self._gap_refill_at is not None and epoch >= self._gap_refill_at:
            self._gap_refill_at = None
            if self.cfg.history_bootstrap and self.history is not None:
                self.history.enqueue_all()

    def _end_session(self) -> None:
        with self._lock:
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

    # ------------------------------------------------------------------ health

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
        if in_hours and not self.config_error:
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
