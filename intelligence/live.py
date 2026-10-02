"""The live intelligence loop: a separate process that only *reads* from PSYGRID.

Each minute (a few seconds after the minute boundary) the runner:

1. records a derivatives snapshot from PSYGRID's existing futures and option
   endpoints over localhost (small payloads; skipped outside market hours and
   when PSYGRID reports the market closed);
2. follows the daily archive PSYGRID already writes (every 5 minutes, and at
   the close). When the file changes, the engine steps through every minute
   boundary it has not processed yet, up to the latest minute whose bar had
   closed before the file was written. Live events are therefore exactly the
   events a replay of the same day produces, with a delay of at most five
   minutes, and the production process does no extra work for them;
3. after the session, backs up the event store once per day and warms the
   similarity cache, a few sessions per minute.

Every step is wrapped: a failure is counted, reported in ``/v2/health`` and
retried at the next minute. Nothing here can stop or slow the production feed.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import requests

from intelligence.archive import EQUITY_FILE, IST, available_days, load_day, session_days
from intelligence.derivatives import ChainRecorder, DerivativesRecorder, chain_rows, snapshot_from_payloads
from intelligence.engine import IntelligenceEngine, Snapshot
from intelligence.event_store import EventStore
from intelligence.events import ARCHIVE_REPLAY, LIVE_SNAPSHOT
from intelligence.frame import as_of_time, frame_at
from intelligence.settings import Settings
from intelligence.similarity import _states_dir, load_states

log = logging.getLogger("psygrid.intelligence.live")

FUTURES_KEYS = ("nifty", "banknifty")
OPTION_KEYS = ("nifty", "banknifty", "midcpnifty")
SPOT_FALLBACK_KEYS = ("nifty", "banknifty")
SESSION_OPEN, SESSION_CLOSE, LAST_AS_OF = "09:15", "15:30", "15:15"
BACKUP_AFTER = "15:45"
TICK_OFFSET_SECONDS = 5
STALE_AFTER_SECONDS = 600  # in session, no new step for this long means the archive stopped moving
WARM_PER_TICK = 3


def http_get_json(url: str, timeout: float = 5.0) -> dict:
    response = requests.get(url, timeout=timeout, headers={"User-Agent": "psygrid-intelligence"})
    response.raise_for_status()
    return response.json()


def _minute_floor(moment: datetime) -> datetime:
    return moment.replace(second=0, microsecond=0)


def _in_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    today = now.strftime("%Y-%m-%d")
    return as_of_time(today, SESSION_OPEN) <= now <= as_of_time(today, SESSION_CLOSE)


class LiveRunner:
    def __init__(
        self,
        settings: Settings,
        fetch: Callable[[str], dict] = http_get_json,
        clock: Callable[[], datetime] | None = None,
    ):
        self.settings = settings
        self.store = EventStore(settings.events_db)
        self.engine = IntelligenceEngine(
            settings.archive_dir, settings.store_dir, source=LIVE_SNAPSHOT, event_store=self.store
        )
        self.recorder = DerivativesRecorder(settings.store_dir)
        self.chains = ChainRecorder(settings.store_dir)
        self.fetch = fetch
        self.clock = clock or (lambda: datetime.now(IST))
        self._lock = threading.Lock()
        self._snapshot: Snapshot | None = None
        self.snapshot_version = 0
        self._seen: tuple[str, float] | None = None
        self._last_as_of: int | None = None
        self._session: str | None = None
        self._last_derivatives_minute: int | None = None
        self._backed_up: str | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.status = {
            "state": "STARTING",
            "ticks": 0,
            "steps": 0,
            "events_emitted": 0,
            "errors": 0,
            "last_error": None,
            "last_error_at": None,
            "last_tick_at": None,
            "last_step_at": None,
            "derivatives_recorded": 0,
            "derivatives_errors": 0,
            "chain_snapshots": 0,
            "last_backup": None,
        }

    # --- state shared with the API ---------------------------------------------------------

    @property
    def snapshot(self) -> Snapshot | None:
        with self._lock:
            return self._snapshot

    def _publish(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._snapshot = snapshot
            self.snapshot_version += 1

    def _error(self, where: str, exc: BaseException) -> None:
        self.status["errors"] += 1
        self.status["last_error"] = f"{where}: {type(exc).__name__}: {exc}"[:500]
        self.status["last_error_at"] = self.clock().strftime("%Y-%m-%d %H:%M:%S IST")
        log.warning("live %s failed: %s", where, exc, exc_info=not isinstance(exc, requests.RequestException))

    # --- one iteration -----------------------------------------------------------------------

    def tick(self) -> None:
        now = self.clock()
        self.status["ticks"] += 1
        self.status["last_tick_at"] = now.strftime("%Y-%m-%d %H:%M:%S IST")
        if self.settings.record_derivatives and _in_session(now):
            try:
                self.record_derivatives(now)
            except Exception as exc:  # never let one source stop the loop
                self.status["derivatives_errors"] += 1
                self._error("derivatives", exc)
        try:
            self.follow_archive(now)
        except Exception as exc:
            self._error("engine", exc)
        try:
            self.housekeeping(now)
        except Exception as exc:
            self._error("housekeeping", exc)
        self.status["state"] = self._state(now)

    def record_derivatives(self, now: datetime) -> bool:
        minute = int(_minute_floor(now).timestamp())
        if minute == self._last_derivatives_minute:
            return False
        base = self.settings.psygrid_url
        futures, options, spot, open_market = {}, {}, {}, False
        for key in FUTURES_KEYS:
            try:
                payload = self.fetch(f"{base}/public/{key}-futures.json")
            except Exception as exc:
                self.status["derivatives_errors"] += 1
                log.info("futures %s unavailable: %s", key, exc)
                continue
            futures[key] = payload
            open_market = open_market or payload.get("market_open") is True
        for key in OPTION_KEYS:
            try:
                payload = self.fetch(f"{base}/public/{key}-options.json")
            except Exception as exc:
                self.status["derivatives_errors"] += 1
                log.info("options %s unavailable: %s", key, exc)
                continue
            options[key] = payload
            spot[key] = payload.get("underlying_ltp")
            open_market = open_market or payload.get("market_status") not in (None, "MARKET_CLOSED")
        if not open_market:
            return False  # a holiday or a paused feed: nothing current to record
        for key in SPOT_FALLBACK_KEYS:
            if spot.get(key) is None:
                try:
                    spot[key] = self.fetch(f"{base}/public/{key}.json").get("ltp")
                except Exception as exc:
                    log.info("spot %s unavailable: %s", key, exc)
        self.recorder.append(now.strftime("%Y-%m-%d"), snapshot_from_payloads(minute, spot, futures, options))
        for key, payload in options.items():  # the whole chain, not only its aggregates
            if self.chains.append(now.strftime("%Y-%m-%d"), chain_rows(minute, key, payload)):
                self.status["chain_snapshots"] += 1
        self._last_derivatives_minute = minute
        self.status["derivatives_recorded"] += 1
        return True

    def follow_archive(self, now: datetime) -> int:
        """Step the engine through every new minute the archive now covers. Returns steps taken."""
        today = now.strftime("%Y-%m-%d")
        # Today counts while it is still filling; an earlier day only if it held a real session.
        qualified = set(session_days(self.settings.archive_dir))
        days = [d for d in available_days(self.settings.archive_dir) if d == today or (d < today and d in qualified)]
        if not days:
            return 0
        session = days[-1]
        path = Path(self.settings.archive_dir) / session / EQUITY_FILE
        mtime = path.stat().st_mtime
        if self._seen == (session, mtime):
            return 0
        day = load_day(self.settings.archive_dir, session)
        written = datetime.fromtimestamp(mtime, IST)
        limit = min(as_of_time(session, LAST_AS_OF), _minute_floor(written), _minute_floor(now))
        if session != self._session:
            self._session, self._last_as_of = session, None
        # Minutes processed during their own session are live; anything else (a restart's catch-up of an
        # earlier day, or a day processed after its close) is labelled as replay.
        source = LIVE_SNAPSHOT if session == today and _in_session(now) else ARCHIVE_REPLAY
        self.engine.source = self.engine.events.source = source
        start = (
            datetime.fromtimestamp(self._last_as_of, IST) + timedelta(minutes=1)
            if self._last_as_of
            else as_of_time(session, SESSION_OPEN)
        )
        steps, moment, snapshot = 0, start, None
        while moment <= limit and not self._stop.is_set():
            snapshot = self.engine.step(frame_at(day, moment))
            self._last_as_of = int(moment.timestamp())
            self.status["events_emitted"] += len(snapshot.events)
            steps += 1
            moment += timedelta(minutes=1)
            if steps % 30 == 0:  # publish progress while catching up
                self._publish(snapshot)
        if snapshot is not None:
            self._publish(snapshot)
            self.status["steps"] += steps
            self.status["last_step_at"] = now.strftime("%Y-%m-%d %H:%M:%S IST")
        if not self._stop.is_set():
            self._seen = (session, mtime)
        return steps

    def housekeeping(self, now: datetime) -> None:
        today = now.strftime("%Y-%m-%d")
        if _in_session(now):
            return
        if now >= as_of_time(today, BACKUP_AFTER) and self._backed_up != today and self.store.stats()["events"]:
            self.status["last_backup"] = str(self.backup(today))
            self._backed_up = today
        self.warm_similarity(today)

    def backup(self, label: str) -> Path:
        folder = self.settings.store_dir / "backups"
        target = self.store.backup(folder / f"events-{label}.db")
        keys = self.settings.keys_file
        if keys.exists():
            (folder / f"api_keys-{label}.json").write_text(keys.read_text())
        backups = sorted(folder.glob("events-*.db"))
        for old in backups[: -self.settings.backup_keep]:
            old.unlink(missing_ok=True)
            (folder / old.name.replace("events-", "api_keys-").replace(".db", ".json")).unlink(missing_ok=True)
        return target

    def warm_similarity(self, today: str) -> int:
        warmed = 0
        sessions = [d for d in available_days(self.settings.archive_dir) if d <= today]
        for session in sessions[-self.settings.similarity_lookback :]:
            if warmed >= WARM_PER_TICK or self._stop.is_set():
                break
            if not (_states_dir(self.settings.store_dir, session) / "complete").exists():
                load_states(self.settings.archive_dir, session, self.settings.store_dir)
                warmed += 1
        return warmed

    def _state(self, now: datetime) -> str:
        if self.snapshot is None:
            return "WAITING_FOR_DATA"
        if not _in_session(now):
            return "CLOSED"
        if self._last_as_of and now.timestamp() - self._last_as_of > STALE_AFTER_SECONDS:
            return "STALE"
        return "LIVE"

    def health(self) -> dict:
        now = self.clock()
        snapshot = self.snapshot
        lag = round(now.timestamp() - self._last_as_of) if self._last_as_of else None
        return {
            **self.status,
            "state": self._state(now) if self.status["ticks"] else self.status["state"],
            "session_date": snapshot.session_date if snapshot else None,
            "as_of": snapshot.as_of if snapshot else None,
            "lag_seconds": lag,
            "last_step_ms": snapshot.timings_ms if snapshot else None,
        }

    # --- thread ------------------------------------------------------------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="psygrid-intelligence-live", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 15.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)
        self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            self.tick()
            now = self.clock()
            next_tick = _minute_floor(now) + timedelta(minutes=1, seconds=TICK_OFFSET_SECONDS)
            log.debug("tick took %.2fs", time.monotonic() - started)
            self._stop.wait(max(1.0, (next_tick - now).total_seconds()))
