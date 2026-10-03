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
from urllib.parse import quote

import requests

from intelligence.archive import EQUITY_FILE, IST, available_days, load_day, session_days
from intelligence.derivatives import (
    STOCK_CHAIN_BYTES_PER_DAY,
    STOCK_CHAINS,
    ChainRecorder,
    DerivativesRecorder,
    chain_rows,
    snapshot_from_payloads,
)
from intelligence.engine import IntelligenceEngine, Snapshot
from intelligence.event_store import EventStore
from intelligence.events import ARCHIVE_REPLAY, LIVE_SNAPSHOT
from intelligence.frame import as_of_time, frame_at
from intelligence.settings import Settings
from intelligence.similarity import _states_dir, load_states
from intelligence.stream import stream_day, stream_path

log = logging.getLogger("psygrid.intelligence.live")

FUTURES_KEYS = ("nifty", "banknifty")
OPTION_KEYS = ("nifty", "banknifty", "midcpnifty")
SPOT_FALLBACK_KEYS = ("nifty", "banknifty")
SESSION_OPEN, SESSION_CLOSE, LAST_AS_OF = "09:15", "15:30", "15:15"
BACKUP_AFTER = "15:45"
MODEL_WARM_BEFORE = "09:00"
TICK_OFFSET_SECONDS = 5
STALE_AFTER_SECONDS = 600  # in session, no new step for this long means the archive stopped moving
WARM_PER_TICK = 3
STREAM_POLL_SECONDS = 2.0


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
        self.stock_chains = ChainRecorder(settings.store_dir, STOCK_CHAIN_BYTES_PER_DAY, folder=STOCK_CHAINS)
        self._stock_chain_seen: dict[str, str] = {}  # symbol -> the updated_at last recorded
        self.fetch = fetch
        self.clock = clock or (lambda: datetime.now(IST))
        self._lock = threading.Lock()
        self._snapshot: Snapshot | None = None
        self.day = None
        self.snapshot_version = 0
        self._seen: tuple[str, float, float] | None = None
        self._days_seen: list[str] | None = None
        self._qualified: set[str] = set()
        self._last_housekeeping: int | None = None
        self._model_warmed: str | None = None
        self._945_failed: str | None = None
        self._945_finalised: str | None = None
        self._945_outcome_at = 0.0
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
            "stock_chain_snapshots": 0,
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
        if self.settings.selector_945:
            try:
                self.run_945(now)
            except Exception as exc:
                self._error("945", exc)
        minute = int(_minute_floor(now).timestamp())
        if minute != self._last_housekeeping:  # once a minute, however often the loop polls
            self._last_housekeeping = minute
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
            self._last_derivatives_minute = minute  # checked again next minute, not at every poll
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
        self.record_stock_chains(now, minute)
        self._last_derivatives_minute = minute
        self.status["derivatives_recorded"] += 1
        return True

    def record_stock_chains(self, now: datetime, minute: int) -> int:
        """Record each NIFTY 50 stock chain PSYGRID has refreshed since it was last recorded. Returns chains written.

        PSYGRID polls the 50 stock chains round-robin (about 2.7 minutes per rotation), so most minutes bring
        a handful of new chains; a chain is recorded once per refresh, stamped with the minute it was read.
        """
        base = self.settings.psygrid_url
        try:
            listing = self.fetch(f"{base}/public/stock-options.json")
        except Exception as exc:
            self.status["derivatives_errors"] += 1
            log.info("stock options listing unavailable: %s", exc)
            return 0
        written = 0
        for entry in listing.get("stocks") or []:
            symbol, updated = entry.get("symbol"), entry.get("updated_at")
            if (
                not symbol
                or not updated
                or entry.get("status") != "LIVE"
                or self._stock_chain_seen.get(symbol) == updated
            ):
                continue
            try:
                payload = self.fetch(f"{base}/public/stock-options/{quote(symbol, safe='')}.json")
            except Exception as exc:
                self.status["derivatives_errors"] += 1
                log.info("stock options %s unavailable: %s", symbol, exc)
                continue
            served = payload.get("updated_at")
            if not served or served == self._stock_chain_seen.get(symbol):
                continue  # nothing new behind the listing entry
            if self.stock_chains.append(now.strftime("%Y-%m-%d"), chain_rows(minute, symbol, payload)):
                written += 1
            self._stock_chain_seen[symbol] = served
        self.status["stock_chain_snapshots"] += written
        return written

    def follow_archive(self, now: datetime) -> int:
        """Step the engine through every new minute the archive and the stream now cover. Returns steps taken."""
        root = Path(self.settings.archive_dir)
        today = now.strftime("%Y-%m-%d")
        days = available_days(root)
        if days != self._days_seen:  # the qualified-session scan reads every manifest; only redo it on change
            self._days_seen, self._qualified = days, set(session_days(root))
        # Today counts while it is still filling; an earlier day only if it held a real session.
        days = [d for d in days if d == today or (d < today and d in self._qualified)]
        streaming = self.settings.use_stream and stream_path(root, today).exists()
        if streaming and today not in days:
            days.append(today)
        if not days:
            return 0
        session = days[-1]
        path = root / session / EQUITY_FILE
        archive_mtime = path.stat().st_mtime if path.exists() else 0.0
        live_stream = streaming and session == today
        stream_mtime = stream_path(root, today).stat().st_mtime if live_stream else 0.0
        if self._seen == (session, archive_mtime, stream_mtime):
            return 0
        covered = archive_mtime  # an archive written at T holds every bar that closed before T
        if live_stream:
            day, last_minute, _ = stream_day(root, session)
            if day is None:
                return 0
            if last_minute is not None:  # a stream block for minute m is written after the bar closed
                covered = max(covered, last_minute + 60)
                self.status["stream_minute"] = datetime.fromtimestamp(last_minute, IST).strftime("%H:%M")
        else:
            day = load_day(root, session)
        self.day = day  # the bars behind the latest snapshot, for the research views
        written = datetime.fromtimestamp(covered, IST)
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
        if snapshot is not None and live_stream:
            self.status["last_step_latency_s"] = round(self.clock().timestamp() - (self._last_as_of or 0), 2)
        if not self._stop.is_set():
            self._seen = (session, archive_mtime, stream_mtime)
        return steps

    def housekeeping(self, now: datetime) -> None:
        today = now.strftime("%Y-%m-%d")
        if _in_session(now):
            return
        if now >= as_of_time(today, BACKUP_AFTER) and self._backed_up != today and self.store.stats()["events"]:
            self.status["last_backup"] = str(self.backup(today))
            self._backed_up = today
        self.warm_similarity(today)
        if now >= as_of_time(today, BACKUP_AFTER) or now < as_of_time(today, MODEL_WARM_BEFORE):
            self.warm_response_model(now)
            if self.settings.selector_945:
                self.finalise_945(now)
                self.warm_945_history(today)

    # --- PSYGRID 945 -------------------------------------------------------------------------------

    def run_945(self, now: datetime) -> dict | None:
        """Decide once, as soon as the engine has every bar that closed by 09:45; then track the outcome."""
        from intelligence.pipeline945 import decide_day
        from intelligence.selector945 import DECISION_TIME, DecisionStore, decision_outcome

        today = now.strftime("%Y-%m-%d")
        day = self.day
        if day is None or day.session_date != today or self._session != today:
            return None
        decision_epoch = as_of_time(today, DECISION_TIME).timestamp()
        if (self._last_as_of or 0) < decision_epoch:
            return None  # the 09:44 bar has not arrived yet
        store = DecisionStore(self.settings.store_dir, "live")
        decision = store.load_decision(today)
        if decision is None:
            if self._945_failed == today:
                return None
            started = time.monotonic()
            try:
                made, _ = decide_day(self.settings.archive_dir, self.settings.store_dir, day, store,
                                     computed_at=now.strftime("%Y-%m-%d %H:%M:%S IST"))  # fmt: skip
            except Exception:
                self._945_failed = today  # reported once; a restart retries
                raise
            decision = made.payload
            self.status["945"] = {"session": today, "symbol": decision["selected"]["symbol"],
                                  "direction": decision["selected"]["direction"],
                                  "decided_at": decision["computed_at"],
                                  "seconds": round(time.monotonic() - started, 2)}  # fmt: skip
        # Provisional outcome while the horizons elapse (final after the close, from the archive).
        if now.timestamp() >= decision_epoch + 31 * 60 and now.timestamp() - self._945_outcome_at >= 300:
            outcome = decision_outcome(decision, day)
            outcome["provisional"] = True
            if not (store.load_outcome(today) or {}).get("final"):
                store.save_outcome(today, outcome)
            self._945_outcome_at = now.timestamp()
        return decision

    def finalise_945(self, now: datetime) -> bool:
        """After the close: the final outcome and today's training record, from the archived day."""
        from intelligence.pipeline945 import finish_day, inputs_at_0945
        from intelligence.selector945 import Decision, DecisionStore

        today = now.strftime("%Y-%m-%d")
        if now < as_of_time(today, BACKUP_AFTER) or self._945_finalised == today:
            return False
        store = DecisionStore(self.settings.store_dir, "live")
        decision = store.load_decision(today)
        self._945_finalised = today
        if decision is None or not (Path(self.settings.archive_dir) / today / EQUITY_FILE).exists():
            return False
        day = load_day(self.settings.archive_dir, today)
        matrix = inputs_at_0945(self.settings.archive_dir, self.settings.store_dir, day)
        outcome = finish_day(store, Decision(decision), matrix, day)
        outcome["final"] = True
        store.save_outcome(today, outcome)
        return True

    def warm_945_history(self, today: str) -> int:
        """Cache earlier sessions' day summaries and market-state profiles, a few per minute, outside market hours."""
        from intelligence.history import load_summary
        from intelligence.market_state import _history_path, session_profile

        warmed = 0
        for session in [d for d in session_days(self.settings.archive_dir) if d < today][-60:]:
            if warmed >= WARM_PER_TICK or self._stop.is_set():
                break
            if not _history_path(self.settings.store_dir, session).exists():
                day = load_day(self.settings.archive_dir, session)
                load_summary(self.settings.archive_dir, session, self.settings.store_dir, day=day)
                session_profile(self.settings.archive_dir, session, self.settings.store_dir, day=day)
                warmed += 1
        return warmed

    def warm_response_model(self, now: datetime) -> bool:
        """Build the next session's response model outside market hours, so the session only loads it."""
        from intelligence.response import model_for

        target = now.date() + timedelta(days=1 if now >= as_of_time(now.strftime("%Y-%m-%d"), BACKUP_AFTER) else 0)
        while target.weekday() >= 5:
            target += timedelta(days=1)
        label = target.strftime("%Y-%m-%d")
        if self._model_warmed == label:
            return False
        self._model_warmed = label  # once per target, success or not (a failure is reported, not retried each minute)
        started = time.monotonic()
        model = model_for(self.settings.archive_dir, label, cache_root=self.settings.store_dir)
        self.status["response_model"] = {
            "session": label,
            "ready": model is not None,
            "seconds": round(time.monotonic() - started, 1),
        }
        return model is not None

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
            wait = max(1.0, (next_tick - now).total_seconds())
            if self.settings.use_stream and _in_session(now):
                wait = min(wait, STREAM_POLL_SECONDS)  # a stat() per poll; a step only when a minute arrives
            self._stop.wait(wait)
