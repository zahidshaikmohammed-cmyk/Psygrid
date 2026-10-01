"""Persist each session's completed 1-minute candles to disk for backtesting.

Live serving stays RAM-only; this module writes a copy of the day's data to
``<archive_dir>/<YYYY-MM-DD>/`` as gzip-compressed CSV, in exactly the form the
public endpoints serve it (IST timestamps, completed candles only):

- ``equity_1m.csv.gz``        symbol, security_id, timestamp, open, high, low, close, volume
- ``equity_reference.csv.gz`` symbol, security_id, previous_close, today_open
- ``index_1m.csv.gz``         index, symbol, timestamp, open, high, low, close, volume
- ``manifest.json``           row counts and write times for the files above

Archiving is strictly best-effort: every failure is recorded and swallowed, so
it can never interrupt market-data acquisition. Writes are atomic, and a
snapshot never replaces an existing file that holds more rows (a restarted
process that has only re-bootstrapped part of the day cannot clobber a fuller
earlier snapshot).
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import threading
from collections.abc import Callable, Iterable
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from output import market_live_json

ARCHIVE_INTERVAL_SECONDS = 300.0
DEFAULT_ARCHIVE_DIR = Path.home() / "psygrid-data"

EQUITY_FILE = "equity_1m.csv.gz"
EQUITY_REFERENCE_FILE = "equity_reference.csv.gz"
INDEX_FILE = "index_1m.csv.gz"
MANIFEST_FILE = "manifest.json"

_CANDLE_FIELDS = ("timestamp", "open", "high", "low", "close", "volume")
EQUITY_COLUMNS = ("symbol", "security_id", *_CANDLE_FIELDS)
EQUITY_REFERENCE_COLUMNS = ("symbol", "security_id", "previous_close", "today_open")
INDEX_COLUMNS = ("index", "symbol", *_CANDLE_FIELDS)


def archive_dir_from_environment() -> Path:
    configured = os.getenv("PSYGRID_ARCHIVE_DIR", "").strip()
    return Path(configured).expanduser() if configured else DEFAULT_ARCHIVE_DIR


def _csv_gz_bytes(columns: tuple[str, ...], rows: Iterable[tuple]) -> bytes:
    text = io.StringIO()
    writer = csv.writer(text, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)
    # Level 6 matches level 9's size at half the CPU; mtime=0 keeps identical
    # content byte-identical across writes.
    return gzip.compress(text.getvalue().encode("utf-8"), compresslevel=6, mtime=0)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


class DailyArchive:
    """Writes one trading day's candles under ``root/<date>/``."""

    def __init__(self, root: Path, timezone: str = "Asia/Kolkata"):
        self.root = Path(root)
        self.tz = ZoneInfo(timezone)
        self._lock = threading.Lock()

    def day_dir(self, session_date: str) -> Path:
        return self.root / session_date

    def _read_manifest(self, session_date: str) -> dict:
        try:
            return json.loads((self.day_dir(session_date) / MANIFEST_FILE).read_text())
        except (OSError, ValueError):
            return {}

    def _write_table(self, session_date: str, filename: str, columns: tuple[str, ...], rows: list[tuple]) -> bool:
        """Write one table unless it is empty or smaller than what is already archived."""
        if not rows:
            return False
        with self._lock:
            manifest = self._read_manifest(session_date)
            files = manifest.setdefault("files", {})
            if files.get(filename, {}).get("rows", 0) > len(rows):
                return False
            _atomic_write(self.day_dir(session_date) / filename, _csv_gz_bytes(columns, rows))
            files[filename] = {"rows": len(rows), "written_at": datetime.now(self.tz).isoformat()}
            manifest["session_date"] = session_date
            manifest["timezone"] = str(self.tz)
            manifest["synthetic_candles"] = False
            _atomic_write(
                self.day_dir(session_date) / MANIFEST_FILE, json.dumps(manifest, indent=2, sort_keys=True).encode()
            )
        return True

    def write_equity(self, payload: dict) -> bool:
        """Archive a ``market_live_json`` payload. Returns True if the candle file was written."""
        session_date = (payload.get("session") or {}).get("date")
        stocks = payload.get("stocks") or {}
        if not session_date or not stocks:
            return False
        candles: list[tuple] = []
        references: list[tuple] = []
        for symbol in sorted(stocks):
            stock = stocks[symbol]
            security_id = stock.get("security_id")
            references.append((symbol, security_id, stock.get("previous_close"), stock.get("today_open")))
            for candle in stock.get("candles_1m") or []:
                candles.append((symbol, security_id, *(candle.get(field) for field in _CANDLE_FIELDS)))
        written = self._write_table(session_date, EQUITY_FILE, EQUITY_COLUMNS, candles)
        if written:
            self._write_table(session_date, EQUITY_REFERENCE_FILE, EQUITY_REFERENCE_COLUMNS, references)
        return written

    def write_indices(self, snapshots: dict[str, dict]) -> bool:
        """Archive index-layer snapshots keyed by route (``nifty``, ``banknifty``, ...)."""
        rows: list[tuple] = []
        session_date = None
        for key in sorted(snapshots):
            snap = snapshots[key]
            session_date = session_date or (snap.get("session") or {}).get("date")
            for candle in snap.get("1m") or []:
                rows.append((key, snap.get("symbol"), *(candle.get(field) for field in _CANDLE_FIELDS)))
        if not session_date:
            return False
        return self._write_table(session_date, INDEX_FILE, INDEX_COLUMNS, rows)


class ArchiveManager:
    """Snapshots the live equity and index state to a ``DailyArchive``.

    ``archive_now`` runs every ``ARCHIVE_INTERVAL_SECONDS`` while a session is
    live and once more from each session's end-of-day hook, after the final
    candle is closed and before state is wiped.
    """

    def __init__(self, archive: DailyArchive, equity_state, index_manager_getter: Callable[[], object | None]):
        self.archive = archive
        self.equity_state = equity_state
        self.index_manager_getter = index_manager_getter
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.last_written_at: str | None = None
        self.last_error = ""
        self.write_count = 0

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-daily-archive")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)
        self.thread = None

    def _loop(self) -> None:
        while not self.stop_event.wait(ARCHIVE_INTERVAL_SECONDS):
            self.archive_now()

    def _record(self, operation: Callable[[], bool], label: str) -> None:
        try:
            if operation():
                self.write_count += 1
                self.last_written_at = datetime.now(self.archive.tz).isoformat()
                self.last_error = ""
        except Exception as exc:
            self.last_error = f"{label}: {type(exc).__name__}: {exc}"

    def archive_equity(self) -> None:
        if self.equity_state.session_status != "LIVE":
            return
        self._record(lambda: self.archive.write_equity(market_live_json(self.equity_state)), "equity")

    def archive_indices(self) -> None:
        manager = self.index_manager_getter()
        if manager is None:
            return
        live = [key for key, state in manager.states.items() if state.session_status == "LIVE"]
        if live:
            self._record(lambda: self.archive.write_indices({key: manager.snapshot(key) for key in live}), "indices")

    def archive_now(self) -> None:
        self.archive_equity()
        self.archive_indices()

    def status(self) -> dict:
        return {
            "archive_dir": str(self.archive.root),
            "last_written_at": self.last_written_at,
            "write_count": self.write_count,
            "interval_seconds": ARCHIVE_INTERVAL_SECONDS,
            **({"error": self.last_error} if self.last_error else {}),
        }
