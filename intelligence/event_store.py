"""Append-only, searchable event history in SQLite (standard library, one file).

Events are stored whole as JSON, with the fields people search by copied into
indexed columns. An event id is deterministic, so storing the same event twice
(a replay of a day already processed live, or a restart) is a no-op. Rows are
never updated or deleted by the engine; a correction is a new event that names
the one it ``supersedes``.

WAL mode lets one writer (the live engine) and many readers (the API) work at
once. Every connection is short-lived and opened per call, so the store is safe
to share between threads and processes.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}
MAX_LIMIT = 1000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    session_date TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    bar_epoch INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    category TEXT NOT NULL,
    scope TEXT NOT NULL,
    subject_key TEXT NOT NULL,
    counterpart TEXT NOT NULL DEFAULT '',
    severity TEXT NOT NULL,
    severity_rank INTEGER NOT NULL,
    magnitude REAL NOT NULL,
    source TEXT NOT NULL,
    payload TEXT NOT NULL,
    stored_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_by_date ON events (session_date, bar_epoch);
CREATE INDEX IF NOT EXISTS events_by_subject ON events (subject_key, event_type, session_date);
CREATE INDEX IF NOT EXISTS events_by_type ON events (event_type, session_date);
CREATE TABLE IF NOT EXISTS event_instruments (
    event_id TEXT NOT NULL REFERENCES events (event_id),
    instrument TEXT NOT NULL,
    PRIMARY KEY (instrument, event_id)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
STORE_VERSION = "1"


class EventStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db, db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(_SCHEMA)
            db.execute("INSERT OR IGNORE INTO meta VALUES ('store_version', ?)", (STORE_VERSION,))

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        db.execute("PRAGMA synchronous=NORMAL")
        return db

    # --- writing -----------------------------------------------------------------------

    def add(self, events: Iterable[dict]) -> list[dict]:
        """Store events; returns those that were new (an id already stored is skipped)."""
        stored_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        new = []
        with closing(self._connect()) as db, db:
            for event in events:
                cursor = db.execute(
                    "INSERT OR IGNORE INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event["event_id"], event["session_date"], event["observed_at"], event["bar_epoch"],
                        event["event_type"], event["category"], event["scope"], event["subject"]["key"],
                        event["subject"].get("counterpart") or "", event["severity"],
                        SEVERITY_RANK[event["severity"]], float(event["magnitude"]["value"]),
                        event["provenance"]["source"], json.dumps(event, sort_keys=True, separators=(",", ":")),
                        stored_at,
                    ),
                )  # fmt: skip
                if cursor.rowcount:
                    db.executemany(
                        "INSERT OR IGNORE INTO event_instruments VALUES (?, ?)",
                        [(event["event_id"], key) for key in event.get("affected_instruments", ())],
                    )
                    new.append(event)
        return new

    # --- reading -----------------------------------------------------------------------

    def get(self, event_id: str) -> dict | None:
        with closing(self._connect()) as db:
            row = db.execute("SELECT payload FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return json.loads(row["payload"]) if row else None

    def search(
        self,
        *,
        session_date: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        subject: str | None = None,
        instrument: str | None = None,
        event_type: str | None = None,
        category: str | None = None,
        scope: str | None = None,
        min_severity: str | None = None,
        after_seq: int | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Matching events, newest first; with ``after_seq``, oldest first after that sequence number.

        Each result carries ``seq``, its position in storage order, usable as a
        cursor for streaming and paging.
        """
        clauses, params = [], []
        for column, value in (
            ("e.session_date", session_date), ("e.subject_key", subject), ("e.event_type", event_type),
            ("e.category", category), ("e.scope", scope),
        ):  # fmt: skip
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if date_from:
            clauses.append("e.session_date >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("e.session_date <= ?")
            params.append(date_to)
        if min_severity:
            clauses.append("e.severity_rank >= ?")
            params.append(SEVERITY_RANK[min_severity])
        if instrument:
            clauses.append("e.event_id IN (SELECT event_id FROM event_instruments WHERE instrument = ?)")
            params.append(instrument)
        if after_seq is not None:
            clauses.append("e.rowid > ?")
            params.append(int(after_seq))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = "e.rowid ASC" if after_seq is not None else "e.bar_epoch DESC, e.rowid DESC"
        params.append(max(1, min(int(limit), MAX_LIMIT)))
        with closing(self._connect()) as db:
            rows = db.execute(
                f"SELECT e.rowid AS seq, e.payload FROM events e {where} ORDER BY {order} LIMIT ?", params
            )
            return [{**json.loads(row["payload"]), "seq": row["seq"]} for row in rows]

    def latest_seq(self) -> int:
        with closing(self._connect()) as db:
            return int(db.execute("SELECT COALESCE(MAX(rowid), 0) FROM events").fetchone()[0])

    def prior_occurrences(self, event_type: str, subject: str, before_date: str, sessions: int) -> int:
        """Times this event type fired for this subject in the last ``sessions`` stored sessions before a date."""
        with closing(self._connect()) as db:
            dates = [
                r[0]
                for r in db.execute(
                    "SELECT DISTINCT session_date FROM events WHERE session_date < ? "
                    "ORDER BY session_date DESC LIMIT ?",
                    (before_date, sessions),
                )
            ]
            if not dates:
                return 0
            return int(
                db.execute(
                    "SELECT COUNT(*) FROM events WHERE event_type = ? AND subject_key = ? "
                    "AND session_date BETWEEN ? AND ?",
                    (event_type, subject, dates[-1], dates[0]),
                ).fetchone()[0]
            )

    def last_fired(self, session_date: str) -> dict[tuple[str, str, str], tuple[int, int]]:
        """(event_type, subject, counterpart) -> (latest bar_epoch, its severity rank) for a session."""
        with closing(self._connect()) as db:
            rows = db.execute(
                "SELECT event_type, subject_key, counterpart, bar_epoch, severity_rank FROM events "
                "WHERE session_date = ? ORDER BY bar_epoch",
                (session_date,),
            )
            return {(r[0], r[1], r[2]): (int(r[3]), int(r[4])) for r in rows}

    def stats(self) -> dict:
        with closing(self._connect()) as db:
            total = db.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            sessions = db.execute("SELECT COUNT(DISTINCT session_date), MAX(session_date) FROM events").fetchone()
        return {"events": int(total), "sessions": int(sessions[0]), "latest_session": sessions[1]}

    def backup(self, destination: Path) -> Path:
        """A consistent copy of the store, taken online with SQLite's backup API."""
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as source, closing(sqlite3.connect(destination)) as target:
            source.backup(target)
        return destination
