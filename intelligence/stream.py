"""Read PSYGRID's low-latency minute stream (``<archive>/.live/<date>/``), complete blocks only.

PSYGRID's microstructure recorder appends one block per closed minute, a few
seconds after the minute ends, to ``bars_1m.csv`` (equity and index 1m bars)
and ``micro_1m.csv`` (Full-packet features). Each block ends with a
``#END <minute> <rows>`` line written in the same ``write`` call, so a reader
that stops at the last end marker never sees a half-written minute. The
intelligence service reads these files; it never talks to the feed process.

``stream_day`` merges the stream with the day's archive (written every five
minutes): archived bars win, stream bars fill the minutes the archive does
not have yet, so a restart of either process loses nothing and a minute is
always the same bar whichever file it came from.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path

from daily_archive import EQUITY_FILE, EQUITY_REFERENCE_FILE, INDEX_FILE, MANIFEST_FILE
from intelligence.archive import ArchiveDay, _read_reference, _read_rows, _to_bars, is_historical, parse_timestamp

STREAM_DIR = ".live"
BARS_STREAM = "bars_1m.csv"
MICRO_STREAM = "micro_1m.csv"
END_MARKER = "#END"
INDEX_PREFIX = "IDX:"


@dataclass(frozen=True)
class StreamRead:
    rows: list[dict]
    minutes: list[int]  # the minutes of the complete blocks, in file order
    mtime: float | None


def stream_path(archive_root: Path, session_date: str, name: str = BARS_STREAM) -> Path:
    return Path(archive_root) / STREAM_DIR / session_date / name


def read_blocks(path: Path) -> StreamRead:
    """Every row of every complete block in an append-only stream file."""
    try:
        stat = path.stat()
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return StreamRead([], [], None)
    end = text.rfind(f"\n{END_MARKER} ")
    if end < 0:
        return StreamRead([], [], stat.st_mtime)
    end = text.find("\n", end + 1)
    body = text[: end + 1] if end >= 0 else text
    rows, minutes, header = [], [], None
    for record in csv.reader(io.StringIO(body)):
        if not record:
            continue
        if record[0].startswith(END_MARKER):
            parts = record[0].split()
            if len(parts) >= 2 and parts[1].isdigit():
                minutes.append(int(parts[1]))
            continue
        if header is None:
            header = record
            continue
        if record == header or len(record) != len(header):
            continue
        rows.append(dict(zip(header, record, strict=True)))
    return StreamRead(rows, minutes, stat.st_mtime)


def _minute(row: dict) -> int | None:
    try:
        return parse_timestamp(row.get("timestamp", ""))
    except (TypeError, ValueError):
        return None


def stream_day(archive_root: Path, session_date: str) -> tuple[ArchiveDay | None, int | None, float | None]:
    """The day's bars from the archive plus the stream: ``(day, last stream minute, stream mtime)``.

    ``day`` is None when neither source has equity bars. Archived rows take
    precedence over stream rows for the same instrument and minute.
    """
    root = Path(archive_root)
    folder = root / session_date
    equity = list(_read_rows(folder / EQUITY_FILE)) if (folder / EQUITY_FILE).exists() else []
    indices = list(_read_rows(folder / INDEX_FILE)) if (folder / INDEX_FILE).exists() else []
    read = read_blocks(stream_path(root, session_date))
    have_eq = {(r.get("symbol"), _minute(r)) for r in equity}
    have_ix = {(r.get("index"), _minute(r)) for r in indices}
    equity, indices = list(equity), list(indices)
    for row in read.rows:
        sid = row.get("security_id", "")
        bar = {k: row.get(k) for k in ("timestamp", "open", "high", "low", "close", "volume")}
        if sid.startswith(INDEX_PREFIX):
            key = sid[len(INDEX_PREFIX) :]
            if (key, _minute(row)) not in have_ix:
                have_ix.add((key, _minute(row)))
                indices.append({"index": key, "symbol": row.get("symbol", key), **bar})
        elif (row.get("symbol"), _minute(row)) not in have_eq:
            have_eq.add((row.get("symbol"), _minute(row)))
            equity.append({"symbol": row.get("symbol"), **bar})
    if not equity:
        return None, None, read.mtime
    try:
        import json

        manifest = json.loads((folder / MANIFEST_FILE).read_text())
    except (OSError, ValueError):
        manifest = {}
    day = ArchiveDay(
        session_date=session_date,
        equity=_to_bars(equity, "symbol", "symbol", drop_filler=is_historical(manifest)),
        indices=_to_bars(indices, "index", "symbol") if indices else None,
        reference=_read_reference(folder / EQUITY_REFERENCE_FILE),
        manifest={**manifest, "stream_minutes": len(read.minutes)},
    )
    return day, (max(read.minutes) if read.minutes else None), read.mtime


def micro_rows(archive_root: Path, session_date: str) -> list[dict]:
    """The day's microstructure rows: the compacted archive after the session, else the live stream."""
    import gzip

    archived = Path(archive_root) / session_date / "microstructure_1m.csv.gz"
    if archived.exists():
        with gzip.open(archived, "rt", newline="") as handle:
            return [row for row in csv.DictReader(handle) if not str(row.get("symbol", "")).startswith(END_MARKER)]
    return read_blocks(stream_path(archive_root, session_date, MICRO_STREAM)).rows
