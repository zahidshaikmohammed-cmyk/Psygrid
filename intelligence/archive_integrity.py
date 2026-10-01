"""Integrity checks for one archived day, whether PSYGRID or the history bootstrap wrote it.

Every file is read in full, streaming, so a truncated gzip is caught. For each
table the check confirms the header, that every timestamp parses, belongs to
the day, is minute-aligned and inside the session, that no (instrument,
minute) pair repeats, that OHLC geometry holds and volume is non-negative,
and that row counts (and, when the manifest records them, SHA-256 digests)
match the manifest. It also reports which expected instruments have no rows.

The checker never modifies anything. A day that fails is reported with the
reasons; the bootstrap rebuilds a failed day it owns and leaves others alone.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import zlib
from datetime import datetime, time
from pathlib import Path

from daily_archive import (
    EQUITY_COLUMNS,
    EQUITY_FILE,
    EQUITY_REFERENCE_COLUMNS,
    EQUITY_REFERENCE_FILE,
    INDEX_COLUMNS,
    INDEX_FILE,
    MANIFEST_FILE,
)
from intelligence.archive import IST, TIMESTAMP_FORMAT

SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)  # bars open strictly before the close
MAX_LISTED_PROBLEMS = 20


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _number(text):
    try:
        value = float(text)
    except (TypeError, ValueError):
        return None
    return value if value == value and abs(value) != float("inf") else None


def _check_table(path: Path, columns: tuple[str, ...], key_column: str, session_date: str) -> dict:
    """Stream one candle table; return counts and problems."""
    stats = {"rows": 0, "instruments": set(), "duplicates": 0, "bad_timestamp": 0, "wrong_day": 0,
             "unaligned": 0, "outside_session": 0, "invalid_ohlc": 0, "missing_value": 0}  # fmt: skip
    problems: list[str] = []
    seen: set[tuple[str, str]] = set()
    with gzip.open(path, "rt", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if tuple(header or ()) != columns:
            return {**stats, "instruments": [], "problems": [f"{path.name}: header {header} != {list(columns)}"]}
        index = {name: i for i, name in enumerate(columns)}
        for row in reader:
            stats["rows"] += 1
            if len(row) != len(columns):
                stats["missing_value"] += 1
                continue
            key, stamp = row[index[key_column]], row[index["timestamp"]]
            stats["instruments"].add(key)
            try:
                moment = datetime.strptime(stamp, TIMESTAMP_FORMAT).replace(tzinfo=IST)
            except ValueError:
                stats["bad_timestamp"] += 1
                continue
            if moment.strftime("%Y-%m-%d") != session_date:
                stats["wrong_day"] += 1
            if moment.second or moment.microsecond:
                stats["unaligned"] += 1
            if not SESSION_OPEN <= moment.time() < SESSION_CLOSE:
                stats["outside_session"] += 1
            if (key, stamp) in seen:
                stats["duplicates"] += 1
            seen.add((key, stamp))
            o, h, low, c, v = (_number(row[index[f]]) for f in ("open", "high", "low", "close", "volume"))
            if None in (o, h, low, c, v):
                stats["missing_value"] += 1
            elif h < max(o, c) or low > min(o, c) or h < low or low <= 0 or v < 0:
                stats["invalid_ohlc"] += 1
    for name in ("duplicates", "bad_timestamp", "wrong_day", "unaligned", "outside_session", "invalid_ohlc",
                 "missing_value"):  # fmt: skip
        if stats[name]:
            problems.append(f"{path.name}: {stats[name]} rows with {name.replace('_', ' ')}")
    return {**stats, "instruments": sorted(stats["instruments"]), "problems": problems}


def verify_day(day_dir: Path, expected_equities: list[str] | None = None) -> dict:
    """Integrity report for one day directory: ``ok`` plus the problems found and per-file statistics."""
    day_dir = Path(day_dir)
    session_date = day_dir.name
    problems: list[str] = []
    files: dict[str, dict] = {}
    try:
        manifest = json.loads((day_dir / MANIFEST_FILE).read_text())
    except FileNotFoundError:
        manifest, problems = {}, ["manifest.json is missing"]
    except ValueError:
        manifest, problems = {}, ["manifest.json is not valid JSON"]
    if manifest and manifest.get("session_date") not in (None, session_date):
        problems.append(f"manifest session_date {manifest.get('session_date')} != directory {session_date}")
    recorded = manifest.get("files", {}) if manifest else {}
    tables = ((EQUITY_FILE, EQUITY_COLUMNS, "symbol"), (INDEX_FILE, INDEX_COLUMNS, "index"))
    for name, columns, key in tables:
        path = day_dir / name
        if not path.exists():
            if name == EQUITY_FILE:
                problems.append(f"{name} is missing")
            continue
        try:
            stats = _check_table(path, columns, key, session_date)
        except (OSError, EOFError, zlib.error, csv.Error, UnicodeDecodeError) as exc:
            problems.append(f"{name}: unreadable ({type(exc).__name__}: {exc})")
            continue
        problems += stats.pop("problems")
        entry = recorded.get(name, {})
        if entry and entry.get("rows") != stats["rows"]:
            problems.append(f"{name}: {stats['rows']} rows but the manifest records {entry.get('rows')}")
        if entry.get("sha256") and entry["sha256"] != sha256_file(path):
            problems.append(f"{name}: SHA-256 does not match the manifest")
        files[name] = {**stats, "instruments": len(stats["instruments"])}
        if name == EQUITY_FILE and expected_equities is not None:
            present = set(stats["instruments"])
            missing = sorted(set(expected_equities) - present)
            unexpected = sorted(present - set(expected_equities))
            files[name]["missing_expected"] = missing
            if unexpected:
                problems.append(f"{name}: {len(unexpected)} symbols outside the universe, e.g. {unexpected[:5]}")
    reference = day_dir / EQUITY_REFERENCE_FILE
    if reference.exists():
        try:
            with gzip.open(reference, "rt", newline="") as handle:
                reader = csv.reader(handle)
                if tuple(next(reader, ())) != EQUITY_REFERENCE_COLUMNS:
                    problems.append(f"{EQUITY_REFERENCE_FILE}: unexpected header")
                files[EQUITY_REFERENCE_FILE] = {"rows": sum(1 for _ in reader)}
        except (OSError, EOFError, zlib.error, csv.Error) as exc:
            problems.append(f"{EQUITY_REFERENCE_FILE}: unreadable ({type(exc).__name__})")
    return {
        "session_date": session_date,
        "ok": not problems,
        "source": manifest.get("source", "PSYGRID_LIVE_ARCHIVE") if manifest else None,
        "problems": problems[:MAX_LISTED_PROBLEMS],
        "files": files,
    }


def digests_match(day_dir: Path) -> bool:
    """Fast check: the manifest exists and every file it records a SHA-256 for still has that digest."""
    try:
        manifest = json.loads((Path(day_dir) / MANIFEST_FILE).read_text())
    except (OSError, ValueError):
        return False
    for name, entry in manifest.get("files", {}).items():
        path = Path(day_dir) / name
        if not path.exists() or (entry.get("sha256") and sha256_file(path) != entry["sha256"]):
            return False
    return True
