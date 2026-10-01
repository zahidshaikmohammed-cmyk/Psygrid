"""Read one archived trading day into columnar arrays.

The archive is written by ``daily_archive.py``: gzip CSV of completed 1m bars
with IST timestamps. This reader keeps every value exactly as archived. A row
that is malformed or breaks OHLC geometry is rejected and counted, never
repaired; a missing bar stays missing (NaN), never filled.
"""

from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from daily_archive import EQUITY_FILE, EQUITY_REFERENCE_FILE, INDEX_FILE, MANIFEST_FILE

IST = ZoneInfo("Asia/Kolkata")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S IST"
BAR_FIELDS = ("open", "high", "low", "close", "volume")


@dataclass(frozen=True)
class Bars:
    """1m bars for a set of instruments: one row per instrument, one column per bar-open minute.

    ``minutes`` holds each column's bar-open time as epoch seconds, ascending.
    A cell with no archived bar is NaN in every field. ``rejected`` lists, per
    instrument, each rejected row's bar-open epoch (None when the timestamp
    itself was unreadable) and the reason.
    """

    keys: tuple[str, ...]
    names: tuple[str, ...]
    minutes: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    rejected: dict[str, list[tuple[int | None, str]]] = field(default_factory=dict)

    def field(self, name: str) -> np.ndarray:
        return getattr(self, name)

    @property
    def shape(self) -> tuple[int, int]:
        return self.close.shape


@dataclass(frozen=True)
class ArchiveDay:
    session_date: str
    equity: Bars
    indices: Bars | None
    reference: dict[str, dict[str, float | None]]
    manifest: dict


@lru_cache(maxsize=4096)  # a day has only a few hundred distinct bar times, repeated per instrument
def parse_timestamp(text: str) -> int:
    return int(datetime.strptime(text, TIMESTAMP_FORMAT).replace(tzinfo=IST).timestamp())


def _number(text: str) -> float | None:
    try:
        value = float(text)
    except (TypeError, ValueError):
        return None
    return value if np.isfinite(value) else None


def _validate(row: dict) -> tuple[int | None, str, tuple | None]:
    """Return (bar-open epoch, rejection reason or '', parsed values or None) for one archived row."""
    try:
        epoch = parse_timestamp(row["timestamp"])
    except (KeyError, TypeError, ValueError):
        return None, "bad_timestamp", None
    values = [_number(row.get(name, "")) for name in BAR_FIELDS]
    if any(v is None for v in values):
        return epoch, "missing_value", None
    o, h, low, c, v = values
    if h < max(o, c) or low > min(o, c) or h < low or v < 0:
        return epoch, "invalid_ohlc", None
    return epoch, "", (o, h, low, c, v)


def _read_rows(path: Path) -> list[dict]:
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.DictReader(handle))


def _to_bars(rows: list[dict], key_column: str, name_column: str) -> Bars:
    parsed: dict[str, dict[int, tuple]] = {}
    names: dict[str, str] = {}
    rejected: dict[str, list[tuple[int | None, str]]] = {}
    for row in rows:
        key = row.get(key_column, "")
        names.setdefault(key, row.get(name_column, key))
        epoch, reason, bar = _validate(row)
        per_key = parsed.setdefault(key, {})
        if not reason and epoch in per_key:
            reason = "duplicate_minute"
        if reason:
            rejected.setdefault(key, []).append((epoch, reason))
            continue
        per_key[epoch] = bar
    keys = tuple(sorted(names))
    minutes = np.array(sorted({epoch for bars in parsed.values() for epoch in bars}), dtype=np.int64)
    column = {int(epoch): j for j, epoch in enumerate(minutes)}
    arrays = {name: np.full((len(keys), len(minutes)), np.nan) for name in BAR_FIELDS}
    for i, key in enumerate(keys):
        for epoch, bar in parsed.get(key, {}).items():
            j = column[epoch]
            for k, name in enumerate(BAR_FIELDS):
                arrays[name][i, j] = bar[k]
    return Bars(keys=keys, names=tuple(names[k] for k in keys), minutes=minutes, rejected=rejected, **arrays)


def _read_reference(path: Path) -> dict[str, dict[str, float | None]]:
    if not path.exists():
        return {}
    return {
        row["symbol"]: {
            "previous_close": _number(row.get("previous_close")),
            "today_open": _number(row.get("today_open")),
        }
        for row in _read_rows(path)
    }


def load_day(root: Path, session_date: str) -> ArchiveDay:
    """Load one archived day. Raises FileNotFoundError if the day has no equity file."""
    day = Path(root) / session_date
    equity_path = day / EQUITY_FILE
    if not equity_path.exists():
        raise FileNotFoundError(f"no archived equity data for {session_date} under {root}")
    index_path = day / INDEX_FILE
    try:
        manifest = json.loads((day / MANIFEST_FILE).read_text())
    except (OSError, ValueError):
        manifest = {}
    return ArchiveDay(
        session_date=session_date,
        equity=_to_bars(_read_rows(equity_path), "symbol", "symbol"),
        indices=_to_bars(_read_rows(index_path), "index", "symbol") if index_path.exists() else None,
        reference=_read_reference(day / EQUITY_REFERENCE_FILE),
        manifest=manifest,
    )


def available_days(root: Path) -> list[str]:
    """Archived session dates under ``root`` that have equity data, oldest first."""
    root = Path(root)
    if not root.exists():
        return []
    return sorted(p.name for p in root.iterdir() if (p / EQUITY_FILE).exists())
