"""Replay validation on an archived day (real or bootstrapped): determinism and no look-ahead.

    python -m intelligence validate-replay 2026-10-01

1. Determinism: the day is replayed twice into two fresh event stores; the
   event ids must be identical, in the same order.
2. No look-ahead: at several checkpoints the engine runs on the real day and on
   a copy in which every bar completing after the checkpoint is replaced by
   wild values. Features, anomaly scores, relationships and events at the
   checkpoint must be identical.

Event stores are temporary; baseline caches are shared with the service.
"""

from __future__ import annotations

import dataclasses
import tempfile
from pathlib import Path

import numpy as np

from intelligence.archive import load_day
from intelligence.engine import IntelligenceEngine, replay_session
from intelligence.event_store import EventStore
from intelligence.frame import as_of_time, frame_at

CHECKPOINTS = ("10:15", "12:00", "14:30")


def _poison(day, cutoff_epoch: int):
    def mangle(bars):
        if bars is None:
            return None
        future = bars.minutes + 60 > cutoff_epoch
        arrays = {}
        for name in ("open", "high", "low", "close"):
            values = getattr(bars, name).copy()
            values[:, future] *= 3.7
            arrays[name] = values
        volume = bars.volume.copy()
        volume[:, future] *= 250
        return dataclasses.replace(bars, volume=volume, **arrays)

    return dataclasses.replace(day, equity=mangle(day.equity), indices=mangle(day.indices))


def _same(a, b) -> list[str]:
    differences = []
    for name in a.features.values:
        if not np.array_equal(a.features.values[name], b.features.values[name], equal_nan=True):
            differences.append(f"feature {name}")
    for name, measure in a.report.measures.items():
        if not np.array_equal(measure.z, b.report.measures[name].z, equal_nan=True):
            differences.append(f"anomaly {name}")
    if a.relationships != b.relationships:
        differences.append("relationships")
    if [e["event_id"] for e in a.events] != [e["event_id"] for e in b.events]:
        differences.append("events")
    return differences


def validate_replay(archive_root: Path, session_date: str, cache_root: Path, every: int = 5) -> dict:
    day = load_day(archive_root, session_date)
    with tempfile.TemporaryDirectory(prefix="psygrid-validate-") as tmp:
        tmp = Path(tmp)
        runs = []
        for name in ("first", "second"):
            store = EventStore(tmp / f"{name}.db")
            summary = replay_session(IntelligenceEngine(archive_root, cache_root, event_store=store), day, every=every)
            runs.append(([e["event_id"] for e in store.search(after_seq=0, limit=1000)], summary))
        deterministic = runs[0][0] == runs[1][0]
        checkpoints = {}
        for hhmm in CHECKPOINTS:
            as_of = as_of_time(session_date, hhmm)
            clean = IntelligenceEngine(archive_root, cache_root, event_store=EventStore(tmp / f"c{hhmm}.db"))
            dirty = IntelligenceEngine(archive_root, cache_root, event_store=EventStore(tmp / f"d{hhmm}.db"))
            a = clean.step(frame_at(day, as_of))
            b = dirty.step(frame_at(_poison(day, int(as_of.timestamp())), as_of))
            later = int(a.frame.grid[-1]) + 60 > int(as_of.timestamp()) if len(a.frame.grid) else False
            checkpoints[hhmm] = {"differences": _same(a, b), "frame_has_later_bar": bool(later),
                                 "bars": int(a.frame.equity.shape[1])}  # fmt: skip
    no_look_ahead = all(not c["differences"] and not c["frame_has_later_bar"] for c in checkpoints.values())
    return {
        "session_date": session_date,
        "instruments": len(day.equity.keys),
        "steps": runs[0][1].steps,
        "events": len(runs[0][0]),
        "events_by_type": runs[0][1].events_by_type,
        "step_ms_p95": round(runs[0][1].step_ms["p95"], 1),
        "deterministic": deterministic,
        "no_look_ahead": no_look_ahead,
        "checkpoints": checkpoints,
        "ok": deterministic and no_look_ahead,
    }
