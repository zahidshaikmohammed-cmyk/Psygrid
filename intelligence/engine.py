"""The whole intelligence pipeline for one frame, shared by replay and live.

``IntelligenceEngine.step(frame)`` runs, in order: data quality, features,
anomalies (against baselines from earlier sessions), relationships (including
recorded derivatives snapshots known by ``as_of``), and events (stored, with
cooldown). It returns a ``Snapshot``: everything known at that minute, with
JSON-ready views for the API. Replay and the live process call the same
``step``; only the source of the frame differs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from intelligence.anomaly import EXTREME, UNUSUAL, AnomalyReport, detect
from intelligence.archive import ArchiveDay
from intelligence.derivatives import load_derivatives
from intelligence.event_store import EventStore
from intelligence.events import ARCHIVE_REPLAY, ENGINE_VERSION, EventEngine
from intelligence.features import FEATURE_VERSION, FeatureSet, compute_features
from intelligence.frame import MarketFrame
from intelligence.history import Baselines, build_baselines
from intelligence.quality import frame_quality
from intelligence.relationships import Relationship, all_relationships
from intelligence.replay import replay

TOP_ANOMALIES = 25


def _num(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return round(value, 8) if np.isfinite(value) else None


def _relationship_view(rel: Relationship) -> dict:
    return {
        "kind": rel.kind,
        "subject": rel.subject,
        "counterpart": rel.counterpart,
        "classification": rel.classification,
        "z": _num(rel.z),
        "evidence": {k: (_num(v) if isinstance(v, float) else v) for k, v in rel.evidence.items()},
    }


@dataclass
class Snapshot:
    """Everything the engine knew at one minute."""

    frame: MarketFrame
    features: FeatureSet
    report: AnomalyReport
    relationships: list[Relationship]
    events: list[dict]  # events newly emitted at this step
    quality: dict
    source: str
    baseline_sessions: tuple[str, ...]
    timings_ms: dict[str, float] = field(default_factory=dict)

    @property
    def session_date(self) -> str:
        return self.frame.session_date

    @property
    def as_of(self) -> str:
        return self.frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST")

    def header(self) -> dict:
        return {
            "session_date": self.session_date,
            "as_of": self.as_of,
            "minute_index": self.features.minute_index,
            "source": self.source,
            "engine_version": ENGINE_VERSION,
            "feature_version": FEATURE_VERSION,
            "baseline_sessions": len(self.baseline_sessions),
            "synthetic_data": self.frame.synthetic,
        }

    def market(self) -> dict:
        anomalies = self.report.anomalies()
        flagged = [r for r in self.relationships if r.classification in (UNUSUAL, EXTREME)]
        by_kind: dict[str, int] = {}
        for rel in flagged:
            by_kind[rel.kind] = by_kind.get(rel.kind, 0) + 1
        return {
            **self.header(),
            "data_quality": self.quality,
            "market_features": {k: _num(v) for k, v in self.features.market.items()},
            "market_measures": self.report.market,
            "anomaly_counts": self.report.counts(),
            "top_anomalies": [
                {"key": a.key, "measure": a.measure, "classification": a.classification, "z": _num(a.z)}
                for a in anomalies[:TOP_ANOMALIES]
            ],
            "flagged_relationships": by_kind,
            "sectors": {s: {k: _num(v) for k, v in vals.items()} for s, vals in self.features.sectors_median.items()},
            "indices": {k: {f: _num(v) for f, v in vals.items()} for k, vals in self.features.indices.items()},
        }

    def instrument(self, key: str) -> dict | None:
        if key not in self.features.keys:
            return None
        i = self.features.keys.index(key)
        quality = next((q for q in self.quality_items() if q.key == key), None)
        return {
            **self.header(),
            "key": key,
            "sector": self.features.sectors[i],
            "features": {name: _num(values[i]) for name, values in self.features.values.items()},
            "anomalies": self.report.of(key),
            "relationships": [_relationship_view(r) for r in self.relationships if key in (r.subject, r.counterpart)],
            "data_quality": {
                "status": quality.status,
                "present_bars": quality.present_bars,
                "missing_bars": quality.missing_bars,
                "rejected_bars": quality.rejected_bars,
                "last_bar": quality.last_bar,
            }
            if quality
            else None,
        }

    def quality_items(self):
        if not hasattr(self, "_quality_items"):
            self._quality_items = frame_quality(self.frame).equity.per_instrument
        return self._quality_items

    def observations(self, keys: list[str] | None = None, fields: list[str] | None = None) -> list[dict]:
        """One row per instrument: its features and the classification of each anomaly measure."""
        wanted = [k for k in (keys or self.features.keys) if k in self.features.keys]
        names = [f for f in (fields or self.features.values) if f in self.features.values]
        rows = []
        for key in wanted:
            i = self.features.keys.index(key)
            rows.append(
                {
                    "key": key,
                    "sector": self.features.sectors[i],
                    **{name: _num(self.features.values[name][i]) for name in names},
                    "classification": {m: str(r.classification[i]) for m, r in self.report.measures.items()},
                }
            )
        return rows

    def anomalies(self, minimum: str = UNUSUAL) -> list[dict]:
        return [
            {"key": a.key, "measure": a.measure, "classification": a.classification, "z": _num(a.z),
             "evidence": a.evidence}
            for a in self.report.anomalies(minimum)
        ]  # fmt: skip

    def relationship_views(self, kind: str | None = None, subject: str | None = None) -> list[dict]:
        return [
            _relationship_view(r)
            for r in self.relationships
            if (kind is None or r.kind == kind) and (subject is None or subject in (r.subject, r.counterpart))
        ]


class IntelligenceEngine:
    """Runs the pipeline for successive frames; caches baselines per session."""

    def __init__(self, archive_root: Path, store_root: Path, source: str = ARCHIVE_REPLAY,
                 event_store: EventStore | None = None, window_sessions: int = 20):  # fmt: skip
        self.archive_root = Path(archive_root)
        self.store_root = Path(store_root)
        self.source = source
        self.events = EventEngine(event_store, source)
        self.event_store = event_store
        self.window_sessions = window_sessions
        self._baselines: tuple[tuple, Baselines] | None = None

    def baselines_for(self, session_date: str, keys: tuple[str, ...]) -> Baselines:
        cache_key = (session_date, keys)
        if self._baselines is None or self._baselines[0] != cache_key:
            baselines = build_baselines(
                self.archive_root, session_date, keys, window_sessions=self.window_sessions, cache_root=self.store_root
            )
            self._baselines = (cache_key, baselines)
        return self._baselines[1]

    def step(self, frame: MarketFrame) -> Snapshot:
        timings = {}
        started = time.perf_counter()

        def lap(name: str) -> None:
            nonlocal started
            now = time.perf_counter()
            timings[name] = round((now - started) * 1000, 2)
            started = now

        baselines = self.baselines_for(frame.session_date, frame.equity.keys)
        lap("baselines")
        quality = frame_quality(frame)
        quality_view = {"equity": quality.equity.summary(),
                        "indices": quality.indices.summary() if quality.indices else None}  # fmt: skip
        lap("quality")
        features = compute_features(frame)
        lap("features")
        report = detect(frame, features, baselines)
        lap("anomalies")
        derivatives = load_derivatives(self.store_root, frame.session_date)
        relationships = all_relationships(frame, features, report, derivatives, only_flagged=False)
        lap("relationships")
        events = self.events.build(frame, features, report, relationships, baselines)
        lap("events")
        timings["total"] = round(sum(timings.values()), 2)
        return Snapshot(frame, features, report, relationships, events, quality_view, self.source,
                        baselines.sessions, timings)  # fmt: skip


@dataclass(frozen=True)
class ReplaySummary:
    session_date: str
    steps: int
    events: int
    events_by_type: dict[str, int]
    step_ms: dict[str, float]  # p50, p95, max of the total per step
    stage_ms_mean: dict[str, float]


def replay_session(engine: IntelligenceEngine, day: ArchiveDay, start: str = "09:15", end: str = "15:15",
                   every: int = 1, on_step=None) -> ReplaySummary:  # fmt: skip
    """Run the engine over a whole archived session, as if live."""
    totals, stages, by_type, count, steps = [], {}, {}, 0, 0
    for frame in replay(day, start, end, every):
        snapshot = engine.step(frame)
        steps += 1
        totals.append(snapshot.timings_ms["total"])
        for name, ms in snapshot.timings_ms.items():
            stages.setdefault(name, []).append(ms)
        for event in snapshot.events:
            by_type[event["event_type"]] = by_type.get(event["event_type"], 0) + 1
        count += len(snapshot.events)
        if on_step:
            on_step(snapshot)
    arr = np.array(totals) if totals else np.zeros(1)
    return ReplaySummary(
        day.session_date,
        steps,
        count,
        dict(sorted(by_type.items())),
        {"p50": float(np.percentile(arr, 50)), "p95": float(np.percentile(arr, 95)), "max": float(arr.max())},
        {name: round(float(np.mean(v)), 2) for name, v in stages.items()},
    )
