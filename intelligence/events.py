"""Turn anomalies and relationship breaks into events (schema ``event/1``).

An event is emitted for every UNUSUAL or EXTREME finding, subject to a
cooldown: once an event type has fired for a subject, it fires again for that
subject within ``COOLDOWN_MINUTES`` only if its severity has risen. The cooldown
state is rebuilt from the store, so a restarted engine does not repeat itself.

Everything in an event comes from the frame, the engines' outputs and the
store's earlier sessions; the same inputs and engine version always give the
same events with the same ids. See ``docs/intelligence/event-schema.md``.
"""

from __future__ import annotations

import hashlib
from datetime import datetime

import numpy as np

from intelligence.anomaly import EXTREME, EXTREME_Z, UNUSUAL, UNUSUAL_Z, AnomalyReport
from intelligence.archive import IST
from intelligence.event_store import SEVERITY_RANK, EventStore
from intelligence.features import FEATURE_VERSION, FeatureSet
from intelligence.frame import MarketFrame
from intelligence.quality import COMPLETE, block_quality
from intelligence.relationships import Relationship

SCHEMA_VERSION = "event/1"
ENGINE_VERSION = "1.0.0"
COOLDOWN_MINUTES = 15
NOVELTY_SESSIONS = 20
BASELINE_WINDOW = 20
ARCHIVE_REPLAY, LIVE_SNAPSHOT = "ARCHIVE_REPLAY", "LIVE_SNAPSHOT"

# Single-minute findings (anomalies and price/volume disagreements) become events only at EXTREME (|z| >= 5):
# with ~1,000 instruments and three measures, UNUSUAL single minutes are routine and stay visible through
# the anomaly API instead. Multi-minute relationship breaks and market measures fire from UNUSUAL (|z| >= 3).
SINGLE_MINUTE_THRESHOLD = EXTREME_Z
# anomaly measure -> (event type when z > 0, event type when z < 0 or None, statistic)
ANOMALY_TYPES = {
    "volume": ("volume_surge", "volume_drought", "robust_z_log_volume"),
    "return": ("return_shock", "return_shock", "return_sigma_z"),
    "range": ("range_expansion", None, "robust_z_log_range"),  # a quiet single bar is not an event
}
MARKET_TYPES = {"breadth": ("breadth_shift", "BREADTH"), "dispersion": ("dispersion_shift", "DISPERSION")}
# relationship kind -> (event type, category, scope, subject kind, counterpart kind, statistic)
RELATIONSHIP_TYPES = {
    "stock_sector": ("sector_divergence", "RELATIONSHIP", "INSTRUMENT", "EQUITY", "SECTOR", "residual_z"),
    "stock_index": ("index_divergence", "RELATIONSHIP", "INSTRUMENT", "EQUITY", "INDEX", "residual_z"),
    "stock_sector_index": ("sector_index_divergence", "RELATIONSHIP", "INSTRUMENT", "EQUITY", "INDEX", "residual_z"),
    "sector_sector": ("sector_spread_shift", "RELATIONSHIP", "SECTOR", "SECTOR", "SECTOR", "spread_z"),
    "spot_futures_basis": ("basis_shift", "DERIVATIVES", "INDEX", "INDEX", None, "robust_z"),
    "futures_spread": ("futures_spread_shift", "DERIVATIVES", "INDEX", "INDEX", None, "robust_z"),
    "option_pcr_oi": ("pcr_shift", "DERIVATIVES", "INDEX", "INDEX", None, "robust_z"),
    "option_iv_skew": ("iv_skew_shift", "DERIVATIVES", "INDEX", "INDEX", None, "robust_z"),
}
PRICE_VOLUME_TYPES = {"volume_without_move", "move_without_volume"}

EVENT_TYPES = sorted(
    {t for pos, neg, _ in ANOMALY_TYPES.values() for t in (pos, neg) if t}
    | {t for t, _ in MARKET_TYPES.values()}
    | {spec[0] for spec in RELATIONSHIP_TYPES.values()}
    | PRICE_VOLUME_TYPES
)
CATEGORIES = ("ANOMALY", "RELATIONSHIP", "DERIVATIVES", "BREADTH", "DISPERSION")
SEVERITIES = tuple(SEVERITY_RANK)


def severity(z: float, threshold: float = UNUSUAL_Z) -> str:
    ratio = abs(z) / threshold
    return "LOW" if ratio < 1.5 else ("MEDIUM" if ratio < 2.5 else "HIGH")


def event_id(event_type: str, subject: str, counterpart: str, bar_epoch: int) -> str:
    material = "|".join((event_type, subject, counterpart, str(bar_epoch), ENGINE_VERSION))
    return "evt_" + hashlib.sha256(material.encode()).hexdigest()[:16]


def _ist(epoch: int) -> str:
    return datetime.fromtimestamp(int(epoch), IST).strftime("%Y-%m-%d %H:%M:%S IST")


def _round(value, digits: int = 6):
    if isinstance(value, float):
        return round(value, digits) if np.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _round(v, digits) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_round(v, digits) for v in value]
    return value


class EventEngine:
    """Builds events for successive frames of one session; keeps the cooldown state."""

    def __init__(self, store: EventStore | None = None, source: str = ARCHIVE_REPLAY):
        self.store = store
        self.source = source
        self._session: str | None = None
        self._last: dict[tuple[str, str, str], tuple[int, int]] = {}
        self._novelty: dict[tuple[str, str], int] = {}

    def _start_session(self, session_date: str) -> None:
        if self._session != session_date:
            self._session = session_date
            self._last = self.store.last_fired(session_date) if self.store else {}
            self._novelty = {}

    def _prior(self, event_type: str, subject: str) -> int:
        key = (event_type, subject)
        if key not in self._novelty:
            self._novelty[key] = (
                self.store.prior_occurrences(event_type, subject, self._session, NOVELTY_SESSIONS) if self.store else 0
            )
        return self._novelty[key]

    def _due(self, key: tuple[str, str, str], bar_epoch: int, rank: int) -> bool:
        last = self._last.get(key)
        return last is None or bar_epoch - last[0] >= COOLDOWN_MINUTES * 60 or rank > last[1]

    def build(
        self,
        frame: MarketFrame,
        features: FeatureSet,
        report: AnomalyReport,
        relationships: list[Relationship] = (),
        baselines=None,
    ) -> list[dict]:
        """Events for one frame, after cooldown. With a store, stores them and returns only those that were new."""
        if not len(frame.grid):
            return []
        self._start_session(frame.session_date)
        context = _Context(frame, features, report, baselines, self.source)
        candidates = context.anomaly_events() + context.market_events() + context.relationship_events(relationships)
        events = []
        for event in sorted(
            candidates, key=lambda e: (-SEVERITY_RANK[e["severity"]], -abs(e["magnitude"]["value"]), e["event_id"])
        ):
            key = (event["event_type"], event["subject"]["key"], event["subject"].get("counterpart") or "")
            rank = SEVERITY_RANK[event["severity"]]
            if not self._due(key, event["bar_epoch"], rank):
                continue
            self._last[key] = (event["bar_epoch"], rank)
            event["novelty"]["prior_occurrences"] = self._prior(event["event_type"], event["subject"]["key"])
            events.append(event)
        if self.store and events:
            events = self.store.add(events)  # an id already stored (a replayed or restarted minute) is not new
        return events


class _Context:
    """Everything shared by the events of one frame."""

    def __init__(self, frame: MarketFrame, features: FeatureSet, report: AnomalyReport, baselines, source: str):
        self.frame, self.features, self.report, self.baselines, self.source = frame, features, report, baselines, source
        self.bar_epoch = int(frame.grid[-1])
        self._quality = None
        self.sessions = len(baselines.sessions) if baselines is not None else 0

    @property
    def quality(self):
        if self._quality is None:
            q = block_quality(self.frame.equity, self.frame.as_of_epoch)
            self._quality = (q, {item.key: item for item in q.per_instrument})
        return self._quality

    def subject_quality(self, key: str | None, flags: list[str]) -> dict:
        block, per = self.quality
        item = per.get(key) if key else None
        if item is not None and item.status != COMPLETE:
            flags.append("SUBJECT_HAS_GAPS")
        return {
            "subject_status": item.status if item else None,
            "subject_missing_bars": item.missing_bars if item else None,
            "frame_coverage": block.coverage,
            "baseline_sessions_complete": self.sessions,
            "flags": sorted(set(flags)),
        }

    def base(self, event_type, category, scope, subject, kind, counterpart, counterpart_kind, statistic, z, evidence,
             affected, relationships=(), flags=(), feature_versions=None, threshold=UNUSUAL_Z):  # fmt: skip
        subject_obj = {"key": subject, "kind": kind}
        if counterpart:
            subject_obj["counterpart"] = counterpart
            subject_obj["counterpart_kind"] = counterpart_kind
        flags = list(flags)
        return _round(
            {
                "schema_version": SCHEMA_VERSION,
                "event_id": event_id(event_type, subject, counterpart or "", self.bar_epoch),
                "event_type": event_type,
                "category": category,
                "session_date": self.frame.session_date,
                "observed_at": self.frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST"),
                "bar_time": _ist(self.bar_epoch),
                "bar_epoch": self.bar_epoch,
                "scope": scope,
                "subject": subject_obj,
                "magnitude": {"statistic": statistic, "value": float(z), "threshold": float(threshold)},
                "severity": severity(z, threshold),
                "classification": EXTREME if abs(z) >= EXTREME_Z else UNUSUAL,
                "novelty": {"lookback_sessions": NOVELTY_SESSIONS, "prior_occurrences": 0},
                "evidence": evidence,
                "affected_instruments": sorted(affected),
                "relationships": list(relationships),
                "historical_context": None,
                "data_quality": self.subject_quality(subject if kind == "EQUITY" else None, flags),
                "provenance": {
                    "engine": f"intelligence.{category.lower()}.{event_type}",
                    "engine_version": ENGINE_VERSION,
                    "feature_versions": feature_versions or {"features": FEATURE_VERSION},
                    "source": self.source,
                },
                "supersedes": None,
                "synthetic_data": self.frame.synthetic,
            }
        )

    def _sector_context(self, measure: str, key: str) -> dict:
        i = self.features.keys.index(key)
        sector = self.features.sectors[i]
        if not sector:
            return {"sector": None}
        z = self.report.measures[measure].z
        members = [j for j, s in enumerate(self.features.sectors) if s == sector and j != i and np.isfinite(z[j])]
        return {
            "sector": sector,
            "sector_members_scored": len(members),
            "sector_median_z": float(np.median(z[members])) if members else None,
        }

    def anomaly_events(self) -> list[dict]:
        out = []
        for anomaly in self.report.anomalies(minimum=EXTREME):
            positive, negative, statistic = ANOMALY_TYPES[anomaly.measure]
            event_type = positive if anomaly.z > 0 else negative
            if event_type is None:
                continue
            baseline = anomaly.evidence["baseline"]
            flags = []
            if baseline["kind"] != "HISTORICAL":
                flags.append(f"BASELINE_{baseline['kind']}")
            elif baseline["sample"] < BASELINE_WINDOW:
                flags.append("BASELINE_SHORT")
            evidence = {
                "observation": {
                    anomaly.measure: anomaly.evidence["value"],
                    "scored_value": anomaly.evidence["scored_value"],
                },
                "baseline": {"method": _BASELINE_METHOD[baseline["kind"]], **baseline},
                "context": self._sector_context(anomaly.measure, anomaly.key),
            }
            out.append(
                self.base(
                    event_type,
                    "ANOMALY",
                    "INSTRUMENT",
                    anomaly.key,
                    "EQUITY",
                    None,
                    None,
                    statistic,
                    anomaly.z,
                    evidence,
                    [anomaly.key],
                    flags=flags,
                    threshold=SINGLE_MINUTE_THRESHOLD,
                )
            )
        return out

    def market_events(self) -> list[dict]:
        out = []
        for name, result in self.report.market.items():
            if result["classification"] not in (UNUSUAL, EXTREME):
                continue
            event_type, category = MARKET_TYPES[name]
            evidence = {
                "observation": {name: result["value"]},
                "baseline": {"method": _BASELINE_METHOD["HISTORICAL"], **result["baseline"]},
                "context": {"instruments_scored": int(np.sum(np.isfinite(self.report.measures["return"].z)))},
            }
            flags = ["BASELINE_SHORT"] if self.sessions < BASELINE_WINDOW else []
            out.append(
                self.base(
                    event_type,
                    category,
                    "MARKET",
                    "MARKET",
                    "MARKET",
                    None,
                    None,
                    "robust_z",
                    result["z"],
                    evidence,
                    [],
                    flags=flags,
                )
            )
        return out

    def relationship_events(self, relationships) -> list[dict]:
        out = []
        for rel in relationships:
            if rel.classification not in (UNUSUAL, EXTREME) or rel.z is None:
                continue
            if rel.kind == "price_volume":
                if abs(rel.z) < SINGLE_MINUTE_THRESHOLD:
                    continue
                evidence = {"observation": dict(rel.evidence), "baseline": {"method": "anomaly engine z-scores"}}
                out.append(
                    self.base(
                        rel.counterpart,
                        "RELATIONSHIP",
                        "INSTRUMENT",
                        rel.subject,
                        "EQUITY",
                        None,
                        None,
                        "volume_z" if rel.counterpart == "volume_without_move" else "return_z",
                        rel.z,
                        evidence,
                        [rel.subject],
                        threshold=SINGLE_MINUTE_THRESHOLD,
                    )
                )
                continue
            event_type, category, scope, kind, counterpart_kind, statistic = RELATIONSHIP_TYPES[rel.kind]
            counterpart = rel.counterpart if counterpart_kind else None
            if rel.kind == "sector_sector":
                affected = [k for k, s in zip(self.features.keys, self.features.sectors, strict=True)
                            if s in (rel.subject, rel.counterpart)]  # fmt: skip
            elif kind == "EQUITY":
                affected = [rel.subject]
            else:
                affected = []
            evidence = {
                "observation": dict(rel.evidence),
                "baseline": {"method": _RELATIONSHIP_METHOD.get(rel.kind, _RELATIONSHIP_METHOD["default"])},
            }
            relationships = [{"subject": rel.subject, "counterpart": rel.counterpart, "kind": rel.kind, **rel.evidence}]
            out.append(
                self.base(
                    event_type,
                    category,
                    scope,
                    rel.subject,
                    kind,
                    counterpart,
                    counterpart_kind,
                    statistic,
                    rel.z,
                    evidence,
                    affected,
                    relationships=relationships,
                )
            )
        return out


_BASELINE_METHOD = {
    "HISTORICAL": "robust median and scaled MAD of the same minute of day (+-2 min) over earlier sessions",
    "INTRADAY": "robust median and scaled MAD of the instrument's earlier minutes today",
    "CROSS_SECTIONAL": "robust median and scaled MAD of every instrument at the same minute",
}
_RELATIONSHIP_METHOD = {
    "stock_sector": "beta and residual SD fitted on the 60 minutes before the 15 judged; counterpart is the "
    "leave-one-out median of the sector's other members",
    "stock_index": "beta and residual SD fitted on the 60 minutes before the 15 judged",
    "stock_sector_index": "beta and residual SD fitted on the 60 minutes before the 15 judged",
    "sector_sector": "15-minute spread of sector median returns vs the spread's mean and SD earlier today",
    "default": "robust median and scaled MAD of the measure's earlier snapshots today",
}
