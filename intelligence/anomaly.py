"""Contextual anomaly classification for every instrument at one minute.

Each measure is compared with the most specific baseline available, in order:

1. ``HISTORICAL``: the instrument's own value at this minute of the day over
   earlier sessions (robust median and scale from ``history.Baselines``);
2. ``INTRADAY``: the instrument's own earlier minutes today (log volume only;
   needs ``INTRADAY_MIN_BARS`` bars);
3. ``CROSS_SECTIONAL``: every instrument at this same minute (returns only:
   comparing raw volume across differently sized companies would mean nothing).

Volume and bar range are scored on a log scale with ``z = (value - median) /
scale`` (robust median and scaled MAD). A return is scored as ``z = r / sigma``,
where sigma is the typical 1m move estimated robustly as median(|r|) / 0.6745
(the median of a normal's absolute value is 0.6745 sigma), so |z| >= 3 means a
three-sigma move. Cross-sectionally a return is ``(r - median) / scaled MAD``.

``|z|`` is classified ``NORMAL`` below
``UNUSUAL_Z``, ``UNUSUAL`` below ``EXTREME_Z``, else ``EXTREME``. Before any
score, the data is checked: an instrument whose latest bar is missing is
``STALE``, one whose latest bar was rejected is ``INVALID``, and a measure with
no usable baseline is ``INSUFFICIENT_DATA``. None of these is ever scored.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np

from intelligence.features import FeatureSet
from intelligence.frame import MarketFrame
from intelligence.history import Baselines

UNUSUAL_Z = 3.0
EXTREME_Z = 5.0
INTRADAY_MIN_BARS = 20
MAD_TO_SD = 1.4826

NORMAL, UNUSUAL, EXTREME, INSUFFICIENT_DATA, STALE, INVALID = (
    "NORMAL", "UNUSUAL", "EXTREME", "INSUFFICIENT_DATA", "STALE", "INVALID",
)  # fmt: skip
CLASSES = (NORMAL, UNUSUAL, EXTREME, INSUFFICIENT_DATA, STALE, INVALID)

HISTORICAL, INTRADAY, CROSS_SECTIONAL, NO_BASELINE = "HISTORICAL", "INTRADAY", "CROSS_SECTIONAL", "NONE"

HALF_NORMAL_MEDIAN = 0.6745  # median(|x|) / sigma for a normal x

# measure -> (feature giving the value, baseline field, transform applied to the value)
MEASURES = {
    "volume": ("volume_1m", "log_volume_1m", np.log1p),
    "return": ("ret_1m", "abs_ret_1m", lambda v: v),
    "range": ("range_1m", "log_range_1m", np.log),
}


@dataclass(frozen=True)
class MeasureResult:
    """One measure across the universe: arrays aligned with ``keys``."""

    measure: str
    value: np.ndarray  # the value scored (log1p volume, signed return, log range)
    raw: np.ndarray  # the feature value before transform (signed return, volume)
    median: np.ndarray
    scale: np.ndarray
    sample: np.ndarray  # sessions (HISTORICAL), bars (INTRADAY) or instruments (CROSS_SECTIONAL)
    z: np.ndarray
    baseline: np.ndarray  # baseline kind per instrument
    classification: np.ndarray  # class name per instrument


@dataclass(frozen=True)
class Anomaly:
    key: str
    measure: str
    classification: str
    z: float
    evidence: dict


@dataclass(frozen=True)
class AnomalyReport:
    as_of: str
    minute_index: int
    keys: tuple[str, ...]
    measures: dict[str, MeasureResult]
    market: dict[str, dict]

    def counts(self) -> dict[str, dict[str, int]]:
        return {name: {c: int(np.sum(r.classification == c)) for c in CLASSES} for name, r in self.measures.items()}

    def anomalies(self, minimum: str = UNUSUAL) -> list[Anomaly]:
        """Scored anomalies at or above ``minimum`` (UNUSUAL or EXTREME), most extreme first."""
        wanted = {UNUSUAL, EXTREME} if minimum == UNUSUAL else {EXTREME}
        out = []
        for name, r in self.measures.items():
            for i in np.flatnonzero(np.isin(r.classification, list(wanted))):
                out.append(
                    Anomaly(
                        key=self.keys[i],
                        measure=name,
                        classification=str(r.classification[i]),
                        z=float(r.z[i]),
                        evidence={
                            "value": _num(r.raw[i]),
                            "scored_value": _num(r.value[i]),
                            "baseline": {
                                "kind": str(r.baseline[i]),
                                "median": _num(r.median[i]),
                                "scale": _num(r.scale[i]),
                                "sample": int(r.sample[i]),
                            },
                        },
                    )
                )
        return sorted(out, key=lambda a: (-abs(a.z), a.key, a.measure))

    def of(self, key: str) -> dict[str, dict]:
        i = self.keys.index(key)
        return {
            name: {
                "classification": str(r.classification[i]),
                "z": _num(r.z[i]),
                "value": _num(r.raw[i]),
                "baseline": str(r.baseline[i]),
            }
            for name, r in self.measures.items()
        }


def _num(value) -> float | None:
    value = float(value)
    return value if np.isfinite(value) else None


def classify_z(z: np.ndarray) -> np.ndarray:
    magnitude = np.abs(z)
    return np.where(magnitude >= EXTREME_Z, EXTREME, np.where(magnitude >= UNUSUAL_Z, UNUSUAL, NORMAL))


def _data_state(frame: MarketFrame) -> np.ndarray:
    """Per instrument: '' when the latest bar is usable, else STALE, INVALID or INSUFFICIENT_DATA."""
    bars = frame.equity
    n, m = bars.shape
    state = np.full(n, "", dtype=object)
    if m == 0:
        state[:] = INSUFFICIENT_DATA
        return state
    present = ~np.isnan(bars.close)
    latest = int(frame.grid[-1])
    for i, key in enumerate(bars.keys):
        if not present[i].any():
            state[i] = INSUFFICIENT_DATA
        elif any(epoch == latest for epoch, _ in bars.rejected.get(key, ())):
            state[i] = INVALID
        elif not present[i, -1]:
            state[i] = STALE
    return state


def _intraday_baseline(frame: MarketFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Median and scaled MAD of each instrument's earlier log volumes today (excluding the latest bar)."""
    history = np.log1p(frame.equity.volume[:, :-1])
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        count = np.sum(~np.isnan(history), axis=1)
        median = np.nanmedian(history, axis=1) if history.shape[1] else np.full(len(count), np.nan)
        scale = (
            np.nanmedian(np.abs(history - median[:, None]), axis=1) * MAD_TO_SD
            if history.shape[1]
            else np.full(len(count), np.nan)
        )
    enough = count >= INTRADAY_MIN_BARS
    return np.where(enough, median, np.nan), np.where(enough, scale, np.nan), count


def _cross_sectional(value: np.ndarray) -> tuple[float, float, int]:
    finite = value[np.isfinite(value)]
    if len(finite) < 10:
        return float("nan"), float("nan"), len(finite)
    median = float(np.median(finite))
    return median, float(np.median(np.abs(finite - median)) * MAD_TO_SD), len(finite)


def score_measure(
    name: str, features: FeatureSet, baselines: Baselines | None, frame: MarketFrame, state
) -> MeasureResult:
    feature, baseline_field, transform = MEASURES[name]
    raw = features.values[feature].astype(float)
    with np.errstate(invalid="ignore", divide="ignore"):
        value = transform(raw)  # a zero range scores as -inf, which is never finite and so never classified
    n = len(raw)
    median, scale, sample = np.full(n, np.nan), np.full(n, np.nan), np.zeros(n, dtype=int)
    kind = np.full(n, NO_BASELINE, dtype=object)
    if baselines is not None and baselines.keys == features.keys:
        h_median, h_scale, h_count = baselines.lookup(baseline_field, features.minute_index)
        if name == "return":  # sigma from the typical absolute move; a return is centred on zero
            h_scale, h_median = h_median / HALF_NORMAL_MEDIAN, np.where(np.isfinite(h_median), 0.0, np.nan)
        usable = np.isfinite(h_median) & np.isfinite(h_scale) & (h_scale > 0)
        median[usable], scale[usable], sample[usable] = h_median[usable], h_scale[usable], h_count[usable]
        kind[usable] = HISTORICAL
    missing = kind == NO_BASELINE
    if name == "volume" and missing.any() and frame.equity.shape[1] > 1:
        i_median, i_scale, i_count = _intraday_baseline(frame)
        usable = missing & np.isfinite(i_median) & np.isfinite(i_scale) & (i_scale > 0)
        median[usable], scale[usable], sample[usable] = i_median[usable], i_scale[usable], i_count[usable]
        kind[usable] = INTRADAY
    elif name == "return" and missing.any():
        c_median, c_scale, c_count = _cross_sectional(raw)  # signed returns across the universe this minute
        if np.isfinite(c_scale) and c_scale > 0:
            median[missing], scale[missing], sample[missing] = c_median, c_scale, c_count
            kind[missing] = CROSS_SECTIONAL
    with np.errstate(invalid="ignore", divide="ignore"):
        z = (value - median) / scale
    classification = classify_z(z).astype(object)
    no_score = (kind == NO_BASELINE) | ~np.isfinite(z)
    classification[no_score] = INSUFFICIENT_DATA
    blocked = state != ""
    classification[blocked] = state[blocked]
    z[no_score | blocked] = np.nan
    return MeasureResult(name, value, raw, median, scale, sample, z, kind, classification)


MARKET_MEASURES = {
    "dispersion": ("dispersion_15m", "dispersion_15m"),
    "breadth": ("breadth_session", "breadth_session"),
}


def score_market(features: FeatureSet, baselines: Baselines | None) -> dict[str, dict]:
    out = {}
    for name, (feature, field) in MARKET_MEASURES.items():
        value = features.market.get(feature, float("nan"))
        median, scale = baselines.market_lookup(field, features.minute_index) if baselines else (float("nan"),) * 2
        if np.isfinite(value) and np.isfinite(median) and np.isfinite(scale) and scale > 0:
            z = (value - median) / scale
            classification = str(classify_z(np.array([z]))[0])
        else:
            z, classification = float("nan"), INSUFFICIENT_DATA
        out[name] = {
            "classification": classification,
            "z": _num(z),
            "value": _num(value),
            "baseline": {"kind": HISTORICAL if classification != INSUFFICIENT_DATA else NO_BASELINE,
                         "median": _num(median), "scale": _num(scale)},
        }  # fmt: skip
    return out


def detect(frame: MarketFrame, features: FeatureSet, baselines: Baselines | None) -> AnomalyReport:
    state = _data_state(frame)
    measures = {name: score_measure(name, features, baselines, frame, state) for name in MEASURES}
    return AnomalyReport(
        features.as_of, features.minute_index, features.keys, measures, score_market(features, baselines)
    )
