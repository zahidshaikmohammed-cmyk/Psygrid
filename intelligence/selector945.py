"""PSYGRID 945: at 09:45:00 IST, rank the 989-stock universe and select exactly one stock and a direction.

Information set. A decision for session D uses only:

- bars that closed by 09:45:00 on D (``frame_at``: the 09:15 .. 09:44 bars);
- norms, response models and market-state history from qualified sessions strictly before D;
- training records (09:45 state matrix + realised outcomes) of sessions strictly before D;
- option-chain snapshots recorded by 09:45:00 on D.

Pipeline: 989 stocks -> data-quality eligibility -> state matrix (``matrix.py``) with expected-response,
market-state, microstructure and derivatives context -> scoring by the active model -> exactly one stock
and UP/DOWN -> an immutable decision record. Outcomes (+5/+15/+30 minutes, from the 09:45 bar's open)
are computed afterwards and stored separately; the selector never sees them for its own day.

Models (``MODEL_VERSIONS``):

- ``v1``: transparent rank, fixed before any data was examined: opening-drive continuation. The
  volatility-scaled market-relative move since the open, confirmed by relative volume, trend agreement and
  opening-range breakout. Direction = the sign of that move.
- ``v2``: walk-forward pooled ridge regression of the absolute 15-minute forward return on the cross-
  sectionally standardised state matrix, trained on up to ``TRAIN_WINDOW`` earlier sessions. Direction =
  the sign of the prediction; selection = the largest |prediction|.

The active model for D is v2 only if, over the ``COMPARE_MIN_DAYS``+ sessions before D where both had
out-of-sample picks, v2's mean net return beat v1's and its pooled out-of-sample IC was positive with t > 2.
Otherwise v1. Complexity has to earn its place on data the decision could have known.

Probability. The score is a rank (0-100), never a probability. ``probability`` is the out-of-sample hit
rate estimate for the selected direction: for v2 a logistic calibration of earlier days' out-of-sample
predictions (|prediction| -> P(direction right)); for v1 the hit rate of v1's earlier out-of-sample picks.
It is labelled ``CALIBRATED_OOS`` only with at least ``CALIBRATION_MIN_DAYS`` out-of-sample days and an
expected calibration error below ``MAX_ECE``; otherwise ``UNCALIBRATED`` (the number is still shown,
with its interval).
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from itertools import pairwise
from pathlib import Path

import numpy as np

from intelligence.archive import IST
from intelligence.frame import as_of_time
from intelligence.matrix import COLUMNS, MATRIX_VERSION, StateMatrix, average_ranks

SELECTOR_VERSION = "1.0.0"
DECISION_TIME = "09:45"
HORIZONS = (5, 15, 30)
PRIMARY_HORIZON = 15
COST_BPS_ROUND_TRIP = 12.0  # brokerage + STT + exchange + stamp ~ 5 bps, plus ~ 3.5 bps slippage per side
TRAIN_WINDOW = 40
V2_MIN_TRAIN_DAYS = 15
COMPARE_MIN_DAYS = 15
CALIBRATION_MIN_DAYS = 20
V1_CALIBRATION_MIN_PICKS = 50  # a single base rate needs more picks before it may be called calibrated
MAX_ECE = 0.05
RIDGE_ALPHA = 50.0  # on standardised features, pooled over ~950 stocks x days
WINSOR = 4.0

ELIGIBILITY = {
    "min_completeness": 0.8,
    "max_last_bar_age_min": 2,
    "min_ltp": 10.0,
    "min_turnover_cr": 1.0,  # INR crore traded by 09:45: tradable size for a single intraday position
}

V2_FEATURES = (
    "rel_market_open", "ret_5m", "ret_15m", "accel_5m", "persistence_10m", "gap_pct", "rel_volume", "volume_accel",
    "pv_corr_15m", "or_position", "breakout_state", "range_position", "range_vs_vol", "trend_state",
    "rel_sector_open", "response_gap_sigma", "pending_response_sigma", "rvol_session", "vol_ratio", "vol_expansion",
    "illiquidity", "market_ret_open", "breadth", "dispersion",
)  # fmt: skip
LOG_FEATURES = ("rel_volume", "volume_accel", "illiquidity", "rvol_session", "vol_ratio", "range_vs_vol")
MARKET_CONTEXT_FEATURES = ("market_ret_open", "breadth", "dispersion")

CONFIG = {
    "selector_version": SELECTOR_VERSION, "matrix_version": MATRIX_VERSION, "decision_time": DECISION_TIME,
    "horizons": HORIZONS, "primary_horizon": PRIMARY_HORIZON, "cost_bps_round_trip": COST_BPS_ROUND_TRIP,
    "train_window": TRAIN_WINDOW, "v2_min_train_days": V2_MIN_TRAIN_DAYS, "compare_min_days": COMPARE_MIN_DAYS,
    "calibration_min_days": CALIBRATION_MIN_DAYS, "v1_calibration_min_picks": V1_CALIBRATION_MIN_PICKS, "max_ece": MAX_ECE, "ridge_alpha": RIDGE_ALPHA,
    "winsor": WINSOR, "eligibility": ELIGIBILITY, "v2_features": V2_FEATURES,
}  # fmt: skip


def config_hash(config: dict | None = None) -> str:
    return hashlib.sha256(json.dumps(config or CONFIG, sort_keys=True, default=list).encode()).hexdigest()[:16]


def _r(x, digits=6):
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return round(x, digits) if math.isfinite(x) else None


# --- eligibility -----------------------------------------------------------------------------


def eligibility(matrix: StateMatrix) -> tuple[np.ndarray, str]:
    """(mask, tier). Tier ``STRICT`` normally; looser tiers only if nothing passes, so one stock is always chosen."""
    v = matrix.values
    with np.errstate(invalid="ignore"):
        core = np.isfinite(v["ltp"]) & np.isfinite(v["ret_open_pct"]) & np.isfinite(v["rvol_session"])
        strict = (core & (v["completeness"] >= ELIGIBILITY["min_completeness"])
                  & (v["last_bar_age_min"] <= ELIGIBILITY["max_last_bar_age_min"]) & (v["frozen"] == 0)
                  & np.isfinite(v["prev_close"]) & (v["ltp"] >= ELIGIBILITY["min_ltp"])
                  & (v["turnover_session_cr"] >= ELIGIBILITY["min_turnover_cr"]))  # fmt: skip
    if strict.any():
        return strict, "STRICT"
    relaxed = core & (v["frozen"] == 0) & (v["ltp"] >= ELIGIBILITY["min_ltp"])
    if relaxed.any():
        return relaxed, "RELAXED"
    if core.any():
        return core, "MINIMAL"
    raise ValueError("no stock has a price and a return at 09:45: the session has no usable data")


# --- v1: transparent rank ------------------------------------------------------------------------


def _robust_z(x: np.ndarray, mask: np.ndarray) -> np.ndarray:
    out = np.full(x.shape, np.nan)
    ok = mask & np.isfinite(x)
    if ok.sum() < 3:
        return out
    med = np.median(x[ok])
    mad = np.median(np.abs(x[ok] - med)) * 1.4826
    if mad <= 0:
        mad = np.std(x[ok]) or 1.0
    out[ok] = np.clip((x[ok] - med) / mad, -WINSOR * 2, WINSOR * 2)
    return out


def score_v1(matrix: StateMatrix, mask: np.ndarray) -> tuple[np.ndarray, np.ndarray, dict]:
    """(signed strength, direction, components). Opening-drive continuation, market-relative, volume-confirmed."""
    v = matrix.values
    bars = max(int(matrix.context.get("bars_elapsed", 30)), 1)
    with np.errstate(divide="ignore", invalid="ignore"):
        drive = v["rel_market_open"] / (v["rvol_session"] * math.sqrt(bars))
    z_drive = _robust_z(drive, mask)
    z_volume = _robust_z(np.log(v["rel_volume"]), mask)
    direction = np.sign(z_drive)
    trend = np.nan_to_num(v["trend_state"]) * direction
    breakout = np.nan_to_num(v["breakout_state"]) * direction
    strength = np.abs(z_drive) + 0.5 * np.clip(np.nan_to_num(z_volume), 0, None) + 0.25 * trend + 0.25 * breakout
    strength[~(mask & np.isfinite(z_drive))] = np.nan
    components = {"drive_z": z_drive, "volume_z": z_volume, "trend_agreement": trend, "breakout_agreement": breakout}
    return strength * direction, direction, components


# --- v2: walk-forward ridge ----------------------------------------------------------------------


def design(values: dict[str, np.ndarray], mask: np.ndarray, features=V2_FEATURES) -> np.ndarray:
    """Cross-sectionally standardised features for the masked rows; missing -> 0 (the cross-sectional mean).

    Market-context columns are constant within a day: they are kept raw (scaled by fixed constants) so they can
    carry day-level information instead of being standardised away.
    """
    cols = []
    for name in features:
        x = values[name][mask].astype(float)
        if name in LOG_FEATURES:
            with np.errstate(divide="ignore", invalid="ignore"):
                x = np.log(np.where(x > 0, x, np.nan))
        if name in MARKET_CONTEXT_FEATURES:
            scale = {"market_ret_open": 100.0, "breadth": 1.0, "dispersion": 100.0}[name]
            cols.append(np.nan_to_num((x - (0.5 if name == "breadth" else 0.0)) * scale))
            continue
        ok = np.isfinite(x)
        if ok.sum() >= 3 and np.std(x[ok]) > 0:
            z = (x - np.mean(x[ok])) / np.std(x[ok])
            cols.append(np.clip(np.nan_to_num(z), -WINSOR, WINSOR))
        else:
            cols.append(np.zeros(mask.sum()))
    return np.column_stack(cols) if cols else np.zeros((int(mask.sum()), 0))


@dataclass
class TrainingDay:
    """One earlier session: its 09:45 matrix values, eligibility and realised outcomes (all stocks)."""

    session_date: str
    keys: tuple[str, ...]
    values: dict[str, np.ndarray]
    eligible: np.ndarray
    forward: dict[int, np.ndarray]  # horizon -> absolute forward return (log) from the 09:45 open entry
    mfe: dict[int, np.ndarray]  # horizon -> max favourable excursion for a LONG (log); short = -mae_long
    mae: dict[int, np.ndarray]  # horizon -> max adverse excursion for a LONG (log, <= 0)
    bars_elapsed: int = 30

    def matrix(self) -> StateMatrix:
        return StateMatrix(self.session_date, f"{self.session_date} 09:45:00 IST", self.keys, (None,) * len(self.keys),
                           self.values, {"bars_elapsed": self.bars_elapsed})  # fmt: skip


def _xy(day: TrainingDay, horizon: int, features=V2_FEATURES) -> tuple[np.ndarray, np.ndarray]:
    y = day.forward[horizon]
    mask = day.eligible & np.isfinite(y)
    x = design(day.values, mask, features)
    target = y[mask] * 1e4
    if len(target) > 10:
        lo, hi = np.quantile(target, [0.01, 0.99])
        target = np.clip(target, lo, hi)
    return x, target


def fit_ridge(days: list[TrainingDay], horizon: int = PRIMARY_HORIZON, features=V2_FEATURES,
              cache: dict | None = None) -> np.ndarray | None:  # fmt: skip
    """Coefficients (intercept first) of the pooled ridge on ``days``; None if too few days."""
    if len(days) < V2_MIN_TRAIN_DAYS:
        return None
    pairs = []
    for d in days:
        key = (d.session_date, horizon, features)
        if cache is None or key not in cache:
            value = _xy(d, horizon, features)
            if cache is None:
                pairs.append(value)
                continue
            cache[key] = value
        pairs.append(cache[key])
    xs, ys = zip(*pairs, strict=True)
    x, y = np.concatenate(xs), np.concatenate(ys)
    if len(y) < 100:
        return None
    x1 = np.column_stack([np.ones(len(y)), x])
    penalty = RIDGE_ALPHA * np.eye(x1.shape[1])
    penalty[0, 0] = 0.0  # the intercept is not shrunk
    return np.linalg.solve(x1.T @ x1 + penalty, x1.T @ y)


def predict_v2(coef: np.ndarray, values: dict[str, np.ndarray], mask: np.ndarray, features=V2_FEATURES) -> np.ndarray:
    out = np.full(len(mask), np.nan)
    if coef is None or not mask.any():
        return out
    x = design(values, mask, features)
    out[mask] = coef[0] + x @ coef[1:]
    return out


# --- out-of-sample history for model choice and calibration ---------------------------------------


def _pick(signed: np.ndarray, keys: tuple[str, ...]) -> int:
    """Index of the largest |signed| (ties: alphabetical key) -- deterministic."""
    strength = np.abs(signed)
    finite = np.flatnonzero(np.isfinite(strength))
    best = max(strength[finite])
    tied = [i for i in finite if strength[i] == best]
    return min(tied, key=lambda i: keys[i])


@dataclass
class OOSRecord:
    session_date: str
    model: str
    key: str
    direction: int
    signed_return: float  # log, in the chosen direction, primary horizon
    prediction: float = float("nan")  # v2 only (bps)


def oos_history(days: list[TrainingDay], horizon: int = PRIMARY_HORIZON, cache: dict | None = None) -> dict:
    """Walk-forward out-of-sample picks of v1 and v2 and v2's stock-level predictions, for every day in ``days``.

    For day j, v2 is trained only on days before j (within ``TRAIN_WINDOW``). Everything returned is something a
    decision on a later day may use: it is all in that day's past.
    """
    cache = {} if cache is None else cache
    picks = {"v1": [], "v2": []}
    v2_points = []  # (day, prediction bps, realised log return) for every eligible stock
    ics = []
    for j, day in enumerate(days):
        y = day.forward[horizon]
        mask = day.eligible & np.isfinite(y)
        if mask.sum() < 3:
            continue
        signed, direction, _ = score_v1(day.matrix(), day.eligible)
        if np.isfinite(signed[mask]).any():
            s = np.where(mask, signed, np.nan)
            i = _pick(s, day.keys)
            picks["v1"].append(
                OOSRecord(day.session_date, "v1", day.keys[i], int(direction[i]), float(direction[i] * y[i]))
            )
        coef = fit_ridge(days[max(0, j - TRAIN_WINDOW) : j], horizon, cache=cache)
        if coef is None:
            continue
        pred = predict_v2(coef, day.values, day.eligible)
        s = np.where(mask, pred, np.nan)
        if not np.isfinite(s).any():
            continue
        i = _pick(s, day.keys)
        d = int(np.sign(s[i]) or 1)
        picks["v2"].append(OOSRecord(day.session_date, "v2", day.keys[i], d, float(d * y[i]), float(s[i])))
        ok = np.isfinite(s)
        v2_points += [(day.session_date, float(p), float(r)) for p, r in zip(s[ok], y[ok], strict=True)]
        ics.append(_spearman(s[ok], y[ok]))
    return {"picks": picks, "v2_points": v2_points, "v2_daily_ic": ics}


def _spearman(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 10:
        return float("nan")
    ra, rb = average_ranks(a), average_ranks(b)
    ra, rb = ra - ra.mean(), rb - rb.mean()
    den = math.sqrt(float((ra @ ra) * (rb @ rb)))
    return float(ra @ rb / den) if den else float("nan")


def _t(values) -> tuple[float, float]:
    v = np.array([x for x in values if np.isfinite(x)])
    if len(v) < 3 or v.std(ddof=1) == 0:
        return (float(v.mean()) if len(v) else float("nan")), 0.0
    return float(v.mean()), float(v.mean() / (v.std(ddof=1) / math.sqrt(len(v))))


def wilson(hits: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return 0.0, 1.0
    p = hits / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return max(0.0, centre - half), min(1.0, centre + half)


def choose_model(history: dict) -> tuple[str, dict]:
    """The pre-registered rule: v2 only if it beat v1 out of sample on the same earlier days."""
    v1 = {r.session_date: r for r in history["picks"]["v1"]}
    v2 = {r.session_date: r for r in history["picks"]["v2"]}
    common = sorted(set(v1) & set(v2))
    cost = COST_BPS_ROUND_TRIP / 1e4
    v1_net = [v1[d].signed_return - cost for d in common]
    v2_net = [v2[d].signed_return - cost for d in common]
    ic_mean, ic_t = _t(history["v2_daily_ic"])
    evidence = {
        "common_oos_days": len(common),
        "v1_mean_net_bps": _r(np.mean(v1_net) * 1e4 if v1_net else float("nan"), 2),
        "v2_mean_net_bps": _r(np.mean(v2_net) * 1e4 if v2_net else float("nan"), 2),
        "v2_oos_ic": _r(ic_mean, 4),
        "v2_oos_ic_t": _r(ic_t, 2),
        "v2_oos_ic_days": len(history["v2_daily_ic"]),
    }
    if len(common) >= COMPARE_MIN_DAYS and np.mean(v2_net) > np.mean(v1_net) and ic_mean > 0 and ic_t > 2:
        evidence["rule"] = "v2 beat v1 out of sample (mean net return) and its OOS IC is positive with t > 2"
        return "v2", evidence
    evidence["rule"] = (f"v1: v2 needs >= {COMPARE_MIN_DAYS} common OOS days, a higher mean net return than v1, "
                        "and OOS IC > 0 with t > 2")  # fmt: skip
    return "v1", evidence


def _logistic(x: np.ndarray, y: np.ndarray, iterations: int = 50) -> tuple[float, float]:
    """Maximum-likelihood a, b of P(y=1) = 1 / (1 + exp(-(a + b x))) by Newton's method (a tiny ridge for safety)."""
    a = b = 0.0
    for _ in range(iterations):
        z = np.clip(a + b * x, -30, 30)
        p = 1 / (1 + np.exp(-z))
        w = p * (1 - p)
        g = np.array([np.sum(y - p), np.sum((y - p) * x)]) - 1e-6 * np.array([a, b])
        h = np.array([[np.sum(w), np.sum(w * x)], [np.sum(w * x), np.sum(w * x * x)]]) + 1e-6 * np.eye(2)
        step = np.linalg.solve(h, g)
        a, b = a + step[0], b + step[1]
        if np.abs(step).max() < 1e-9:
            break
    return float(a), float(b)


def calibration_v2(points: list[tuple[str, float, float]]) -> dict:
    """Map |v2 prediction| to P(direction right) from out-of-sample points, with an ECE check on the same points."""
    days = sorted({d for d, _, _ in points})
    if not points:
        return {"status": "UNCALIBRATED", "reason": "no out-of-sample predictions yet", "days": 0}
    pred = np.array([p for _, p, _ in points])
    real = np.array([r for _, _, r in points])
    keep = real != 0
    pred, real = pred[keep], real[keep]
    x = np.abs(pred) / (np.std(pred) or 1.0)
    hit = (np.sign(pred) == np.sign(real)).astype(float)
    a, b = _logistic(x, hit)
    p = 1 / (1 + np.exp(-(a + b * x)))
    bins = np.quantile(p, np.linspace(0, 1, 11))
    ece = 0.0
    for lo, hi in pairwise(bins):
        sel = (p >= lo) & (p <= hi)
        if sel.any():
            ece += sel.mean() * abs(hit[sel].mean() - p[sel].mean())
    status = "CALIBRATED_OOS" if len(days) >= CALIBRATION_MIN_DAYS and ece <= MAX_ECE else "UNCALIBRATED"
    reason = ("" if status == "CALIBRATED_OOS" else
              f"needs >= {CALIBRATION_MIN_DAYS} OOS days (has {len(days)}) and ECE <= {MAX_ECE} (has {ece:.3f})")  # fmt: skip
    return {"status": status, "reason": reason, "days": len(days), "points": len(hit), "a": a, "b": b,
            "scale": float(np.std(pred) or 1.0), "ece": round(float(ece), 4), "base_hit_rate": round(float(hit.mean()), 4)}  # fmt: skip


# --- the decision ---------------------------------------------------------------------------------


@dataclass
class Decision:
    payload: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        return self.payload["selected"]["symbol"]

    @property
    def direction(self) -> str:
        return self.payload["selected"]["direction"]


def _contributions(model: str, matrix: StateMatrix, i: int, mask: np.ndarray, components: dict,
                   coef: np.ndarray | None) -> list[dict]:  # fmt: skip
    """The selected stock's largest score contributions, from the model's own terms."""
    if model == "v1":
        weights = {"drive_z": 1.0, "volume_z": 0.5, "trend_agreement": 0.25, "breakout_agreement": 0.25}
        rows = []
        for name, w in weights.items():
            value = components[name][i]
            contribution = (abs(value) if name == "drive_z" else (max(value, 0) if name == "volume_z" else value)) * w
            rows.append({"term": name, "value": _r(value, 4), "weight": w, "contribution": _r(contribution, 4)})
        return sorted(rows, key=lambda r: -abs(r["contribution"] or 0))
    x = design(matrix.values, mask)
    row = list(np.flatnonzero(mask)).index(i)
    contributions = x[row] * coef[1:]
    order = np.argsort(-np.abs(contributions))[:8]
    return [{"term": V2_FEATURES[k], "standardised_value": _r(x[row, k], 4), "coefficient_bps": _r(coef[1 + k], 3),
             "contribution_bps": _r(contributions[k], 3)} for k in order]  # fmt: skip


def decide(matrix: StateMatrix, training: list[TrainingDay], context: dict | None = None) -> Decision:
    """Select exactly one stock and a direction at 09:45 from ``matrix`` and earlier sessions' ``training``."""
    if any(t.session_date >= matrix.session_date for t in training):
        raise ValueError("training data must come only from sessions before the decision day")
    context = context or {}
    training = sorted(training, key=lambda t: t.session_date)[-(TRAIN_WINDOW + CALIBRATION_MIN_DAYS * 2) :]
    mask, tier = eligibility(matrix)
    cache: dict = {}
    history = oos_history(training, cache=cache)
    model, evidence = choose_model(history)
    signed_v1, _, components = score_v1(matrix, mask)
    coef = fit_ridge(training[-TRAIN_WINDOW:], cache=cache) if training else None
    pred = predict_v2(coef, matrix.values, mask)
    signed = signed_v1 if model == "v1" else pred
    if not np.isfinite(np.where(mask, signed, np.nan)).any():  # v2 could not score: fall back to v1, recorded
        model, signed = "v1", signed_v1
        evidence["rule"] += "; v2 produced no finite score today, v1 used"
    signed = np.where(mask, signed, np.nan)
    i = _pick(signed, matrix.keys)
    direction = int(np.sign(signed[i]) or 1)
    strength = np.abs(signed)
    finite = np.isfinite(strength)
    score = float((average_ranks(strength[finite])[list(np.flatnonzero(finite)).index(i)] - 0.5) / finite.sum() * 100)
    ranking = np.argsort(-np.nan_to_num(strength, nan=-1), kind="mergesort")
    runner_up = [{"symbol": matrix.keys[k], "direction": "UP" if signed[k] > 0 else "DOWN",
                  "strength": _r(strength[k], 4)} for k in ranking[1:6] if finite[k]]  # fmt: skip

    # probability and expectations, from out-of-sample history only
    picks = history["picks"][model]
    cost = COST_BPS_ROUND_TRIP / 1e4
    hits = sum(r.signed_return > 0 for r in picks)
    lo, hi = wilson(hits, len(picks))
    prob = {"value": None, "status": "UNCALIBRATED", "interval95": [_r(lo, 3), _r(hi, 3)], "oos_picks": len(picks),
            "oos_pick_hit_rate": _r(hits / len(picks), 4) if picks else None}  # fmt: skip
    if model == "v2":
        cal = calibration_v2(history["v2_points"])
        if "a" in cal:
            x = abs(pred[i]) / cal["scale"]
            prob["value"] = _r(1 / (1 + math.exp(-(cal["a"] + cal["b"] * x))), 4)
        prob.update(status=cal["status"], method="logistic calibration of |prediction| on OOS stock-level points",
                    calibration=cal)  # fmt: skip
    else:
        if picks:
            prob["value"] = _r((hits + 1) / (len(picks) + 2), 4)  # Laplace-smoothed OOS hit rate of v1 picks
        enough = len(picks) >= V1_CALIBRATION_MIN_PICKS
        prob.update(status="CALIBRATED_OOS" if enough else "UNCALIBRATED",
                    method="hit rate of v1's earlier out-of-sample picks (not conditional on today's score)",
                    reason=None if enough else f"needs >= {V1_CALIBRATION_MIN_PICKS} OOS picks (has {len(picks)})")  # fmt: skip
    returns = np.array([r.signed_return for r in picks])
    expected = {
        "horizon_minutes": PRIMARY_HORIZON,
        "expected_return_pct": _r(pred[i] * direction / 100, 4) if model == "v2" else
        (_r(returns.mean() * 100, 4) if len(returns) else None),
        "basis": "v2 prediction for this stock" if model == "v2" else "mean of v1's earlier OOS picks",
        "oos_mean_return_pct": _r(returns.mean() * 100, 4) if len(returns) else None,
        "oos_mean_net_return_pct": _r((returns.mean() - cost) * 100, 4) if len(returns) else None,
        "oos_median_return_pct": _r(np.median(returns) * 100, 4) if len(returns) else None,
        "oos_return_sd_pct": _r(returns.std(ddof=1) * 100, 4) if len(returns) > 1 else None,
        "cost_bps_round_trip": COST_BPS_ROUND_TRIP,
    }  # fmt: skip
    mfe, mae = _excursion_expectations(training, picks)
    expected.update(mfe_pct=mfe, mae_pct=mae)
    v = matrix.values
    row = matrix.row(matrix.keys[i])
    quality = {
        "eligibility_tier": tier,
        "status": "PASS" if tier == "STRICT" and not row["stale"] else "DEGRADED",
        "completeness": row["completeness"], "last_bar_age_min": row["last_bar_age_min"],
        "universe_missing_share": _r(float(np.mean(~np.isfinite(v["ltp"]))), 4),
        "training_days": len(training), "history_sessions": matrix.context.get("history_sessions"),
        "response_model": bool(matrix.context.get("response_model")),
    }  # fmt: skip
    payload = {
        "product": "PSYGRID 945",
        "session_date": matrix.session_date,
        "decision_time": matrix.as_of,
        "computed_at": context.get("computed_at") or datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "universe": len(matrix.keys),
        "eligible": int(mask.sum()),
        "selected": {
            "symbol": matrix.keys[i],
            "direction": "UP" if direction > 0 else "DOWN",
            "sector": matrix.sectors[i],
        },
        "selection_score": _r(score, 2),
        "score_meaning": "percentile rank of the model's strength among eligible stocks (not a probability)",
        "probability": prob,
        "expected": expected,
        "model": {
            "active": model,
            "selector_version": SELECTOR_VERSION,
            "matrix_version": MATRIX_VERSION,
            "model_choice": evidence,
            "v2_coefficients_hash": _hash_array(coef),
        },
        "contributions": _contributions(model, matrix, i, mask, components, coef),
        "state": {
            k: row[k]
            for k in (
                "ltp",
                "prev_close",
                "gap_pct",
                "ret_open_pct",
                "rel_market_open",
                "rel_volume",
                "trend_state",
                "breakout_state",
                "or_position",
                "rvol_session",
                "response_gap_sigma",
                "pending_response_sigma",
                "rel_sector_open",
                "spread_bps",
                "turnover_session_cr",
            )
        },
        "market": {
            "state": matrix.context.get("market_state"),
            "market_ret_open": _r(matrix.context.get("market_ret_open")),
            "market_source": matrix.context.get("market_source"),
            "breadth": row["breadth"],
            "dispersion": row["dispersion"],
            "sector_ret_open": row["sector_ret_open"],
            "sector_breadth": row["sector_breadth"],
        },
        "runners_up": runner_up,
        "data_quality": quality,
        "hashes": {
            "input": matrix.input_hash,
            "config": config_hash(),
            "training": hashlib.sha256("|".join(t.session_date for t in training).encode()).hexdigest()[:16],
        },
        "immutable": True,
    }
    payload["hashes"]["decision"] = decision_hash(payload)
    return Decision(payload)


def decision_hash(payload: dict) -> str:
    """Hash of the decision content (everything except when it was computed and the hash itself)."""
    body = {k: v for k, v in payload.items() if k not in ("computed_at",)}
    body["hashes"] = {k: v for k, v in payload.get("hashes", {}).items() if k != "decision"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()


def _hash_array(a) -> str | None:
    if a is None:
        return None
    return hashlib.sha256(np.ascontiguousarray(np.round(a, 10)).tobytes()).hexdigest()[:16]


def _excursion_expectations(training: list[TrainingDay], picks: list[OOSRecord]) -> tuple[float | None, float | None]:
    by_day = {t.session_date: t for t in training}
    mfe, mae = [], []
    for r in picks:
        t = by_day.get(r.session_date)
        if t is None or r.key not in t.keys:
            continue
        i = t.keys.index(r.key)
        up, down = t.mfe[PRIMARY_HORIZON][i], t.mae[PRIMARY_HORIZON][i]
        if r.direction > 0:
            mfe.append(up)
            mae.append(down)
        else:
            mfe.append(-down)
            mae.append(-up)
    if not mfe:
        return None, None
    return _r(np.nanmean(mfe) * 100, 4), _r(np.nanmean(mae) * 100, 4)


# --- outcomes ---------------------------------------------------------------------------------------


def outcomes(day, keys: tuple[str, ...] | None = None) -> dict:
    """Per-stock outcomes after 09:45 from a full day: entry at the 09:45 bar's open (the first price after the
    decision), exit at the close of the bar ending at 09:45 + h. Long-side MFE/MAE; times in minutes.

    Uses bars after 09:45 by design: it is the evaluator, run only after the decision is stored."""
    bars = day.equity
    index = {k: i for i, k in enumerate(bars.keys)}
    keys = keys or bars.keys
    start = int(as_of_time(day.session_date, DECISION_TIME).timestamp())
    n = len(keys)
    out = {"entry": np.full(n, np.nan), "forward": {}, "mfe": {}, "mae": {}, "t_mfe": {}, "t_mae": {}, "exit": {}}
    minutes = bars.minutes
    for h in HORIZONS:
        for name in ("forward", "mfe", "mae", "t_mfe", "t_mae", "exit"):
            out[name][h] = np.full(n, np.nan)
    columns = {h: np.flatnonzero((minutes >= start) & (minutes < start + 60 * h)) for h in HORIZONS}
    first = np.flatnonzero((minutes >= start) & (minutes < start + 300))  # entry within 5 minutes
    for row, key in enumerate(keys):
        i = index.get(key)
        if i is None:
            continue
        opens = bars.open[i, first]
        ok = np.flatnonzero(np.isfinite(opens))
        if not len(ok):
            continue
        entry_col = first[ok[0]]
        entry = bars.open[i, entry_col]
        out["entry"][row] = entry
        for h in HORIZONS:
            cols = columns[h][columns[h] >= entry_col]
            closes = bars.close[i, cols]
            have = np.flatnonzero(np.isfinite(closes))
            if not len(have):
                continue
            exit_price = closes[have[-1]]
            out["exit"][h][row] = exit_price
            out["forward"][h][row] = math.log(exit_price / entry)
            highs, lows = bars.high[i, cols], bars.low[i, cols]
            with np.errstate(invalid="ignore", divide="ignore"):
                up = np.log(highs / entry)
                down = np.log(lows / entry)
            if np.isfinite(up).any():
                k = int(np.nanargmax(up))
                out["mfe"][h][row] = max(float(up[k]), 0.0)
                out["t_mfe"][h][row] = (minutes[cols[k]] - start) / 60 + 1
            if np.isfinite(down).any():
                k = int(np.nanargmin(down))
                out["mae"][h][row] = min(float(down[k]), 0.0)
                out["t_mae"][h][row] = (minutes[cols[k]] - start) / 60 + 1
    return out


def decision_outcome(decision: dict, day) -> dict:
    """The selected stock's outcome at each horizon, in the decision's direction, gross and net of costs."""
    key = decision["selected"]["symbol"]
    sign = 1 if decision["selected"]["direction"] == "UP" else -1
    o = outcomes(day, (key,))
    cost = COST_BPS_ROUND_TRIP / 1e4
    result = {"symbol": key, "direction": decision["selected"]["direction"], "entry": _r(o["entry"][0], 4),
              "entry_rule": "open of the 09:45 bar", "horizons": {}}  # fmt: skip
    for h in HORIZONS:
        fwd = o["forward"][h][0]
        mfe = o["mfe"][h][0] if sign > 0 else -o["mae"][h][0]
        mae = o["mae"][h][0] if sign > 0 else -o["mfe"][h][0]
        t_mfe = o["t_mfe"][h][0] if sign > 0 else o["t_mae"][h][0]
        t_mae = o["t_mae"][h][0] if sign > 0 else o["t_mfe"][h][0]
        result["horizons"][f"{h}m"] = {
            "exit": _r(o["exit"][h][0], 4),
            "return_pct": _r(sign * fwd * 100, 4),
            "net_return_pct": _r((sign * fwd - cost) * 100, 4),
            "direction_hit": None if not np.isfinite(fwd) else bool(sign * fwd > 0),
            "mfe_pct": _r(mfe * 100, 4), "mae_pct": _r(mae * 100, 4),
            "minutes_to_mfe": _r(t_mfe, 1), "minutes_to_mae": _r(t_mae, 1),
        }  # fmt: skip
    return result


def training_day(matrix: StateMatrix, day) -> TrainingDay:
    """The record a later decision may learn from: this day's 09:45 matrix with its realised outcomes."""
    mask, _ = eligibility(matrix)
    o = outcomes(day, matrix.keys)
    return TrainingDay(matrix.session_date, matrix.keys, {k: v.copy() for k, v in matrix.values.items()}, mask,
                       o["forward"], o["mfe"], o["mae"], int(matrix.context.get("bars_elapsed", 30)))  # fmt: skip


# --- storage ------------------------------------------------------------------------------------


class DecisionStore:
    """Immutable decisions, separate outcomes, and training records under ``<store>/945/<namespace>``.

    A decision file is created exclusively (``O_EXCL``) and made read-only; writing a different decision for a day
    that already has one raises. Outcomes are replaceable (they only grow more complete during the day).
    """

    def __init__(self, store_root: Path, namespace: str = "live"):
        self.root = Path(store_root) / "945" / namespace
        self.training_root = Path(store_root) / "945" / "training"

    def decision_path(self, session_date: str) -> Path:
        return self.root / "decisions" / f"{session_date}.json"

    def outcome_path(self, session_date: str) -> Path:
        return self.root / "outcomes" / f"{session_date}.json"

    def save_decision(self, decision: Decision) -> bool:
        """True if written; False if an identical decision already exists. Raises on a conflicting rewrite."""
        path = self.decision_path(decision.payload["session_date"])
        path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(decision.payload, indent=2, sort_keys=True, default=str)
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
        except FileExistsError:
            existing = json.loads(path.read_text())
            if existing.get("hashes", {}).get("decision") != decision.payload["hashes"]["decision"]:
                raise ValueError(
                    f"a different decision for {path.stem} already exists; decisions are immutable"
                ) from None
            return False
        with os.fdopen(fd, "w") as handle:
            handle.write(data)
        return True

    def load_decision(self, session_date: str) -> dict | None:
        try:
            return json.loads(self.decision_path(session_date).read_text())
        except (OSError, ValueError):
            return None

    def save_outcome(self, session_date: str, outcome: dict) -> None:
        path = self.outcome_path(session_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(outcome, indent=2, sort_keys=True, default=str))
        tmp.replace(path)

    def load_outcome(self, session_date: str) -> dict | None:
        try:
            return json.loads(self.outcome_path(session_date).read_text())
        except (OSError, ValueError):
            return None

    def decisions(self) -> list[str]:
        folder = self.root / "decisions"
        return sorted(p.stem for p in folder.glob("*.json")) if folder.exists() else []

    # training records are shared by every namespace: they hold no decision, only data and outcomes
    def training_path(self, session_date: str) -> Path:
        return self.training_root / f"{session_date}.v{MATRIX_VERSION}.npz"

    def save_training(self, t: TrainingDay) -> None:
        path = self.training_path(t.session_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        arrays = {f"v_{k}": v for k, v in t.values.items()}
        for h in HORIZONS:
            arrays[f"f_{h}"], arrays[f"u_{h}"], arrays[f"d_{h}"] = t.forward[h], t.mfe[h], t.mae[h]
        tmp = path.with_name(path.name + ".tmp.npz")
        np.savez_compressed(tmp, keys=np.array(t.keys), eligible=t.eligible, bars=np.array(t.bars_elapsed), **arrays)
        os.replace(tmp, path)

    def load_training(self, before: str, limit: int = TRAIN_WINDOW + 2 * CALIBRATION_MIN_DAYS) -> list[TrainingDay]:
        """Training records of sessions strictly before ``before`` (most recent ``limit``)."""
        if not self.training_root.exists():
            return []
        suffix = f".v{MATRIX_VERSION}.npz"
        dates = sorted(p.name[: -len(suffix)] for p in self.training_root.glob(f"*{suffix}"))
        out = []
        for d in [d for d in dates if d < before][-limit:]:
            data = np.load(self.training_path(d), allow_pickle=False)
            values = {k[2:]: data[k] for k in data.files if k.startswith("v_")}
            if set(values) != set(COLUMNS):
                continue  # written by another matrix layout
            out.append(TrainingDay(d, tuple(str(k) for k in data["keys"]), values, data["eligible"],
                                   {h: data[f"f_{h}"] for h in HORIZONS}, {h: data[f"u_{h}"] for h in HORIZONS},
                                   {h: data[f"d_{h}"] for h in HORIZONS}, int(data["bars"])))  # fmt: skip
        return out


def render(decision: dict, outcome: dict | None = None) -> str:
    """The plain-text decision card."""
    sel, prob, exp = decision["selected"], decision["probability"], decision["expected"]
    lines = [
        "PSYGRID 945", "===========", "",
        f"TIME: {decision['decision_time']}",
        f"UNIVERSE: {decision['universe']}", f"ELIGIBLE: {decision['eligible']}", "",
        f"SELECTED: {sel['symbol']}" + (f" ({sel['sector']})" if sel.get("sector") else ""),
        f"DIRECTION: {sel['direction']}",
        f"SELECTION SCORE: {decision['selection_score']} / 100 (rank, not a probability)",
        f"PROBABILITY: {prob['value'] if prob['value'] is not None else 'n/a'} "
        f"(95% interval of OOS pick hit rate {prob['interval95'][0]}-{prob['interval95'][1]}, n={prob['oos_picks']})",
        f"PROBABILITY STATUS: {prob['status']}",
        f"HORIZON: {exp['horizon_minutes']} MIN",
        f"EXPECTED RETURN: {exp['expected_return_pct']}% ({exp['basis']}); cost {exp['cost_bps_round_trip']} bps",
        f"MFE / MAE EXPECTATION: {exp['mfe_pct']}% / {exp['mae_pct']}%",
        f"MODEL: {decision['model']['active']} (selector {decision['model']['selector_version']})",
        f"MARKET STATE: {decision['market']['state']} (market since open {decision['market']['market_ret_open']})",
        f"EXPECTED RESPONSE GAP: {decision['state']['response_gap_sigma']} sigma",
        "TOP CONTRIBUTIONS: " + ", ".join(f"{c['term']}" for c in decision["contributions"][:4]),
        f"DATA QUALITY: {decision['data_quality']['status']} ({decision['data_quality']['eligibility_tier']})",
        f"DECISION HASH: {decision['hashes']['decision'][:16]}",
        "DECISION: IMMUTABLE",
    ]  # fmt: skip
    if outcome:
        lines.append("")
        lines.append("OUTCOME (entry at the 09:45 bar open):")
        for h in [f"{h}m" for h in HORIZONS if f"{h}m" in outcome["horizons"]]:
            o = outcome["horizons"][h]
            lines.append(f"  +{h}: {o['return_pct']}% (net {o['net_return_pct']}%), hit={o['direction_hit']}, "
                         f"MFE {o['mfe_pct']}% at {o['minutes_to_mfe']}m, MAE {o['mae_pct']}% at {o['minutes_to_mae']}m")  # fmt: skip
    return "\n".join(lines)
