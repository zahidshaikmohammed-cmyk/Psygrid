"""Outcome evaluation harness: can a signal predict anything, out of sample, after costs and placebos?

Walk-forward by day: for each test session the model is fitted on the
sessions strictly before it, signals are computed at every ``every``-th
minute from bars completed by then, and compared with what happened next.

Target: the forward *residual* return over h minutes, the stock's forward
log return minus its contemporaneous exposure to the factors realised over
those minutes (lag-0 market, sector and statistical terms). What is left is
the stock-specific move plus any delayed response: exactly what a
"before the move" signal must predict.

Metrics, per signal and horizon:
- rank IC (Spearman) across stocks each minute, averaged per day; the mean
  over days with its t-statistic (days are the independent unit), and the
  share of days with a positive IC (hit rate);
- decay across horizons;
- top-minus-bottom decile forward residual (bps), gross and net of a cost
  estimate (Roll's spread from 1m closes when no quote history exists);
- breakdowns by liquidity decile (training turnover), time of day and the
  day's market regime (high/low cross-sectional dispersion);
- placebos: the same statistic with the signal shuffled across minutes
  within the day (time placebo), computed on the time-reversed session
  (reversed-time placebo), and with its sign flipped;
- Benjamini-Hochberg false-discovery control across every (signal, horizon,
  subgroup) hypothesis tested.

Stocks whose latest bar is missing at the signal minute are excluded (stale),
and so are stock-minutes whose forward window has no bars. A signal is
reported as supported only if its IC survives FDR at the chosen level, its
placebos do not, and its sign is stable across the two halves of the test
period. Everything else is reported as unproven.
"""

from __future__ import annotations

import math
import warnings
from dataclasses import dataclass, field

import numpy as np

from intelligence.archive import load_day, session_days
from intelligence.response import (
    LAGS,
    ResponseModel,
    SessionReturns,
    _design,
    _sector_groups,
    evaluate_state,
    fit,
    lag_parts,
    session_returns,
    stat_realisations,
)

HORIZONS = (1, 5, 15, 30)
MIN_NAMES = 50


def _rank(x: np.ndarray) -> np.ndarray:
    order = np.argsort(x, kind="mergesort")
    ranks = np.empty(len(x))
    ranks[order] = np.arange(len(x))
    return ranks


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < MIN_NAMES:
        return float("nan")
    ra, rb = _rank(a[ok]), _rank(b[ok])
    ra -= ra.mean()
    rb -= rb.mean()
    denom = math.sqrt(float((ra * ra).sum() * (rb * rb).sum()))
    return float((ra * rb).sum() / denom) if denom else float("nan")


def benjamini_hochberg(pvalues: list[float], q: float = 0.05) -> list[bool]:
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    passed = [False] * m
    cutoff = -1
    for rank, i in enumerate(order, start=1):
        if pvalues[i] <= q * rank / m:
            cutoff = rank
    for rank, i in enumerate(order, start=1):
        if rank <= cutoff:
            passed[i] = True
    return passed


def _t_pvalue(values: list[float]) -> tuple[float, float, float]:
    """(mean, t, two-sided p) of the daily values; normal approximation for p."""
    v = np.array([x for x in values if np.isfinite(x)])
    if len(v) < 3 or v.std(ddof=1) == 0:
        return (float(v.mean()) if len(v) else float("nan")), float("nan"), 1.0
    t = float(v.mean() / (v.std(ddof=1) / math.sqrt(len(v))))
    p = math.erfc(abs(t) / math.sqrt(2))
    return float(v.mean()), t, p


def roll_spread(close: np.ndarray) -> np.ndarray:
    """Roll's implied bid-ask spread (as a fraction of price) per stock from 1m closes; NaN when undefined."""
    with np.errstate(divide="ignore", invalid="ignore"):
        dp = np.diff(np.log(close), axis=1)
    out = np.full(close.shape[0], np.nan)
    for i in range(close.shape[0]):
        x = dp[i][np.isfinite(dp[i])]
        if len(x) < 30:
            continue
        cov = np.cov(x[1:], x[:-1])[0, 1]
        out[i] = 2 * math.sqrt(-cov) if cov < 0 else 0.0
    return out


def forward_residual(model: ResponseModel, sr: SessionReturns, h: int) -> np.ndarray:
    """(n, m) forward h-minute return minus its lag-0 exposure to factors realised over those minutes."""
    groups = _sector_groups(sr.keys)
    m_lags, s_lags = _design(sr, groups)
    market_part, sector_part = lag_parts(model.beta_market, model.beta_sector, m_lags, s_lags)
    p = stat_realisations(sr.r - market_part - sector_part, model.loadings)
    # Remove only the lag-0 exposures to factors realised during the horizon; delayed responses stay in.
    contemporaneous = (model.beta_market[:, :1] * np.nan_to_num(m_lags[0])[None, :]
                       + model.beta_sector[:, :1] * np.nan_to_num(s_lags[:, 0, :])
                       + (model.beta_stat @ p if p.shape[0] else 0.0))  # fmt: skip
    resid = sr.r - contemporaneous
    n, m = resid.shape
    out = np.full((n, m), np.nan)
    valid = np.isfinite(resid)
    filled = np.where(valid, resid, 0.0)
    csum = np.concatenate([np.zeros((n, 1)), np.cumsum(filled, axis=1)], axis=1)
    ccount = np.concatenate([np.zeros((n, 1)), np.cumsum(valid, axis=1)], axis=1)
    for t in range(m - h):
        total = csum[:, t + 1 + h] - csum[:, t + 1]
        count = ccount[:, t + 1 + h] - ccount[:, t + 1]
        out[:, t] = np.where(count >= max(1, h // 2), total, np.nan)
    return out


SIGNALS = {
    # name: (field of ResponseState, sign so that a positive value predicts a positive forward residual)
    "pending_response": ("pending", 1.0),
    "response_gap": ("gap", 1.0),
    "response_gap_sigma": ("gap_sigma", 1.0),
}


@dataclass
class DayResult:
    session_date: str
    ic: dict[tuple[str, int], float] = field(default_factory=dict)
    ic_by: dict[tuple[str, int, str], float] = field(default_factory=dict)
    spread_bps: dict[tuple[str, int], float] = field(default_factory=dict)
    net_bps: dict[tuple[str, int], float] = field(default_factory=dict)
    placebo_time: dict[tuple[str, int], float] = field(default_factory=dict)
    placebo_reversed: dict[tuple[str, int], float] = field(default_factory=dict)
    dispersion: float = float("nan")


def _signal_matrix(model, sr, every: int, start: int) -> dict[str, list[tuple[int, np.ndarray]]]:
    out = {name: [] for name in SIGNALS}
    m = sr.r.shape[1]
    for t in range(start, m, every):
        state = evaluate_state(model, sr, upto=t)
        for name, (attr, sign) in SIGNALS.items():
            values = sign * getattr(state, attr).astype(float)
            values[state.stale] = np.nan
            out[name].append((t, values))
    return out


def _bucket(minute: int) -> str:
    return "open" if minute < 60 else ("close" if minute >= 300 else "midday")


def evaluate_day(model: ResponseModel, sr: SessionReturns, every: int = 5, seed: int = 0) -> DayResult:
    rng = np.random.default_rng(seed)
    start = LAGS + 15
    signals = _signal_matrix(model, sr, every, start)
    targets = {h: forward_residual(model, sr, h) for h in HORIZONS}
    liquidity = model.median_turnover
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        deciles = np.nanquantile(liquidity, [0.3, 0.7]) if np.isfinite(liquidity).any() else [np.nan, np.nan]
    liq_bucket = np.where(liquidity <= deciles[0], "illiquid", np.where(liquidity >= deciles[1], "liquid", "mid"))
    cost = roll_spread(sr.close) / 2  # half the spread per side
    reversed_sr = SessionReturns(sr.session_date, sr.keys, sr.grid, sr.r[:, ::-1].copy(), sr.volume[:, ::-1],
                                 sr.close[:, ::-1], sr.market[::-1].copy(), sr.market_source)  # fmt: skip
    rev_signals = _signal_matrix(model, reversed_sr, every, start)
    rev_targets = {h: forward_residual(model, reversed_sr, h) for h in HORIZONS}
    result = DayResult(sr.session_date)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        result.dispersion = float(np.nanmean(np.nanstd(sr.r, axis=0)))
    for name, series in signals.items():
        for h in HORIZONS:
            ics, spreads, nets, shuffled, groups = [], [], [], [], {}
            minutes = [t for t, _ in series]
            for t, values in series:
                target = targets[h][:, t]
                ic = spearman(values, target)
                if np.isfinite(ic):
                    ics.append(ic)
                    groups.setdefault(f"time:{_bucket(t)}", []).append(ic)
                    for label in ("illiquid", "mid", "liquid"):
                        mask = liq_bucket == label
                        groups.setdefault(f"liquidity:{label}", []).append(
                            spearman(np.where(mask, values, np.nan), target)
                        )
                ok = np.isfinite(values) & np.isfinite(target)
                if ok.sum() >= MIN_NAMES:
                    lo, hi = np.quantile(values[ok], [0.1, 0.9])
                    top, bottom = ok & (values >= hi), ok & (values <= lo)
                    if top.any() and bottom.any():
                        gross = float(target[top].mean() - target[bottom].mean())
                        spreads.append(gross * 1e4)
                        c = np.nan_to_num(np.concatenate([cost[top], cost[bottom]]), nan=0.0).mean() * 2  # round trip
                        nets.append((gross - 2 * c) * 1e4)
                other = minutes[rng.integers(len(minutes))]  # the signal at a random other minute of the day
                shuffled.append(spearman(values, targets[h][:, other]))
            result.ic[(name, h)] = float(np.nanmean(ics)) if ics else float("nan")
            result.spread_bps[(name, h)] = float(np.mean(spreads)) if spreads else float("nan")
            result.net_bps[(name, h)] = float(np.mean(nets)) if nets else float("nan")
            result.placebo_time[(name, h)] = float(np.nanmean(shuffled)) if shuffled else float("nan")
            for label, values in groups.items():
                finite = [v for v in values if np.isfinite(v)]
                result.ic_by[(name, h, label)] = float(np.mean(finite)) if finite else float("nan")
            rev = [spearman(v, rev_targets[h][:, t]) for t, v in rev_signals[name]]
            result.placebo_reversed[(name, h)] = float(np.nanmean(rev)) if rev else float("nan")
    return result


def evaluate(archive_root, cache_root, train_sessions: int = 20, test_days: int | None = None, every: int = 5,
             fdr_q: float = 0.05, log=lambda m: None) -> dict:  # fmt: skip
    """Walk-forward evaluation over every session that has ``train_sessions`` sessions before it."""
    days = session_days(archive_root)
    candidates = days[train_sessions:]
    if test_days:
        candidates = candidates[-test_days:]
    results: list[DayResult] = []
    loaded: dict[str, SessionReturns] = {}  # a rolling window: each archived day is read once
    for i, day in enumerate(candidates):
        position = days.index(day)
        needed = days[position - train_sessions : position + 1]
        for old in [d for d in loaded if d not in needed]:
            del loaded[old]
        for d in needed:
            if d not in loaded:
                loaded[d] = session_returns(load_day(archive_root, d))
        sr = loaded[day]
        model = fit([loaded[d] for d in needed[:-1]], sr.keys)  # sessions strictly before the day judged
        results.append(evaluate_day(model, sr, every=every, seed=i))
        log(f"evaluated {day} ({i + 1}/{len(candidates)})")
    return summarise(results, fdr_q)


def summarise(results: list[DayResult], fdr_q: float = 0.05) -> dict:
    if not results:
        return {"status": "INSUFFICIENT_HISTORY", "test_days": 0}
    hypotheses, pvalues = [], []
    table = {}
    median_dispersion = float(np.nanmedian([r.dispersion for r in results]))
    half = len(results) // 2
    for name in SIGNALS:
        for h in HORIZONS:
            daily = [r.ic[(name, h)] for r in results]
            mean, t, p = _t_pvalue(daily)
            first, _, _ = _t_pvalue(daily[:half]) if half >= 3 else (float("nan"), 0, 1)
            second, _, _ = _t_pvalue(daily[half:]) if len(daily) - half >= 3 else (float("nan"), 0, 1)
            placebo_t = _t_pvalue([r.placebo_time[(name, h)] for r in results])
            placebo_r = _t_pvalue([r.placebo_reversed[(name, h)] for r in results])
            entry = {
                "mean_ic": _round(mean), "t": _round(t), "p": _round(p),
                "hit_rate": _round(np.mean([x > 0 for x in daily if np.isfinite(x)]) if daily else float("nan")),
                "first_half_ic": _round(first), "second_half_ic": _round(second),
                "top_minus_bottom_bps": _round(np.nanmean([r.spread_bps[(name, h)] for r in results])),
                "net_of_spread_bps": _round(np.nanmean([r.net_bps[(name, h)] for r in results])),
                "placebo_time_ic": _round(placebo_t[0]), "placebo_time_p": _round(placebo_t[2]),
                "placebo_reversed_ic": _round(placebo_r[0]), "placebo_reversed_p": _round(placebo_r[2]),
                "sign_flipped_ic": _round(-mean),
                "by": {},
            }  # fmt: skip
            hypotheses.append((name, h, "all"))
            pvalues.append(p)
            labels = sorted({key[2] for r in results for key in r.ic_by if key[0] == name and key[1] == h})
            regimes = {"regime:high_dispersion": [r for r in results if r.dispersion >= median_dispersion],
                       "regime:low_dispersion": [r for r in results if r.dispersion < median_dispersion]}  # fmt: skip
            for label in labels:
                sub_mean, sub_t, sub_p = _t_pvalue([r.ic_by.get((name, h, label), float("nan")) for r in results])
                entry["by"][label] = {"mean_ic": _round(sub_mean), "t": _round(sub_t), "p": _round(sub_p)}
                hypotheses.append((name, h, label))
                pvalues.append(sub_p)
            for label, subset in regimes.items():
                sub_mean, sub_t, sub_p = _t_pvalue([r.ic[(name, h)] for r in subset])
                entry["by"][label] = {"mean_ic": _round(sub_mean), "t": _round(sub_t), "p": _round(sub_p),
                                      "days": len(subset)}  # fmt: skip
                hypotheses.append((name, h, label))
                pvalues.append(sub_p)
            table[f"{name}@{h}m"] = entry
    passed = benjamini_hochberg(pvalues, fdr_q)
    survivors = [f"{n}@{h}m[{label}]" for (n, h, label), ok in zip(hypotheses, passed, strict=True) if ok]
    for key, entry in table.items():
        name, h = key.split("@")
        h = int(h[:-1])
        ok = passed[hypotheses.index((name, h, "all"))]
        stable = np.sign(entry["first_half_ic"] or 0) == np.sign(entry["second_half_ic"] or 0) != 0
        placebo_clean = (entry["placebo_time_p"] or 1) > fdr_q and (entry["placebo_reversed_p"] or 1) > fdr_q
        entry["verdict"] = ("SUPPORTED" if ok and stable and placebo_clean and (entry["mean_ic"] or 0) > 0
                            else "UNPROVEN")  # fmt: skip
    return {
        "status": "OK",
        "test_days": len(results),
        "first_day": results[0].session_date,
        "last_day": results[-1].session_date,
        "hypotheses_tested": len(hypotheses),
        "fdr_q": fdr_q,
        "fdr_survivors": survivors,
        "signals": table,
        "notes": [
            "Target: forward residual return after removing lag-0 exposure to factors realised over the horizon.",
            "Cost: Roll's spread estimate from 1m closes (no historical quotes yet); net = gross - round-trip spread.",
            "Mid-price comparison needs quote history: it becomes available once the microstructure archive has "
            "enough sessions.",
        ],
    }


def _round(x) -> float | None:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return round(x, 6) if np.isfinite(x) else None
