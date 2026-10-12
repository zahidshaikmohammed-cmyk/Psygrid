"""Inference helpers: week-clustered statistics, BH FDR, moving-block bootstrap, drawdown."""

from __future__ import annotations

import math

import numpy as np

_erfc = np.vectorize(math.erfc, otypes=[float])


def p_upper(t) -> np.ndarray:
    """One-sided P(Z >= t) under a normal approximation."""
    return 0.5 * _erfc(np.asarray(t, float) / math.sqrt(2))


def cluster_summary(x: np.ndarray, g: np.ndarray) -> dict:
    """Column-wise stats of x [n, k] with standard errors clustered on g (e.g. calendar week).

    Clustering makes trades in the same week one block, so serial dependence inside a week and the
    shared market move of a week do not inflate t. Returns arrays of length k.
    """
    x = np.asarray(x, float)
    if x.ndim == 1:
        x = x[:, None]
    n, k = x.shape
    out = {"n": n, "weeks": 0}
    if n == 0:
        nan = np.full(k, np.nan)
        out.update(mean=nan, median=nan, std=nan, win=nan, se=nan, t=nan)
        return out
    _, gi = np.unique(g, return_inverse=True)
    G = int(gi.max()) + 1
    m = x.mean(0)
    out.update(weeks=G, mean=m, median=np.median(x, 0), win=(x > 0).mean(0) * 100)
    out["std"] = x.std(0, ddof=1) if n > 1 else np.full(k, np.nan)
    if G < 2:
        out["se"] = np.full(k, np.nan)
        out["t"] = np.full(k, np.nan)
        return out
    S = np.zeros((G, k))
    np.add.at(S, gi, x - m)
    se = np.sqrt((S**2).sum(0) * G / (G - 1)) / n
    out["se"] = se
    with np.errstate(divide="ignore", invalid="ignore"):
        out["t"] = np.where(se > 0, m / se, 0.0)
    return out


def bh(p: np.ndarray, q: float) -> np.ndarray:
    """Benjamini-Hochberg: boolean mask of discoveries at FDR q (NaN p never passes)."""
    p = np.asarray(p, float)
    ok = ~np.isnan(p)
    res = np.zeros(p.shape, bool)
    if not ok.any():
        return res
    pv = p[ok]
    order = np.argsort(pv)
    m = len(pv)
    thr = q * (np.arange(1, m + 1) / m)
    passed = pv[order] <= thr
    if passed.any():
        kmax = np.max(np.nonzero(passed)[0])
        sel = np.zeros(m, bool)
        sel[order[: kmax + 1]] = True
        res[np.nonzero(ok)[0]] = sel
    return res


def qvalues(p: np.ndarray) -> np.ndarray:
    p = np.asarray(p, float)
    q = np.full(p.shape, np.nan)
    ok = ~np.isnan(p)
    pv = p[ok]
    if not len(pv):
        return q
    order = np.argsort(pv)
    m = len(pv)
    ranked = pv[order] * m / np.arange(1, m + 1)
    ranked = np.minimum.accumulate(ranked[::-1])[::-1]
    out = np.empty(m)
    out[order] = np.minimum(ranked, 1.0)
    q[ok] = out
    return q


def block_bootstrap_ci(
    x: np.ndarray, weeks: np.ndarray, level: float = 0.90, block: int = 4, reps: int = 2000, seed: int = 7
):
    """CI of the per-trade mean by resampling consecutive blocks of calendar weeks (moving-block bootstrap).

    Weeks without trades are kept as empty weeks so that time structure is preserved.
    """
    x = np.asarray(x, float)
    if len(x) < 5:
        return (np.nan, np.nan)
    w0, w1 = int(weeks.min()), int(weeks.max())
    W = w1 - w0 + 1
    S = np.bincount(weeks - w0, weights=x, minlength=W)
    N = np.bincount(weeks - w0, minlength=W).astype(float)
    b = min(block, W)
    nb = math.ceil(W / b)
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, W - b + 1, size=(reps, nb))
    idx = (starts[:, :, None] + np.arange(b)[None, None, :]).reshape(reps, -1)[:, :W]
    s, n = S[idx].sum(1), N[idx].sum(1)
    with np.errstate(invalid="ignore", divide="ignore"):
        means = np.where(n > 0, s / n, np.nan)
    a = (1 - level) / 2
    return (float(np.nanquantile(means, a)), float(np.nanquantile(means, 1 - a)))


def max_drawdown(x: np.ndarray) -> float:
    """Largest peak-to-trough fall of the cumulative sum of per-trade results (same units as x)."""
    if len(x) == 0:
        return 0.0
    c = np.cumsum(x)
    peak = np.maximum.accumulate(np.r_[0.0, c])[1:]
    return float(np.max(peak - c))
