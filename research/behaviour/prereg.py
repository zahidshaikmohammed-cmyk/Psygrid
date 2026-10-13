"""PRE-REGISTERED mechanistic hypotheses H1-H6 (FROZEN before the confirmation data is fetched).

Do not edit after the confirmation fetch. prereg.lock records the SHA-256 of this file, execution.py,
integrity.py, confirm.py and power.py; tests/test_behaviour_prereg.py fails if they change. Any
change before the fetch must be a separate commit that updates the lock and explains why.
Rationale, power and decision rules: docs/research/behaviour/PREREGISTRATION.md.

All rules use 5-minute decision bars built from 1-minute candles (bar t = 09:15 + 5t, t = 0..74)
and only information available at the close of bar t. Universe: equities only (indices are
context). One trade per hypothesis per instrument per session: the first qualifying bar.
No parameter in this file is fitted to data.
"""

# ruff: noqa: E741  (O/H/L/C/V are the conventional OHLCV names)

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from .execution import Signal

WARM = 20  # sessions used by rolling baselines before any signal
DRIFT_WINDOW = 60  # H5 trailing sessions
FIRST_BAR, LAST_BAR = 6, 57  # decision bars 09:45 .. 14:00 (bar start times) unless stated
STOP_PAD = 0.10  # stops sit 0.1 x typical 5-minute range beyond the structural level

HYPOTHESES = {
    "H1": {
        "name": "Liquidity sweep, reclaim and failed continuation",
        "rationale": "Stops beyond an obvious 1-hour extreme are run; if the sweep is reclaimed and the next bar cannot extend it, trapped breakout traders unwind.",
        "long": "bar t-1 trades below the lowest low of the 12 bars before it and closes back above that low; bar t does not make a lower low and closes above bar t-1's close",
        "short": "mirror image at the 12-bar high",
        "bars": "t in 7..57 (signal bar t-1 >= 6)",
        "stop": "beyond the sweep extreme by 0.1 x typical 5-minute range",
        "target_r": 1.5,
        "hold_min": 60,
    },
    "H2": {
        "name": "Trend-day pullback with relative-volume confirmation",
        "rationale": "On a genuine trend day, a light-volume 30-70% retracement followed by a volume-confirmed resumption continues the institutional flow that created the trend.",
        "long": "session high so far is >= 4 typical 5-minute ranges above the open; EMA9 > EMA20 and EMA20 rising over 3 bars; low since the session high retraced 30-70% of (high - open); mean relative volume of the pullback bars < 1.0; bar t closes back above EMA9 (previous close at or below EMA9) with relative volume >= 1.2",
        "short": "mirror image",
        "bars": "t in 6..51 (until 13:30)",
        "stop": "beyond the pullback extreme by 0.1 x typical 5-minute range",
        "target_r": 2.0,
        "hold_min": None,
    },
    "H3": {
        "name": "Failed breakout while weak against the sector",
        "rationale": "A breakout by a stock that is lagging its own sector lacks sponsorship; when it fails within 15 minutes the breakout buyers become supply.",
        "long": "mirror: a breakdown below the 12-bar low that closes back above it within 3 bars while the stock is >= 1 typical 5-minute range STRONGER than its sector peers (day-so-far)",
        "short": "close above the highest high of the prior 12 bars at bar j, then within 3 bars a close back below that level, while the stock's day-so-far return is >= 1 typical 5-minute range below the equal-weight day-so-far return of its sector peers (>= 2 peers)",
        "bars": "breakout bar j in 12..57, signal bar t in j+1..j+3",
        "stop": "beyond the most extreme price since the breakout bar by 0.1 x typical 5-minute range",
        "target_r": 1.5,
        "hold_min": 60,
    },
    "H4": {
        "name": "VWAP rejection in the direction of NIFTY on a normal-or-active day",
        "rationale": "When the index trends, a stock below VWAP that tests VWAP and is rejected confirms sellers defending value; low-volatility days are excluded because VWAP tests are noise there.",
        "short": "NIFTY day-so-far < 0 and NIFTY 30-minute momentum < 0; the stock's day range so far >= 0.7 x its usual range at that time; the 3 previous closes below VWAP; bar t high >= VWAP, close < VWAP and close < open",
        "long": "mirror image with NIFTY up and the stock above VWAP",
        "bars": "t in 6..57",
        "stop": "beyond bar t's extreme by 0.1 x typical 5-minute range",
        "target_r": 1.5,
        "hold_min": 60,
    },
    "H5": {
        "name": "Multi-month drift continuation (the LICI / ANANDRATHI effect)",
        "rationale": "Persistent institutional accumulation/distribution leaks into intraday drift; the earlier search's best gross results were stock-level trends, not intraday patterns.",
        "long": "previous 60 sessions' cumulative return in excess of beta x NIFTY (beta and dispersion from the same 60 sessions, corporate-action days removed) has z >= 1.5",
        "short": "z <= -1.5",
        "bars": "signal at the close of bar 2 (09:30), entry 09:31",
        "stop": "none",
        "target_r": None,
        "hold_min": None,
        "note": "Survivorship-biased in a backward confirmation year (the universe is today's most-traded stocks); its best possible verdict there is INCONCLUSIVE. Only forward data can confirm it.",
    },
    "H6": {
        "name": "Volatility compression followed by a volume-confirmed expansion break",
        "rationale": "A quiet session where the last 30 minutes are unusually tight stores orders on both sides; a break with relative volume >= 1.5 releases them in its direction (generalises the BSE behaviour to all stocks).",
        "long": "at bar t-1 the 6-bar range (t-6..t-1) is < 0.6 x its median at that slot over the previous 20 sessions and the day range so far is < 0.8 x usual; bar t closes above the 6-bar high with relative volume >= 1.5",
        "short": "mirror image below the 6-bar low",
        "bars": "t in 9..57",
        "stop": "beyond the opposite side of the 6-bar box by 0.1 x typical 5-minute range",
        "target_r": 2.0,
        "hold_min": 60,
    },
}


# ---------------------------------------------------------------- frozen helpers (copies, so the lock covers them)


def _lag(X, k, fill):
    out = np.full(X.shape, fill, dtype=X.dtype if not isinstance(fill, float) else float)
    if k < X.shape[1]:
        out[:, k:] = X[:, : X.shape[1] - k]
    return out


def _prior(X, w, fn):
    out = np.full(X.shape, np.nan)
    out[:, w:] = fn(sliding_window_view(X, w, axis=1), axis=-1)[:, : X.shape[1] - w]
    return out


def _trailing(X, w, fn):
    out = np.full(X.shape, np.nan)
    out[:, w - 1 :] = fn(sliding_window_view(X, w, axis=1), axis=-1)
    return out


def _prior_days_median(X, w=WARM):
    out = np.full(X.shape, np.nan)
    for d in range(w, X.shape[0]):
        out[d] = np.nanmedian(X[d - w : d], axis=0)
    return out


def _ema(C, n):
    x = C.ravel()
    a = 2.0 / (n + 1)
    out = np.empty_like(x)
    e = x[0]
    for i, v in enumerate(x):
        e += a * (v - e)
        out[i] = e
    return out.reshape(C.shape)


@dataclass
class Ctx:
    O: np.ndarray
    H: np.ndarray
    L: np.ndarray
    C: np.ndarray
    V: np.ndarray
    rng: np.ndarray  # [D] typical 5-minute range (fraction), previous 20 sessions
    ema9: np.ndarray
    ema20: np.ndarray
    vwap: np.ndarray
    rvol: np.ndarray
    dso: np.ndarray  # day-so-far return vs previous close
    sec_dso: np.ndarray  # equal-weight sector peers' dso, NaN if < 2 peers
    nifty_dso: np.ndarray
    nifty_mom6: np.ndarray
    dayvol: np.ndarray  # day range so far / its usual value at that slot
    drift_z: np.ndarray  # [D] H5 statistic known before the session
    allowed: np.ndarray  # [D] warm-up done, inside the evaluation window, not near a corporate action


def build_ctx(O, H, L, C, V, prev_close, sec_dso, nifty_dso, nifty_mom6, daily_excess, allowed) -> Ctx:
    """daily_excess[D]: stock daily return minus beta x NIFTY (NaN on corporate-action days)."""
    D, _ = C.shape
    rng5 = (H - L) / C
    dm = np.median(rng5, axis=1)
    rng = np.full(D, np.nan)
    for d in range(WARM, D):
        rng[d] = np.median(dm[d - WARM : d])
    e9, e20 = _ema(C, 9), _ema(C, 20)
    tp = (H + L + C) / 3
    cv = np.cumsum(V, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        vwap = np.where(cv > 0, np.cumsum(tp * V, axis=1) / np.where(cv > 0, cv, 1), np.nan)
        rvol = V / _prior_days_median(V)
        rs = (np.maximum.accumulate(H, axis=1) - np.minimum.accumulate(L, axis=1)) / O[:, :1]
        dayvol = rs / _prior_days_median(rs)
        dso = C / prev_close[:, None] - 1
    z = np.full(D, np.nan)
    for d in range(DRIFT_WINDOW, D):
        w = daily_excess[d - DRIFT_WINDOW : d]
        w = w[np.isfinite(w)]
        if len(w) >= 40 and np.std(w) > 0:
            z[d] = w.sum() / (np.std(w, ddof=1) * np.sqrt(len(w)))
    ok = allowed & (np.arange(D) >= WARM) & np.isfinite(rng)
    return Ctx(O, H, L, C, V, rng, e9, e20, vwap, rvol, dso, sec_dso, nifty_dso, nifty_mom6, dayvol, z, ok)


def _first(mask: np.ndarray, x: Ctx, lo: int, hi: int):
    m = mask.copy()
    m[:, :lo] = False
    m[:, hi + 1 :] = False
    m &= x.allowed[:, None]
    has = m.any(1)
    return [(int(d), int(m[d].argmax())) for d in np.nonzero(has)[0]]


def _pad(x: Ctx, d: int) -> float:
    return STOP_PAD * x.rng[d] * x.C[d, 0]


def h1(x: Ctx) -> list[Signal]:
    ll12, hh12 = _prior(x.L, 12, np.min), _prior(x.H, 12, np.max)
    sweep_lo = (ll12 > x.L) & (ll12 < x.C)
    sweep_hi = (hh12 < x.H) & (hh12 > x.C)
    sl1, sh1 = _lag(sweep_lo, 1, False), _lag(sweep_hi, 1, False)
    L1, H1, C1 = _lag(x.L, 1, np.nan), _lag(x.H, 1, np.nan), _lag(x.C, 1, np.nan)
    long_m = sl1 & (x.L >= L1) & (x.C > C1)
    short_m = sh1 & (x.H <= H1) & (x.C < C1)
    out = []
    for side, m, ext in ((1, long_m, L1), (-1, short_m, H1)):
        for d, t in _first(m, x, FIRST_BAR + 1, LAST_BAR):
            out.append(Signal(d, t, side, ext[d, t] - side * _pad(x, d), 1.5, 60))
    return out


def h2(x: Ctx) -> list[Signal]:
    out = []
    up = (x.ema9 > x.ema20) & (x.ema20 > np.r_[np.full(3, np.nan), x.ema20.ravel()[:-3]].reshape(x.C.shape))
    dn = (x.ema9 < x.ema20) & (x.ema20 < np.r_[np.full(3, np.nan), x.ema20.ravel()[:-3]].reshape(x.C.shape))
    for d in np.nonzero(x.allowed)[0]:
        O0, r = x.O[d, 0], x.rng[d]
        done = {1: False, -1: False}
        for t in range(FIRST_BAR, 52):
            for side in (1, -1):
                if done[side]:
                    continue
                Hs, Ls = x.H[d, : t + 1], x.L[d, : t + 1]
                k = int(np.argmax(Hs)) if side > 0 else int(np.argmin(Ls))
                ext = Hs[k] if side > 0 else Ls[k]
                move = side * (ext - O0)
                if move < 4 * r * O0 or k >= t - 1:
                    continue
                if not (up if side > 0 else dn)[d, t]:
                    continue
                pb = Ls[k + 1 : t].min() if side > 0 else Hs[k + 1 : t].max()
                depth = side * (ext - pb) / move
                if not 0.3 <= depth <= 0.7:
                    continue
                rv_pb = np.nanmean(x.rvol[d, k + 1 : t])
                cross = side * (x.C[d, t] - x.ema9[d, t]) > 0 and side * (x.C[d, t - 1] - x.ema9[d, t - 1]) <= 0
                if cross and rv_pb < 1.0 and x.rvol[d, t] >= 1.2:
                    out.append(Signal(int(d), t, side, pb - side * _pad(x, d), 2.0, None))
                    done[side] = True
    return out


def h3(x: Ctx) -> list[Signal]:
    hh12, ll12 = _prior(x.H, 12, np.max), _prior(x.L, 12, np.min)
    rel = (x.dso - x.sec_dso) / x.rng[:, None]
    out = []
    for side in (-1, 1):  # -1: failed up-breakout while weak; +1: failed breakdown while strong
        brk = (hh12 < x.C) if side < 0 else (ll12 > x.C)
        lvl = hh12 if side < 0 else ll12
        m = np.zeros(x.C.shape, bool)
        for k in (1, 2, 3):
            b = _lag(brk, k, False)
            b[:, : 12 + k] = False
            back = (_lag(lvl, k, np.nan) > x.C) if side < 0 else (_lag(lvl, k, np.nan) < x.C)
            m |= b & back
        m &= (rel <= -1.0) if side < 0 else (rel >= 1.0)
        for d, t in _first(m, x, 13, LAST_BAR):
            j0 = max(0, t - 3)
            ext = x.H[d, j0 : t + 1].max() if side < 0 else x.L[d, j0 : t + 1].min()
            out.append(Signal(d, t, side, ext - side * _pad(x, d), 1.5, 60))
    return out


def h4(x: Ctx) -> list[Signal]:
    below3 = _lag(x.vwap > x.C, 1, False) & _lag(x.vwap > x.C, 2, False) & _lag(x.vwap > x.C, 3, False)
    above3 = _lag(x.vwap < x.C, 1, False) & _lag(x.vwap < x.C, 2, False) & _lag(x.vwap < x.C, 3, False)
    active = x.dayvol >= 0.7
    short_m = (x.nifty_dso < 0) & (x.nifty_mom6 < 0) & active & below3 & (x.vwap <= x.H) & (x.vwap > x.C) & (x.C < x.O)
    long_m = (x.nifty_dso > 0) & (x.nifty_mom6 > 0) & active & above3 & (x.vwap >= x.L) & (x.vwap < x.C) & (x.C > x.O)
    out = []
    for side, m, ext in ((-1, short_m, x.H), (1, long_m, x.L)):
        for d, t in _first(m, x, FIRST_BAR, LAST_BAR):
            out.append(Signal(d, t, side, ext[d, t] - side * _pad(x, d), 1.5, 60))
    return out


def h5(x: Ctx) -> list[Signal]:
    out = []
    for d in np.nonzero(x.allowed & np.isfinite(x.drift_z))[0]:
        if x.drift_z[d] >= 1.5:
            out.append(Signal(int(d), 2, 1, None, None, None))
        elif x.drift_z[d] <= -1.5:
            out.append(Signal(int(d), 2, -1, None, None, None))
    return out


def h6(x: Ctx) -> list[Signal]:
    r6 = _trailing(x.H, 6, np.max) - _trailing(x.L, 6, np.min)
    comp = r6 / _prior_days_median(r6)
    c1, dv1 = _lag(comp, 1, np.nan), _lag(x.dayvol, 1, np.nan)
    hh6, ll6 = _prior(x.H, 6, np.max), _prior(x.L, 6, np.min)
    quiet = (c1 < 0.6) & (dv1 < 0.8) & (x.rvol >= 1.5)
    out = []
    for side, m, opp in ((1, quiet & (hh6 < x.C), ll6), (-1, quiet & (ll6 > x.C), hh6)):
        for d, t in _first(m, x, 9, LAST_BAR):
            out.append(Signal(d, t, side, opp[d, t] - side * _pad(x, d), 2.0, 60))
    return out


SIGNALS = {"H1": h1, "H2": h2, "H3": h3, "H4": h4, "H5": h5, "H6": h6}
