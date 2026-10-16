"""Causal states, events and forward outcomes on 5-minute bars.

Every state/event at bar t uses only bars <= t of the same session plus earlier sessions.
Outcomes start at the OPEN of bar t+1 (the first price available after the decision) and never
look past the 15:10 bar's close (15:15). Rolling baselines use the 20 PREVIOUS sessions only.
"""

# ruff: noqa: E741  (O/H/L/C/V are the conventional OHLCV names)

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

from .data import BARS, Instrument

TRADE_LAST = 69  # last decision bar -> entry at bar 70 (15:05)
EXIT_BAR = 71  # 15:10-15:15; its close is the latest exit
HORIZONS = (3, 6, 12, 0)  # bars; 0 = hold to 15:15
HNAMES = ("15m", "30m", "60m", "EOD")
WARM = 20
TOD_NAMES = ("09:15-09:45", "09:45-11:00", "11:00-13:00", "13:00-14:30", "14:30-15:05")
TOD = np.array([0 if t < 6 else 1 if t < 21 else 2 if t < 45 else 3 if t < 63 else 4 for t in range(BARS)])
CLASS_NAMES = ("bigDn", "dn", "flat", "up", "bigUp")
GAP_NAMES = ("gap<=-1%", "gap-1..-0.3%", "flat-open", "gap+0.3..1%", "gap>=+1%")
DOW_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def lag(X: np.ndarray, k: int, fill=np.nan) -> np.ndarray:
    """X shifted k bars later within each session (bar t sees bar t-k)."""
    out = np.full_like(X, fill, dtype=X.dtype if fill is not np.nan else float)
    if k < X.shape[1]:
        out[:, k:] = X[:, : X.shape[1] - k]
    return out


def prior_window(X: np.ndarray, w: int, fn) -> np.ndarray:
    """fn over the previous w bars (t-w .. t-1) within the session; NaN for t < w."""
    out = np.full(X.shape, np.nan)
    win = sliding_window_view(X, w, axis=1)  # window j covers bars j .. j+w-1
    out[:, w:] = fn(win, axis=-1)[:, : X.shape[1] - w]
    return out


def trailing_window(X: np.ndarray, w: int, fn) -> np.ndarray:
    """fn over bars t-w+1 .. t (includes t); NaN for t < w-1."""
    out = np.full(X.shape, np.nan)
    out[:, w - 1 :] = fn(sliding_window_view(X, w, axis=1), axis=-1)
    return out


def prior_days_median(X: np.ndarray, w: int = WARM) -> np.ndarray:
    """Per bar slot: median of the same slot over the w PREVIOUS sessions."""
    out = np.full(X.shape, np.nan)
    for d in range(w, X.shape[0]):
        out[d] = np.nanmedian(X[d - w : d], axis=0)
    return out


def ema_flat(C: np.ndarray, n: int) -> np.ndarray:
    x = C.ravel()
    a = 2.0 / (n + 1)
    out = np.empty_like(x)
    e = x[0]
    for i, v in enumerate(x):
        e = e + a * (v - e)
        out[i] = e
    return out.reshape(C.shape)


def align(src_dates: np.ndarray, dst_dates: np.ndarray, arr: np.ndarray, axis: int = 0) -> np.ndarray:
    """Rows of arr (indexed by src_dates along `axis`) placed at dst_dates; NaN where missing."""
    pos = np.clip(np.searchsorted(src_dates, dst_dates), 0, max(len(src_dates) - 1, 0))
    ok = (src_dates[pos] == dst_dates) if len(src_dates) else np.zeros(len(dst_dates), bool)
    taken = np.take(arr, pos, axis=axis).astype(float)
    shape = [1] * arr.ndim
    shape[axis] = len(dst_dates)
    return np.where(ok.reshape(shape), taken, np.nan)


@dataclass
class Base:
    """Per-instrument arrays that do not need any other instrument."""

    inst: Instrument
    has_volume: bool
    pc: np.ndarray  # [D] previous session close
    rng20: np.ndarray  # [D] typical 5-min range (fraction), previous 20 sessions
    dso: np.ndarray  # [D,T] day-so-far return incl. gap
    mom6: np.ndarray  # [D,T] 6-bar momentum in rng20 units
    F: np.ndarray  # [4,D,T] forward return (long) from O[t+1]
    MFE: np.ndarray
    MAE: np.ndarray
    dret: np.ndarray  # [D] close-to-close daily return
    extra: dict = field(default_factory=dict)


def compute_base(inst: Instrument) -> Base:
    O, H, L, C, V = inst.O, inst.H, inst.L, inst.C, inst.V
    D, T = C.shape
    pc = np.r_[np.nan, C[:-1, -1]]
    rng = (H - L) / C
    dm = np.median(rng, axis=1)
    rng20 = np.full(D, np.nan)
    for d in range(WARM, D):
        rng20[d] = np.median(dm[d - WARM : d])
    rng20 = np.where(rng20 > 0, rng20, np.nan)
    dso = C / pc[:, None] - 1
    c6 = lag(C, 6)
    mom = np.where(np.isnan(c6), C / O[:, :1] - 1, C / c6 - 1) / rng20[:, None]
    entry = np.full((D, T), np.nan)
    entry[:, : TRADE_LAST + 1] = O[:, 1 : TRADE_LAST + 2]
    F = np.full((4, D, T), np.nan)
    MFE = np.full((4, D, T), np.nan)
    MAE = np.full((4, D, T), np.nan)
    tt = np.arange(T)
    Hc, Lc = H[:, : EXIT_BAR + 1], L[:, : EXIT_BAR + 1]
    suf_h = np.maximum.accumulate(Hc[:, ::-1], axis=1)[:, ::-1]
    suf_l = np.minimum.accumulate(Lc[:, ::-1], axis=1)[:, ::-1]
    for hi, h in enumerate(HORIZONS):
        if h:
            ex = np.minimum(tt + h, EXIT_BAR)
            hi_run = np.full((D, T), -np.inf)
            lo_run = np.full((D, T), np.inf)
            for k in range(1, h + 1):
                idx = np.minimum(tt + k, EXIT_BAR)
                hi_run = np.maximum(hi_run, H[:, idx])
                lo_run = np.minimum(lo_run, L[:, idx])
        else:
            ex = np.full(T, EXIT_BAR)
            nxt = np.minimum(tt + 1, EXIT_BAR)
            hi_run, lo_run = suf_h[:, nxt], suf_l[:, nxt]
        F[hi] = C[:, ex] / entry - 1
        MFE[hi] = hi_run / entry - 1
        MAE[hi] = lo_run / entry - 1
    dret = C[:, -1] / pc - 1
    b = Base(inst, bool(np.nansum(V) > 0), pc, rng20, dso, mom, F, MFE, MAE, dret)
    return b


@dataclass
class Context:
    """Market/sector context aligned to one instrument's sessions."""

    beta: np.ndarray  # [D] rolling 60-session beta to NIFTY (previous sessions only)
    idx_dso: np.ndarray  # [D,T]
    idx_mom6: np.ndarray  # [D,T]
    idx_F: np.ndarray  # [4,D,T]
    sec_dso: np.ndarray  # [D,T]
    sec_F: np.ndarray  # [4,D,T]
    regime: np.ndarray  # [D] 0..3 = (NIFTY 20d trend up/down) x (NIFTY 20d vol low/high), causal; -1 unknown
    sec_peers: int = 0


def rolling_beta(r: np.ndarray, m: np.ndarray, w: int = 60, min_n: int = 20) -> np.ndarray:
    out = np.full(len(r), np.nan)
    for d in range(1, len(r)):
        a, b = r[max(0, d - w) : d], m[max(0, d - w) : d]
        ok = ~(np.isnan(a) | np.isnan(b))
        if ok.sum() >= min_n:
            vb = np.var(b[ok])
            if vb > 0:
                out[d] = np.mean((a[ok] - a[ok].mean()) * (b[ok] - b[ok].mean())) / vb
    return out


def nifty_regime(nb: Base) -> np.ndarray:
    """Causal regime label per NIFTY session: trend (prior 20d return) x volatility (prior 20d vs expanding median)."""
    close = nb.inst.C[:, -1]
    r = nb.dret
    D = len(close)
    lab = np.full(D, -1)
    vols = []
    for d in range(21, D):
        tr = close[d - 1] / close[d - 21] - 1
        rv = np.nanstd(r[d - 20 : d])
        vols.append(rv)
        hi = rv > np.median(vols)
        lab[d] = (0 if tr > 0 else 1) * 2 + (1 if hi else 0)
    return lab


REGIME_NAMES = ("bull-calm", "bull-volatile", "bear-calm", "bear-volatile")


def bucket3(x: np.ndarray, lo: float, hi: float) -> np.ndarray:
    return np.where(np.isnan(x), -1, np.where(x < lo, 0, np.where(x > hi, 2, 1)))


def states_events(b: Base, ctx: Context | None, is_index_ref: bool) -> tuple[dict, dict, dict]:
    """(conditions, events, discrete) — all [D,T] boolean masks or int arrays, causal."""
    inst = b.inst
    O, H, L, C, V = inst.O, inst.H, inst.L, inst.C, inst.V
    D, T = C.shape
    r20 = b.rng20[:, None]
    e9, e20 = ema_flat(C, 9), ema_flat(C, 20)
    e20_l3 = np.r_[np.full(3, np.nan), e20.ravel()[:-3]].reshape(D, T)
    up = (e9 > e20) & (e20 > e20_l3)
    dn = (e9 < e20) & (e20 < e20_l3)
    rngb = H - L
    body = (C - O) / O / r20
    cls = np.where(np.isnan(body), -1, np.digitize(body, (-1.5, -0.5, 0.5, 1.5)))
    upper = np.where(rngb > 0, (H - np.maximum(O, C)) / np.where(rngb > 0, rngb, 1), 0)
    lower = np.where(rngb > 0, (np.minimum(O, C) - L) / np.where(rngb > 0, rngb, 1), 0)
    tod = np.broadcast_to(TOD, (D, T))

    cond: dict[str, np.ndarray] = {"all": np.ones((D, T), bool)}
    for k, nm in enumerate(TOD_NAMES):
        cond[f"tod={nm}"] = tod == k
    cond["trend=up"], cond["trend=down"], cond["trend=mixed"] = up, dn, ~(up | dn)
    ev: dict[str, np.ndarray] = {}
    vwap = rvol = None
    if b.has_volume:
        tp = (H + L + C) / 3
        cv = np.cumsum(V, axis=1)
        vwap = np.where(cv > 0, np.cumsum(tp * V, axis=1) / np.where(cv > 0, cv, 1), np.nan)
        cond["vwap=above"], cond["vwap=below"] = vwap < C, vwap > C
        rvol = V / prior_days_median(V)
        cond["rvol=low"], cond["rvol=normal"], cond["rvol=high"] = rvol < 0.7, (rvol >= 0.7) & (rvol < 2), rvol >= 2
    rs_day = (np.maximum.accumulate(H, axis=1) - np.minimum.accumulate(L, axis=1)) / O[:, :1]
    vr = rs_day / prior_days_median(rs_day)
    cond["dayvol=compressed"], cond["dayvol=normal"], cond["dayvol=expanded"] = (
        vr < 0.7,
        (vr >= 0.7) & (vr <= 1.5),
        vr > 1.5,
    )
    if ctx is not None and not is_index_ref:
        im = ctx.idx_mom6
        cond["nifty=up"], cond["nifty=flat"], cond["nifty=down"] = im > 1, (im >= -1) & (im <= 1), im < -1
        z = (b.dso - ctx.beta[:, None] * ctx.idx_dso) / r20
        cond["vsNifty=strong"], cond["vsNifty=inline"], cond["vsNifty=weak"] = z > 2, (z >= -2) & (z <= 2), z < -2
        if ctx.sec_peers >= 2:
            zs = (b.dso - ctx.sec_dso) / r20
            cond["vsSector=strong"], cond["vsSector=inline"], cond["vsSector=weak"] = (
                zs > 2,
                (zs >= -2) & (zs <= 2),
                zs < -2,
            )

    # ---------------------------------------------------------------- events (bar t, causal)
    hh12, ll12 = prior_window(H, 12, np.max), prior_window(L, 12, np.min)
    ev["breakout_up"], ev["breakout_dn"] = hh12 < C, ll12 > C
    for side, brk, lvl, cmp in (("up", hh12 < C, hh12, np.less), ("dn", ll12 > C, ll12, np.greater)):
        failed = np.zeros((D, T), bool)
        for k in (1, 2, 3):
            failed |= lag(brk, k, False) & cmp(C, lag(lvl, k))
        ev[f"failed_breakout_{side}"] = failed
    ev["sweep_low_reclaim"] = (ll12 > L) & (ll12 < C)
    ev["sweep_high_reject"] = (hh12 < H) & (hh12 > C)
    ev["ema20_pullback_up"] = up & (e20 >= L) & (e20 < C)
    ev["ema20_pullback_dn"] = dn & (e20 <= H) & (e20 > C)
    dayhi, daylo = np.maximum.accumulate(H, axis=1), np.minimum.accumulate(L, axis=1)
    up6, dn6 = lag(up, 6, False), lag(dn, 6, False)
    ev["deep_pullback_up"] = up6 & ((dayhi - L) / C / r20 >= 3) & (C > O)
    ev["deep_pullback_dn"] = dn6 & ((H - daylo) / C / r20 >= 3) & (C < O)
    ev["trend_fail_up"] = lag(up, 3, False) & (e9 < e20) & (e20 > C)
    ev["trend_fail_dn"] = lag(dn, 3, False) & (e9 > e20) & (e20 < C)
    r6 = trailing_window(H, 6, np.max) - trailing_window(L, 6, np.min)
    comp = r6 / prior_days_median(r6)
    comp_prev = lag(comp, 1)
    hh6, ll6 = prior_window(H, 6, np.max), prior_window(L, 6, np.min)
    ev["squeeze_break_up"] = (comp_prev < 0.6) & (hh6 < C)
    ev["squeeze_break_dn"] = (comp_prev < 0.6) & (ll6 > C)
    big_mom_up, big_mom_dn = b.mom6 >= 4, b.mom6 <= -4
    if rvol is not None:
        big_mom_up &= rvol >= 2
        big_mom_dn &= rvol >= 2
        ev["rvol_thrust_up"] = (rvol >= 2.5) & (body >= 1.5)
        ev["rvol_thrust_dn"] = (rvol >= 2.5) & (body <= -1.5)
        below3 = lag(vwap > C, 1, False) & lag(vwap > C, 2, False) & lag(vwap > C, 3, False)
        above3 = lag(vwap < C, 1, False) & lag(vwap < C, 2, False) & lag(vwap < C, 3, False)
        ev["vwap_reclaim_up"] = below3 & (vwap < C)
        ev["vwap_loss_dn"] = above3 & (vwap > C)
    ev["exhaustion_up"] = big_mom_up & (upper >= 0.5)
    ev["exhaustion_dn"] = big_mom_dn & (lower >= 0.5)
    orh, orl = H[:, :3].max(1)[:, None], L[:, :3].min(1)[:, None]
    t_ge3 = np.arange(T)[None, :] >= 3
    ev["or_break_up"] = t_ge3 & (orh < C)
    ev["or_break_dn"] = t_ge3 & (orl > C)
    inside = (orl <= C) & (orh >= C)
    trap_up = np.zeros((D, T), bool)
    trap_dn = np.zeros((D, T), bool)
    for k in (1, 2, 3):
        trap_up |= lag(ev["or_break_up"], k, False) & inside
        trap_dn |= lag(ev["or_break_dn"], k, False) & inside
    ev["or_trap_up"], ev["or_trap_dn"] = trap_up, trap_dn
    pdh, pdl = np.r_[np.nan, H[:-1].max(1)][:, None], np.r_[np.nan, L[:-1].min(1)][:, None]
    prevC = np.concatenate([C[:, :1] * np.nan, C[:, :-1]], axis=1)
    ev["pdh_break"] = (pdh < C) & ~(prevC > pdh)
    ev["pdl_break"] = (pdl > C) & ~(prevC < pdl)
    ev["pdh_reject"] = (pdh < H) & (pdh > C)
    ev["pdl_reclaim"] = (pdl > L) & (pdl < C)

    disc = {"cls": cls, "tod": tod, "trend": np.where(up, 2, np.where(dn, 0, 1)), "mom": bucket3(b.mom6, -1.5, 1.5)}
    if b.has_volume:
        disc["vwap"] = np.where(np.isnan(vwap), -1, (vwap < C).astype(int))
        disc["rvol"] = np.where(np.isnan(rvol), -1, (rvol >= 1.5).astype(int))
    gap = (O[:, 0] / b.pc - 1) * 100
    disc["gap"] = np.where(np.isnan(gap), -1, np.digitize(gap, (-1.0, -0.3, 0.3, 1.0)))
    return cond, ev, disc
