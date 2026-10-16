"""Hypothesis space, chronological validation, walk-forward and per-stock behavioural fingerprints.

Periods (by session date): DISCOVERY < 2024-10-01 <= VALIDATION < 2025-10-01 <= HOLDOUT.
Selection uses DISCOVERY only; the VALIDATION gate is fixed in advance; HOLDOUT numbers are only
reported for what passed both. Walk-forward re-runs the same selection rule on rolling windows.

A hypothesis = a causal condition at bar t (an event, a state, a time slot, a candle sequence or an
opening configuration). Its "trade" is the FIRST bar per session where it is true: entry at the
next bar's open, exit after 15m / 30m / 60m or at 15:15. One trade per hypothesis per session, so
trades of one hypothesis never overlap; standard errors are clustered by calendar week.
"""

# ruff: noqa: E741  (O/H/L/C/V are the conventional OHLCV names)

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np

from .data import BARS, Instrument
from .features import (
    CLASS_NAMES,
    DOW_NAMES,
    GAP_NAMES,
    HNAMES,
    REGIME_NAMES,
    TOD,
    TOD_NAMES,
    TRADE_LAST,
    WARM,
    Base,
    Context,
    align,
    compute_base,
    lag,
    nifty_regime,
    rolling_beta,
    states_events,
)
from .stats import bh, block_bootstrap_ci, cluster_summary, max_drawdown, p_upper, qvalues


def daynum(y: int, m: int, d: int) -> int:
    return (dt.date(y, m, d) - dt.date(1970, 1, 1)).days


def datestr(n: int) -> str:
    return str(dt.date(1970, 1, 1) + dt.timedelta(days=int(n)))


D_END, V_END = daynum(2024, 10, 1), daynum(2025, 10, 1)
PERIODS = ("discovery", "validation", "holdout")
# round-trip cost in % of the underlying = fees + multiple x estimated spread (see AUDIT/FINDINGS)
COSTS = {"low": (0.03, 1.0), "base": (0.07, 1.0), "high": (0.13, 2.0)}
N_MIN_D, W_MIN_D = 40, 20
Q_STOCK, Q_GLOBAL, Q_VAL = 0.10, 0.10, 0.10
P_CAND = 1e-4
N_PRE_REC, FREQ_REC = 60, 0.25  # recurrence: >= 60 occurrences and >= 1 per 4 weeks before the holdout
WF_STARTS = [daynum(y, m, 1) for y in (2024, 2025, 2026) for m in (1, 4, 7, 10)]
WF_STARTS = [s for s in WF_STARTS if s <= daynum(2026, 7, 1)]
WF_TRAIN = 548


def slot_label(t: int) -> str:
    m = 555 + 5 * t
    return f"{m // 60:02d}:{m % 60:02d}"


@dataclass
class Prepared:
    inst: Instrument
    base: Base
    ctx: Context | None
    cond: dict
    ev: dict
    disc: dict
    ok: np.ndarray  # [D,T] tradable decision bars on usable sessions
    pid: np.ndarray  # [D] 0/1/2 period
    wk: np.ndarray
    yr: np.ndarray
    rg: np.ndarray
    dow: np.ndarray
    F: np.ndarray  # [4,D,T] %, long
    MFE: np.ndarray
    MAE: np.ndarray
    XI: np.ndarray  # excess vs beta x NIFTY, %
    XS: np.ndarray  # excess vs sector peers, %
    baseline: np.ndarray  # [3,4,5] mean F per period/horizon/time-of-day (unconditional)
    spread: float
    costs: dict


def spread_pct(inst: Instrument) -> float:
    """Round-trip spread estimate in %, from 1-minute Corwin-Schultz on discovery sessions, floored at one tick."""
    if inst.kind == "INDEX":
        return 0.01
    px = float(np.nanmedian(inst.C))
    tick = 0.05 / px * 100 if px > 0 else 0.02
    if inst.m1 is None:
        return float(min(max(tick, 0.02), 0.10))
    sel = inst.dates < D_END
    if sel.sum() < 20:
        sel = np.ones(len(inst.dates), bool)
    Hm, Lm = inst.m1[sel, :, 1], inst.m1[sel, :, 2]
    with np.errstate(divide="ignore", invalid="ignore"):
        lh = np.log(Hm / Lm) ** 2
        beta = lh[:, 1:] + lh[:, :-1]
        gamma = np.log(np.maximum(Hm[:, 1:], Hm[:, :-1]) / np.minimum(Lm[:, 1:], Lm[:, :-1])) ** 2
        k = 3 - 2 * np.sqrt(2)
        alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / k - np.sqrt(gamma / k)
        s = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    s = np.where(np.isfinite(s) & (s > 0), s, 0.0)
    cs = float(np.median(s.mean(axis=1))) * 100
    return float(min(max(cs, tick), 0.10))


def prepare_all(instruments: list[Instrument], log=print) -> dict[str, Prepared]:
    usable = [i for i in instruments if i.C is not None and len(i.dates) > WARM + 30]
    bases = {i.name: compute_base(i) for i in usable}
    nb = bases.get("NIFTY")
    if nb is None:
        raise RuntimeError("NIFTY is required as the market reference")
    regime = nifty_regime(nb)
    grid = np.unique(np.concatenate([b.inst.dates for b in bases.values()]))
    G, T = len(grid), BARS
    sectors: dict[str, list[str]] = {}
    for n, b in bases.items():
        if b.inst.kind == "EQUITY" and b.inst.sector not in ("OTHER", "INDEX", ""):
            sectors.setdefault(b.inst.sector, []).append(n)
    sec_sum = {}
    for s, names in sectors.items():
        sd, cd = np.zeros((G, T)), np.zeros((G, T))
        sf, cf = np.zeros((4, G, T)), np.zeros((4, G, T))
        for n in names:
            b = bases[n]
            pos = np.searchsorted(grid, b.inst.dates)
            sd[pos] += np.nan_to_num(b.dso)
            cd[pos] += ~np.isnan(b.dso)
            sf[:, pos] += np.nan_to_num(b.F)
            cf[:, pos] += ~np.isnan(b.F)
        sec_sum[s] = (sd, cd, sf, cf)
    out = {}
    for n, b in bases.items():
        inst = b.inst
        D = len(inst.dates)
        is_ref = n == "NIFTY"
        al = lambda arr, axis=0, inst=inst: align(nb.inst.dates, inst.dates, arr, axis)  # noqa: E731
        beta = np.ones(D) if is_ref else rolling_beta(b.dret, al(nb.dret))
        idx_F = al(nb.F, 1)
        ctx = Context(
            beta,
            al(nb.dso),
            al(nb.mom6),
            idx_F,
            np.full((D, T), np.nan),
            np.full((4, D, T), np.nan),
            al(regime).astype(float),
        )
        if any(n in names for names in sectors.values()):
            sd, cd, sf, cf = sec_sum[inst.sector]
            pos = np.searchsorted(grid, inst.dates)
            ctx.sec_peers = len(sectors[inst.sector]) - 1
            with np.errstate(invalid="ignore", divide="ignore"):
                nd = cd[pos] - ~np.isnan(b.dso)
                ctx.sec_dso = np.where(nd >= 2, (sd[pos] - np.nan_to_num(b.dso)) / np.where(nd > 0, nd, 1), np.nan)
                nf = cf[:, pos] - ~np.isnan(b.F)
                ctx.sec_F = np.where(nf >= 2, (sf[:, pos] - np.nan_to_num(b.F)) / np.where(nf > 0, nf, 1), np.nan)
        with np.errstate(invalid="ignore", divide="ignore"):
            cond, ev, disc = states_events(b, ctx, is_ref)
        dates = inst.dates
        pid = np.where(dates < D_END, 0, np.where(dates < V_END, 1, 2))
        warm = np.arange(D) >= WARM
        warm &= np.isfinite(b.rng20) & (np.isfinite(ctx.beta) | is_ref)
        ok = warm[:, None] & (np.arange(T)[None, :] <= TRADE_LAST) & np.isfinite(b.F[1])
        F = b.F * 100
        XI = np.full_like(F, np.nan) if is_ref else (b.F - ctx.beta[None, :, None] * idx_F) * 100
        XS = (b.F - ctx.sec_F) * 100
        baseline = np.full((3, 4, 5), np.nan)
        for p in range(3):
            for k in range(5):
                sel = ok & (pid[:, None] == p) & (TOD[None, :] == k)
                if sel.any():
                    baseline[p, :, k] = np.nanmean(F[:, sel], axis=1)
        sp = spread_pct(inst)
        costs = {c: f + mult * sp for c, (f, mult) in COSTS.items()}
        yr = dates.astype("datetime64[D]").astype("datetime64[Y]").astype(int) + 1970
        out[n] = Prepared(
            inst,
            b,
            ctx,
            cond,
            ev,
            disc,
            ok,
            pid,
            (dates + 3) // 7,
            yr,
            ctx.regime,
            (dates + 3) % 7,
            F,
            b.MFE * 100,
            b.MAE * 100,
            XI,
            XS,
            baseline,
            sp,
            costs,
        )
    log(f"prepared {len(out)} instruments; sectors with peers: {sum(1 for s in sectors.values() if len(s) >= 3)}")
    return out


# --------------------------------------------------------------------------- hypotheses


def hypotheses(P: Prepared):
    """Yield (family, label, mask[D,T]). Masks are causal by construction (features.py)."""
    cond, ev, disc = P.cond, P.ev, P.disc
    D, T = P.ok.shape
    for e, em in ev.items():
        for c, cm in cond.items():
            yield "EVENT", f"{e} | {c}", em & cm
    tt = np.arange(T)[None, :]
    for k in range(23):
        t = 3 * k + 2  # decision at the close of a 15-minute boundary; entry at its open
        m = np.broadcast_to(tt == t, (D, T))
        yield "SLOT", f"enter {slot_label(t + 1)} daily", m
        for w in range(5):
            yield "DOWSLOT", f"enter {slot_label(t + 1)} on {DOW_NAMES[w]}", m & (P.dow[:, None] == w)
    cls = disc["cls"]
    c1, c2 = lag(cls, 1, -1), lag(cls, 2, -1)
    code = np.where((cls >= 0) & (c1 >= 0) & (c2 >= 0), c2 * 25 + c1 * 5 + cls, -1)
    tod = disc["tod"]
    for c in range(5):
        for k, nm in enumerate(TOD_NAMES):
            yield "CANDLE", f"5m candle {CLASS_NAMES[c]} | {nm}", (cls == c) & (tod == k)
    for v in np.unique(code[code >= 0]):
        cm = code == v
        names = "-".join(CLASS_NAMES[i] for i in (v // 25, (v // 5) % 5, v % 5))
        for k, nm in enumerate(TOD_NAMES):
            yield "MOTIF", f"5m candles {names} | {nm}", cm & (tod == k)
    trend, mom = disc["trend"], disc["mom"]
    tnames, mnames = ("down", "mixed", "up"), ("weak", "neutral", "strong")
    vol_parts = [(None, None)]
    if "vwap" in disc:
        vol_parts = [(vw, rv) for vw in (0, 1) for rv in (0, 1)]
    for ti in range(3):
        for mi in range(3):
            for vw, rv in vol_parts:
                base_m = (trend == ti) & (mom == mi)
                lbl = f"trend {tnames[ti]}, 30m momentum {mnames[mi]}"
                if vw is not None:
                    base_m = base_m & (disc["vwap"] == vw) & (disc["rvol"] == rv)
                    lbl += f", {'above' if vw else 'below'} VWAP, rvol {'high' if rv else 'normal'}"
                for k, nm in enumerate(TOD_NAMES):
                    yield "STATE", f"{lbl} | {nm}", base_m & (tod == k)
    gap = disc["gap"]
    for g in range(5):
        for c in range(5):
            m = np.zeros((D, T), bool)
            m[:, 0] = (gap == g) & (cls[:, 0] == c)
            yield "OPEN", f"{GAP_NAMES[g]}, first 5m {CLASS_NAMES[c]}", m


# --------------------------------------------------------------------------- per-instrument evaluation


def _pstats(x, wk):
    return cluster_summary(x, wk)


def evaluate(P: Prepared) -> dict:
    """All statistics for one instrument. Returns rows (per hypothesis x horizon), candidates,
    walk-forward trades and the fingerprint."""
    name = P.inst.name
    cb, cl, chh = P.costs["base"], P.costs["low"], P.costs["high"]
    rows = []
    records = []  # for walk-forward: (row index base, d, F[n,4])
    pre_weeks = len(np.unique(P.wk[P.pid < 2]))
    for fam, label, mask in hypotheses(P):
        valid = mask & P.ok
        has = valid.any(1)
        if has.sum() < 30:
            continue
        d = np.nonzero(has)[0]
        t = valid[d].argmax(1)
        F = P.F[:, d, t].T
        if np.isnan(F).any():
            keep = ~np.isnan(F).any(1)
            d, t, F = d[keep], t[keep], F[keep]
        MFE, MAE = P.MFE[:, d, t].T, P.MAE[:, d, t].T
        XI, XS = P.XI[:, d, t].T, P.XS[:, d, t].T
        pid, wk, yr, rg = P.pid[d], P.wk[d], P.yr[d], P.rg[d]
        EB = F - P.baseline[pid, :, TOD[t]]  # excess over the stock's own unconditional move at that time of day
        records.append((len(rows), d, wk, F))
        mD, mV, mH, mP = pid == 0, pid == 1, pid == 2, pid < 2
        sD = _pstats(F[mD], wk[mD])
        sP = _pstats(EB[mP], wk[mP])
        side = np.where(np.nan_to_num(sD["mean"]) >= 0, 1.0, -1.0)
        sideE = np.where(np.nan_to_num(sP["mean"]) >= 0, 1.0, -1.0)
        sV = _pstats(F[mV] * side, wk[mV])
        sH = _pstats(F[mH] * side, wk[mH])
        sHE = _pstats(EB[mH], wk[mH])
        sPg = _pstats(F[mP] * sideE, wk[mP])
        for hi in range(4):
            seD = sD["se"][hi]
            net_D = side[hi] * sD["mean"][hi] - cb
            t_net = net_D / seD if seD and seD > 0 else np.nan
            p_D = min(1.0, 2 * float(p_upper(t_net))) if np.isfinite(t_net) else np.nan
            eligible = sD["n"] >= N_MIN_D and sD["weeks"] >= W_MIN_D
            # descriptive (pre-holdout, excess over the stock's own time-of-day baseline)
            xP = EB[mP][:, hi] * sideE[hi]
            yrs = np.unique(yr[mP])
            ys = [(xP[yr[mP] == y].mean() > 0) for y in yrs if (yr[mP] == y).sum() >= 10]
            rgs = [(xP[rg[mP] == r].mean() > 0) for r in range(4) if (rg[mP] == r).sum() >= 10]
            rows.append(
                {
                    "stock": name,
                    "family": fam,
                    "hypothesis": label,
                    "horizon": HNAMES[hi],
                    # recurrence (descriptive, before the holdout)
                    "n_pre": int(sP["n"]),
                    "per_week": round(sP["n"] / max(1, pre_weeks), 3),
                    "direction": "up" if sideE[hi] > 0 else "down",
                    "excess_vs_baseline_pct": float(sP["mean"][hi] * sideE[hi]) if sP["n"] else np.nan,
                    "t_excess": float(abs(sP["t"][hi])) if sP["n"] else np.nan,
                    "p_excess": float(2 * p_upper(abs(sP["t"][hi])))
                    if sP["n"] and np.isfinite(sP["t"][hi])
                    else np.nan,
                    "win_pct_pre": float(sPg["win"][hi]) if sPg["n"] else np.nan,
                    "median_move_pct_pre": float(sPg["median"][hi]) if sPg["n"] else np.nan,
                    "mean_move_pct_pre": float(sPg["mean"][hi]) if sPg["n"] else np.nan,
                    "std_pct_pre": float(sPg["std"][hi]) if sPg["n"] else np.nan,
                    "mfe_median_pct": float(np.median((MFE if sideE[hi] > 0 else -MAE)[mP][:, hi]))
                    if mP.any()
                    else np.nan,
                    "mae_median_pct": float(np.median((MAE if sideE[hi] > 0 else -MFE)[mP][:, hi]))
                    if mP.any()
                    else np.nan,
                    "vs_nifty_pct": float(np.nanmean(XI[mP][:, hi]) * sideE[hi]) if mP.any() else np.nan,
                    "vs_sector_pct": float(np.nanmean(XS[mP][:, hi]) * sideE[hi])
                    if mP.any() and np.isfinite(XS[mP][:, hi]).any()
                    else np.nan,
                    "years_same_sign": f"{sum(ys)}/{len(ys)}",
                    "regimes_same_sign": f"{sum(rgs)}/{len(rgs)}",
                    "holdout_n": int(sHE["n"]),
                    "holdout_excess_pct": float(sHE["mean"][hi] * sideE[hi]) if sHE["n"] else np.nan,
                    # profitability pipeline
                    "side": "long" if side[hi] > 0 else "short",
                    "n_D": int(sD["n"]),
                    "weeks_D": int(sD["weeks"]),
                    "gross_D": float(side[hi] * sD["mean"][hi]) if sD["n"] else np.nan,
                    "net_base_D": float(net_D) if sD["n"] else np.nan,
                    "t_net_D": float(t_net) if np.isfinite(t_net) else np.nan,
                    "p_D": p_D if eligible else np.nan,
                    "n_V": int(sV["n"]),
                    "net_base_V": float(sV["mean"][hi] - cb) if sV["n"] else np.nan,
                    "t_V": float((sV["mean"][hi] - cb) / sV["se"][hi]) if sV["n"] > 2 and sV["se"][hi] > 0 else np.nan,
                    "n_H": int(sH["n"]),
                    "gross_H": float(sH["mean"][hi]) if sH["n"] else np.nan,
                    "net_low_H": float(sH["mean"][hi] - cl) if sH["n"] else np.nan,
                    "net_base_H": float(sH["mean"][hi] - cb) if sH["n"] else np.nan,
                    "net_high_H": float(sH["mean"][hi] - chh) if sH["n"] else np.nan,
                    "win_H": float(((F[mH][:, hi] * side[hi] - cb) > 0).mean() * 100) if sH["n"] else np.nan,
                }
            )
    # stock-level FDR on discovery p-values and on descriptive recurrence p-values
    pD = np.array([r["p_D"] for r in rows])
    stock_pass = bh(pD, Q_STOCK)
    pE = np.array([r["p_excess"] if r["n_pre"] >= N_PRE_REC and r["per_week"] >= FREQ_REC else np.nan for r in rows])
    qE = qvalues(pE)
    for i, r in enumerate(rows):
        r["discovery_pass_stock_fdr"] = bool(stock_pass[i] and r["net_base_D"] > 0)
        r["q_excess"] = float(qE[i]) if np.isfinite(qE[i]) else np.nan
        ys, yn = (int(v) for v in r["years_same_sign"].split("/"))
        r["recurring"] = bool(np.isfinite(qE[i]) and qE[i] <= 0.05 and yn >= 2 and ys >= yn - 1 and r["t_excess"] >= 3)
        r["persists_in_holdout"] = bool(r["holdout_n"] >= 10 and r["holdout_excess_pct"] > 0)
        r["candidate"] = bool(
            (r["discovery_pass_stock_fdr"] or (np.isfinite(r["p_D"]) and r["p_D"] < P_CAND)) and r["net_base_D"] > 0
        )
    cands = _candidate_details(P, rows, records)
    wf = _walk_forward(P, rows, records)
    fp = fingerprint(P, rows)
    return {"rows": rows, "candidates": cands, "wf": wf, "fingerprint": fp}


def _candidate_details(P: Prepared, rows: list, records: list) -> list:
    out = []
    cb = P.costs["base"]
    for base_i, d, wk, F in records:
        for hi in range(4):
            r = rows[base_i + hi] if base_i + hi < len(rows) else None
            if r is None or not r["candidate"] or r["horizon"] != HNAMES[hi]:
                continue
            side = 1.0 if r["side"] == "long" else -1.0
            x = F[:, hi] * side - cb
            pid = P.pid[d]
            det = {"stock": r["stock"], "hypothesis": r["hypothesis"], "horizon": r["horizon"], "side": r["side"]}
            for p, nm in ((0, "D"), (1, "V"), (2, "H")):
                m = pid == p
                lo, hi_ = block_bootstrap_ci(x[m], wk[m]) if m.sum() >= 5 else (np.nan, np.nan)
                det[f"ci90_lo_{nm}"], det[f"ci90_hi_{nm}"] = lo, hi_
                det[f"maxdd_{nm}"] = max_drawdown(x[m])
                det[f"trades_{nm}"] = int(m.sum())
            yrs = P.yr[d]
            det["net_base_by_year"] = {int(y): round(float(x[yrs == y].mean()), 4) for y in np.unique(yrs)}
            rg = P.rg[d]
            det["net_base_by_regime"] = {
                REGIME_NAMES[k]: round(float(x[rg == k].mean()), 4) for k in range(4) if (rg == k).sum() >= 5
            }
            out.append(det)
    return out


def _walk_forward(P: Prepared, rows: list, records: list) -> list:
    """Rolling: select with the discovery rule on the previous 18 months, trade the next quarter."""
    trades = []
    cb, cl, chh = P.costs["base"], P.costs["low"], P.costs["high"]
    dates = P.inst.dates
    for w, s0 in enumerate(WF_STARTS):
        e0 = WF_STARTS[w + 1] if w + 1 < len(WF_STARTS) else s0 + 105
        tr0 = s0 - WF_TRAIN
        cand = []
        for base_i, d, wk, F in records:
            dd = dates[d]
            mt = (dd >= tr0) & (dd < s0)
            if mt.sum() < 30:
                continue
            st = cluster_summary(F[mt], wk[mt])
            if st["weeks"] < 15:
                continue
            for hi in range(4):
                side = 1.0 if st["mean"][hi] >= 0 else -1.0
                se = st["se"][hi]
                if not (se and se > 0):
                    continue
                tn = (side * st["mean"][hi] - cb) / se
                cand.append((min(1.0, 2 * float(p_upper(tn))), base_i, hi, side, d, F, dd))
        if not cand:
            continue
        sel = bh(np.array([c[0] for c in cand]), Q_STOCK)
        for (_p, base_i, hi, side, _d, F, dd), ok in zip(cand, sel, strict=True):
            if not ok:
                continue
            mtest = (dd >= s0) & (dd < e0)
            for j in np.nonzero(mtest)[0]:
                g = float(F[j, hi] * side)
                trades.append(
                    (
                        P.inst.name,
                        w,
                        rows[base_i + hi]["hypothesis"],
                        HNAMES[hi],
                        int(dd[j]),
                        g,
                        g - cl,
                        g - cb,
                        g - chh,
                    )
                )
    return trades


def fingerprint(P: Prepared, rows: list) -> dict:
    inst, b = P.inst, P.base
    pre = P.pid < 2
    O, H, L, C = inst.O, inst.H, inst.L, inst.C
    ok_days = pre & (np.arange(len(inst.dates)) >= WARM)
    gap = (O[:, 0] / b.pc - 1) * 100
    g_ok = ok_days & np.isfinite(gap)
    big = g_ok & (np.abs(gap) >= 0.3)
    filled = np.where(gap > 0, L[:, :72].min(1) <= b.pc, H[:, :72].max(1) >= b.pc)
    o2c = (C[:, 71] / O[:, 0] - 1) * 100
    first15 = np.sign(C[:, 2] - O[:, 0])
    rest = np.sign(C[:, 71] - C[:, 2])
    rng = (H - L) / C * 100
    rec = [r for r in rows if r["n_pre"] >= N_PRE_REC and r["per_week"] >= FREQ_REC and np.isfinite(r["t_excess"])]
    rec.sort(key=lambda r: -r["t_excess"])
    keep = (
        "family",
        "hypothesis",
        "horizon",
        "direction",
        "per_week",
        "n_pre",
        "excess_vs_baseline_pct",
        "t_excess",
        "q_excess",
        "win_pct_pre",
        "median_move_pct_pre",
        "std_pct_pre",
        "mfe_median_pct",
        "mae_median_pct",
        "vs_nifty_pct",
        "vs_sector_pct",
        "years_same_sign",
        "regimes_same_sign",
        "recurring",
        "holdout_n",
        "holdout_excess_pct",
        "persists_in_holdout",
    )
    top = [{k: r[k] for k in keep} for r in rec[:10]]
    for t_, r in zip(top, rec[:10], strict=True):
        t_["net_after_base_cost_pre_pct"] = round(r["mean_move_pct_pre"] - P.costs["base"], 4)
    n_rec = sum(1 for r in rows if r["recurring"])
    n_rec_persist = sum(1 for r in rows if r["recurring"] and r["persists_in_holdout"])
    return {
        "stock": inst.name,
        "kind": inst.kind,
        "sector": inst.sector,
        "sector_peers": P.ctx.sec_peers if P.ctx else 0,
        "coverage": inst.audit,
        "sessions_by_period": {PERIODS[p]: int((P.pid == p).sum()) for p in range(3)},
        "spread_est_pct": round(P.spread, 4),
        "cost_pct": {k: round(v, 4) for k, v in P.costs.items()},
        "typical_5m_range_pct": round(float(np.nanmedian(b.rng20)) * 100, 4),
        "beta_to_nifty_median": round(float(np.nanmedian(P.ctx.beta)), 3) if P.ctx is not None else None,
        "profile_pre_holdout": {
            "mean_abs_gap_pct": round(float(np.nanmean(np.abs(gap[g_ok]))), 3) if g_ok.any() else None,
            "gap_fill_rate_pct": round(float(filled[big].mean() * 100), 1) if big.any() else None,
            "open_to_1515_mean_pct": round(float(np.nanmean(o2c[ok_days])), 4),
            "up_day_pct": round(float((o2c[ok_days] > 0).mean() * 100), 1),
            "first15_reversal_pct": round(float((first15[ok_days] * rest[ok_days] < 0).mean() * 100), 1),
            "median_5m_range_by_tod_pct": {
                TOD_NAMES[k]: round(float(np.nanmedian(rng[ok_days][:, k == TOD])), 4) for k in range(5)
            },
            "baseline_30m_move_by_tod_pct": {
                TOD_NAMES[k]: round(float(np.nanmean(P.baseline[:2, 1, k])), 4) for k in range(5)
            },
        },
        "tests": len(rows),
        "recurring_behaviours": n_rec,
        "recurring_and_persisting_in_holdout": n_rec_persist,
        "top_recurring": top,
    }


def finalize(results: list[dict]) -> dict:
    """Cross-stock steps: global FDR, the validation gate, tiers, walk-forward summary."""
    rows = [r for res in results for r in res["rows"]]
    pD = np.array([r["p_D"] for r in rows])
    gpass = bh(pD, Q_GLOBAL)
    for r, g in zip(rows, gpass, strict=True):
        r["discovery_pass_global_fdr"] = bool(g and r["net_base_D"] > 0)
    cands = [r for r in rows if r["candidate"]]
    pV = np.array([float(p_upper(r["t_V"])) if np.isfinite(r["t_V"]) else np.nan for r in cands])
    vpass = bh(pV, Q_VAL)
    det = {(c["stock"], c["hypothesis"], c["horizon"]): c for res in results for c in res["candidates"]}
    tiers = []
    for r, vp in zip(cands, vpass, strict=True):
        d = det.get((r["stock"], r["hypothesis"], r["horizon"]), {})
        v_ok = bool(vp and r["net_base_V"] > 0)
        if not v_ok:
            tier = "DISCOVERY_ONLY"
        elif r["n_H"] >= 20 and d.get("ci90_lo_H", np.nan) > 0 and r["net_high_H"] > 0:
            tier = "VALIDATED_EDGE"
        elif r["net_base_H"] > 0:
            tier = "PROMISING"
        else:
            tier = "FAILED_HOLDOUT"
        r["validation_pass"], r["tier"] = v_ok, tier
        tiers.append({**{k: r[k] for k in r if k not in ("years_same_sign", "regimes_same_sign")}, **d})
    wf = [t for res in results for t in res["wf"]]
    return {"rows": rows, "profit_catalogue": tiers, "wf_trades": wf}


def wf_summary(trades: list) -> dict:
    if not trades:
        return {"trades": 0}
    arr = np.array([t[5:] for t in trades], float)  # gross, low, base, high
    wk = (np.array([t[4] for t in trades]) + 3) // 7
    s = cluster_summary(arr, wk)
    per_window = {}
    for w in sorted({t[1] for t in trades}):
        m = np.array([t[1] == w for t in trades])
        sw = cluster_summary(arr[m], wk[m])
        per_window[datestr(WF_STARTS[w])] = {
            "trades": int(m.sum()),
            "rules": len({(t[0], t[2], t[3]) for t, mm in zip(trades, m, strict=True) if mm}),
            "net_base_mean_pct": round(float(sw["mean"][2]), 4),
            "t": round(float(sw["t"][2]), 2) if np.isfinite(sw["t"][2]) else None,
        }
    return {
        "trades": int(s["n"]),
        "weeks": int(s["weeks"]),
        "mean_pct": dict(
            zip(("gross", "net_low", "net_base", "net_high"), (round(float(v), 4) for v in s["mean"]), strict=True)
        ),
        "t_week_clustered": dict(
            zip(("gross", "net_low", "net_base", "net_high"), (round(float(v), 2) for v in s["t"]), strict=True)
        ),
        "win_pct_net_base": round(float(s["win"][2]), 1),
        "per_window": per_window,
    }
