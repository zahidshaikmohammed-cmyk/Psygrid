"""Confirmation of the pre-registered hypotheses (FROZEN, see prereg.lock and PREREGISTRATION.md).

Windows (session dates):
    CONFIRMATION  2021-10-01 .. 2022-10-12  never fetched before; the only untouched historical data
    IN-SAMPLE     2022-10-13 .. 2025-09-30  seen by the earlier search; used only for a direction check
    CONTAMINATED  2025-10-01 .. 2026-10-12  its results were inspected; reported, never decisive
The first WARM (20) sessions of any fetched span only warm up rolling baselines; H5 additionally
needs 60 sessions. Eligibility, corporate-action exclusions and decisions follow the constants below.
"""

from __future__ import annotations

import datetime as dt

import numpy as np

from . import prereg
from .execution import Costs, simulate, spread_estimate_pct
from .integrity import corporate_action_flags, exclusion_mask
from .stats import block_bootstrap_ci, cluster_summary, max_drawdown, p_upper

EPOCH = dt.date(1970, 1, 1)
CONFIRMATION = (dt.date(2021, 10, 1), dt.date(2022, 10, 12))
IN_SAMPLE = (dt.date(2022, 10, 13), dt.date(2025, 9, 30))
CONTAMINATED = (dt.date(2025, 10, 1), dt.date(2026, 10, 12))

COST_SCENARIOS = {
    "zero": Costs(0.0, 0.0, 0.0),  # diagnostic only, never executable
    "low": Costs(0.02, 0.00, 0.5),
    "base": Costs(0.05, 0.02, 1.0),
    "high": Costs(0.10, 0.05, 2.0),
}
FAMILY_ALPHA = 0.05  # one-sided, Holm across the six hypotheses
MIN_TRADES, MIN_WEEKS, MIN_INSTRUMENTS = 300, 30, 15
NA_MIN_TRADES = 100
MAX_INSTRUMENT_SHARE, MAX_MONTH_SHARE = 0.20, 0.35
MIN_SESSIONS_ELIGIBLE = 150


def dnum(d: dt.date) -> int:
    return (d - EPOCH).days


def _align(src_dates, dst_dates, arr):
    pos = np.clip(np.searchsorted(src_dates, dst_dates), 0, len(src_dates) - 1)
    ok = src_dates[pos] == dst_dates
    out = np.take(arr, pos, axis=0).astype(float)
    shape = [len(dst_dates)] + [1] * (arr.ndim - 1)
    return np.where(ok.reshape(shape), out, np.nan)


def _beta_excess(r, rn, flag):
    """daily excess return r - beta*rn, beta from the previous 60 sessions; flagged days removed."""
    r = np.where(flag, np.nan, r)
    out = np.full(len(r), np.nan)
    for d in range(1, len(r)):
        a, b = r[max(0, d - 60) : d], rn[max(0, d - 60) : d]
        ok = np.isfinite(a) & np.isfinite(b)
        if ok.sum() >= 20 and np.var(b[ok]) > 0:
            beta = np.mean((a[ok] - a[ok].mean()) * (b[ok] - b[ok].mean())) / np.var(b[ok])
            out[d] = r[d] - beta * rn[d]
    return out


def prepare(instruments, sector_of, start: dt.date, end: dt.date):
    """Contexts for every eligible equity in [start, end]. Returns (prepared, report)."""
    by = {i.name: i for i in instruments if i.C is not None and len(i.dates)}
    nifty = by.get("NIFTY")
    if nifty is None:
        raise RuntimeError("NIFTY is required")
    n_pc = np.r_[np.nan, nifty.C[:-1, -1]]
    n_gap = (nifty.O[:, 0] / n_pc - 1) * 100
    n_ret = nifty.C[:, -1] / n_pc - 1
    n_dso = nifty.C / n_pc[:, None] - 1
    n_mom6 = nifty.C / prereg._lag(nifty.C, 6, np.nan) - 1
    eq = [i for i in by.values() if i.kind == "EQUITY" and i.m1 is not None]
    grid = np.unique(np.concatenate([i.dates for i in eq])) if eq else np.zeros(0, int)
    dso = {}
    for i in eq:
        pc = np.r_[np.nan, i.C[:-1, -1]]
        dso[i.name] = i.C / pc[:, None] - 1
    sectors: dict[str, list] = {}
    for i in eq:
        s = sector_of(i.name)
        if s not in ("OTHER", "", None):
            sectors.setdefault(s, []).append(i.name)
    sec_sum = {}
    for s, names in sectors.items():
        S, N = np.zeros((len(grid), 75)), np.zeros((len(grid), 75))
        for n in names:
            pos = np.searchsorted(grid, by[n].dates)
            S[pos] += np.nan_to_num(dso[n])
            N[pos] += np.isfinite(dso[n])
        sec_sum[s] = (S, N)
    prepared, report = {}, {}
    lo, hi = dnum(start), dnum(end)
    for i in eq:
        inwin = (i.dates >= lo) & (i.dates <= hi)
        n_sessions = int(inwin.sum())
        if n_sessions < MIN_SESSIONS_ELIGIBLE:
            report[i.name] = {"eligible": False, "reason": f"only {n_sessions} complete sessions in window"}
            continue
        pc = np.r_[np.nan, i.C[:-1, -1]]
        ng = _align(nifty.dates, i.dates, n_gap)
        flag, split = corporate_action_flags(i.O[:, 0], pc, ng)
        excl = exclusion_mask(flag)
        r = i.C[:, -1] / pc - 1
        excess = _beta_excess(r, _align(nifty.dates, i.dates, n_ret), flag)
        s = sector_of(i.name)
        sec = np.full(i.C.shape, np.nan)
        if s in sec_sum and len(sectors[s]) >= 3:
            S, N = sec_sum[s]
            pos = np.searchsorted(grid, i.dates)
            own = dso[i.name]
            with np.errstate(invalid="ignore", divide="ignore"):
                n = N[pos] - np.isfinite(own)
                sec = np.where(n >= 2, (S[pos] - np.nan_to_num(own)) / np.where(n > 0, n, 1), np.nan)
        ctx = prereg.build_ctx(
            i.O,
            i.H,
            i.L,
            i.C,
            i.V,
            pc,
            sec,
            _align(nifty.dates, i.dates, n_dso),
            _align(nifty.dates, i.dates, n_mom6),
            excess,
            inwin & ~excl,
        )
        sp, capped = spread_estimate_pct(i.m1[inwin])
        prepared[i.name] = (ctx, i.m1, i.dates, sp)
        report[i.name] = {
            "eligible": True,
            "sessions": n_sessions,
            "spread_pct": round(sp, 4),
            "spread_capped": capped,
            "corporate_action_flags": [str(EPOCH + dt.timedelta(days=int(i.dates[k]))) for k in np.nonzero(flag)[0]],
            "likely_splits": [str(EPOCH + dt.timedelta(days=int(i.dates[k]))) for k in np.nonzero(split)[0]],
            "sessions_excluded": int((excl & inwin).sum()),
            "has_sector_peers": bool(np.isfinite(sec).any()),
        }
    return prepared, report


def run(prepared) -> list[dict]:
    trades = []
    base = COST_SCENARIOS["base"]
    for name, (ctx, m1, dates, sp) in prepared.items():
        for h, fn in prereg.SIGNALS.items():
            for sig in fn(ctx):
                res = simulate(m1[sig.d], sig, sp, base)
                if res is None or res["skipped"]:
                    continue
                market_fills = res["market_fills"]
                row = {
                    "hypothesis": h,
                    "stock": name,
                    "date": int(dates[sig.d]),
                    "side": sig.side,
                    "reason": res["reason"],
                    "gross": res["gross"],
                    "spread_pct": sp,
                    "market_fills": market_fills,
                }
                for c, cs in COST_SCENARIOS.items():
                    row[f"net_{c}"] = res["gross"] - cs.fees_pct - cs.slip_pct * market_fills - cs.spread_mult * sp
                trades.append(row)
    return trades


def holm(p: dict) -> dict:
    keys = sorted(p, key=lambda k: p[k])
    m = len(keys)
    out, run_max = {}, 0.0
    for j, k in enumerate(keys):
        run_max = max(run_max, min(1.0, (m - j) * p[k]))
        out[k] = run_max
    return out


def evaluate(trades: list[dict], in_sample_gross: dict | None = None) -> dict:
    """Pre-registered decision for each hypothesis from the trades of ONE window."""
    res = {}
    for h in prereg.SIGNALS:
        tr = sorted((t for t in trades if t["hypothesis"] == h), key=lambda t: (t["date"], t["stock"]))
        n = len(tr)
        r = {"trades": n}
        if n < NA_MIN_TRADES:
            r.update(verdict="NOT_APPLICABLE", reason=f"only {n} trades")
            res[h] = r
            continue
        dates = np.array([t["date"] for t in tr])
        wk = (dates + 3) // 7
        x = {c: np.array([t[f"net_{c}"] for t in tr]) for c in COST_SCENARIOS}
        g = np.array([t["gross"] for t in tr])
        s = cluster_summary(x["base"], wk)
        lo, hi_ = block_bootstrap_ci(x["base"], wk)
        stocks = np.array([t["stock"] for t in tr])
        months = np.array([str(EPOCH + dt.timedelta(days=int(d)))[:7] for d in dates])
        tot = x["base"].sum()
        inst_share = max(x["base"][stocks == k].sum() / tot for k in np.unique(stocks)) if tot > 0 else np.nan
        month_share = max(x["base"][months == k].sum() / tot for k in np.unique(months)) if tot > 0 else np.nan
        half = np.median(dates)
        r.update(
            weeks=int(s["weeks"]),
            instruments=len(np.unique(stocks)),
            mean_gross=float(g.mean()),
            mean_net={c: float(v.mean()) for c, v in x.items()},
            t_base=float(s["t"][0]),
            p_base=float(p_upper(s["t"][0])),
            ci90_base=(lo, hi_),
            win_pct_base=float((x["base"] > 0).mean() * 100),
            max_drawdown_base=max_drawdown(x["base"]),
            max_instrument_share=float(inst_share),
            max_month_share=float(month_share),
            halves_positive=bool(x["base"][dates < half].mean() > 0 and x["base"][dates >= half].mean() > 0),
            exits={k: int(sum(1 for t in tr if t["reason"] == k)) for k in ("stop", "target", "time")},
            in_sample_gross=None if in_sample_gross is None else in_sample_gross.get(h),
        )
        res[h] = r
    ps = {h: r["p_base"] for h, r in res.items() if "p_base" in r}
    adj = holm(ps)
    for h, r in res.items():
        if "p_base" not in r:
            continue
        r["p_holm"] = adj[h]
        checks = {
            "sample": r["trades"] >= MIN_TRADES and r["weeks"] >= MIN_WEEKS and r["instruments"] >= MIN_INSTRUMENTS,
            "significant": r["p_holm"] <= FAMILY_ALPHA,
            "ci_above_zero": r["ci90_base"][0] > 0,
            "high_cost_positive": r["mean_net"]["high"] > 0,
            "not_concentrated": r["max_instrument_share"] <= MAX_INSTRUMENT_SHARE
            and r["max_month_share"] <= MAX_MONTH_SHARE,
            "both_halves_positive": r["halves_positive"],
            "in_sample_direction": r["in_sample_gross"] is not None and r["in_sample_gross"] > 0,
        }
        r["checks"] = checks
        if r["mean_net"]["base"] <= 0:
            r["verdict"] = "REJECTED"
        elif all(checks.values()):
            r["verdict"] = "CONFIRMED"
        else:
            r["verdict"] = "INCONCLUSIVE"
        if h == "H5" and r["verdict"] == "CONFIRMED":
            r["verdict"] = "INCONCLUSIVE"
            r["reason"] = "survivorship-biased in a backward window; needs forward data"
    return res
