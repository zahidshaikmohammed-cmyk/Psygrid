"""Run the behaviour discovery and write machine-readable results.

    python -m research.behaviour.run --stream - --out DIR      # stream from behaviour_fetch.py on stdin
    python -m research.behaviour.run --synthetic --out DIR     # dry run on synthetic data

Outputs (aggregate statistics only, no market data):
    summary.json               run metadata, coverage, counts, walk-forward summary
    fingerprints.json          one behavioural fingerprint per instrument
    recurring_catalogue.csv    recurring stock-specific behaviours (descriptive), ranked by reliability
    profit_catalogue.csv       every discovery candidate with validation/holdout results and its tier
    walk_forward.json          rolling out-of-sample summary
    all_tests.csv.gz           every hypothesis x horizon tested (large; artifact only)
    REPORT.md                  human-readable summary
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import multiprocessing as mp
import os
import sys
import time

import numpy as np

from . import engine
from .data import read_stream
from .features import HNAMES

PREP: dict = {}


def _eval(name):
    import warnings

    warnings.simplefilter("ignore")
    with np.errstate(all="ignore"):
        return engine.evaluate(PREP[name])


def _clean(o):
    if isinstance(o, dict):
        return {str(k): _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    if isinstance(o, (np.floating, float)):
        return None if not math.isfinite(float(o)) else round(float(o), 6)
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return o


def _csv(path, rows, cols, opener=open):
    with opener(path, "wt", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([_clean(r.get(c)) for c in cols])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream")
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--out", required=True)
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2)))
    a = ap.parse_args(argv)
    t0 = time.time()

    def log(m):
        print(f"[{time.time() - t0:7.1f}s] {m}", file=sys.stderr, flush=True)

    if a.synthetic:
        from .synth import universe

        meta, insts = {"interval": 5, "synthetic": True, "complete": True}, universe()
    else:
        if a.stream == "-":
            meta, insts = read_stream(sys.stdin.buffer)
        else:
            with open(a.stream, "rb") as fh:
                meta, insts = read_stream(fh)
    from sector_taxonomy import sector_for_symbol

    for i in insts:
        if i.kind == "EQUITY" and i.sector in ("-", ""):
            i.sector = sector_for_symbol(i.name)
    log(f"stream: {len(insts)} instruments, meta={meta}")
    global PREP
    with np.errstate(all="ignore"):
        import warnings

        warnings.simplefilter("ignore")
        PREP = engine.prepare_all(insts, log)
    names = list(PREP)
    results = []
    if a.workers > 1 and len(names) > 1:
        with mp.get_context("fork").Pool(a.workers) as pool:
            for i, res in enumerate(pool.imap(_eval, names), 1):
                results.append(res)
                log(f"evaluated {i}/{len(names)} {res['fingerprint']['stock']}: {len(res['rows'])} tests")
    else:
        for i, n in enumerate(names, 1):
            results.append(_eval(n))
            log(f"evaluated {i}/{len(names)} {n}")
    fin = engine.finalize(results)
    wf = engine.wf_summary(fin["wf_trades"])
    os.makedirs(a.out, exist_ok=True)

    rows = fin["rows"]
    rec = sorted((r for r in rows if r["recurring"]), key=lambda r: -r["t_excess"])
    tiers = fin["profit_catalogue"]
    order = {"VALIDATED_EDGE": 0, "PROMISING": 1, "FAILED_HOLDOUT": 2, "DISCOVERY_ONLY": 3}
    tiers.sort(key=lambda r: (order[r["tier"]], -(r["net_base_H"] if r["net_base_H"] == r["net_base_H"] else -9)))
    counts = {t: sum(1 for r in tiers if r["tier"] == t) for t in order}
    fps = [res["fingerprint"] for res in results]
    coverage = {i.name: i.audit | {"status": i.status} for i in insts}
    summary = {
        "meta": meta,
        "periods": {
            "discovery": f"< {engine.datestr(engine.D_END)}",
            "validation": f"< {engine.datestr(engine.V_END)}",
            "holdout": "rest",
        },
        "costs_pct": {k: {"fees": f, "spread_multiple": m} for k, (f, m) in engine.COSTS.items()},
        "selection": {
            "discovery": f"n>={engine.N_MIN_D}, weeks>={engine.W_MIN_D}, net(base)>0, BH q={engine.Q_STOCK} per stock (or p<{engine.P_CAND})",
            "validation": f"one-sided week-clustered t on net(base), BH q={engine.Q_VAL} across all candidates",
            "validated_edge": "validation pass AND holdout: >=20 trades, 90% block-bootstrap CI of net(base) > 0, net(high) > 0",
        },
        "instruments_streamed": len(insts),
        "instruments_evaluated": len(results),
        "tests": len(rows),
        "eligible_discovery_tests": int(sum(1 for r in rows if r["p_D"] == r["p_D"])),
        "discovery_pass_stock_fdr": int(sum(r["discovery_pass_stock_fdr"] for r in rows)),
        "discovery_pass_global_fdr": int(sum(r["discovery_pass_global_fdr"] for r in rows)),
        "candidates": len(tiers),
        "tiers": counts,
        "recurring_behaviours": len(rec),
        "recurring_persisting_in_holdout": int(sum(r["persists_in_holdout"] for r in rec)),
        "walk_forward": wf,
        "coverage": coverage,
        "runtime_s": round(time.time() - t0, 1),
    }
    with open(os.path.join(a.out, "summary.json"), "w") as fh:
        json.dump(_clean(summary), fh, indent=1)
    with open(os.path.join(a.out, "fingerprints.json"), "w") as fh:
        json.dump(_clean(fps), fh, indent=1)
    with open(os.path.join(a.out, "walk_forward.json"), "w") as fh:
        json.dump(_clean(wf), fh, indent=1)
    rcols = [
        "stock",
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
        "mean_move_pct_pre",
        "std_pct_pre",
        "mfe_median_pct",
        "mae_median_pct",
        "vs_nifty_pct",
        "vs_sector_pct",
        "years_same_sign",
        "regimes_same_sign",
        "holdout_n",
        "holdout_excess_pct",
        "persists_in_holdout",
        "net_base_D",
        "candidate",
    ]
    _csv(os.path.join(a.out, "recurring_catalogue.csv"), rec, rcols)
    pcols = [
        "tier",
        "stock",
        "family",
        "hypothesis",
        "horizon",
        "side",
        "n_D",
        "gross_D",
        "net_base_D",
        "t_net_D",
        "p_D",
        "discovery_pass_stock_fdr",
        "discovery_pass_global_fdr",
        "n_V",
        "net_base_V",
        "t_V",
        "validation_pass",
        "n_H",
        "gross_H",
        "net_low_H",
        "net_base_H",
        "net_high_H",
        "win_H",
        "ci90_lo_H",
        "ci90_hi_H",
        "maxdd_H",
        "ci90_lo_V",
        "maxdd_V",
        "net_base_by_year",
        "net_base_by_regime",
    ]
    _csv(os.path.join(a.out, "profit_catalogue.csv"), tiers, pcols)
    acols = list(rows[0].keys()) if rows else []
    _csv(os.path.join(a.out, "all_tests.csv.gz"), rows, acols, opener=gzip.open)
    with gzip.open(os.path.join(a.out, "walk_forward_trades.csv.gz"), "wt") as fh:
        w = csv.writer(fh)
        w.writerow(["stock", "window", "hypothesis", "horizon", "date", "gross", "net_low", "net_base", "net_high"])
        for t in fin["wf_trades"]:
            w.writerow([t[0], t[1], t[2], t[3], engine.datestr(t[4]), *(round(x, 5) for x in t[5:])])
    with open(os.path.join(a.out, "REPORT.md"), "w") as fh:
        fh.write(report(summary, fps, rec, tiers))
    log(
        f"done: {json.dumps(_clean({k: summary[k] for k in ('tests', 'candidates', 'tiers', 'recurring_behaviours')}))}"
    )
    print(json.dumps(_clean({k: v for k, v in summary.items() if k != "coverage"}), indent=1))
    return 0


def report(summary, fps, rec, tiers) -> str:
    L = ["# Behaviour discovery — automatic report", ""]
    s = summary
    L.append(
        f"Instruments evaluated: {s['instruments_evaluated']} / streamed {s['instruments_streamed']}. Tests: {s['tests']}."
    )
    L.append(
        f"Discovery passes: stock-FDR {s['discovery_pass_stock_fdr']}, global-FDR {s['discovery_pass_global_fdr']}. Candidates: {s['candidates']}."
    )
    L.append(f"Tiers: {s['tiers']}")
    L.append(
        f"Recurring behaviours (descriptive): {s['recurring_behaviours']}, persisting in holdout: {s['recurring_persisting_in_holdout']}"
    )
    L.append(f"Walk-forward: {json.dumps(_clean({k: v for k, v in s['walk_forward'].items() if k != 'per_window'}))}")
    L += [
        "",
        "## Profit tiers (top 40)",
        "",
        "| tier | stock | hypothesis | h | side | n_D | net_D | n_V | net_V | n_H | net_base_H | CI90_H | net_high_H |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in tiers[:40]:
        ci = f"{_fmt(r.get('ci90_lo_H'))}..{_fmt(r.get('ci90_hi_H'))}"
        L.append(
            f"| {r['tier']} | {r['stock']} | {r['hypothesis']} | {r['horizon']} | {r['side']} | {r['n_D']} | {_fmt(r['net_base_D'])} | "
            f"{r['n_V']} | {_fmt(r['net_base_V'])} | {r['n_H']} | {_fmt(r['net_base_H'])} | {ci} | {_fmt(r['net_high_H'])} |"
        )
    L += [
        "",
        "## Recurring behaviours (top 60 by reliability; descriptive, not trade signals)",
        "",
        "| stock | behaviour | h | dir | /week | excess % | t | years | regimes | holdout excess % | net after base cost (pre) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rec[:60]:
        L.append(
            f"| {r['stock']} | {r['hypothesis']} | {r['horizon']} | {r['direction']} | {r['per_week']} | {_fmt(r['excess_vs_baseline_pct'])} | "
            f"{_fmt(r['t_excess'], 1)} | {r['years_same_sign']} | {r['regimes_same_sign']} | {_fmt(r['holdout_excess_pct'])} | {_fmt(r['net_base_D'])} |"
        )
    L += ["", "## Fingerprint headline per instrument", ""]
    for f in fps:
        top = f["top_recurring"][0] if f["top_recurring"] else None
        head = (
            f"{top['hypothesis']} ({top['horizon']}, {top['direction']}, t {_fmt(top['t_excess'], 1)})"
            if top
            else "none"
        )
        L.append(
            f"- **{f['stock']}** recurring {f['recurring_behaviours']} (persisting {f['recurring_and_persisting_in_holdout']}); strongest: {head}"
        )
    return "\n".join(L) + "\n"


def _fmt(v, nd=3):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    return "-" if not math.isfinite(v) else f"{v:+.{nd}f}" if nd == 3 else f"{v:.{nd}f}"


_ = HNAMES

if __name__ == "__main__":
    sys.exit(main())
