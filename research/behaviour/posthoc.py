"""Post-hoc analysis of a finished behaviour-discovery run (no new market data).

    python -m research.behaviour.posthoc RESULTS_DIR

Reads all_tests.csv.gz, walk_forward_trades.csv.gz and fingerprints.json from RESULTS_DIR and writes:
    cost_sensitivity.json        the full discovery -> validation -> holdout funnel re-run at zero, low,
                                 base and high cost (selection rules unchanged; holdout never selects)
    walk_forward_breakdown.json  walk-forward by window, stock, family; concentration checks
    recurring_with_holdout.csv   recurring behaviours with their holdout result after costs
    promising_hypotheses.csv     recurring + persisting + cost-positive before the holdout
"""

from __future__ import annotations

import json
import os
import sys

import numpy as np
import pandas as pd

from .engine import P_CAND, Q_STOCK, Q_VAL
from .run import _clean
from .stats import bh, cluster_summary, p_upper


def funnel(df: pd.DataFrame, cost: pd.Series, label: str) -> dict:
    d = df[df["p_D"].notna()].copy()
    c = cost.reindex(d["stock"]).to_numpy()
    se = (d["net_base_D"] / d["t_net_D"]).to_numpy()
    with np.errstate(all="ignore"):
        t = (d["gross_D"].to_numpy() - c) / se
    p = np.minimum(1, 2 * p_upper(np.where(np.isfinite(t), t, -10)))
    d["p_c"], d["net_c_D"] = p, d["gross_D"].to_numpy() - c
    sel = np.zeros(len(d), bool)
    for _, idx in d.groupby("stock").indices.items():
        sel[idx] = bh(p[idx], Q_STOCK)
    cand = d[((sel) | (d["p_c"] < P_CAND)) & (d["net_c_D"] > 0)].copy()
    cb = cost.reindex(cand["stock"]).to_numpy()
    base_c = df.attrs["cost_base"].reindex(cand["stock"]).to_numpy()
    seV = (cand["net_base_V"] / cand["t_V"]).to_numpy()
    with np.errstate(all="ignore"):
        tV = (cand["net_base_V"].to_numpy() + base_c - cb) / seV
    pV = p_upper(np.where(np.isfinite(tV), tV, -10))
    vpass = bh(pV, Q_VAL) & ((cand["net_base_V"].to_numpy() + base_c - cb) > 0)
    v = cand[vpass].copy()
    v["net_c_H"] = v["gross_H"] - cost.reindex(v["stock"]).to_numpy()
    held = v[v["n_H"] >= 20]
    return {
        "cost": label,
        "eligible_tests": len(d),
        "discovery_candidates": len(cand),
        "candidates_by_stock": cand["stock"].value_counts().head(15).to_dict(),
        "validation_pass": len(v),
        "holdout_n_ge_20": len(held),
        "holdout_positive": int((held["net_c_H"] > 0).sum()),
        "holdout_mean_net_pct": round(float(held["net_c_H"].mean()), 4) if len(held) else None,
        "validated_list": [
            {
                k: r[k]
                for k in (
                    "stock",
                    "hypothesis",
                    "horizon",
                    "side",
                    "n_D",
                    "net_c_D",
                    "n_V",
                    "net_base_V",
                    "n_H",
                    "gross_H",
                    "net_c_H",
                    "win_H",
                )
            }
            for _, r in v.sort_values("net_c_H", ascending=False).head(40).iterrows()
        ],
    }


def _dump(obj, out, name):
    with open(os.path.join(out, name), "w") as fh:
        json.dump(_clean(obj), fh, indent=1)


def main(argv=None) -> int:
    out = (argv or sys.argv[1:])[0]
    df = pd.read_csv(os.path.join(out, "all_tests.csv.gz"))
    with open(os.path.join(out, "fingerprints.json")) as fh:
        fps = json.load(fh)
    costs = pd.DataFrame({f["stock"]: f["cost_pct"] for f in fps}).T
    df.attrs["cost_base"] = costs["base"]
    res = {
        "note": "Same selection rules as the main run; only the cost changes. Holdout is reported, never used to select."
    }
    zero = pd.Series(0.0, index=costs.index)
    res["funnels"] = [
        funnel(df, zero, "zero (gross)"),
        funnel(df, costs["low"], "low"),
        funnel(df, costs["base"], "base"),
        funnel(df, costs["high"], "high"),
    ]
    with np.errstate(all="ignore"):
        se = df["net_base_D"] / df["t_net_D"]
        tg = df["gross_D"] / se
    elig = df["p_D"].notna()
    res["gross_t_distribution_eligible"] = {f">={k}": int((tg[elig] >= k).sum()) for k in (2, 3, 4, 5, 6)} | {
        "tests": int(elig.sum()),
        "expected_ge3_if_pure_noise_two_sided": round(float(elig.sum() * 2 * p_upper(3.0)), 1),
    }
    _dump(res, out, "cost_sensitivity.json")

    # ---------------------------------------------------------------- walk-forward breakdown
    wf = pd.read_csv(os.path.join(out, "walk_forward_trades.csv.gz"))
    fam = df.drop_duplicates(["stock", "hypothesis"]).set_index(["stock", "hypothesis"])["family"]
    wf["family"] = [fam.get((s, h), "?") for s, h in zip(wf["stock"], wf["hypothesis"], strict=True)]
    wf["week"] = (pd.to_datetime(wf["date"]).astype("int64") // 86_400_000_000_000 + 3) // 7

    def summ(x: pd.DataFrame) -> dict:
        s = cluster_summary(x[["gross", "net_low", "net_base", "net_high"]].to_numpy(), x["week"].to_numpy())
        return {
            "trades": int(s["n"]),
            "weeks": int(s["weeks"]),
            "rules": int(x.groupby(["stock", "hypothesis", "horizon"]).ngroups),
            "stocks": int(x["stock"].nunique()),
            "mean": dict(
                zip(("gross", "net_low", "net_base", "net_high"), np.round(s["mean"], 4).tolist(), strict=True)
            ),
            "t": dict(zip(("gross", "net_low", "net_base", "net_high"), np.round(s["t"], 2).tolist(), strict=True))
            if s["weeks"] > 1
            else None,
        }

    last = wf["window"].max()
    br = {
        "all": summ(wf),
        "excluding_last_window": summ(wf[wf["window"] != last]) if (wf["window"] != last).any() else None,
        "last_window": summ(wf[wf["window"] == last]),
        "by_window": {int(w): summ(g) for w, g in wf.groupby("window")},
        "by_family": {f: summ(g) for f, g in wf.groupby("family")},
        "by_stock_top": {s: summ(g) for s, g in sorted(wf.groupby("stock"), key=lambda kv: -len(kv[1]))[:15]},
        "last_window_rules": (
            wf[wf["window"] == last]
            .groupby(["stock", "family", "hypothesis", "horizon"])["net_base"]
            .agg(["count", "mean"])
            .reset_index()
            .sort_values("count", ascending=False)
            .head(70)
            .to_dict("records")
        ),
    }
    _dump(br, out, "walk_forward_breakdown.json")

    # ---------------------------------------------------------------- recurring behaviours after costs in the holdout
    rec = df[df["recurring"] == True].copy()  # noqa: E712
    same = ((rec["direction"] == "up") & (rec["side"] == "long")) | (
        (rec["direction"] == "down") & (rec["side"] == "short")
    )
    rec["holdout_direction_matches_side"] = same
    rec["holdout_net_base_in_direction"] = np.where(same, rec["net_base_H"], np.nan)
    rec["holdout_gross_in_direction"] = np.where(same, rec["gross_H"], np.nan)
    cols = [
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
        "mean_move_pct_pre",
        "mfe_median_pct",
        "mae_median_pct",
        "vs_nifty_pct",
        "vs_sector_pct",
        "years_same_sign",
        "regimes_same_sign",
        "net_base_D",
        "holdout_n",
        "holdout_excess_pct",
        "persists_in_holdout",
        "holdout_gross_in_direction",
        "holdout_net_base_in_direction",
        "win_H",
    ]
    rec.sort_values("t_excess", ascending=False)[cols].to_csv(
        os.path.join(out, "recurring_with_holdout.csv"), index=False
    )
    prom = rec[(rec["persists_in_holdout"] == True) & (rec["net_base_D"] > 0)]  # noqa: E712
    prom.sort_values("t_excess", ascending=False)[cols].to_csv(
        os.path.join(out, "promising_hypotheses.csv"), index=False
    )
    loose = df[(df["q_excess"] <= 0.10) & (df["t_excess"] >= 3)]
    res2 = {
        "recurring_strict": len(rec),
        "recurring_strict_by_stock": rec["stock"].value_counts().to_dict(),
        "recurring_strict_by_family": rec["family"].value_counts().to_dict(),
        "recurring_loose_q10": len(loose),
        "recurring_loose_by_stock": loose["stock"].value_counts().head(25).to_dict(),
        "stocks_with_any_strict_recurring": int(rec["stock"].nunique()),
        "promising": len(prom),
    }
    _dump(res2, out, "recurrence_summary.json")
    print(
        json.dumps(
            _clean(
                {
                    "funnels": [
                        {k: f[k] for k in f if k not in ("validated_list", "candidates_by_stock")}
                        for f in res["funnels"]
                    ],
                    "gross_t": res["gross_t_distribution_eligible"],
                    "wf_all": br["all"],
                    "wf_ex_last": br["excluding_last_window"],
                    "recurrence": res2,
                }
            ),
            indent=1,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
