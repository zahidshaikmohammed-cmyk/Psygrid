"""Run the pre-registered confirmation (PREREGISTRATION.md) on a 1-minute stream; aggregates only.

    python -m research.behaviour.confirm_run --stream - --out DIR    # stream from behaviour_fetch.py

Order of work:
  1. verify every pre-registered file against prereg.lock (refuse to run on any mismatch);
  2. parse the stream in RAM for 2021-10-01 .. 2026-10-12, auditing every session of every
     instrument (data.session_audit) and every gap against NIFTY's session calendar; the parse stops
     if the runner's free memory falls below MIN_FREE_MB;
  3. if the fetch stopped early (rate limit, live-service health, node memory, market hours) or the
     stream is incomplete: write the integrity outputs, status ABORTED, and no verdicts;
  4. IN-SAMPLE (seen) -> gross direction per hypothesis, trades per week and same-week correlation;
     CONFIRMATION -> the pre-registered verdicts (confirm.evaluate, unchanged);
     CONTAMINATED -> descriptive only.

Outputs (no prices, no candles, no per-trade rows):
    summary.json             status, lock verification, coverage, verdicts, cost sensitivity, CIs
    integrity_summary.json   per instrument: session statuses, missing sessions, anomalies, eligibility
    session_anomalies.csv    every session not KEPT, or with duplicates, early rows or off-minute stamps
    session_audit.csv.gz     every session of every instrument (timestamps and counts only)
    per_stock.csv            per hypothesis x stock x window: trades and mean returns (descriptive)
    per_month.csv            per hypothesis x month (confirmation window)
    REPORT.md                human-readable report with the required labels
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import math
import os
import sys
import time

import numpy as np

from . import confirm, power, prereg
from .data import AUDIT_COLS, KEPT, KEPT_FILLED, read_stream
from .lockcheck import verify

SPAN = (confirm.CONFIRMATION[0], confirm.CONTAMINATED[1])
WINDOWS = {"in_sample": confirm.IN_SAMPLE, "confirmation": confirm.CONFIRMATION, "contaminated": confirm.CONTAMINATED}
MIN_FREE_MB = 1024

LABEL = (
    "2021-10-01 .. 2022-10-12 is a HISTORICAL CONFIRMATION PERIOD THAT PREDATES THE DISCOVERY DATA "
    "(2022-10-13 onwards). It is untouched by the earlier search, but it is a backward out-of-sample "
    "check, not a forward chronological test."
)
NON_CONFIRMATION = (
    "A hypothesis that is not CONFIRMED means no edge was established under the pre-registered test. "
    "It does not prove that no edge exists: the test can only detect effects above its minimum "
    "detectable effect."
)


class MemoryGuard(RuntimeError):
    pass


def mem_available_mb() -> float | None:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


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
    if isinstance(o, np.str_):
        return str(o)
    return o


def day(d: int) -> str:
    return str(confirm.EPOCH + dt.timedelta(days=int(d)))


def _in(dates: np.ndarray, window) -> np.ndarray:
    return (dates >= confirm.dnum(window[0])) & (dates <= confirm.dnum(window[1]))


# ---------------------------------------------------------------- integrity


def integrity(insts) -> tuple[dict, list[dict]]:
    """Per-instrument integrity summary and the list of anomalous sessions."""
    nifty = next((i for i in insts if i.name == "NIFTY"), None)
    cal = nifty.sessions["date"][np.isin(nifty.sessions["status"], (KEPT, KEPT_FILLED))] if nifty else None
    out, anomalies = {}, []
    for i in insts:
        a = i.sessions
        rec = {"kind": i.kind, "fetch_status": i.status} | i.audit
        if len(a.get("date", [])) == 0:
            rec["note"] = "no rows in the span (not listed, no security id, or fetch failed)"
            out[i.name] = rec
            continue
        dates = a["date"].astype(np.int64)
        st = a["status"]
        if cal is not None and i.name != "NIFTY":
            lo, hi = dates.min(), dates.max()
            expected = cal[(cal >= lo) & (cal <= hi)]
            missing = np.setdiff1d(expected, dates)
            rec["sessions_missing_vs_nifty_calendar"] = len(missing)
            rec["missing_session_dates"] = [day(d) for d in missing[:50]]
            rec["sessions_not_in_nifty_calendar"] = int(np.setdiff1d(dates, cal).size)
            rec["first_session_with_rows"] = day(lo)
        rec["per_window"] = {}
        for w, span in WINDOWS.items():
            m = _in(dates, span)
            rec["per_window"][w] = {
                "sessions_with_rows": int(m.sum()),
                "kept": int(np.isin(st[m], (KEPT, KEPT_FILLED)).sum()),
                "excluded": {s: int((st[m] == s).sum()) for s in np.unique(st[m]) if s not in (KEPT, KEPT_FILLED)},
                "rows_from_1530_sessions": int((a["post_1530"][m] > 0).sum()),
            }
        bad = (
            ~np.isin(st, (KEPT,))
            | (a["dup_identical"] > 0)
            | (a["dup_conflict"] > 0)
            | (a["off_minute"] > 0)
            | (a["pre_0915"] > 0)
        )
        for k in np.nonzero(bad)[0]:
            anomalies.append(
                {"instrument": i.name} | {c: _clean(a[c][k]) for c in AUDIT_COLS} | {"date": day(dates[k])}
            )
        out[i.name] = rec
    return out, anomalies


def write_session_audit(path, insts) -> None:
    with gzip.open(path, "wt", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(("instrument", *AUDIT_COLS))
        for i in insts:
            a = i.sessions
            for k in range(len(a.get("date", []))):
                w.writerow([i.name, day(a["date"][k])] + [_clean(a[c][k]) for c in AUDIT_COLS[1:]])


# ---------------------------------------------------------------- analysis


def dependence(trades) -> dict:
    """Per hypothesis: trades per week, per-trade SD and same-week correlation (one-way ICC)."""
    out = {}
    for h in prereg.SIGNALS:
        g = np.array([t["gross"] for t in trades if t["hypothesis"] == h])
        if len(g) < 30:
            out[h] = {"trades": len(g)}
            continue
        wk = np.array([(t["date"] + 3) // 7 for t in trades if t["hypothesis"] == h])
        weeks, inv, cnt = np.unique(wk, return_inverse=True, return_counts=True)
        mu = np.bincount(inv, weights=g) / cnt
        within = ((g - mu[inv]) ** 2).sum() / max(1, len(g) - len(weeks))
        m0 = (len(g) - (cnt**2).sum() / len(g)) / max(1, len(weeks) - 1)
        between = (cnt * (mu - g.mean()) ** 2).sum() / max(1, len(weeks) - 1)
        s2b = max(0.0, (between - within) / max(m0, 1e-9))
        rho = s2b / (s2b + within) if s2b + within > 0 else 0.0
        out[h] = {
            "trades": len(g),
            "weeks": len(weeks),
            "trades_per_week": len(g) / len(weeks),
            "sigma_pct": float(g.std(ddof=1)),
            "rho_same_week": float(rho),
            "mde_pct_45_weeks": power.mde(float(g.std(ddof=1)), rho, len(g) / len(weeks), 45),
        }
    return out


def per_stock(trades, window) -> list[dict]:
    rows = {}
    for t in trades:
        r = rows.setdefault(
            (t["hypothesis"], t["stock"]), {"n": 0, "gross": 0.0} | dict.fromkeys(confirm.COST_SCENARIOS, 0.0)
        )
        r["n"] += 1
        r["gross"] += t["gross"]
        for c in confirm.COST_SCENARIOS:
            r[c] += t[f"net_{c}"]
    return [
        {"window": window, "hypothesis": h, "stock": s, "trades": r["n"], "mean_gross": r["gross"] / r["n"]}
        | {f"mean_net_{c}": r[c] / r["n"] for c in confirm.COST_SCENARIOS}
        for (h, s), r in sorted(rows.items())
    ]


def per_month(trades) -> list[dict]:
    rows = {}
    for t in trades:
        k = (t["hypothesis"], day(t["date"])[:7])
        r = rows.setdefault(k, [0, 0.0, 0.0])
        r[0] += 1
        r[1] += t["gross"]
        r[2] += t["net_base"]
    return [
        {"hypothesis": h, "month": m, "trades": n, "mean_gross": g / n, "mean_net_base": b / n, "sum_net_base": b}
        for (h, m), (n, g, b) in sorted(rows.items())
    ]


def _json(path, obj) -> None:
    with open(path, "w") as fh:
        json.dump(_clean(obj), fh, indent=1)


def _csv(path, rows, cols=None):
    cols = cols or (list(rows[0]) if rows else ["empty"])
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for r in rows:
            w.writerow([_clean(r.get(c)) for c in cols])


def analyse(insts, sector_of, log) -> dict:
    res: dict = {"windows": {}, "eligibility": {}}
    trades_by = {}
    for w in ("in_sample", "confirmation", "contaminated"):
        span = WINDOWS[w]
        prepared, report = confirm.prepare(insts, sector_of, span[0], span[1])
        trades = confirm.run(prepared)
        del prepared
        trades_by[w] = trades
        res["eligibility"][w] = report
        log(f"{w}: {sum(1 for r in report.values() if r['eligible'])} eligible instruments, {len(trades)} trades")
    is_gross = {}
    for h in prereg.SIGNALS:
        g = [t["gross"] for t in trades_by["in_sample"] if t["hypothesis"] == h]
        is_gross[h] = float(np.mean(g)) if g else None
    res["in_sample_gross_mean_pct"] = is_gross
    res["in_sample_dependence"] = dependence(trades_by["in_sample"])
    res["verdicts"] = confirm.evaluate(trades_by["confirmation"], is_gross)
    res["descriptive"] = {
        "in_sample": confirm.evaluate(trades_by["in_sample"]),
        "contaminated": confirm.evaluate(trades_by["contaminated"]),
    }
    res["per_stock"] = [r for w, tr in trades_by.items() for r in per_stock(tr, w)]
    res["per_month"] = per_month(trades_by["confirmation"])
    return res


# ---------------------------------------------------------------- report


def report_md(summary: dict, integ: dict) -> str:
    L = [
        "# Pre-registered confirmation run",
        "",
        f"- Status: **{summary['status']}**",
        f"- Pre-registration lock: **{'intact' if summary['lock']['ok'] else 'CHANGED'}**",
        f"- Stream: {summary['meta']}",
        "",
        f"> {LABEL}",
        "",
    ]
    if summary["status"] != "COMPLETE":
        L += [f"No verdicts: {summary.get('reason')}", ""]
    else:
        L += [
            "## Verdicts (confirmation window, base cost, Holm across six)",
            "",
            "| | Verdict | Trades | Weeks | Stocks | Mean net base % | 90% CI | Holm p | Net zero / low / high % | Failed checks |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for h, r in summary["verdicts"].items():
            if "mean_net" not in r:
                L.append(f"| {h} | {r['verdict']} | {r['trades']} | | | | | | | {r.get('reason', '')} |")
                continue
            mn = r["mean_net"]
            failed = ", ".join(k for k, v in r["checks"].items() if not v)
            L.append(
                f"| {h} | {r['verdict']} | {r['trades']} | {r['weeks']} | {r['instruments']} | {mn['base']:.4f} | "
                f"{r['ci90_base'][0]:.4f} .. {r['ci90_base'][1]:.4f} | {r['p_holm']:.4f} | "
                f"{mn['zero']:.4f} / {mn['low']:.4f} / {mn['high']:.4f} | {failed} {r.get('reason', '')} |"
            )
        L += ["", NON_CONFIRMATION, ""]
    L += [
        "## Known biases that the verdicts cannot remove",
        "",
        "- **Survivorship:** the universe is today's top-99 F&O names (chosen 2026-10-12). In 2021-22 it "
        "over-represents stocks that later did well. H5 (drift) is capped at INCONCLUSIVE for this reason; "
        "H1-H4 and H6 trade both sides, so the bias is smaller but not zero.",
        "- **Corporate actions:** Dhan's 1-minute history is not known to be split/bonus adjusted. Sessions "
        "with an index-adjusted overnight gap of 8% or more are flagged and +-5 sessions around them are "
        "excluded (integrity.py). A stock with flags inside a window has per-stock results marked "
        "CA_UNCERTAIN; per-stock results are descriptive only in any case.",
        "- Instruments with fewer than 150 complete sessions in a window are NOT_APPLICABLE there, never failures.",
        "",
        "## Data integrity",
        "",
        "| Instrument | Sessions with rows | Kept (complete / filled) | Excluded | Missing vs NIFTY calendar | Missing 09:15 | Rows from 15:30 | Dup. identical / conflicting |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for n, r in integ.items():
        g = r.get("integrity")
        if not g:
            L.append(f"| {n} | 0 | | | | | | {r.get('note', '')} |")
            continue
        exc = ", ".join(f"{k.replace('EXCLUDED_', '')} {v}" for k, v in g["sessions_excluded"].items() if v) or "0"
        L.append(
            f"| {n} | {g['sessions_with_rows']} | {g['sessions_kept_complete']} / {g['sessions_kept_filled']} | {exc} | "
            f"{r.get('sessions_missing_vs_nifty_calendar', '-')} | {g['sessions_missing_0915']} | "
            f"{g['sessions_with_rows_from_1530']} | {g['duplicate_timestamps_identical']} / {g['duplicate_timestamps_conflicting']} |"
        )
    return "\n".join(L) + "\n"


# ---------------------------------------------------------------- main


def main(argv=None, sector_of=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    t0 = time.time()

    def log(m):
        print(f"[{time.time() - t0:7.1f}s] {m}", file=sys.stderr, flush=True)

    os.makedirs(a.out, exist_ok=True)
    lock = verify()
    summary: dict = {"label": LABEL, "lock": lock, "meta": {}, "status": "STARTED"}
    if not lock["ok"]:
        summary.update(status="REFUSED", reason="pre-registered files differ from prereg.lock")
        _json(os.path.join(a.out, "summary.json"), summary)
        log("pre-registration changed: refusing to run")
        return 3
    log("pre-registration lock verified")

    def guard(inst):
        mem = mem_available_mb()
        log(
            f"parsed {inst.name}: {inst.audit.get('sessions_kept', 0)} sessions kept; runner MemAvailable {mem or 0:.0f} MB"
        )
        if mem is not None and mem < MIN_FREE_MB:
            raise MemoryGuard(f"runner MemAvailable {mem:.0f} MB < {MIN_FREE_MB} MB")

    span = (confirm.dnum(SPAN[0]), confirm.dnum(SPAN[1]))
    insts, reason = [], None
    try:
        if a.stream == "-":
            meta, insts = read_stream(sys.stdin.buffer, span[0], span[1], guard)
        else:
            with open(a.stream, "rb") as fh:
                meta, insts = read_stream(fh, span[0], span[1], guard)
    except MemoryGuard as exc:
        meta, reason = {}, str(exc)
    summary["meta"] = meta
    if reason is None and meta.get("stopped"):
        reason = f"fetch stopped: {meta['stopped']}"
    elif reason is None and not meta.get("complete"):
        reason = "stream ended before END"
    elif reason is None and int(meta.get("interval", 0)) != 1:
        reason = "the pre-registered execution needs 1-minute candles"
    if sector_of is None:
        from sector_taxonomy import sector_for_symbol as sector_of
    integ, anomalies = integrity(insts)
    _json(os.path.join(a.out, "integrity_summary.json"), _clean(integ))
    _csv(os.path.join(a.out, "session_anomalies.csv"), anomalies, ["instrument", *AUDIT_COLS])
    write_session_audit(os.path.join(a.out, "session_audit.csv.gz"), insts)
    summary["instruments_streamed"] = len(insts)
    summary["sessions_audited"] = int(sum(len(i.sessions.get("date", [])) for i in insts))
    summary["anomalous_sessions"] = len(anomalies)
    if reason is not None:
        summary.update(status="ABORTED", reason=reason)
        log(f"ABORTED: {reason} (integrity outputs written, no verdicts)")
    else:
        with np.errstate(all="ignore"):
            import warnings

            warnings.simplefilter("ignore")
            res = analyse(insts, sector_of, log)
        ca = {
            n: r["corporate_action_flags"]
            for n, r in res["eligibility"]["confirmation"].items()
            if r.get("eligible") and r["corporate_action_flags"]
        }
        for r in res["per_stock"]:
            elig = res["eligibility"][r["window"]].get(r["stock"], {})
            r["applicability"] = "CA_UNCERTAIN" if elig.get("corporate_action_flags") else "DESCRIPTIVE"
        summary.update(
            status="COMPLETE",
            windows={k: [str(v[0]), str(v[1])] for k, v in WINDOWS.items()},
            verdicts=res["verdicts"],
            in_sample_gross_mean_pct=res["in_sample_gross_mean_pct"],
            in_sample_dependence=res["in_sample_dependence"],
            descriptive=res["descriptive"],
            not_applicable_instruments={
                w: {n: r["reason"] for n, r in rep.items() if not r["eligible"]}
                for w, rep in res["eligibility"].items()
            },
            corporate_action_flags_confirmation=ca,
            cost_scenarios_pct={
                c: {"fees": s.fees_pct, "slippage_per_market_fill": s.slip_pct, "spread_multiple": s.spread_mult}
                for c, s in confirm.COST_SCENARIOS.items()
            },
            note=NON_CONFIRMATION,
        )
        _json(os.path.join(a.out, "eligibility.json"), _clean(res["eligibility"]))
        _csv(os.path.join(a.out, "per_stock.csv"), res["per_stock"])
        _csv(os.path.join(a.out, "per_month.csv"), res["per_month"])
        log("verdicts: " + ", ".join(f"{h} {r['verdict']}" for h, r in res["verdicts"].items()))
    _json(os.path.join(a.out, "summary.json"), summary)
    with open(os.path.join(a.out, "REPORT.md"), "w") as fh:
        fh.write(report_md(summary, integ))
    log(f"written to {a.out}")
    return 0 if summary["status"] == "COMPLETE" else 4


if __name__ == "__main__":
    sys.exit(main())
