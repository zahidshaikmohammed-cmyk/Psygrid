"""Pre-registered confirmation: frozen definitions, causality, chronology, execution and cost
application, corporate actions, multiple testing and decision rules. Synthetic data only — these
tests prove the machinery, not any market edge."""

import datetime as dt
import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from research.behaviour import confirm, power, prereg
from research.behaviour.data import build
from research.behaviour.execution import EXIT_MIN, LATENCY_MIN, Costs, Signal, simulate
from research.behaviour.integrity import corporate_action_flags, exclusion_mask
from research.behaviour.synth import make_rows_1m, sessions

ROOT = Path(__file__).resolve().parents[1]
LOCKED = ["prereg.py", "execution.py", "integrity.py", "confirm.py", "power.py"]


def test_definitions_are_frozen():
    lock = json.loads((ROOT / "research/behaviour/prereg.lock").read_text())
    for f in LOCKED:
        h = hashlib.sha256((ROOT / "research/behaviour" / f).read_bytes()).hexdigest()
        assert lock[f] == h, f"{f} changed after pre-registration"
    assert set(prereg.SIGNALS) == set(prereg.HYPOTHESES) == {"H1", "H2", "H3", "H4", "H5", "H6"}


def test_windows_are_disjoint_and_ordered():
    c, i, k = confirm.CONFIRMATION, confirm.IN_SAMPLE, confirm.CONTAMINATED
    assert c[0] < c[1] < i[0] < i[1] < k[0] < k[1]


def _universe(n_days=200, seed=0, n_stocks=4, start=dt.date(2021, 10, 1)):
    days = [d for d in sessions(start, start + dt.timedelta(days=n_days * 1.5)) if d.weekday() < 5][:n_days]
    mk = np.random.default_rng(seed).normal(0, 0.0003, size=(len(days), 375))
    insts = [build("NIFTY", "INDEX", "INDEX", make_rows_1m(1, days, mk, sigma=0.0002, volume=False), 1)]
    for k in range(n_stocks):
        insts.append(build(f"BANK{k}", "EQUITY", "BANKING", make_rows_1m(10 + k + seed, days, mk), 1))
    return days, insts


def _signals(inst, nifty, upto=None):
    n_pc = np.r_[np.nan, nifty.C[:-1, -1]]
    D = len(inst.dates)
    pc = np.r_[np.nan, inst.C[:-1, -1]]
    ctx = prereg.build_ctx(
        inst.O,
        inst.H,
        inst.L,
        inst.C,
        inst.V,
        pc,
        np.zeros(inst.C.shape),
        nifty.C / n_pc[:, None] - 1,
        nifty.C / prereg._lag(nifty.C, 6, np.nan) - 1,
        inst.C[:, -1] / pc - 1 - (nifty.C[:, -1] / n_pc - 1),
        np.ones(D, bool),
    )
    return {h: fn(ctx) for h, fn in prereg.SIGNALS.items()}


def test_signals_never_use_future_bars():
    _, insts = _universe(n_days=120)
    nifty, x = insts[0], insts[1]
    base = _signals(x, nifty)
    d0, t0 = 90, 40
    rng = np.random.default_rng(3)
    for arr in (x.O, x.H, x.L, x.C, x.V):
        arr[d0, t0 + 1 :] *= 1 + rng.normal(0, 0.03, size=arr[d0, t0 + 1 :].shape)
    x.H[d0] = np.maximum.reduce([x.H[d0], x.O[d0], x.C[d0]])
    x.L[d0] = np.minimum.reduce([x.L[d0], x.O[d0], x.C[d0]])
    after = _signals(x, nifty)

    def early(sigs):
        return {(s.d, s.t, s.side, round(s.stop or 0, 8)) for s in sigs if s.d < d0 or (s.d == d0 and s.t <= t0)}

    for h in base:
        assert early(base[h]) == early(after[h]), h
    assert sum(len(v) for v in base.values()) > 0


def _day(n=375, px=100.0):
    m = np.full((n, 5), px, float)
    m[:, 4] = 1000
    return m


COST = Costs(0.05, 0.02, 1.0)


def test_entry_latency_stop_first_and_trade_through():
    m = _day()
    e = 5 * (10 + 1) + LATENCY_MIN
    m[e, 0] = 100.0
    m[e + 3, 1], m[e + 3, 2] = 103.1, 98.5  # same minute touches stop (99) and target (101.5): stop wins
    r = simulate(m, Signal(0, 10, 1, 99.0, 1.5, 60), 0.03, COST)
    assert r["entry_min"] == e and r["reason"] == "stop" and r["gross"] == pytest.approx(-1.0)
    m2 = _day()
    m2[e + 2, 1] = 101.5  # touches the target exactly: not filled
    m2[e + 4, 1] = 101.6
    r2 = simulate(m2, Signal(0, 10, 1, 99.0, 1.5, 60), 0.03, COST)
    assert r2["reason"] == "target" and r2["exit_min"] == e + 4 and r2["gross"] == pytest.approx(1.5)
    assert r2["market_fills"] == 1 and r2["net"] == pytest.approx(1.5 - 0.05 - 0.02 - 0.03)


def test_gap_through_stop_time_exit_and_skip():
    m = _day()
    e = 5 * 11 + LATENCY_MIN
    m[e + 1, 0], m[e + 1, 2] = 98.0, 97.9  # opens through the 99 stop: fills at 98
    r = simulate(m, Signal(0, 10, 1, 99.0, 1.5, 60), 0.0, COST)
    assert r["reason"] == "stop" and r["gross"] == pytest.approx(-2.0)
    m2 = _day()
    m2[:, 3] = 100.2
    r2 = simulate(m2, Signal(0, 10, -1, None, None, 30), 0.0, COST)
    assert r2["reason"] == "time" and r2["exit_min"] == e + 29 and r2["market_fills"] == 2
    assert r2["net"] == pytest.approx(-0.2 - 0.05 - 0.04)
    r3 = simulate(_day(), Signal(0, 10, 1, 100.5, 1.5, 60), 0.0, COST)
    assert r3 == {"skipped": True}
    assert simulate(_day(), Signal(0, 69, 1, None, None, None), 0.0, COST) is None  # after 15:00
    r4 = simulate(_day(), Signal(0, 2, 1, None, None, None), 0.0, COST)
    assert r4["entry_min"] == 16 and r4["exit_min"] == EXIT_MIN


def test_corporate_action_flags_and_exclusion():
    pc = np.array([np.nan, 100, 100, 100, 100.0])
    o = np.array([100, 50.5, 103, 91, 100.0])  # 1:2 split, ordinary 3% gap, market-wide -9% gap, normal
    idx_gap = np.array([0, 0, 0, -9.0, 0])
    flag, split = corporate_action_flags(o, pc, idx_gap)
    assert flag.tolist() == [False, True, False, False, False]
    assert split.tolist() == [False, True, False, False, False]
    ex = exclusion_mask(np.array([False] * 6 + [True] + [False] * 8), 5)
    assert ex.tolist() == [False] + [True] * 11 + [False] * 3


def test_holm_and_decision_rules():
    assert confirm.holm({"a": 0.01, "b": 0.04, "c": 0.03}) == pytest.approx({"a": 0.03, "c": 0.06, "b": 0.06})
    rng = np.random.default_rng(1)
    start = confirm.dnum(dt.date(2021, 11, 1))

    def mk(h, mean, n=600, stocks=20, one_stock=False):
        out = []
        for k in range(n):
            g = rng.normal(mean, 0.3)
            row = {
                "hypothesis": h,
                "stock": "S0" if one_stock and k % 2 else f"S{k % stocks}",
                "date": start + (k * 3) % 330,
                "side": 1,
                "reason": "time",
                "gross": g,
                "spread_pct": 0.03,
                "market_fills": 2,
            }
            for c, cs in confirm.COST_SCENARIOS.items():
                row[f"net_{c}"] = g - cs.fees_pct - 2 * cs.slip_pct - cs.spread_mult * 0.03
            out.append(row)
        return out

    trades = mk("H1", 0.6) + mk("H2", -0.2) + mk("H3", 0.6, one_stock=True) + mk("H5", 0.6) + mk("H6", 0.6, n=50)
    res = confirm.evaluate(trades, in_sample_gross=dict.fromkeys(prereg.SIGNALS, 0.1))
    assert res["H1"]["verdict"] == "CONFIRMED"
    assert res["H2"]["verdict"] == "REJECTED"
    assert res["H3"]["verdict"] == "INCONCLUSIVE" and not res["H3"]["checks"]["not_concentrated"]
    assert res["H4"]["verdict"] == "NOT_APPLICABLE" and res["H6"]["verdict"] == "NOT_APPLICABLE"
    assert res["H5"]["verdict"] == "INCONCLUSIVE"  # survivorship cap
    no_is = confirm.evaluate(mk("H1", 0.6))
    assert no_is["H1"]["verdict"] == "INCONCLUSIVE"  # in-sample direction check is required


def test_cost_scenarios_are_monotonic_and_null_is_not_confirmed():
    days, insts = _universe(n_days=200, seed=5)
    import warnings

    warnings.simplefilter("ignore")
    prepared, report = confirm.prepare(insts, lambda s: "BANKING", days[0], days[-1])
    assert all(r["eligible"] for r in report.values()) and len(prepared) == 4
    trades = confirm.run(prepared)
    assert trades
    for t in trades:
        assert t["net_zero"] >= t["net_low"] >= t["net_base"] >= t["net_high"]
        assert t["net_zero"] == pytest.approx(t["gross"])
    first_allowed = min(np.nonzero(c.allowed)[0][0] for c, *_ in prepared.values())
    assert first_allowed >= prereg.WARM
    res = confirm.evaluate(trades, in_sample_gross=dict.fromkeys(prereg.SIGNALS, 0.1))
    assert not [h for h, r in res.items() if r["verdict"] == "CONFIRMED"]


def test_power_formula():
    a = power.mde(0.4, 0.2, 30, 50)
    assert power.mde(0.4, 0.2, 30, 100) < a < power.mde(0.4, 0.4, 30, 50)
    assert power.se(1.0, 0.0, 1.0, 1) == pytest.approx(1.0)
