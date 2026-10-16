"""Behaviour discovery: causality (no look-ahead), outcome timing, inference helpers, and an
end-to-end check that a planted behaviour is found while pure noise is not."""

import io

import numpy as np
import pytest

from research.behaviour import engine
from research.behaviour.data import build, read_stream, write_instrument
from research.behaviour.features import HORIZONS, TRADE_LAST, compute_base, states_events
from research.behaviour.stats import bh, block_bootstrap_ci, cluster_summary, max_drawdown
from research.behaviour.synth import make_rows, market, sessions


def _inst(seed=3, n_days=80, plant=False):
    days = sessions()[:n_days]
    return build("X", "EQUITY", "BANKING", make_rows(seed, days, plant=plant), 5)


def test_stream_roundtrip_and_session_filter():
    days = sessions()[:5]
    rows = make_rows(1, days)
    rows = np.delete(rows, np.arange(75 * 2, 75 * 2 + 10), axis=0)  # session 3 loses 10 of 75 bars -> dropped
    buf = io.BytesIO()
    buf.write(b"META interval=5\n")
    write_instrument(buf, "X", "EQUITY", "-", rows)
    buf.write(b"END\n")
    buf.seek(0)
    meta, insts = read_stream(buf)
    assert meta["complete"] and len(insts) == 1
    x = insts[0]
    assert x.audit["sessions_raw"] == 5 and x.audit["sessions_kept"] == 4
    assert x.C.shape == (4, 75)


def test_states_and_events_never_use_future_bars():
    inst = _inst()
    b = compute_base(inst)
    cond, ev, disc = states_events(b, None, False)
    d0, t0 = 60, 30
    for arr in (inst.O, inst.H, inst.L, inst.C, inst.V):
        rng = np.random.default_rng(5)
        arr[d0, t0 + 1 :] *= 1 + rng.normal(0, 0.05, size=arr[d0, t0 + 1 :].shape)
    inst.H[d0] = np.maximum.reduce([inst.H[d0], inst.O[d0], inst.C[d0]])
    inst.L[d0] = np.minimum.reduce([inst.L[d0], inst.O[d0], inst.C[d0]])
    b2 = compute_base(inst)
    cond2, ev2, disc2 = states_events(b2, None, False)
    for k in cond:
        assert np.array_equal(cond[k][d0, : t0 + 1], cond2[k][d0, : t0 + 1]), k
        assert np.array_equal(cond[k][:d0], cond2[k][:d0]), k
    for k in ev:
        assert np.array_equal(ev[k][d0, : t0 + 1], ev2[k][d0, : t0 + 1]), k
    for k in disc:
        a, c = disc[k], disc2[k]
        if a.ndim == 2:
            assert np.array_equal(a[d0, : t0 + 1], c[d0, : t0 + 1]), k


def test_outcomes_enter_next_open_and_exit_by_1515():
    inst = _inst()
    b = compute_base(inst)
    d, t = 40, 10
    assert b.F[1, d, t] == pytest.approx(inst.C[d, t + 6] / inst.O[d, t + 1] - 1)
    assert b.F[3, d, t] == pytest.approx(inst.C[d, 71] / inst.O[d, t + 1] - 1)
    assert np.isnan(b.F[:, d, TRADE_LAST + 1]).all()
    assert b.MFE[0, d, t] == pytest.approx(inst.H[d, t + 1 : t + 4].max() / inst.O[d, t + 1] - 1)
    assert HORIZONS[-1] == 0


def test_cluster_t_and_bh():
    rng = np.random.default_rng(0)
    x = rng.normal(0.5, 1, size=400)
    g = np.repeat(np.arange(80), 5)
    s = cluster_summary(x, g)
    assert s["n"] == 400 and s["weeks"] == 80 and s["t"][0] > 5
    p = np.r_[np.full(95, 0.5), [1e-6, 1e-5, 1e-4, 0.002, 0.3]]
    assert bh(p, 0.10).sum() == 4
    lo, hi = block_bootstrap_ci(x, g)
    assert lo < x.mean() < hi
    assert max_drawdown(np.array([1, -2, -1, 3])) == 3


def test_planted_behaviour_is_validated_and_noise_is_not():
    import warnings

    from research.behaviour.synth import universe

    warnings.simplefilter("ignore")
    with np.errstate(all="ignore"):
        prep = engine.prepare_all(universe(n_noise=2), log=lambda m: None)
        results = [engine.evaluate(prep[n]) for n in prep]
    fin = engine.finalize(results)
    tiers = fin["profit_catalogue"]
    validated = [r for r in tiers if r["tier"] == "VALIDATED_EDGE"]
    assert any(r["stock"] == "PLANT" and r["side"] == "long" for r in validated)
    assert not [r for r in tiers if r["stock"].startswith("NOISE") and r["tier"] == "VALIDATED_EDGE"]
    rec = [r for r in fin["rows"] if r["recurring"]]
    assert any(r["stock"] == "PLANT" for r in rec)


def test_market_reference_required():
    _, _, insts = market(n_days=60)
    with pytest.raises(RuntimeError):
        engine.prepare_all([i for i in insts if i.name != "NIFTY"] + [_inst()], log=lambda m: None)
