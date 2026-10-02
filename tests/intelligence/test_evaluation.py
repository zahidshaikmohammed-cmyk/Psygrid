"""The evaluation harness must find a real delayed response and must not find one where none exists."""

import numpy as np
import pytest

from intelligence.evaluation import benjamini_hochberg, evaluate_day, spearman, summarise
from intelligence.response import SessionReturns, fit

N = 120
KEYS = tuple(f"S{i:03d}" for i in range(N))


def market_day(seed: int, delayed: bool, minutes: int = 360) -> SessionReturns:
    rng = np.random.default_rng(seed)
    m = rng.normal(0, 0.0015, minutes)
    r = rng.normal(0, 0.0006, (N, minutes))
    lag_share = np.linspace(0, 0.6, N) if delayed else np.zeros(N)  # stocks differ in how late they respond
    for i in range(N):
        late = np.concatenate([[0, 0], m[:-2]])
        r[i] += (1 - lag_share[i]) * m + lag_share[i] * late
    close = 100 * np.exp(np.cumsum(r, axis=1))
    return SessionReturns(f"d{seed}", KEYS, np.arange(minutes) * 60, r, np.full((N, minutes), 1e3), close, m, "t")


@pytest.fixture(scope="module")
def results():
    out = {}
    for delayed in (True, False):
        model = fit([market_day(s, delayed) for s in range(10)], KEYS)
        out[delayed] = summarise([evaluate_day(model, market_day(100 + s, delayed), every=10, seed=s)
                                  for s in range(8)])  # fmt: skip
    return out


def test_finds_a_real_delayed_response(results):
    report = results[True]
    pending = report["signals"]["pending_response@1m"]
    assert pending["mean_ic"] > 0.1 and pending["t"] > 3 and pending["hit_rate"] == 1.0
    assert pending["verdict"] == "SUPPORTED"
    assert "pending_response@1m[all]" in report["fdr_survivors"]
    assert abs(pending["placebo_time_ic"]) < 0.05  # the shuffled-time placebo finds nothing
    assert report["signals"]["pending_response@30m"]["mean_ic"] < pending["mean_ic"]  # it decays with horizon


def test_finds_nothing_where_nothing_exists(results):
    report = results[False]
    for key, entry in report["signals"].items():
        assert entry["verdict"] == "UNPROVEN", key
        assert entry["mean_ic"] is None or abs(entry["mean_ic"]) < 0.05, key


def test_helpers():
    a = np.arange(100.0)
    assert spearman(a, a) == pytest.approx(1.0) and spearman(a, -a) == pytest.approx(-1.0)
    assert np.isnan(spearman(a[:10], a[:10]))  # too few names
    assert benjamini_hochberg([0.001, 0.02, 0.04, 0.5], q=0.05) == [True, True, False, False]
    assert summarise([])["status"] == "INSUFFICIENT_HISTORY"
