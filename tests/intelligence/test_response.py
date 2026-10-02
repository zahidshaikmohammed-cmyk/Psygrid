"""Expected-response engine: known-answer recovery, pending response, gaps, and no look-ahead."""

import numpy as np
import pytest

from intelligence.response import LAGS, SessionReturns, evaluate_state, fit, model_for, session_returns

KEYS = ("LAGGY", "FAST", "TCS", "INFY", "WIPRO", "HCLTECH", *[f"X{i:02d}" for i in range(30)])


def synthetic(seed: int, minutes: int = 360, jump_at: int | None = None) -> SessionReturns:
    rng = np.random.default_rng(seed)
    m = rng.normal(0, 0.001, minutes)
    if jump_at is not None:
        m[jump_at] = 0.01
    n = len(KEYS)
    r = rng.normal(0, 0.0005, (n, minutes))
    lag2 = np.concatenate([[0, 0], m[:-2]])
    r[0] += 0.6 * m + 0.4 * lag2  # LAGGY: 40% of its response arrives two minutes late
    r[1] += 1.0 * m  # FAST: immediate
    it = rng.normal(0, 0.0008, minutes)
    for i in range(2, 6):
        r[i] += 0.9 * m + it  # an IT block with a shared sector shock
    for i in range(6, n):
        r[i] += rng.uniform(0.5, 1.5) * m
    volume = np.full((n, minutes), 1000.0)
    close = 100 * np.exp(np.cumsum(r, axis=1))
    return SessionReturns(f"2026-09-{seed:02d}", KEYS, np.arange(minutes) * 60, r, volume, close, m, "test")


@pytest.fixture(scope="module")
def model():
    return fit([synthetic(s) for s in range(1, 13)], KEYS)


def test_recovers_the_lag_structure(model):
    laggy, fast = model.beta_market[0], model.beta_market[1]
    assert laggy[0] == pytest.approx(0.6, abs=0.05) and laggy[2] == pytest.approx(0.4, abs=0.05)
    assert abs(laggy[1]) < 0.05 and abs(laggy[3:]).max() < 0.05
    assert fast[0] == pytest.approx(1.0, abs=0.05) and abs(fast[1:]).max() < 0.05
    delay = model.delay_profile()
    assert delay["delay_index"][0] == pytest.approx(0.4, abs=0.06) and delay["delay_index"][1] < 0.06
    assert delay["half_life"][0] == 0.0  # 60% arrives at once: half the response is immediate
    assert delay["mean_lag"][0] == pytest.approx(0.8, abs=0.12) and delay["mean_lag"][1] < 0.1
    assert model.sectors[2] == "INFORMATION_TECHNOLOGY" and model.sectors[0] is None
    assert model.beta_sector[2].sum() > 0.5  # the IT block's shared shock is picked up by the sector factor


def test_pending_response_after_a_market_jump(model):
    day = synthetic(40, jump_at=200)
    state = evaluate_state(model, day, upto=200)  # just after the jump
    owed = model.beta_market[0, 1] * 0.01 + model.beta_market[0, 2] * 0.01  # lags 1 and 2 still to come
    assert state.pending[0] == pytest.approx(owed, rel=0.25)
    assert state.pending[0] > 0.003 and abs(state.pending[1]) < 0.001  # the fast stock owes nothing
    assert state.pending_sigma[0] > 2


def test_gap_measures_a_stock_that_did_not_respond(model):
    day = synthetic(41)
    day.r[1, 300:315] = day.r[1, 300:315] - day.market[300:315]  # FAST stops following the market
    day.market[300:315] = 0.004  # while the market rises
    state = evaluate_state(model, day, upto=314)
    assert state.gap[1] > 0.04 and state.gap_sigma[1] > 5  # far behind what the market implies
    assert state.observed[1] == 15 and not state.stale[1]
    assert state.market_part[1] == pytest.approx(state.expected[1] - state.sector_part[1] - state.stat_part[1])


def test_no_look_ahead(model):
    day = synthetic(42)
    future_changed = synthetic(42)
    future_changed.r[:, 251:] *= -7.0
    future_changed.market[251:] = 0.05
    a, b = evaluate_state(model, day, upto=250), evaluate_state(model, future_changed, upto=250)
    for name in ("expected", "actual", "gap", "pending", "market_part", "sector_part", "stat_part"):
        np.testing.assert_allclose(getattr(a, name), getattr(b, name), equal_nan=True, err_msg=name)


def test_missing_bars_are_missing_not_zero(model):
    day = synthetic(43)
    day.r[0, 100:115] = np.nan
    state = evaluate_state(model, day, upto=114)
    assert state.observed[0] == 0 and np.isnan(state.gap[0]) and state.stale[0]


def test_trained_only_on_earlier_sessions_and_cached(market_root, store_root):
    from intelligence.archive import load_day
    from tests.intelligence.conftest import DATES, TODAY

    keys = load_day(market_root, TODAY).equity.keys
    model = model_for(market_root, TODAY, keys, train_sessions=20, cache_root=store_root)
    assert model.trained_on == tuple(DATES[:-1]) and LAGS == 5
    again = model_for(market_root, TODAY, keys, train_sessions=20, cache_root=store_root)
    np.testing.assert_array_equal(model.beta_market, again.beta_market)
    assert model_for(market_root, DATES[2], keys, cache_root=store_root) is None  # too little history
    sr = session_returns(load_day(market_root, TODAY))
    state = evaluate_state(model, sr, upto=200)
    assert np.isfinite(state.gap).sum() > 20


def test_a_partial_day_is_aligned_to_the_model_universe(model):
    from intelligence.response import align

    day = synthetic(42)
    early = SessionReturns(day.session_date, day.keys[:10], day.grid[:20], day.r[:10, :20], day.volume[:10, :20],
                           day.close[:10, :20], day.market[:20], "test")  # fmt: skip
    aligned = align(early, model.keys)
    assert aligned.keys == model.keys and np.isnan(aligned.r[10:]).all()
    state = evaluate_state(model, aligned)
    assert state.observed[0] == 15 and state.observed[20] == 0 and state.stale[20]
    assert np.isnan(state.gap[20]) and np.isfinite(state.gap[0])
