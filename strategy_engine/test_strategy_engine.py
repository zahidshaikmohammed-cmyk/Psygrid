from datetime import datetime, timedelta

import pytest

from strategy_engine.engine import StrategyEngine
from strategy_engine.math_utils import aggregate, directional_efficiency, session_vwap
from strategy_engine.models import Candle, Features


def candles_from_closes(closes, volume=1000):
    start = datetime(2026, 1, 2, 9, 15)
    rows = []
    prev = closes[0]
    for i, close in enumerate(closes):
        ts = start + timedelta(minutes=i)
        o = prev if i else close
        rows.append(Candle(ts, o, max(o, close) + 0.10, min(o, close) - 0.10, close, volume))
        prev = close
    return rows


def test_session_aggregation_starts_at_0915():
    rows = candles_from_closes([100, 101, 102, 103, 104, 105])
    five = aggregate(rows, 5)
    assert len(five) == 2
    assert five[0].open == 100
    assert five[0].close == 104
    assert five[0].volume == 5000


def test_vwap_is_volume_weighted_typical_price():
    rows = candles_from_closes([100, 110], volume=100)
    v = session_vwap(rows)
    expected0 = (99.9 + 100.1 + 100) / 3
    expected1 = (109.9 + 110.1 + 110) / 3
    assert v[0] == pytest.approx(expected0)
    assert v[1] == pytest.approx((expected0 * 100 + expected1 * 100) / 200)


def test_directional_efficiency_is_one_for_monotonic_closes():
    rows = candles_from_closes([100, 101, 102, 103])
    assert directional_efficiency(rows) == pytest.approx(1.0)


def test_directional_efficiency_is_low_for_chop():
    rows = candles_from_closes([100, 102, 100, 102, 100])
    assert directional_efficiency(rows) < 0.2


def test_engine_freezes_and_does_not_recompute_ranking():
    features = [
        Features("AAA", "LONG", 0, 0, .02, 2, .9, .52, .5, .03, .02, 101, .1, .5, 1, 2, .5, .2, .9, 0, 90),
        Features("BBB", "LONG", 0, 0, .02, 2, .9, .52, .5, .02, .01, 101, .1, .5, 1, 2, .5, .2, .9, 0, 80),
    ]
    engine = StrategyEngine()
    ranked = engine.freeze(features)
    assert ranked[0].symbol == "AAA"
    features[0].score = -999
    assert engine.rank_frozen()[0].symbol == "AAA"


def test_extreme_gap_is_hard_ineligible():
    from strategy_engine.features import build_features
    rows = candles_from_closes([100, 102, 104, 106, 108, 110])
    f = build_features("AAA", rows, 90, 0.0, 0.0, [0.0] * 20, "LONG")
    assert not f.eligible
    assert "EXTREME_GAP" in f.reasons


def test_shallow_retracement_is_hard_ineligible():
    from strategy_engine.features import build_features
    rows = candles_from_closes([100, 102, 104, 106, 108, 109])
    f = build_features("AAA", rows, 100, 0.0, 0.0, [0.0] * 20, "LONG")
    assert not f.eligible
    assert "SHALLOW_RETRACEMENT" in f.reasons


def test_forced_signal_returns_a_signal_when_all_candidates_are_ineligible():
    bad = Features("BAD", "LONG", .04, 5, .02, 1, .5, .1, 1, 0, 0, 100, 0, 0, 1, 1, 0, 1, .5, 100, -10, False, ["EXTREME_GAP"])
    signal = StrategyEngine().generate_signal([bad], {"BAD": 100.0}, datetime(2026, 1, 2, 9, 31))
    assert signal.symbol == "BAD"
    assert signal.entry == 100.0
    assert signal.stop_loss < signal.entry
    assert signal.target > signal.entry
