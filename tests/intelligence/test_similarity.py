import shutil
from datetime import timedelta

import numpy as np
import pytest

import intelligence.similarity as similarity
from intelligence.archive import load_day
from intelligence.frame import as_of_time, frame_at
from intelligence.similarity import (
    INSTRUMENT_STATE,
    MARKET_OUTCOMES,
    MARKET_STATE,
    _separation,
    instrument_matches,
    load_states,
    market_matches,
    state_series,
)
from intelligence.synthetic import SyntheticMarket, trading_dates
from tests.intelligence.conftest import DATES, INJECTIONS, TODAY


def at(day, minute):
    return frame_at(day, as_of_time(day.session_date, "09:15") + timedelta(minutes=minute + 1))


@pytest.fixture(scope="module")
def day(market_root):
    return load_day(market_root, TODAY)


def test_a_past_minute_state_is_what_a_frame_at_that_minute_showed(market_root, store_root):
    past = load_day(market_root, DATES[3])
    states = load_states(market_root, DATES[3], store_root)
    for minute in (20, 100, 245):
        series = state_series(at(past, minute))
        market = np.stack([series[n] for n in MARKET_STATE], axis=-1)[-1]
        np.testing.assert_allclose(states.market_state[minute], market, rtol=1e-5, equal_nan=True)
        i = states.row("INFY")
        col = list(states.instrument_minutes).index(minute) if minute % 5 == 0 else None
        if col is not None:
            instrument = np.stack([series[n] for n in INSTRUMENT_STATE], axis=-1)[i, -1]
            np.testing.assert_allclose(states.instrument_state[i, col], instrument, rtol=1e-4, equal_nan=True)


def test_past_outcomes_are_the_forward_index_moves(market_root, store_root):
    past = load_day(market_root, DATES[1])
    states = load_states(market_root, DATES[1], store_root)
    close = past.indices.close[past.indices.keys.index("nifty")]
    full = at(past, 359)
    close = full.indices.close[full.indices.keys.index("nifty")]
    assert states.market_outcome[100, 0] == pytest.approx(np.log(close[115] / close[100]), rel=1e-4)
    assert np.isnan(states.market_outcome[350, MARKET_OUTCOMES.index("index_fwd_ret_60m")])  # past the close


def test_market_matches_report_distributions_not_predictions(day, market_root, store_root):
    result = market_matches(at(day, 120), market_root, store_root, k=5)
    assert result["status"] == "OK" and result["scope"] == "MARKET"
    sessions = [m["session_date"] for m in result["matches"]]
    assert len(sessions) == len(set(sessions)) == 5  # one match per session
    assert all(s < TODAY for s in sessions)
    assert all(abs(m["minute_index"] - 120) <= similarity.WINDOW_MINUTES for m in result["matches"])
    assert [m["distance"] for m in result["matches"]] == sorted(m["distance"] for m in result["matches"])
    outcome = result["outcomes"]["index_fwd_ret_30m"]
    assert outcome["matched"]["count"] == 5 and outcome["base_rate"]["count"] == len(DATES) - 1
    assert set(outcome["matched"]["quantiles"]) == {"p10", "p25", "p50", "p75", "p90"}
    assert 0 <= outcome["separation"] <= 1
    assert "FEW_SESSIONS" in result["uncertainty"]["flags"]
    assert "not a forecast" in result["caveat"]


def test_instrument_matches(day, market_root, store_root):
    result = instrument_matches(at(day, 200), "SUNPHARMA", market_root, store_root)
    assert result["status"] == "OK" and result["key"] == "SUNPHARMA"
    assert set(result["outcomes"]) == {"fwd_ret_15m", "fwd_ret_30m", "fwd_ret_60m", "fwd_rel_index_ret_30m"}
    assert instrument_matches(at(day, 200), "NOPE", market_root, store_root)["status"] == "UNKNOWN_INSTRUMENT"


def test_too_little_history_gives_no_distribution(market_root, store_root):
    early = load_day(market_root, DATES[2])
    result = market_matches(at(early, 120), market_root, store_root)
    assert result["status"] == "INSUFFICIENT_HISTORY" and "outcomes" not in result
    assert market_matches(frame_at(early, as_of_time(DATES[2], "09:15")), market_root, store_root)["status"] == (
        "INSUFFICIENT_DATA"
    )


def test_later_sessions_are_never_searched(day, market_root, store_root, tmp_path):
    root = tmp_path / "archive"
    shutil.copytree(market_root, root)
    later = trading_dates(TODAY, 3)[1:]  # two sessions after today
    SyntheticMarket(injections=list(INJECTIONS)).write_days(root, later)
    with_future = market_matches(at(day, 120), root, tmp_path / "cache")
    without = market_matches(at(day, 120), market_root, store_root)
    assert with_future["matches"] == without["matches"]
    assert with_future["outcomes"] == without["outcomes"]


def test_states_are_cached(market_root, store_root, monkeypatch):
    load_states(market_root, DATES[0], store_root)
    monkeypatch.setattr(similarity, "summarise_states", lambda day: pytest.fail("cache was not used"))
    assert load_states(market_root, DATES[0], store_root).session_date == DATES[0]


def test_separation():
    assert _separation(np.array([1.0, 2.0]), np.array([1.0, 2.0])) == 0.5
    assert _separation(np.array([5.0]), np.array([1.0, 2.0])) == 1.0
    assert _separation(np.array([np.nan]), np.array([1.0])) is None
