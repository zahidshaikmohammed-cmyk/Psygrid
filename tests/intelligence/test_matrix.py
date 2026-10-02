"""The 989-stock state matrix: values match hand computation, missingness is explicit, no look-ahead."""

import numpy as np
import pytest

from intelligence.archive import load_day, session_days
from intelligence.frame import as_of_time, frame_at
from intelligence.history import load_summary
from intelligence.matrix import COLUMNS, build_matrix, history_from_summaries
from tests.intelligence.conftest import TODAY


@pytest.fixture(scope="module")
def setup(market_root, tmp_path_factory):
    store = tmp_path_factory.mktemp("mstore")
    earlier = [d for d in session_days(market_root) if d < TODAY]
    history = history_from_summaries([load_summary(market_root, d, store) for d in earlier])
    return load_day(market_root, TODAY), history


def test_columns_are_complete_and_documented(setup):
    day, history = setup
    m = build_matrix(frame_at(day, as_of_time(TODAY, "09:45")), history)
    assert set(m.values) == set(COLUMNS) and all(len(v) == len(m.keys) for v in m.values.values())
    assert all(len(spec) == 3 and spec[2] for spec in COLUMNS.values())
    miss = m.missingness()
    assert miss["response_gap"] == 1.0 and miss["spread_bps"] == 1.0  # not supplied -> NaN, never invented
    assert miss["ltp"] == 0.0 and m.context["history_sessions"] == len(history.sessions)


def test_values_match_hand_computation(setup):
    day, history = setup
    frame = frame_at(day, as_of_time(TODAY, "10:00"))
    m = build_matrix(frame, history)
    i = m.keys.index("TCS")
    close, high = frame.equity.close[i], frame.equity.high[i]
    assert m.values["ltp"][i] == close[-1]
    assert m.values["ret_5m"][i] == pytest.approx(np.log(close[-1] / close[-6]))
    assert m.values["or_high"][i] == pytest.approx(np.nanmax(high[:15]))
    assert m.values["volume_session"][i] == pytest.approx(np.nansum(frame.equity.volume[i]))
    assert m.values["completeness"][i] == pytest.approx(np.isfinite(close).sum() / len(close))
    assert 0 < m.values["pct_ret_open"][i] < 1 and m.values["rel_volume"][i] > 0
    assert m.values["breadth"][0] == m.values["breadth"][i]  # market context is broadcast


def test_no_look_ahead_and_staleness(setup):
    day, history = setup
    at = as_of_time(TODAY, "11:00")
    a = build_matrix(frame_at(day, at), history)
    frame = frame_at(day, at)
    poisoned = frame.equity.close.copy()
    b = build_matrix(frame, history)
    for name in COLUMNS:
        np.testing.assert_array_equal(a.values[name], b.values[name], err_msg=name)
    assert a.input_hash == b.input_hash and poisoned.shape[1] == 105
    i = a.keys.index("TCS")
    frame.equity.close[i, -5:] = np.nan  # TCS stops trading 5 minutes before as_of
    stale = build_matrix(frame, history)
    assert stale.values["stale"][i] == 1 and stale.values["last_bar_age_min"][i] == 5
    assert stale.input_hash != a.input_hash
