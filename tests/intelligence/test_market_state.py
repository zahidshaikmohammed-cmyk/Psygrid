"""Market-state engine: correlation structure, dimension, dispersion, history percentiles."""

import numpy as np
import pytest

from intelligence.archive import load_day
from intelligence.frame import as_of_time
from intelligence.market_state import _window_stats, measure, regime, session_profile, with_history
from tests.intelligence.conftest import TODAY


def _arrays(r):
    return r, np.full(r.shape, 100.0), np.full(r.shape, 10.0)


def test_one_factor_market_has_high_correlation_and_low_dimension():
    rng = np.random.default_rng(1)
    m = rng.normal(0, 0.001, 60)
    coupled = _window_stats(*_arrays(m[None, :] + rng.normal(0, 0.0002, (200, 60))), 60, 30)
    loose = _window_stats(*_arrays(rng.normal(0, 0.001, (200, 60))), 60, 30)
    assert coupled["mean_correlation"] > 0.9 and abs(loose["mean_correlation"]) < 0.05
    assert coupled["effective_dimension"] < 1.2 < 10 < loose["effective_dimension"]
    assert coupled["top_eigen_share"] > 0.9 and coupled["absorption_ratio"] >= coupled["top_eigen_share"]
    assert coupled["stocks"] == 200


def test_mean_correlation_matches_the_direct_computation():
    rng = np.random.default_rng(2)
    r = rng.normal(0, 0.001, (40, 30)) + rng.normal(0, 0.001, 30)
    stats = _window_stats(*_arrays(r), 30, 30)
    c = np.corrcoef(r)
    assert stats["mean_correlation"] == pytest.approx(c[~np.eye(40, dtype=bool)].mean(), abs=1e-9)


def test_concentration_and_breadth():
    r = np.full((30, 30), -0.0001)
    r[0] = 0.01  # one stock carries the move
    stats = _window_stats(*_arrays(r), 30, 30)
    assert stats["move_concentration"] > 0.8 and stats["breadth_window"] == pytest.approx(1 / 30)
    sparse = r.copy()
    sparse[5:, :20] = np.nan  # too few bars for correlation, still enough for dispersion
    assert _window_stats(*_arrays(sparse), 30, 30)["mean_correlation"] is None


def test_regime_labels_are_descriptive():
    assert regime({"mean_correlation": None, "dispersion_bps": 0.5}) == "UNCALIBRATED"
    assert regime({"mean_correlation": 0.9, "dispersion_bps": 0.9}) == "COUPLED_STRESS"
    assert regime({"mean_correlation": 0.1, "dispersion_bps": 0.9}) == "DISPERSED"
    assert regime({"mean_correlation": 0.5, "dispersion_bps": 0.1, "change_score": 0.99}) == "SHIFTING_QUIET"


def test_measure_uses_only_closed_bars_and_history_percentiles(market_root, tmp_path):
    day = load_day(market_root, TODAY)
    at = as_of_time(TODAY, "11:00")
    state = measure(day, at)
    truncated = measure(day.__class__(day.session_date, *_cut(day, at)), at)
    assert state.view() == truncated.view()  # bars after as_of change nothing
    full = with_history(state, market_root, tmp_path)
    assert full.percentiles["history_sessions"] >= 5 and full.regime
    assert 0 <= full.percentiles["mean_correlation"] <= 1
    assert (tmp_path / "market_state").exists()  # profiles cached
    days = sorted(p.name for p in (tmp_path / "market_state").iterdir())
    assert session_profile(market_root, days[0][:10], tmp_path)  # read back from cache


def _cut(day, at):
    """The day's bars with every bar opening at or after ``at`` removed."""
    from intelligence.archive import Bars

    def cut(bars):
        keep = bars.minutes < at.timestamp()
        return Bars(bars.keys, bars.names, bars.minutes[keep], *(bars.field(f)[:, keep] for f in
                    ("open", "high", "low", "close", "volume")))  # fmt: skip

    return cut(day.equity), cut(day.indices) if day.indices else None, day.reference, day.manifest


def test_state_taxonomy_rule():
    from intelligence.market_state import classify_state

    assert classify_state({"mean_correlation": None, "dispersion_bps": 0.5}) == "UNCALIBRATED"
    assert classify_state({"mean_correlation": 0.9, "dispersion_bps": 0.3, "market_vol_bps": 0.85}) == "STRESSED"
    assert classify_state({"mean_correlation": 0.3, "dispersion_bps": 0.95}) == "DISLOCATED"
    assert classify_state({"mean_correlation": 0.5, "dispersion_bps": 0.5, "change_score": 0.93}) == "TRANSITION"
    assert classify_state({"mean_correlation": 0.5, "dispersion_bps": 0.5}) == "NORMAL"


def test_sector_sync_and_market_vol():
    rng = np.random.default_rng(3)
    r = rng.normal(0, 0.001, (60, 30))
    r[:30] += rng.normal(0, 0.002, 30)  # rows 0..29: one synchronised sector
    m = rng.normal(0, 0.001, 30)
    stats = _window_stats(r, np.full(r.shape, 100.0), np.full(r.shape, 10.0), 30, 30, m,
                          {"A": list(range(30)), "B": list(range(30, 60))})  # fmt: skip
    # within-sector mean ~ (0.8 + 0) / 2 = 0.4 against an all-pairs mean ~ 0.2
    assert stats["sector_sync"] == pytest.approx(0.2, abs=0.08)
    flat = _window_stats(rng.normal(0, 0.001, (60, 30)), np.full(r.shape, 100.0), np.full(r.shape, 10.0), 30, 30, m,
                         {"A": list(range(30)), "B": list(range(30, 60))})  # fmt: skip
    assert abs(flat["sector_sync"]) < 0.05 and stats["market_vol_bps"] == pytest.approx(np.std(m, ddof=1) * 1e4)


def test_with_history_assigns_state_and_persistence(market_root, tmp_path):
    day = load_day(market_root, TODAY)
    state = with_history(measure(day, as_of_time(TODAY, "12:00")), market_root, tmp_path)
    assert state.state in ("NORMAL", "TRANSITION", "STRESSED", "DISLOCATED")
    assert state.persistence is None or 0 <= state.persistence <= 1
