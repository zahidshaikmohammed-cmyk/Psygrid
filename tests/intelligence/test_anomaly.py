from datetime import timedelta

import numpy as np
import pytest

from intelligence.anomaly import (
    CROSS_SECTIONAL,
    EXTREME,
    HISTORICAL,
    INSUFFICIENT_DATA,
    INTRADAY,
    INVALID,
    NORMAL,
    STALE,
    UNUSUAL,
    classify_z,
    detect,
)
from intelligence.archive import load_day
from intelligence.features import compute_features
from intelligence.frame import as_of_time, frame_at
from intelligence.history import build_baselines
from tests.intelligence.conftest import DATES, TODAY


def minute_time(minute: int):
    """as_of right after bar ``minute`` (0 = 09:15) completes."""
    return as_of_time(TODAY, "09:15") + timedelta(minutes=minute + 1)


@pytest.fixture(scope="module")
def setup(market_root, store_root):
    day = load_day(market_root, TODAY)
    baselines = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    return day, baselines


def report_at(day, baselines, minute):
    frame = frame_at(day, minute_time(minute))
    return detect(frame, compute_features(frame), baselines)


def test_z_thresholds():
    assert list(classify_z(np.array([0.0, 2.99, 3.0, -4.9, 5.0, -7.0]))) == [
        NORMAL, NORMAL, UNUSUAL, UNUSUAL, EXTREME, EXTREME,
    ]  # fmt: skip


def test_injected_volume_surge_is_extreme_against_its_own_history(setup):
    day, baselines = setup
    report = report_at(day, baselines, 120)
    tcs = report.of("TCS")["volume"]
    assert tcs["classification"] == EXTREME and tcs["baseline"] == HISTORICAL
    assert report.of("TCS")["return"]["classification"] in (NORMAL, UNUSUAL)  # only volume was injected
    found = [a for a in report.anomalies() if a.key == "TCS" and a.measure == "volume"]
    evidence = found[0].evidence
    assert evidence["baseline"]["sample"] == len(DATES) - 1
    assert evidence["scored_value"] == pytest.approx(np.log1p(evidence["value"]))
    assert found[0].z == pytest.approx(
        (evidence["scored_value"] - evidence["baseline"]["median"]) / evidence["baseline"]["scale"]
    )


def test_injected_price_shock_is_extreme(setup):
    day, baselines = setup
    report = report_at(day, baselines, 150)
    assert report.of("HDFCBANK")["return"]["classification"] == EXTREME
    assert report.anomalies(minimum=EXTREME)[0].z >= 5


def test_quiet_minutes_are_mostly_normal(setup):
    day, baselines = setup
    flagged = total = 0
    for minute in (30, 90, 180, 240, 300):
        counts = report_at(day, baselines, minute).counts()
        for measure in counts.values():
            flagged += measure[UNUSUAL] + measure[EXTREME]
            total += sum(measure.values())
    assert flagged / total < 0.05


def test_stale_and_invalid_instruments_are_never_scored(setup):
    day, baselines = setup
    frame = frame_at(day, minute_time(100))
    i = frame.equity.keys.index("ITC")
    for name in ("open", "high", "low", "close", "volume"):
        frame.equity.field(name)[i, -1] = np.nan
    j = frame.equity.keys.index("CIPLA")
    frame.equity.rejected["CIPLA"] = [(int(frame.grid[-1]), "invalid_ohlc")]
    report = detect(frame, compute_features(frame), baselines)
    for measure in report.measures.values():
        assert measure.classification[i] == STALE and np.isnan(measure.z[i])
        assert measure.classification[j] == INVALID and np.isnan(measure.z[j])


def test_without_history_returns_fall_back_to_cross_section_and_volume_to_intraday(setup, market_root, store_root):
    day, _ = setup
    short = build_baselines(market_root, TODAY, day.equity.keys, window_sessions=2, cache_root=store_root)
    assert not short.available
    early = report_at(day, short, 10)  # 11 bars: too few for an intraday volume baseline
    assert set(early.measures["volume"].classification) == {INSUFFICIENT_DATA}
    assert set(early.measures["return"].baseline) == {CROSS_SECTIONAL}
    later = report_at(day, short, 120)
    assert set(later.measures["volume"].baseline) == {INTRADAY}
    assert later.of("TCS")["volume"]["classification"] in (UNUSUAL, EXTREME)  # still caught without history
    assert set(later.measures["range"].classification) == {INSUFFICIENT_DATA}  # no honest fallback for range


def test_market_measures_need_history(setup, market_root, store_root):
    day, baselines = setup
    report = report_at(day, baselines, 200)
    assert report.market["dispersion"]["baseline"]["kind"] == HISTORICAL
    assert report.market["dispersion"]["classification"] in (NORMAL, UNUSUAL, EXTREME)
    none = detect(frame_at(day, minute_time(200)), compute_features(frame_at(day, minute_time(200))), None)
    assert none.market["breadth"]["classification"] == INSUFFICIENT_DATA


def test_nothing_completed_means_everything_is_insufficient(setup):
    day, baselines = setup
    frame = frame_at(day, as_of_time(TODAY, "09:15"))
    report = detect(frame, compute_features(frame), baselines)
    assert all(set(m.classification) == {INSUFFICIENT_DATA} for m in report.measures.values())
