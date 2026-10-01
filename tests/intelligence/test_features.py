import numpy as np
import pytest

from intelligence.archive import load_day
from intelligence.features import CATALOGUE, MARKET_FEATURES, compute_features
from intelligence.frame import as_of_time, frame_at
from intelligence.history import build_baselines, load_summary
from intelligence.universe import sector_of
from tests.intelligence.conftest import DATES, TODAY


@pytest.fixture(scope="module")
def day(market_root):
    return load_day(market_root, TODAY)


def test_every_computed_feature_is_catalogued_with_a_purpose(day):
    features = compute_features(frame_at(day, as_of_time(TODAY, "10:00")))
    assert set(features.values) == set(CATALOGUE)
    assert set(features.market) == set(MARKET_FEATURES)
    assert all(spec.purpose for spec in CATALOGUE.values())


def test_returns_match_the_bars(day):
    frame = frame_at(day, as_of_time(TODAY, "10:00"))
    features = compute_features(frame)
    i = frame.equity.keys.index("INFY")
    close = frame.equity.close[i]
    assert features.get("ret_1m", "INFY") == pytest.approx(np.log(close[-1] / close[-2]))
    assert features.get("ret_15m", "INFY") == pytest.approx(np.log(close[-1] / close[-16]))
    today_open = day.reference["INFY"]["today_open"]
    assert features.get("ret_session", "INFY") == pytest.approx(np.log(close[-1] / today_open))
    assert features.minute_index == 44  # 09:15 .. 09:59


def test_relative_features_subtract_the_right_benchmark(day):
    features = compute_features(frame_at(day, as_of_time(TODAY, "11:00")))
    session = features.values["ret_session"]
    assert features.values["rel_market_session"] == pytest.approx(session - np.nanmedian(session))
    it = [i for i, k in enumerate(features.keys) if sector_of(k) == "INFORMATION_TECHNOLOGY"]
    it_median = np.median(session[it])
    assert features.sectors_median["INFORMATION_TECHNOLOGY"]["ret_session"] == pytest.approx(it_median)
    assert features.get("rel_sector_session", "INFY") == pytest.approx(features.get("ret_session", "INFY") - it_median)
    broad = features.indices["nifty500"]["ret_session"]
    assert features.get("rel_index_session", "INFY") == pytest.approx(features.get("ret_session", "INFY") - broad)
    niftyit = features.indices["niftyit"]["ret_session"]
    assert features.get("rel_sector_index_session", "INFY") == pytest.approx(
        features.get("ret_session", "INFY") - niftyit
    )


def test_market_features(day):
    features = compute_features(frame_at(day, as_of_time(TODAY, "11:00")))
    session = features.values["ret_session"]
    up, down = np.sum(session > 0), np.sum(session < 0)
    assert features.market["breadth_session"] == pytest.approx((up - down) / (up + down))
    assert features.market["dispersion_session"] == pytest.approx(np.std(session, ddof=1))
    assert features.market["active_share_1m"] == 1.0


def test_missing_bar_gives_nan_not_a_guess(day):
    frame = frame_at(day, as_of_time(TODAY, "10:00"))
    frame.equity.close[0, -1] = np.nan  # the latest bar of one instrument is missing
    features = compute_features(frame)
    assert np.isnan(features.values["ret_1m"][0])
    assert np.isnan(features.values["ret_5m"][0])


def test_before_the_first_bar_everything_is_nan(day):
    features = compute_features(frame_at(day, as_of_time(TODAY, "09:15")))
    assert features.minute_index == -1
    assert all(np.isnan(v).all() for v in features.values.values())


def test_short_windows_need_enough_bars(day):
    features = compute_features(frame_at(day, as_of_time(TODAY, "09:20")))  # 5 bars
    assert np.isnan(features.values["ret_15m"]).all()
    assert np.isnan(features.values["rvol_15m"]).all()  # needs 10 returns
    assert not np.isnan(features.values["ret_1m"]).any()


# --- baselines -------------------------------------------------------------------------


def test_baselines_use_only_earlier_sessions(market_root, store_root, day):
    baselines = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    assert baselines.sessions == tuple(DATES[:-1])
    assert all(d < TODAY for d in baselines.sessions)
    assert baselines.available


def test_baseline_is_unavailable_with_too_little_history(market_root, store_root, day):
    baselines = build_baselines(market_root, DATES[2], day.equity.keys, min_sessions=5, cache_root=store_root)
    assert len(baselines.sessions) == 2 and not baselines.available
    median, scale, count = baselines.lookup("log_volume_1m", 60)
    assert np.isnan(median).all() and np.isnan(scale).all() and (count == 2).all()


def test_baseline_matches_the_history_it_summarises(market_root, store_root, day):
    baselines = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    i = day.equity.keys.index("ITC")
    stack = np.stack(
        [load_summary(market_root, d, store_root).instrument["log_volume_1m"][i, 98:103] for d in DATES[:-1]]
    )
    median, scale, count = baselines.lookup("log_volume_1m", 100)
    assert median[i] == pytest.approx(np.median(stack), rel=1e-5)
    assert scale[i] > 0 and count[i] == len(DATES) - 1


def test_baselines_ignore_the_day_being_judged(market_root, store_root, day, tmp_path):
    """Changing today's archive must not change today's baselines (no look-ahead)."""
    before = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    from intelligence.synthetic import Injection, SyntheticMarket

    SyntheticMarket(injections=[Injection(TODAY, "ITC", 0, 360, volume_multiplier=50.0)]).write_days(tmp_path, DATES)
    after = build_baselines(tmp_path, TODAY, day.equity.keys, cache_root=tmp_path / "cache")
    for field in before.instrument_median:
        assert np.array_equal(before.instrument_median[field], after.instrument_median[field], equal_nan=True)


def test_summaries_are_cached(market_root, store_root, monkeypatch):
    load_summary(market_root, DATES[0], store_root)
    import intelligence.history as history

    monkeypatch.setattr(history, "summarise_day", lambda day: pytest.fail("cache was not used"))
    assert load_summary(market_root, DATES[0], store_root).session_date == DATES[0]
