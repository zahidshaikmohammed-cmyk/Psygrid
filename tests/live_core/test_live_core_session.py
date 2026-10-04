"""Session lifecycle: 09:15 open, 15:15 close and wipe, trading-day gating, auth failures, gap refill."""

import time
from datetime import timedelta

from live_core_helpers import FakeFeed, feed_minutes, ist

from dhan_auth import DhanTokenRateLimited

SATURDAY = (2026, 10, 3)


def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def test_no_feed_and_no_dhan_calls_before_the_open(make_runtime):
    runtime = make_runtime(0, when=ist(9, 14, 59))
    runtime.tick()
    assert runtime.feed is None
    assert runtime.state.session_status == "CLOSED"
    assert runtime.test_api.verify_calls == 0 and runtime.test_refresh_calls == []


def test_session_opens_at_0915_with_exactly_this_nodes_partition(make_runtime, universe):
    runtime = make_runtime(1, when=ist(9, 15))
    runtime.tick()
    assert runtime.state.session_status == "LIVE"
    assert runtime.state.session_date == "2026-10-05"
    feed = runtime.feed
    assert isinstance(feed, FakeFeed) and feed.started
    assert [item.symbol for item in feed.instruments] == list(universe.symbols[495:])
    assert len(feed.instruments) == 494
    assert runtime.state.subscribed_count == 494
    assert runtime.test_api.verify_calls == 1
    # Opened at 09:15 sharp: there is no earlier bar to load.
    assert runtime.history.status()["queued"] == 0 and runtime.test_api.history_calls == []


def test_session_close_stops_the_feed_and_clears_all_market_state(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.tick()
    ids = [series.security_id for series in runtime.state.ordered[:20]]
    feed_minutes(runtime.state, ids, int(ist(9, 15).timestamp()), 10)
    runtime.test_clock.set(ist(9, 30))
    runtime.tick()
    assert runtime.state.memory_summary()["completed_candles_in_ram"] == 200
    feed = runtime.feed

    runtime.test_clock.set(ist(15, 15))
    runtime.tick()
    assert feed.stopped
    assert runtime.feed is None and runtime.history is None and runtime.dhan_api is None
    assert runtime.state.session_status == "CLOSED"
    assert runtime.state.instruments == {} and runtime.state.ordered == []
    assert runtime.state.memory_summary() == {
        "stocks_in_ram": 0,
        "completed_candles_in_ram": 0,
        "forming_candles_in_ram": 0,
        "approx_candle_bytes": 0,
    }
    assert runtime.sessions_ended == 1
    # Nothing restarts after the close on the same day.
    runtime.test_clock.set(ist(15, 40))
    runtime.tick()
    assert runtime.feed is None and len(FakeFeed.instances) and runtime.sessions_started == 1


def test_next_day_starts_from_an_empty_state(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.tick()
    feed_minutes(runtime.state, [runtime.state.ordered[0].security_id], int(ist(9, 15).timestamp()), 3)
    runtime.state.finalize_all()
    tuesday = ist(9, 15, 0, day=(2026, 10, 6))
    runtime.test_clock.set(tuesday)
    runtime.tick()  # the process never saw 15:15 (e.g. it was suspended): the day change still wipes state
    assert runtime.state.session_date == "2026-10-06"
    assert runtime.state.memory_summary()["completed_candles_in_ram"] == 0
    assert runtime.sessions_started == 2 and runtime.sessions_ended == 1


def test_weekends_and_listed_holidays_never_open_a_session(make_runtime, monkeypatch):
    runtime = make_runtime(0, when=ist(11, 0, day=SATURDAY))
    runtime.tick()
    assert runtime.feed is None and runtime.test_api.verify_calls == 0
    monkeypatch.setenv("PSYGRID_MARKET_HOLIDAYS", "2026-10-05")
    holiday = make_runtime(0, when=ist(11, 0))
    holiday.tick()
    assert holiday.feed is None
    assert holiday.node_health()["session"]["market_window"] == "NON_TRADING_DAY"


def test_a_late_start_loads_todays_genuine_bars_from_dhan(make_runtime, universe):
    runtime = make_runtime(0, when=ist(10, 30))
    start = int(ist(9, 15).timestamp())
    runtime.test_api.history = {
        "RELIANCE": [
            {"timestamp": start + 60 * i, "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 100 + i}
            for i in range(75)
        ]
    }
    runtime.tick()
    assert _wait_for(lambda: len(runtime.test_api.history_calls) == 495)
    assert _wait_for(lambda: runtime.state.by_symbol["RELIANCE"].candle_count() == 75)
    assert set(runtime.test_api.history_calls) == set(universe.symbols[:495])
    assert runtime.state.by_symbol["TCS"].candle_count() == 0  # Dhan returned nothing: nothing is invented
    assert runtime.history.status()["candles_merged"] == 75


def test_a_feed_reconnect_refills_the_gap_from_dhan_after_the_minutes_close(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.tick()
    assert runtime.test_api.history_calls == []
    runtime.state.mark_websocket_reconnecting("websocket closed by peer")
    runtime.test_clock.set(ist(10, 0))
    runtime.tick()
    assert runtime.test_api.history_calls == []  # waits for the interrupted minutes to close
    runtime.test_clock.set(ist(10, 0) + timedelta(seconds=91))
    runtime.tick()
    assert _wait_for(lambda: len(runtime.test_api.history_calls) == 495)


def test_an_authentication_failure_retries_without_opening_the_feed(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.test_api.verify_error = RuntimeError("DHAN_DATA_PLAN_NOT_ACTIVE")
    runtime.tick()
    assert runtime.state.session_status == "AUTH_ERROR" and runtime.feed is None
    runtime.test_api.verify_error = None
    runtime.test_clock.set(ist(9, 15, 10))
    runtime.tick()
    assert runtime.state.session_status == "AUTH_ERROR"  # still inside the 30 s retry delay
    runtime.test_clock.set(ist(9, 15, 31))
    runtime.tick()
    assert runtime.state.session_status == "LIVE" and runtime.feed is not None


def test_an_expired_token_is_regenerated_once(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    api = runtime.test_api
    original = api.verify_data_access
    failures = {"left": 1}

    def verify_once_expired():
        if failures["left"]:
            failures["left"] -= 1
            raise RuntimeError("Dhan API HTTP error: 401 token expired (807)")
        return original()

    api.verify_data_access = verify_once_expired
    runtime.tick()
    assert runtime.state.session_status == "LIVE"
    assert runtime.test_refresh_calls == [False, True]


def test_token_generation_rate_limit_waits_for_dhan(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    calls = []

    def limited(settings, force=False):
        calls.append(force)
        raise DhanTokenRateLimited("once every 2 minutes", 130)

    runtime._token_refresher = limited
    runtime.tick()
    assert runtime.state.session_status == "AUTH_WAITING"
    runtime.test_clock.set(ist(9, 16))
    runtime.tick()
    assert len(calls) == 1  # no hammering inside Dhan's cool-down


def test_a_failed_session_is_cleared_when_the_window_closes(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.test_api.verify_error = RuntimeError("DHAN_DATA_PLAN_NOT_ACTIVE")
    runtime.tick()
    assert runtime.state.ordered
    runtime.test_clock.set(ist(15, 15))
    runtime.tick()
    assert runtime.state.session_status == "CLOSED" and runtime.state.ordered == []


def test_missing_credentials_are_a_visible_config_error(make_runtime):
    runtime = make_runtime(0, when=ist(8, 0))

    def no_credentials():
        raise RuntimeError("Missing required environment variable: DHAN_CLIENT_ID")

    runtime._settings_loader = no_credentials
    runtime.preflight()
    health = runtime.node_health()
    assert health["status"] == "CONFIG_ERROR"
    assert "DHAN_CLIENT_ID" in health["reasons"][0]
    runtime.test_clock.set(ist(9, 15))
    runtime.tick()
    assert runtime.feed is None and runtime.state.session_status == "CONFIG_ERROR"


def test_universe_resolution_must_match_the_canonical_order(make_runtime, instruments):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime._instrument_loader = lambda: list(reversed(instruments))
    runtime.tick()
    assert runtime.feed is None and runtime.state.session_status == "CONFIG_ERROR"
    assert "canonical universe" in runtime.config_error


def test_minutes_are_published_by_the_session_loop(make_runtime):
    runtime = make_runtime(0, when=ist(9, 15))
    runtime.tick()
    security_id = runtime.state.ordered[0].security_id
    runtime.state.update_quote(security_id, {"LTT_EPOCH": int(ist(9, 15, 10).timestamp()), "LTP": 5.0, "volume": 1})
    runtime.test_clock.set(ist(9, 16, 2))
    runtime.tick()
    assert runtime.state.ordered[0].candle_count() == 0
    runtime.test_clock.set(ist(9, 16, 3))
    runtime.tick()
    assert runtime.state.ordered[0].candle_count() == 1


def test_security_ids_are_resolved_before_the_open_once_per_day(make_runtime, instruments):
    calls = []
    runtime = make_runtime(0, when=ist(8, 59))

    def loader():
        calls.append(runtime.test_clock.now())
        return list(instruments)

    runtime._instrument_loader = loader
    runtime.tick()
    assert calls == []  # not before 09:00
    runtime.test_clock.set(ist(9, 0, 30))
    runtime.tick()
    runtime.test_clock.set(ist(9, 10))
    runtime.tick()
    assert len(calls) == 1
    runtime.test_clock.set(ist(9, 15))
    runtime.tick()
    assert runtime.state.session_status == "LIVE" and len(calls) == 1  # 09:15 reuses the pre-open result
    assert runtime.feed is not None and runtime.state.ordered  # no feed or market state before the open


def test_a_failed_pre_open_resolution_is_retried_at_the_open(make_runtime, instruments):
    runtime = make_runtime(0, when=ist(9, 5))
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("instrument master download failed")
        return list(instruments)

    runtime._instrument_loader = flaky
    runtime.tick()
    assert runtime.feed is None and runtime.state.session_status == "CLOSED"
    assert any("pre-open" in error["error"] for error in runtime.state.errors)
    runtime.test_clock.set(ist(9, 15))
    runtime.tick()
    assert runtime.state.session_status == "LIVE" and attempts["n"] == 2


def test_a_feed_that_cannot_start_is_retried_not_reported_live(make_runtime):
    attempts = {"n": 0}

    def broken_then_fine(settings, state, instruments):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("cannot build MarketFeed")
        return FakeFeed(settings, state, instruments)

    runtime = make_runtime(0, when=ist(9, 15), feed_factory=broken_then_fine)
    runtime.tick()
    assert runtime.feed is None and runtime.state.session_status == "FEED_ERROR"
    assert runtime.node_health()["status"] == "DEGRADED"
    runtime.test_clock.set(ist(9, 15, 31))
    runtime.tick()
    assert runtime.state.session_status == "LIVE" and runtime.feed is not None and attempts["n"] == 2
