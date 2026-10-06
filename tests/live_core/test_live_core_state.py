"""Compact Live Core state: candle rules, contract parity with the full PSYGRID, staleness and reset."""

from types import SimpleNamespace

import orjson
from live_core_helpers import feed_minutes, ist, tick_payload

from live_core.render import assemble, local_fragments, stock_body
from live_core.state import SOURCE_HISTORICAL, NodeState
from output import market_live_json, stock_json
from state import PsygridState

START = int(ist(9, 15).timestamp())


def _instruments(count=3):
    symbols = ["RELIANCE", "M&M", "BAJAJ-AUTO", "TCS", "INFY"][:count]
    return [
        SimpleNamespace(symbol=s, security_id=str(1000 + i), exchange_segment="NSE_EQ", instrument="EQUITY")
        for i, s in enumerate(symbols)
    ]


def _live_state(count=3, clock=None):
    state = NodeState(clock=clock or (lambda: START + 3600))
    state.begin("2026-10-05", _instruments(count))
    state.set_session_status("LIVE")
    return state


def _full_state(count=3):
    settings = SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=30)
    state = PsygridState(settings)
    state.begin("2026-10-05", _instruments(count))
    return state


def _drive_both(core, full, ticks):
    for security_id, quote in ticks:
        core.update_quote(security_id, quote)
        full.update_quote(security_id, quote)


def _ticks():
    rows = []
    cumulative = {"1000": 5000, "1001": 700, "1002": 90}
    for minute in range(6):
        for second in (1, 20, 20, 59):  # the repeated second exercises duplicate-trade handling
            for security_id in ("1000", "1001", "1002"):
                if security_id == "1002" and minute in (2, 3):
                    continue  # an illiquid stock with gaps: no candle may be invented for those minutes
                cumulative[security_id] += 0 if second == 20 and minute % 2 else 7
                price = 100 + minute + second / 100 + int(security_id) % 7
                ltt = START + minute * 60 + second
                rows.append((security_id, tick_payload(security_id, ltt, price, cumulative[security_id], 3)))
    # A late trade for an older minute and a trade for a stock outside the partition are ignored.
    rows.append(("1000", tick_payload("1000", START + 30, 999.0, 99999)))
    rows.append(("424242", tick_payload("424242", START + 400, 10.0, 1)))
    return rows


def test_stock_payloads_match_the_full_psygrid_output_byte_for_byte():
    core, full = _live_state(), _full_state()
    for state in (core, full):
        state.set_market_reference("1000", previous_close=2900.5, today_open=2911.25)
        state.set_market_reference("1001", previous_close="3100")
    _drive_both(core, full, _ticks())

    expected = market_live_json(full)
    body = assemble(
        status="OK",
        session_status="LIVE",
        session_date="2026-10-05",
        universe_size=989,
        items=local_fragments(core, 0, 10),
        sort_by_symbol=True,
    )
    payload = orjson.loads(body)
    assert payload["stocks"] == expected["stocks"]
    assert list(payload["stocks"]) == list(expected["stocks"])  # same symbol order
    assert list(payload) == list(expected)  # same top-level keys in the same order
    for key in ("service", "schema_version", "status", "universe_size", "stock_count", "data_policy"):
        assert payload[key] == expected[key]
    assert payload["synthetic_candles"] is False
    assert {k: v for k, v in payload["session"].items() if k != "current_time_ist"} == {
        k: v for k, v in expected["session"].items() if k != "current_time_ist"
    }
    # Gaps stay gaps: minutes 2 and 3 of the illiquid stock were never traded.
    illiquid = payload["stocks"]["BAJAJ-AUTO"]["candles_1m"]
    assert [c["timestamp"][11:16] for c in illiquid] == ["09:15", "09:16", "09:19"]
    for symbol in ("RELIANCE", "M&M", "BAJAJ-AUTO"):
        assert orjson.loads(stock_body(core, symbol)) == stock_json(full, symbol)
    assert orjson.loads(stock_body(core, "NOPE")) == stock_json(full, "NOPE")


def test_published_minute_is_immutable_and_late_trade_volume_carries_forward():
    state = _live_state(1)
    state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10))
    state.update_quote("1000", tick_payload("1000", START + 40, 101.0, 15))
    assert state.finalize_due(START + 60 + 2, grace_seconds=3) == 0  # still inside the grace period
    assert state.instruments["1000"].candle_count() == 0
    assert state.finalize_due(START + 60 + 3, grace_seconds=3) == 1
    series = state.instruments["1000"]
    published = (
        list(series.epochs),
        series.opens[0],
        series.highs[0],
        series.lows[0],
        series.closes[0],
        series.volumes[0],
    )
    assert published == ([START], 100.0, 101.0, 100.0, 101.0, 5)
    version = state.version
    # A late trade for the published 09:15 minute is dropped, as in the full PSYGRID.
    assert state.update_quote("1000", tick_payload("1000", START + 59, 99.0, 18)) is False
    assert state.rejected["late_minute"] == 1
    assert (
        list(series.epochs),
        series.opens[0],
        series.highs[0],
        series.lows[0],
        series.closes[0],
        series.volumes[0],
    ) == published
    assert state.version == version
    # Its 3 shares are not lost: they are carried into the next trade's volume delta.
    state.update_quote("1000", tick_payload("1000", START + 61, 102.0, 20))
    assert series.current[5] == 5
    state.update_quote("1000", tick_payload("1000", START - 60, 50.0, 30))  # an older minute: ignored
    assert series.candle_count() == 1 and series.lows[0] == 100.0


def test_no_trade_means_no_candle():
    state = _live_state(2)
    state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10))
    state.finalize_due(START + 3600, grace_seconds=3)
    assert state.instruments["1000"].candle_count() == 1
    assert state.instruments["1001"].candle_count() == 0
    assert orjson.loads(stock_body(state, "M&M"))["candles_1m"] == []


def test_historical_bar_is_authoritative_over_websocket_candle():
    state = _live_state(1)
    feed_minutes(state, ["1000"], START, 3)
    state.finalize_due(START + 600, grace_seconds=3)
    series = state.instruments["1000"]
    assert series.candle_count() == 3
    rows = [
        {"timestamp": START + 60, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 42, "complete": True},
        {"timestamp": START + 240, "open": 3, "high": 4, "low": 2, "close": 3, "volume": 7},
        {"timestamp": START + 300, "open": 3, "high": 4, "low": 2, "close": 3, "volume": 7, "complete": False},
        {"timestamp": START + 360, "open": -1, "high": 4, "low": 2, "close": 3, "volume": 7},
    ]
    assert state.merge_history("1000", rows) == 2
    assert list(series.epochs) == [START, START + 60, START + 120, START + 240]
    assert series.volumes[1] == 42 and series.sources[1] == SOURCE_HISTORICAL
    # A WebSocket candle for the same minute can no longer replace the historical bar.
    state.update_quote("1000", tick_payload("1000", START + 61, 555.0, 999999))
    assert series.closes[1] == 1.5


def test_history_clears_a_forming_candle_for_the_same_minute():
    state = _live_state(1)
    state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10))
    assert state.instruments["1000"].current is not None
    state.merge_history("1000", [{"timestamp": START, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}])
    assert state.instruments["1000"].current is None


def test_snapshot_seeds_reference_prices_and_volume_baseline():
    state = _live_state(1)
    state.seed_from_snapshot({"1000": {"ohlc": {"open": 10.5, "close": 10.0}, "volume": 500, "last_price": 11}})
    series = state.instruments["1000"]
    assert (series.previous_close, series.today_open, series.previous_cumulative_volume) == (10.0, 10.5, 500)
    state.update_quote("1000", tick_payload("1000", START + 5, 11.0, 520))
    assert series.current[5] == 20  # delta from the snapshot baseline, not the day total


def test_nothing_is_recorded_unless_the_session_is_live():
    state = NodeState(clock=lambda: START)
    state.begin("2026-10-05", _instruments(1))
    state.update_quote("1000", tick_payload("1000", START + 5, 11.0, 520))
    state.record_live_quote("1000", START + 5)
    state.record_feed_message("full data")
    assert state.instruments["1000"].current is None
    assert state.feed_messages == 0 and state.live_quotes == 0


def _accept(state, security_id, ltt, price=100.0, volume=None):
    series = state.instruments[security_id]
    volume = volume if volume is not None else (series.previous_cumulative_volume or 0) + 1
    accepted = state.update_quote(security_id, tick_payload(security_id, ltt, price, volume))
    state.record_live_quote(security_id, ltt)
    return accepted


def test_stale_means_last_valid_packet_more_than_120_seconds_old():
    now = {"t": START + 100.0}
    state = _live_state(3, clock=lambda: now["t"])
    for security_id in ("1000", "1001"):
        assert _accept(state, security_id, START + 99)
    fresh = state.freshness()
    assert fresh["fresh"] is True and fresh["stale"] is False
    assert (fresh["live_stock_count"], fresh["stale_stock_count"], fresh["no_quote_stock_count"]) == (2, 0, 1)
    assert fresh["stream_health"] == "PARTIAL_LIVE"
    assert fresh["max_live_age_seconds"] == 120.0
    now["t"] += 120.0  # exactly 120 s: still FRESH
    at_limit = state.freshness()
    assert at_limit["fresh"] is True and at_limit["live_stock_count"] == 2 and at_limit["stale_stock_count"] == 0
    now["t"] += 0.001  # just over 120 s: STALE
    stale = state.freshness()
    assert stale["fresh"] is False and stale["stale"] is True
    assert stale["last_tick_age_seconds"] == 120.001
    assert (stale["live_stock_count"], stale["stale_stock_count"]) == (0, 2)
    assert stale["stale_symbols_sample"] == ["RELIANCE", "M&M"]
    assert stale["stream_health"] == "NO_LIVE_QUOTES"
    # 60 s and 90 s gaps are not staleness under the locked rule.
    now["t"] = START + 100.0 + 60.0
    assert state.freshness()["fresh"] is True
    # An early exchange timestamp on a just-received packet does not make it stale.
    assert _accept(state, "1002", START - 3600)
    assert state.freshness()["live_stock_count"] == 3


def test_a_rejected_packet_is_not_evidence_of_fresh_data():
    now = {"t": START + 100.0}
    state = _live_state(1, clock=lambda: now["t"])
    state.update_quote("1000", tick_payload("1000", START + 99, float("nan"), 5))
    state.record_live_quote("1000", START + 99)
    assert state.freshness()["no_quote_stock_count"] == 1 and state.live_quotes == 0


def test_bad_packets_are_rejected_one_by_one_without_touching_any_valid_state():
    state = _live_state(3)
    assert _accept(state, "1000", START + 5, 100.0, 1000)
    assert _accept(state, "1001", START + 5, 200.0, 50)
    before = (list(state.instruments["1000"].current), state.instruments["1000"].previous_cumulative_volume)
    bad = [
        {"LTT_EPOCH": START + 6, "LTP": float("nan"), "volume": 1001},
        {"LTT_EPOCH": START + 6, "LTP": float("inf"), "volume": 1001},
        {"LTT_EPOCH": START + 6, "LTP": -5.0, "volume": 1001},
        {"LTT_EPOCH": START + 6, "LTP": 0.0, "volume": 1001},
        {"LTT_EPOCH": START + 6, "LTP": 101.0, "volume": -1},
        {"LTT_EPOCH": START + 6, "LTP": "abc", "volume": 1001},
        {"LTT_EPOCH": "x", "LTP": 101.0, "volume": 1001},
        {"LTP": 101.0, "volume": 1001},
        {"LTT_EPOCH": START + 6, "volume": 1001},
        {"LTT_EPOCH": START + 6, "LTP": 101.0, "volume": 10**30},
        None,
    ]
    for packet in bad:
        assert state.update_quote("1000", packet if packet is not None else {}) is False
    assert (list(state.instruments["1000"].current), state.instruments["1000"].previous_cumulative_volume) == before
    assert sum(state.rejected.values()) == len(bad)
    assert state.rejected["non_finite"] == 2 and state.rejected["malformed"] >= 4
    # The other stock never noticed, and the bad stock keeps taking valid trades.
    assert state.instruments["1001"].current == [START, 200.0, 200.0, 200.0, 200.0, 0]
    assert _accept(state, "1000", START + 7, 102.0, 1010)
    assert state.instruments["1000"].current[2] == 102.0 and state.instruments["1000"].current[5] == 10


def test_previous_day_packet_never_creates_a_candle_or_moves_the_volume_baseline():
    state = _live_state(1)
    yesterday_close = START - 18 * 3600  # 15:15 IST the previous day
    assert state.update_quote("1000", tick_payload("1000", yesterday_close, 95.0, 9_000_000)) is False
    assert state.rejected["outside_session_day"] == 1
    assert state.instruments["1000"].current is None
    assert state.instruments["1000"].previous_cumulative_volume is None
    assert _accept(state, "1000", START + 3, 100.0, 1200)
    assert _accept(state, "1000", START + 9, 100.5, 1300)
    state.finalize_all()
    series = state.instruments["1000"]
    assert list(series.epochs) == [START] and series.volumes[0] == 100


def test_a_volume_regression_is_rejected_instead_of_double_counted():
    state = _live_state(1)
    assert _accept(state, "1000", START + 1, 100.0, 5000)
    assert state.update_quote("1000", tick_payload("1000", START + 2, 100.0, 0)) is False  # corrupt frame
    assert state.rejected["volume_regressed"] == 1
    assert _accept(state, "1000", START + 3, 100.0, 5010)
    assert state.instruments["1000"].current[5] == 10  # not 5010


def test_duplicate_packets_are_idempotent():
    state = _live_state(1)
    packet = tick_payload("1000", START + 5, 100.0, 1000, 7)
    for _ in range(10):
        state.update_quote("1000", dict(packet))
    state.update_quote("1000", tick_payload("1000", START + 6, 101.0, 1010, 10))
    for _ in range(10):
        state.update_quote("1000", tick_payload("1000", START + 6, 101.0, 1010, 10))
    state.finalize_all()
    series = state.instruments["1000"]
    assert list(series.epochs) == [START] and series.volumes[0] == 10 and state.duplicate_trades == 19


def test_feed_status_recovers_when_market_data_flows_after_an_error():
    state = _live_state(1)
    state.mark_websocket_connected(1)
    state.mark_websocket_error("websocket: one malformed frame")
    assert state.feed_status == "ERROR"
    state.record_feed_message("Full Data")
    assert state.feed_status == "CONNECTED"
    assert "malformed" in state.last_feed_error  # the error stays visible


def test_historical_bars_are_validated_and_bound_to_their_session():
    state = _live_state(1)
    good = {"timestamp": START, "open": 10, "high": 11, "low": 9, "close": 10.5, "volume": 1}
    bad = [
        {**good, "timestamp": START + 60, "high": float("nan")},
        {**good, "timestamp": START + 120, "high": 9.5},  # high below open/close
        {**good, "timestamp": START + 180, "low": 10.6},  # low above open/close
        {**good, "timestamp": START - 86_400},  # another day
    ]
    assert state.merge_history("1000", [good, *bad]) == 1
    assert state.merge_history("1000", [{**good, "timestamp": START + 240}], session_date="2026-10-04") == 0
    assert list(state.instruments["1000"].epochs) == [START]


def test_random_valid_streams_match_the_full_psygrid_exactly():
    """Fuzz parity: any valid packet stream (duplicates, late and out-of-order trades, gaps) gives
    the same published candles as the full PSYGRID's PsygridState + output.py."""
    import random

    rng = random.Random(20261005)
    for _round in range(40):
        core, full = _live_state(3), _full_state(3)
        volume = {"1000": 100, "1001": 100, "1002": 100}
        clock = START
        packets = []
        last: dict = {}
        for _ in range(rng.randint(5, 400)):
            security_id = rng.choice(["1000", "1001", "1002"])
            clock += rng.choice([0, 0, 1, 2, 7, 30, 61])
            ltt = clock - rng.choice([0, 0, 0, 1, 45, 90])  # some trades arrive late
            if rng.random() < 0.15 and security_id in last:
                packets.append(last[security_id])  # Dhan re-sending the stock's last packet
                continue
            volume[security_id] += rng.choice([0, 1, 5, 100])
            price = round(rng.uniform(90, 110), 2)
            last[security_id] = (
                security_id,
                tick_payload(security_id, ltt, price, volume[security_id], rng.randint(1, 9)),
            )
            packets.append(last[security_id])
        for security_id, quote in packets:
            core.update_quote(security_id, dict(quote))
            full.update_quote(security_id, dict(quote))
        body = assemble(status="OK", session_status="LIVE", session_date="2026-10-05", universe_size=989,
                        items=local_fragments(core, 0, 10), sort_by_symbol=True)  # fmt: skip
        assert orjson.loads(body)["stocks"] == market_live_json(full)["stocks"]


def test_reset_drops_every_market_value():
    state = _live_state(3)
    feed_minutes(state, ["1000", "1001", "1002"], START, 5)
    state.set_market_reference("1000", previous_close=1, today_open=2)
    state.finalize_all()
    assert state.memory_summary()["completed_candles_in_ram"] == 15
    state.reset()
    assert state.instruments == {} and state.ordered == [] and state.by_symbol == {}
    assert state.session_status == "CLOSED" and state.session_date is None
    assert state.memory_summary()["completed_candles_in_ram"] == 0
    assert local_fragments(state, 0, 989) == []


def test_fragment_cache_extends_in_place_and_rebuilds_after_a_rewrite():
    state = _live_state(1)
    feed_minutes(state, ["1000"], START, 2)
    state.finalize_due(START + 600, grace_seconds=3)
    series = state.instruments["1000"]
    first = local_fragments(state, 0, 1)[0][2]
    assert local_fragments(state, 0, 1)[0][2] is first  # unchanged revision: cached object reused
    feed_minutes(state, ["1000"], START + 120, 1)
    state.finalize_due(START + 600, grace_seconds=3)
    second = local_fragments(state, 0, 1)[0][2]
    assert len(orjson.loads(second)["candles_1m"]) == 3
    state.merge_history("1000", [{"timestamp": START, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}])
    third = orjson.loads(local_fragments(state, 0, 1)[0][2])
    assert third["candles_1m"][0]["close"] == 1.0 and len(third["candles_1m"]) == 3
    assert series.generation >= 1


def test_a_stock_that_has_not_traded_today_never_gets_a_candle():
    """Dhan repeats yesterday's last trade (date-less HH:MM:SS, read as today) with zero day volume."""
    now = {"t": START + 3600.0}
    state = _live_state(1, clock=lambda: now["t"])
    yesterday_time_read_as_today = START + 300  # 09:20 "today"
    for _ in range(3):
        assert state.update_quote("1000", tick_payload("1000", yesterday_time_read_as_today, 95.5, 0)) is True
        state.record_live_quote("1000", yesterday_time_read_as_today)
    state.finalize_all()
    series = state.instruments["1000"]
    assert series.candle_count() == 0 and series.current is None
    assert state.no_trade_today_packets == 3
    assert state.freshness()["live_stock_count"] == 1  # the subscription is alive
    assert _accept(state, "1000", START + 3500, 96.0, 40)  # first real trade today
    state.finalize_all()
    assert list(series.epochs) == [START + 3480] and series.volumes[0] == 40


def test_a_quiet_stock_repeating_its_last_trade_stays_fresh_without_touching_candles():
    """Live finding: Dhan re-sends a quiet stock's last trade on every quote/depth change.

    Those packets carry an old trade time and unchanged day volume. They are not late trades: they
    prove the stock is live (fresh) and must change no candle. A late packet with NEW volume is
    still rejected.
    """
    clock = {"now": START + 10}
    state = _live_state(1, clock=lambda: clock["now"])
    state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10))
    state.finalize_due(START + 63, grace_seconds=3)
    series = state.instruments["1000"]
    published = (list(series.epochs), series.closes[0], series.volumes[0])
    version = state.version

    # Five minutes later the stock still has not traded, but Dhan keeps re-sending its last trade.
    clock["now"] = START + 300
    assert state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10)) is True
    state.record_live_quote("1000", START + 10)
    assert state.rejected["late_minute"] == 0
    assert state.repeated_last_trade_packets == 1
    freshness = state.freshness()
    assert freshness["stale_stock_count"] == 0 and freshness["live_stock_count"] == 1
    assert (list(series.epochs), series.closes[0], series.volumes[0]) == published
    assert series.current is None and state.version == version

    # A late packet that does carry new volume is a genuine late trade: still dropped.
    assert state.update_quote("1000", tick_payload("1000", START + 30, 99.0, 14)) is False
    assert state.rejected["late_minute"] == 1
    assert (list(series.epochs), series.closes[0], series.volumes[0]) == published


def test_a_mid_session_start_takes_its_volume_baseline_from_a_repeated_last_trade():
    state = _live_state(1)
    series = state.instruments["1000"]
    # History already published 09:15; the first live packet repeats that minute's last trade.
    stored = state.merge_history(
        "1000",
        [
            {
                "epoch": START,
                "timestamp": START,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 500,
            }
        ],
        session_date="2026-10-05",
    )
    assert stored == 1 and list(series.epochs) == [START]
    assert state.update_quote("1000", tick_payload("1000", START + 50, 100.5, 5000)) is True
    assert series.previous_cumulative_volume == 5000
    # The next real trade gets exactly its own volume, not the whole day's.
    assert state.update_quote("1000", tick_payload("1000", START + 70, 101.0, 5012)) is True
    assert series.current[5] == 12
