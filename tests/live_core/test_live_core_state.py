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


def test_minute_is_published_after_it_ends_and_late_trades_fold_in():
    state = _live_state(1)
    state.update_quote("1000", tick_payload("1000", START + 10, 100.0, 10))
    state.update_quote("1000", tick_payload("1000", START + 40, 101.0, 15))
    assert state.finalize_due(START + 60 + 2, grace_seconds=3) == 0  # still inside the grace period
    assert state.instruments["1000"].candle_count() == 0
    assert state.finalize_due(START + 60 + 3, grace_seconds=3) == 1
    series = state.instruments["1000"]
    assert list(series.epochs) == [START] and series.closes[0] == 101.0 and series.volumes[0] == 5
    version = state.version
    state.update_quote("1000", tick_payload("1000", START + 59, 99.0, 18))  # late trade for 09:15
    assert (series.lows[0], series.closes[0], series.volumes[0]) == (99.0, 99.0, 8)
    assert state.version > version
    state.update_quote("1000", tick_payload("1000", START - 60, 50.0, 30))  # older minute: ignored
    assert series.candle_count() == 1 and series.lows[0] == 99.0


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


def test_stale_data_is_detected_from_packet_receipt_time():
    now = {"t": START + 100.0}
    state = _live_state(3, clock=lambda: now["t"])
    for security_id in ("1000", "1001"):
        state.record_live_quote(security_id, START + 99)
    fresh = state.freshness()
    assert fresh["fresh"] is True and fresh["stale"] is False
    assert (fresh["live_stock_count"], fresh["stale_stock_count"], fresh["no_quote_stock_count"]) == (2, 0, 1)
    assert fresh["stream_health"] == "PARTIAL_LIVE"
    now["t"] += 31
    stale = state.freshness()
    assert stale["fresh"] is False and stale["stale"] is True
    assert stale["last_tick_age_seconds"] == 31.0
    assert (stale["live_stock_count"], stale["stale_stock_count"]) == (0, 2)
    assert stale["stream_health"] == "NO_LIVE_QUOTES"
    # An old exchange timestamp does not make a just-received packet stale.
    state.record_live_quote("1002", START - 3600)
    assert state.freshness()["fresh"] is True


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
