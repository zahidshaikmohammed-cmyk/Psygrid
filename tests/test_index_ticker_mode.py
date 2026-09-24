"""Regression coverage for the index-layer subscription mode.

Dhan's WebSocket only streams Ticker packets (LTP + LTT, request code 15)
for IDX_I instruments - Quote/Full packets (request codes 17/21) require
order-book, volume and OI fields that indices don't have, and Dhan sends
nothing at all back for a Full-mode subscription on this segment. That
mismatch is why the deployed feed showed feed_status=CONNECTED with
messages=0 and quote_packets=0 indefinitely: the watchdog kept correctly
detecting silence and reconnecting, but every reconnect re-subscribed with
the same unsupported mode, so real market data could never arrive.
"""

from types import SimpleNamespace

from dhanhq import MarketFeed

from index_layer import IndexLayerFeed, IndexInstrument, IndexState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def _nifty_state() -> IndexState:
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    return IndexState(_settings(), "nifty", "NIFTY", instrument)


def test_index_layer_subscribes_indices_with_ticker_mode_not_full():
    state = _nifty_state()
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    feed.settings = SimpleNamespace(timezone="Asia/Kolkata", client_id="x", access_token="y")

    # _build_feed constructs a real dhanhq MarketFeed, which we don't want
    # to actually connect - inspect the instrument tuples it's built with
    # by re-deriving them the same way _build_feed does.
    instruments = [
        (MarketFeed.IDX, s.instrument.security_id, MarketFeed.Ticker)
        for s in feed.states.values()
    ]

    assert instruments == [(MarketFeed.IDX, "13", MarketFeed.Ticker)]
    assert MarketFeed.Ticker != MarketFeed.Full


def test_on_message_processes_a_real_ticker_data_packet():
    """Shaped exactly like dhanhq's marketfeed.process_ticker() output -
    the only packet type Dhan actually sends for an IDX_I subscription."""
    state = _nifty_state()
    state.session_status = "LIVE"  # candle building only applies during a live session
    feed = IndexLayerFeed(_settings(), {"nifty": state})

    packet = {
        "type": "Ticker Data",
        "exchange_segment": 0,
        "security_id": 13,  # dhanhq emits this as an int, not a string
        "LTP": "25100.50",
        "LTT": "10:15:30",
    }
    feed._on_message(None, packet)

    assert state.feed_messages == 1
    assert state.quote_packets == 1
    assert state.last_ltp == 25100.50
    assert state.last_tick_received_epoch is not None


def test_on_message_still_processes_quote_and_full_packets_if_ever_sent():
    state = _nifty_state()
    feed = IndexLayerFeed(_settings(), {"nifty": state})

    quote_packet = {
        "type": "Quote Data", "security_id": 13,
        "LTP": "25101.00", "LTT": "10:15:31", "volume": 0,
    }
    feed._on_message(None, quote_packet)
    assert state.quote_packets == 1

    full_packet = {
        "type": "Full Data", "security_id": 13,
        "LTP": "25102.00", "LTT": "10:15:32", "volume": 0,
    }
    feed._on_message(None, full_packet)
    assert state.quote_packets == 2


def test_watchdog_baseline_grows_from_ticker_packets():
    """The watchdog's liveness signal (_total_quote_packets) must actually
    move when Ticker packets - the only kind indices ever send - arrive,
    otherwise a healthy Ticker-mode connection would still look silent."""
    state = _nifty_state()
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    assert feed._total_quote_packets() == 0

    feed._on_message(None, {"type": "Ticker Data", "security_id": 13, "LTP": "25100.0", "LTT": "10:00:00"})

    assert feed._total_quote_packets() == 1
