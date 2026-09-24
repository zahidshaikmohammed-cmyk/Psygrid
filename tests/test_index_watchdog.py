"""Regression coverage for the index-layer watchdog: a Dhan WebSocket that
completes its handshake (on_connect fires, feed_status -> CONNECTED) but then
never delivers a quote packet must be detected and force-reconnected, instead
of leaving feed_status stuck at CONNECTED forever with messages=0,
quote_packets=0, ltp=null and empty candle arrays - the exact production
symptom this fix addresses.
"""

import threading
import time
from types import SimpleNamespace

from index_layer import IndexLayerFeed, IndexInstrument, IndexState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def _nifty_state(quote_packets: int = 0) -> IndexState:
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    state = IndexState(_settings(), "nifty", "NIFTY", instrument)
    state.feed_status = "CONNECTED"
    state.quote_packets = quote_packets
    return state


class _FakeFeed:
    """Stands in for dhanhq's MarketFeed: run() blocks until closed, just
    like the real client blocks inside its event loop while the socket
    stays open and idle."""

    def __init__(self):
        self._closed = threading.Event()
        self.close_calls = 0

    def close_connection(self):
        self.close_calls += 1
        self._closed.set()

    def run(self):
        self._closed.wait(timeout=10)


def test_watchdog_force_closes_a_connected_but_silent_feed():
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    feed._connection_started_epoch = time.time() - 100  # already past the threshold
    feed._connection_quote_baseline = 0
    fake = _FakeFeed()

    feed._watch_connection(fake)

    assert fake.close_calls == 1
    assert state.feed_status == "RECONNECTING"
    assert "quote packets" in state.last_feed_error


def test_watchdog_leaves_a_feed_alone_once_packets_are_flowing():
    state = _nifty_state(quote_packets=1)  # a tick arrived after connect
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    feed._connection_started_epoch = time.time() - 100
    feed._connection_quote_baseline = 0  # baseline recorded at connect time
    fake = _FakeFeed()

    feed._watch_connection(fake)

    assert fake.close_calls == 0
    assert state.feed_status == "CONNECTED"


def test_run_connected_session_unblocks_and_reconnects_when_watchdog_fires():
    """Before this fix, a MarketFeed.run() that blocks forever on an idle
    socket meant feed_status stayed CONNECTED indefinitely - this proves the
    watchdog now forces run() to return well within the 10s safety timeout."""
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    feed._connection_started_epoch = time.time() - 100
    feed._connection_quote_baseline = 0
    fake = _FakeFeed()

    started = time.time()
    feed._run_connected_session(fake)
    elapsed = time.time() - started

    assert elapsed < 5.0
    assert fake.close_calls == 1
    assert state.feed_status == "RECONNECTING"


def test_watchdog_thread_exits_cleanly_on_stop_without_touching_feed_status():
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    feed._connection_started_epoch = time.time()  # well within the threshold
    feed._connection_quote_baseline = 0
    feed._stop.set()  # simulate shutdown racing the watchdog
    fake = _FakeFeed()

    feed._watch_connection(fake)

    assert fake.close_calls == 0
    assert state.feed_status == "CONNECTED"
