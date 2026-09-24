"""Regression coverage for the index-layer watchdog.

Two failure modes are covered:

1. A Dhan WebSocket that completes its handshake (on_connect fires,
   feed_status -> CONNECTED) but then never delivers a quote packet must be
   detected and force-reconnected, instead of leaving feed_status stuck at
   CONNECTED forever with messages=0, quote_packets=0, ltp=null and empty
   candle arrays - the exact production symptom the watchdog addresses.

2. The watchdog thread must never act on a stale _connection_started_epoch /
   _connection_quote_baseline snapshot left over from a *previous* connection
   cycle - it must wait for the *current* connection's own on_connect to
   populate those fields before evaluating anything.
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
    """Stands in for dhanhq's MarketFeed: optionally fires an on_connect
    callback (like a completed handshake) as soon as run() starts, then
    blocks - like the real client blocks inside its event loop - until
    closed."""

    def __init__(self, on_connect=None, run_timeout=10):
        self._closed = threading.Event()
        self.close_calls = 0
        self._on_connect = on_connect
        self._run_timeout = run_timeout

    def close_connection(self):
        self.close_calls += 1
        self._closed.set()

    def run(self):
        if self._on_connect is not None:
            self._on_connect(self)
        self._closed.wait(timeout=self._run_timeout)


def test_watchdog_cannot_act_on_stale_state_before_new_on_connects_fires():
    """Requirement 6: simulate a previous connection that ended with a
    non-zero packet baseline and a long-elapsed started epoch (exactly the
    state that, pre-fix, a freshly-spawned watchdog thread could read before
    the new connection's on_connect callback ran). While the new
    connection's on_connect has not yet fired, the watchdog must sit idle -
    never force-closing based on the leftover values."""
    state = _nifty_state(quote_packets=500)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    # Leftover instance state from a previous, already-finished connection cycle.
    feed._connection_started_epoch = time.time() - 500
    feed._connection_quote_baseline = 500
    fake = _FakeFeed()

    watchdog_thread = threading.Thread(target=feed._watch_connection, args=(fake,), daemon=True)
    watchdog_thread.start()

    # The new connection's on_connect has deliberately NOT fired yet, so
    # _connected_event is still clear. Give the watchdog thread every chance
    # to misbehave before asserting it hasn't.
    time.sleep(0.5)
    assert fake.close_calls == 0
    assert state.feed_status == "CONNECTED"

    # Now the new connection actually completes its handshake.
    feed._on_connect(fake)
    # Fresh values: started "now", baseline = current total (500, unchanged
    # since no new packet has arrived yet) - well within the 25s threshold.
    assert time.time() - feed._connection_started_epoch < 1.0
    assert feed._connection_quote_baseline == 500

    time.sleep(1.0)
    assert fake.close_calls == 0  # correctly still waiting out the fresh 25s window
    assert state.feed_status == "CONNECTED"

    feed._stop.set()
    feed._connection_stop.set()
    watchdog_thread.join(timeout=5)
    assert not watchdog_thread.is_alive()


def test_watchdog_fires_after_25s_of_silence_once_connected():
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    fake = _FakeFeed()

    feed._on_connect(fake)  # simulate the handshake completing, for real
    feed._connection_started_epoch -= 100  # fast-forward past the 25s threshold

    feed._watch_connection(fake)  # blocking; returns once it force-closes

    assert fake.close_calls == 1
    assert state.feed_status == "RECONNECTING"
    assert "quote packets" in state.last_feed_error


def test_watchdog_stays_inactive_once_packets_arrive():
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    fake = _FakeFeed()

    feed._on_connect(fake)
    feed._connection_started_epoch -= 100  # past the threshold
    state.quote_packets = 1  # a real tick arrived on this connection

    feed._watch_connection(fake)  # blocking; returns promptly since packets grew

    assert fake.close_calls == 0
    assert state.feed_status == "CONNECTED"


def test_stop_terminates_watchdog_cleanly_mid_connection():
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    fake = _FakeFeed(on_connect=feed._on_connect, run_timeout=30)
    feed._feed = fake

    session_thread = threading.Thread(target=feed._run_connected_session, args=(fake,))
    session_thread.start()
    time.sleep(0.3)  # let on_connect fire and the watchdog settle into its wait

    feed.stop()
    session_thread.join(timeout=5)

    assert not session_thread.is_alive()
    assert feed._watchdog is None
    # stop() itself must not be reported as a watchdog-detected silence.
    assert "quote packets" not in state.last_feed_error


def test_run_connected_session_unblocks_and_reconnects_when_watchdog_fires():
    """Integration check: before this fix, a MarketFeed.run() that blocks
    forever on an idle socket meant feed_status stayed CONNECTED
    indefinitely. This proves the watchdog now forces run() to return, and
    that the existing reconnect path (the caller's _run loop / backoff) is
    what's left to pick it back up - no second reconnect mechanism."""
    state = _nifty_state(quote_packets=0)
    feed = IndexLayerFeed(_settings(), {"nifty": state})

    def _fast_on_connect(f):
        feed._on_connect(f)
        feed._connection_started_epoch -= 100  # fast-forward past the threshold

    fake = _FakeFeed(on_connect=_fast_on_connect)

    started = time.time()
    feed._run_connected_session(fake)
    elapsed = time.time() - started

    assert elapsed < 5.0
    assert fake.close_calls == 1
    assert state.feed_status == "RECONNECTING"
    assert feed._watchdog is None  # joined and cleared, not leaked


def test_run_connected_session_handles_a_handshake_that_never_connects():
    """If on_connect never fires this cycle (e.g. the handshake itself
    fails), the watchdog must not hang forever nor touch feed_status."""
    state = _nifty_state(quote_packets=0)
    state.feed_status = "CONNECTING"  # on_connect never fires in this scenario
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    fake = _FakeFeed(on_connect=None, run_timeout=1)  # run() returns on its own, no connect

    started = time.time()
    feed._run_connected_session(fake)
    elapsed = time.time() - started

    assert elapsed < 3.0
    assert fake.close_calls == 0
    assert state.feed_status == "CONNECTING"  # unchanged - watchdog took no action
    assert feed._watchdog is None
