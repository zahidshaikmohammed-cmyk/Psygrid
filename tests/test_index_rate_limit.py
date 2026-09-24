"""Regression coverage for index-layer rate-limit/connection-limit handling.

IndexLayerFeed's reconnect loop had no way to recognize a Dhan
rate/connection-limit rejection - it always retried at normal speed
(5s doubling to 120s max), the same way it would after an ordinary
transient disconnect. The sealed equity feed.py already backs off for a
full RATE_LIMIT_COOLDOWN (300s) when Dhan rejects a connection this way,
avoiding a fast retry loop that just keeps getting rejected. Given this
session redeployed (and thus reconnected both the equity and index
WebSockets) six times in under an hour, and a real production instance
showed a ~14 minute gap in live index data right after one such restart,
this is a concrete, structural gap worth closing the same way the
watchdog and auth-retry gaps were.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

from index_layer import IndexLayerFeed, IndexInstrument, IndexState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def _feed_with_one_state():
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    state = IndexState(_settings(), "nifty", "NIFTY", instrument)
    feed = IndexLayerFeed(_settings(), {"nifty": state})
    return feed, state


def test_is_rate_limited_error_recognizes_dhan_signals():
    assert IndexLayerFeed._is_rate_limited_error("805")
    assert IndexLayerFeed._is_rate_limited_error("HTTP 429 Too Many Requests")
    assert IndexLayerFeed._is_rate_limited_error("Too many active connections")
    assert IndexLayerFeed._is_rate_limited_error("connection limit exceeded")
    assert not IndexLayerFeed._is_rate_limited_error("websocket closed by peer")
    assert not IndexLayerFeed._is_rate_limited_error("Connection reset by peer")


def test_on_message_error_packet_with_rate_limit_code_sets_extended_cooldown():
    feed, state = _feed_with_one_state()
    fake_feed = MagicMock()
    feed._backoff = feed.NORMAL_INITIAL_BACKOFF

    feed._on_message(fake_feed, {"type": "error", "error_code": 805, "message": "Too many active connections"})

    assert feed._backoff == feed.RATE_LIMIT_COOLDOWN
    assert state.feed_status == "ERROR"
    assert "rate/connection limit" in state.last_feed_error
    fake_feed.close_connection.assert_called_once()


def test_on_message_error_packet_without_rate_limit_code_keeps_normal_backoff():
    feed, state = _feed_with_one_state()
    fake_feed = MagicMock()
    feed._backoff = feed.NORMAL_INITIAL_BACKOFF

    feed._on_message(fake_feed, {"type": "error", "error_code": 500, "message": "internal error"})

    assert feed._backoff == feed.NORMAL_INITIAL_BACKOFF
    assert state.feed_status == "ERROR"
    fake_feed.close_connection.assert_called_once()


def test_run_loop_extends_backoff_on_a_rate_limited_exception():
    feed, state = _feed_with_one_state()

    def _raise_rate_limited(*_args, **_kwargs):
        raise RuntimeError("805 too many connections")

    feed._build_feed = _raise_rate_limited
    feed._stop.set()  # run exactly one iteration, then stop

    # Reset _stop right before the loop body checks it, so one iteration runs.
    feed._stop.clear()

    def _stop_after_one(*_args, **_kwargs):
        feed._stop.set()
        return True

    feed._stop.wait = _stop_after_one
    feed._run()

    assert feed._backoff == feed.RATE_LIMIT_COOLDOWN
    assert "rate/connection limit" in state.last_feed_error


def test_run_loop_uses_normal_backoff_for_an_ordinary_exception():
    feed, state = _feed_with_one_state()

    def _raise_ordinary(*_args, **_kwargs):
        raise RuntimeError("connection reset by peer")

    feed._build_feed = _raise_ordinary
    feed._stop.clear()

    def _stop_after_one(*_args, **_kwargs):
        feed._stop.set()
        return True

    feed._stop.wait = _stop_after_one
    feed._run()

    assert feed._backoff == feed.NORMAL_INITIAL_BACKOFF
    assert "index websocket" in state.last_feed_error
