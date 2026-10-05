"""A stale-symbol resubscribe failure must not mark a streaming feed as ERROR or stall the health pass."""

from types import SimpleNamespace

from feed_runtime import LiveFeed


class _State:
    def __init__(self):
        self.session_status = "LIVE"
        self.feed_status = "CONNECTED"
        self.last_feed_error = ""
        self.settings = SimpleNamespace(max_live_age_seconds=30)
        self.rest_snapshots = 0

    def freshness(self, _security_id, _now):
        return {"data_age_seconds": None}  # every symbol stale

    def set_feed_status(self, status, error=""):
        self.feed_status = status
        if error:
            self.last_feed_error = error

    def mark_websocket_error(self, error):
        self.feed_status = "ERROR"
        self.last_feed_error = error

    def apply_rest_snapshot(self, _snapshot):
        self.rest_snapshots += 1


class _TimedOutFuture:
    def result(self, timeout=None):
        raise TimeoutError()


def test_resubscribe_timeout_keeps_feed_connected_and_stops_the_pass(monkeypatch):
    state = _State()
    instruments = [SimpleNamespace(security_id=str(i), exchange_segment="NSE_EQ", symbol=f"S{i}") for i in range(5)]
    feed = LiveFeed.__new__(LiveFeed)
    feed.state = state
    feed.instruments = instruments
    feed._last_resubscribe = {}
    feed._last_rest_fallback = 0.0
    feed._stop_requested = SimpleNamespace(is_set=lambda: False)
    feed.dhan_api = SimpleNamespace(quote_snapshot=lambda _items: {})
    attempts = []

    def fake_submit(coro, _loop):
        coro.close()
        attempts.append(1)
        return _TimedOutFuture()

    monkeypatch.setattr("feed_runtime.asyncio.run_coroutine_threadsafe", fake_submit)
    ws_feed = SimpleNamespace(ws=object(), loop=SimpleNamespace(is_closed=lambda: False))

    feed._health_pass(ws_feed)

    assert state.feed_status == "CONNECTED"
    assert state.last_feed_error.startswith("RESUBSCRIBE:TimeoutError")
    assert len(attempts) == 1  # stopped after the first timeout
    assert state.rest_snapshots == 1  # REST recovery still runs for the stale symbols
