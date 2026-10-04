"""Feed lifecycle: every Dhan connection cycle closes its MarketFeed and asyncio loop; reconnects never leak.

These drive the production reconnect loop (``feed.LiveFeed._run``) with dhanhq's real
``MarketFeed`` objects - each constructor really creates a private event loop - and only replace
the network connect, so what is verified is the same cleanup path production uses.
"""

import asyncio
import os
import threading
import time
from types import SimpleNamespace

import pytest
from live_core_helpers import FakeSettings, ist

import feed as feed_module
from live_core.feed import LiveCoreFeed
from live_core.state import NodeState
from runtime_guard import open_fd_count

START = int(ist(9, 15).timestamp())


def _instruments(count=3):
    return [
        SimpleNamespace(symbol=f"S{i}", security_id=str(2000 + i), exchange_segment="NSE_EQ", instrument="EQUITY")
        for i in range(count)
    ]


def _state(instruments):
    state = NodeState()
    state.begin("2026-10-05", instruments)
    state.set_session_status("LIVE")
    return state


def _fast(feed):
    feed.NORMAL_INITIAL_BACKOFF = 0.001
    feed.NORMAL_MAX_BACKOFF = 0.002
    feed._backoff = 0.001
    return feed


def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def _run_cycles(feed, cycles):
    assert _wait_for(lambda: feed.feeds_closed >= cycles), feed.lifecycle()
    feed.stop()
    assert not feed.thread_alive()


def test_every_failed_connection_closes_its_real_marketfeed_loop(monkeypatch):
    created = []
    original_init = feed_module.MarketFeed.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    async def refused(self):
        raise ConnectionRefusedError("dhan unreachable")

    monkeypatch.setattr(feed_module.MarketFeed, "__init__", tracking_init)
    monkeypatch.setattr(feed_module.MarketFeed, "connect", refused)
    instruments = _instruments()
    state = _state(instruments)
    feed = _fast(LiveCoreFeed(FakeSettings(), state, instruments))
    feed.start()
    _run_cycles(feed, 20)

    assert len(created) >= 20
    assert all(market_feed.loop.is_closed() for market_feed in created)
    life = feed.lifecycle()
    assert life["event_loops_leaked"] == 0
    assert life["event_loops_closed"] == life["feeds_closed"] == life["connection_cycles"] == len(created)
    assert state.websocket_reconnects >= 20
    assert "dhan unreachable" in state.last_feed_error or state.feed_status == "STOPPED"


def test_pending_tasks_on_the_feed_loop_are_cancelled_before_the_loop_closes(monkeypatch):
    seen = []

    async def leaves_a_task_then_fails(self):
        task = asyncio.get_running_loop().create_task(asyncio.sleep(3600))
        seen.append(task)
        raise ConnectionResetError("reset by peer")

    monkeypatch.setattr(feed_module.MarketFeed, "connect", leaves_a_task_then_fails)
    instruments = _instruments()
    feed = _fast(LiveCoreFeed(FakeSettings(), _state(instruments), instruments))
    feed.start()
    _run_cycles(feed, 5)
    assert seen and all(task.cancelled() for task in seen)
    assert feed.lifecycle()["event_loops_leaked"] == 0


def test_a_session_that_ends_normally_is_also_cleaned_up(monkeypatch):
    async def connects_and_returns(self):
        return None

    monkeypatch.setattr(feed_module.MarketFeed, "connect", connects_and_returns)
    monkeypatch.setattr(feed_module.MarketFeed, "_is_ws_closed", lambda self: True)

    # run() keeps going while _running; end it the way production does, via close_connection.
    async def run_once(self):
        await self.connect()
        self._running = False

    monkeypatch.setattr(feed_module.MarketFeed, "_run_async", run_once)
    instruments = _instruments()
    state = _state(instruments)
    feed = _fast(LiveCoreFeed(FakeSettings(), state, instruments))
    feed.start()
    _run_cycles(feed, 5)
    assert feed.lifecycle()["event_loops_leaked"] == 0
    # A loop that ended without an error is still counted as a reconnect, then retried.
    assert state.websocket_reconnects >= 5


def test_stop_closes_a_running_connection_and_its_loop(monkeypatch):
    connected = threading.Event()
    created = []
    original_init = feed_module.MarketFeed.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    async def hangs_until_disconnected(self):
        self._stop_waiter = asyncio.Event()
        connected.set()
        if self.on_connect:
            self.on_connect(self)
        await self._stop_waiter.wait()

    async def disconnect(self):
        self._stop_waiter.set()
        if self.on_close:
            self.on_close(self)

    monkeypatch.setattr(feed_module.MarketFeed, "__init__", tracking_init)
    monkeypatch.setattr(feed_module.MarketFeed, "_run_async", hangs_until_disconnected)
    monkeypatch.setattr(feed_module.MarketFeed, "disconnect", disconnect)
    instruments = _instruments()
    state = _state(instruments)
    feed = LiveCoreFeed(FakeSettings(), state, instruments)
    feed.start()
    assert connected.wait(5)
    assert state.feed_status == "CONNECTED" and state.subscribed_count == 3
    feed.stop()
    assert not feed.thread_alive()
    assert state.feed_status == "STOPPED"
    assert created and created[-1].loop.is_closed()
    assert feed.lifecycle()["event_loops_leaked"] == 0


def test_feed_messages_build_candles_through_the_production_packet_handler():
    instruments = _instruments(2)
    state = _state(instruments)
    feed = LiveCoreFeed(FakeSettings(), state, instruments)
    now = int(time.time())
    minute = now - now % 60 - 120
    packets = [
        {"type": "Previous Close", "security_id": 2000, "prev_close": 99.5},
        {"type": "Full Data", "security_id": 2000, "LTP": "100.5", "LTT": minute + 3, "volume": 10, "open": 100.0},
        {"type": "Full Data", "security_id": 2000, "LTP": "101.0", "LTT": minute + 30, "volume": 25},
        {"type": "Full Data", "security_id": 2000, "LTP": "100.0", "LTT": minute + 61, "volume": 40},
        {"type": "Full Data", "security_id": 9999, "LTP": "5", "LTT": minute + 61, "volume": 40},
    ]
    for packet in packets:
        feed._on_message(None, packet)
    series = state.instruments["2000"]
    assert (series.previous_close, series.today_open) == (99.5, 100.0)
    assert list(series.epochs) == [minute]
    assert (series.opens[0], series.highs[0], series.lows[0], series.closes[0]) == (100.5, 101.0, 100.5, 101.0)
    assert series.volumes[0] == 15
    assert state.quote_packets == 4 and state.feed_messages == 5


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
def test_hundreds_of_reconnects_do_not_grow_file_descriptors_under_uvloop(monkeypatch):
    uvloop = pytest.importorskip("uvloop")
    # Production runs under uvicorn, which makes asyncio.new_event_loop() return uvloop loops: the
    # original leak kept each abandoned loop's epoll/eventfd/pipe descriptors open.
    monkeypatch.setattr(feed_module.MarketFeed, "connect", _refuse)
    instruments = _instruments()
    result = {}

    def run():
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        try:
            warm = _fast(LiveCoreFeed(FakeSettings(), _state(instruments), instruments))
            warm.start()
            _run_cycles(warm, 5)
            before = open_fd_count()
            feed = _fast(LiveCoreFeed(FakeSettings(), _state(instruments), instruments))
            feed.start()
            _run_cycles(feed, 300)
            result["growth"] = open_fd_count() - before
            result["life"] = feed.lifecycle()
        finally:
            asyncio.set_event_loop_policy(None)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(120)
    assert result["life"]["event_loops_leaked"] == 0
    assert result["life"]["connection_cycles"] >= 300
    assert result["growth"] <= 5, result


async def _refuse(self):
    raise ConnectionRefusedError("dhan unreachable")


def test_stop_landing_between_connect_and_run_still_closes_everything(monkeypatch):
    """stop() arrives after a MarketFeed is built but before its loop runs; the connection then starts.

    The inherited LiveFeed.stop() would run the feed's loop from the stopping thread and then wait
    while the connection started afterwards stayed open. LiveCoreFeed.stop() must end it anyway.
    """
    built = threading.Event()
    original_run = feed_module.MarketFeed.run
    created = []

    def delayed_run(self):
        created.append(self)
        built.set()
        time.sleep(0.3)
        original_run(self)

    async def connected_until_disconnected(self):
        self._stop_waiter = asyncio.Event()
        while self._running:
            try:
                await asyncio.wait_for(self._stop_waiter.wait(), 0.05)
            except TimeoutError:
                continue

    async def disconnect(self):
        if getattr(self, "_stop_waiter", None) is not None:
            self._stop_waiter.set()

    monkeypatch.setattr(feed_module.MarketFeed, "run", delayed_run)
    monkeypatch.setattr(feed_module.MarketFeed, "_run_async", connected_until_disconnected)
    monkeypatch.setattr(feed_module.MarketFeed, "disconnect", disconnect)
    instruments = _instruments()
    feed = LiveCoreFeed(FakeSettings(), _state(instruments), instruments)
    feed.start()
    assert built.wait(5)
    started = time.monotonic()
    feed.stop()
    assert time.monotonic() - started < 6
    assert not feed.thread_alive()
    assert created and all(market_feed.loop.is_closed() for market_feed in created)
    assert feed.lifecycle()["event_loops_leaked"] == 0
