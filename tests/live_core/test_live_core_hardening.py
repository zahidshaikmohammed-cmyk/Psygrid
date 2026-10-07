"""Production hardening: feed supervision, token renewal, zombie feeds, leak-free replacement,
pre-open readiness, fail-soft stocks and peers, HTTP isolation, streaming and metrics.

The feed tests run the production ``LiveCoreFeed`` with dhanhq's real ``MarketFeed`` objects (each
constructor really creates a private asyncio loop); only the network connect is replaced.
"""

from __future__ import annotations

import asyncio
import gc
import gzip
import os
import threading
import time
from types import SimpleNamespace

import orjson
import pytest
from fastapi.testclient import TestClient
from live_core_helpers import FakeFeed, FakeSettings, feed_minutes, ist

import feed as feed_module
from live_core import aggregate
from live_core.aggregate import PeerClient, PeerUnavailable, backoff_after
from live_core.api import create_app
from live_core.feed import DetachedState, LiveCoreFeed, close_market_feed_bounded
from live_core.metrics import HttpMetrics, endpoint_key
from live_core.runtime import HARD_RESET_SILENCE_SECONDS, RECOVERY_TIMEOUT_SECONDS
from live_core.state import NodeState
from runtime_guard import open_fd_count, rss_bytes

DAY = (2026, 10, 5)


def _wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


@pytest.fixture
def streaming_marketfeed(monkeypatch):
    """dhanhq MarketFeed that 'connects' at once and streams until it is disconnected."""
    created = []
    original_init = feed_module.MarketFeed.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    async def connected_until_disconnected(self):
        if self.on_connect:
            self.on_connect(self)
        while self._running:
            await asyncio.sleep(0.005)

    async def disconnect(self):
        self._running = False

    monkeypatch.setattr(feed_module.MarketFeed, "__init__", tracking_init)
    monkeypatch.setattr(feed_module.MarketFeed, "_run_async", connected_until_disconnected)
    monkeypatch.setattr(feed_module.MarketFeed, "disconnect", disconnect)
    return created


def _trade(state, security_id, ltt, price, volume):
    state.record_feed_message("Full Data")
    accepted = state.update_quote(security_id, {"LTT_EPOCH": ltt, "LTP": price, "volume": volume, "LTQ": 1})
    state.record_live_quote(security_id, ltt)
    return accepted


# --------------------------------------------------------------------------- bounded teardown


def test_teardown_is_bounded_even_when_tasks_ignore_cancellation():
    """The 2026-10-07 stuck feed: a teardown that waits on a task forever can never happen again."""
    loop = asyncio.new_event_loop()

    async def stubborn():
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                continue  # swallows every cancellation

    async def hangs_forever():
        await asyncio.Event().wait()

    background = {loop.create_task(stubborn())}  # kept referenced until the loop closes
    feed = SimpleNamespace(loop=loop, _running=True, disconnect=hangs_forever)
    started = time.monotonic()
    assert close_market_feed_bounded(feed, step_seconds=0.2) is True
    assert loop.is_closed()
    assert time.monotonic() - started < 3.0
    background.clear()


def test_a_failed_connect_that_leaves_an_uncancellable_task_never_stalls_the_feed(monkeypatch):
    """Reproduces the 2026-10-07 09:38 hang: the first connect fails and leaves a websockets task
    behind that swallows cancellation. The old teardown waited on it forever (connection_cycles
    stayed at 1); now each teardown is bounded and the feed keeps making new attempts."""
    created = []
    original_init = feed_module.MarketFeed.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    async def stubborn():
        while True:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                continue

    async def failing_connect(self):
        self._stubborn = asyncio.get_running_loop().create_task(stubborn())
        raise ConnectionResetError("handshake interrupted")

    monkeypatch.setattr(feed_module.MarketFeed, "__init__", tracking_init)
    monkeypatch.setattr(feed_module.MarketFeed, "connect", failing_connect)
    instruments = [SimpleNamespace(symbol="A", security_id="1", exchange_segment="NSE_EQ")]
    state = NodeState()
    state.begin("2026-10-05", instruments)
    state.set_session_status("LIVE")
    feed = LiveCoreFeed(FakeSettings(), state, instruments)
    feed.CLOSE_STEP_SECONDS = 0.1
    feed.NORMAL_INITIAL_BACKOFF = feed.NORMAL_MAX_BACKOFF = feed._backoff = 0.01
    feed.start()
    try:
        assert _wait_for(lambda: feed.connection_cycles >= 3, 10), feed.lifecycle()
    finally:
        feed.stop()
    assert all(m.loop.is_closed() for m in created)
    assert feed.lifecycle()["event_loops_leaked"] == 0 and not feed.abandoned


def test_a_retired_feed_can_never_write_to_the_node_state():
    instruments = [SimpleNamespace(symbol="A", security_id="1", exchange_segment="NSE_EQ")]
    state = NodeState()
    state.begin("2026-10-05", instruments)
    state.set_session_status("LIVE")
    feed = LiveCoreFeed(FakeSettings(), state, instruments)
    feed.stop()
    assert feed.retired and isinstance(feed.state, DetachedState)
    version, packets = state.version, state.quote_packets
    minute = int(ist(10, 0, day=DAY).timestamp())
    feed._on_message(None, {"type": "Full Data", "security_id": 1, "LTP": 5.0, "LTT": minute, "volume": 9})
    feed._on_connect(None)
    feed._on_error(None, RuntimeError("late error from an abandoned thread"))
    assert state.quote_packets == packets and state.version == version
    assert state.feed_status == "STOPPED"  # not CONNECTED by the retired feed's late on_connect
    assert state.instruments["1"].current is None


# --------------------------------------------------------------------------- zombie detection


def test_frames_without_any_accepted_packet_are_a_zombie_feed():
    instruments = [SimpleNamespace(symbol="A", security_id="1", exchange_segment="NSE_EQ")]
    now = {"t": float(ist(10, 0, day=DAY).timestamp())}
    state = NodeState(clock=lambda: now["t"])
    state.begin("2026-10-05", instruments)
    state.set_session_status("LIVE")
    feed = LiveCoreFeed(FakeSettings(), state, instruments)
    feed._connected_at = now["t"]
    market_feed = SimpleNamespace(loop=None, ws=None, _running=True)
    for _ in range(4):  # frames keep arriving, but every one is rejected (outside the session day)
        now["t"] += 10
        state.record_feed_message("Full Data")
        state.update_quote("1", {"LTT_EPOCH": 86400, "LTP": 5.0, "volume": 1})
        assert feed.monitor_tick(market_feed) == "ok"
    now["t"] += 10  # 50 s connected, frames flowing, nothing accepted
    state.record_feed_message("Full Data")
    assert feed.monitor_tick(market_feed) == "reconnect"
    assert feed.lifecycle()["zombie_reconnects"] == 1
    assert "zombie" in state.last_feed_error


# --------------------------------------------------------------------------- supervisor (fake feed)


def _live_runtime(make_runtime, node_id=0, when=None):
    runtime = make_runtime(node_id, when=when or ist(10, 0))
    runtime.tick()
    ids = [series.security_id for series in runtime.state.ordered]
    feed_minutes(runtime.state, ids[:30], int(ist(9, 50).timestamp()), 10)  # 09:50-09:59 candles
    runtime.test_clock.set(ist(10, 0, 30))
    for series_id in ids[:10]:
        _trade(runtime.state, series_id, int(ist(10, 0, 20).timestamp()), 101.0, 10**6)
    runtime.tick()
    return runtime, ids


def test_supervisor_reaches_live_only_after_staged_validation(make_runtime):
    runtime = make_runtime(0, when=ist(10, 0))
    runtime.tick()
    assert runtime.supervisor_state == "CONNECTING"
    runtime.tick()
    # FakeFeed connects at once, but no packet has arrived: connected is not enough for LIVE.
    assert runtime.supervisor_state == "RECOVERING"
    stages = runtime.recovery_stages()
    assert stages["connected"] and not stages["accepted_packet"]
    runtime.test_clock.set(ist(10, 0, 5))
    for series in runtime.state.ordered[:6]:
        _trade(runtime.state, series.security_id, int(ist(10, 0, 4).timestamp()), 100.0, 5000)
    runtime.tick()
    assert runtime.supervisor_state == "LIVE"
    assert [t["to"] for t in runtime.transitions][-2:] == ["RECOVERING", "LIVE"]


def test_a_connected_feed_that_never_delivers_data_is_replaced_and_resets_are_spaced(make_runtime):
    runtime = make_runtime(0, when=ist(10, 0))
    runtime.tick()
    first = runtime.feed
    runtime.test_clock.set(ist(10, 0) + _seconds(RECOVERY_TIMEOUT_SECONDS + 1))
    runtime.tick()
    assert runtime.feed is not first and first.stopped and runtime.hard_resets == 1
    second = runtime.feed
    # Still nothing: the next replacement waits for the back-off instead of storming Dhan.
    runtime.test_clock.set(ist(10, 0) + _seconds(2 * RECOVERY_TIMEOUT_SECONDS + 2))
    runtime.tick()
    runtime.test_clock.set(ist(10, 0) + _seconds(2 * RECOVERY_TIMEOUT_SECONDS + 10))
    runtime.tick()
    assert runtime.hard_resets <= 2
    assert runtime.feed is second or runtime.hard_resets == 2
    live = [f for f in FakeFeed.instances if f.started and not f.stopped and f.state is runtime.state]
    assert live == [runtime.feed]  # exactly one authoritative feed


def test_a_silent_live_feed_is_degraded_then_hard_reset_without_losing_candles(make_runtime):
    runtime, ids = _live_runtime(make_runtime)
    assert runtime.supervisor_state == "LIVE"
    before = {sid: list(runtime.state.instruments[sid].epochs) for sid in ids[:30]}
    old = runtime.feed
    runtime.test_clock.set(ist(10, 1, 20))  # 50 s without an accepted packet
    runtime.tick()
    assert runtime.supervisor_state == "DEGRADED" and runtime.feed is old
    runtime.test_clock.set(ist(10, 0, 30) + _seconds(HARD_RESET_SILENCE_SECONDS + 1))
    runtime.tick()
    assert runtime.feed is not old and old.stopped
    assert runtime.supervisor_state in ("CONNECTING", "RECOVERING")
    # Every published candle survived; nothing was wiped or duplicated.
    for sid, epochs in before.items():
        assert list(runtime.state.instruments[sid].epochs)[: len(epochs)] == epochs
        assert len(set(runtime.state.instruments[sid].epochs)) == len(runtime.state.instruments[sid].epochs)
    # New trades resume cleanly after the gap and LIVE is declared again.
    resume = int(ist(10, 4).timestamp())
    runtime.test_clock.set(ist(10, 4, 5))
    for sid in ids[:10]:
        _trade(runtime.state, sid, resume + 2, 102.0, 2 * 10**6)
    runtime.tick()
    assert runtime.supervisor_state == "LIVE"
    runtime.test_clock.set(ist(10, 5, 5))
    runtime.tick()
    series = runtime.state.instruments[ids[0]]
    assert list(series.epochs)[-1] == resume and list(series.epochs) == sorted(set(series.epochs))
    # The replacement counts as a reconnect, so once the interrupted minutes closed (90 s) the gap
    # was refilled from Dhan's own 1m bars.
    assert runtime.state.websocket_reconnects >= 1
    assert _wait_for(lambda: runtime.history.status()["requests"] > 0)


def test_token_rejection_after_renewal_keeps_retrying_without_a_storm(make_runtime):
    runtime, _ids = _live_runtime(make_runtime)
    replaced = []
    for second in range(0, 600, 15):  # 10 minutes of an authority renewing on every poll
        runtime.test_clock.set(ist(10, 2) + _seconds(second))
        runtime.settings.access_token = f"token-{second}"
        runtime._replace_feed("Dhan token renewed by the token authority", runtime.test_clock.epoch(), token=True)
        replaced.append(runtime.feed)
    live = [f for f in FakeFeed.instances if f.started and not f.stopped and f.state is runtime.state]
    assert live == [runtime.feed] and all(f.stopped for f in replaced[:-1])
    assert runtime.feed.settings.access_token == "token-585"


def test_market_close_during_a_reconnect_is_a_clean_close(make_runtime):
    runtime, _ids = _live_runtime(make_runtime)
    client = TestClient(create_app(runtime, start_runtime=False))
    runtime._replace_feed("simulated failure at the close", runtime.test_clock.epoch())
    replacement = runtime.feed
    runtime.test_clock.set(ist(15, 15))
    runtime.tick()
    assert replacement.stopped and runtime.feed is None and runtime.supervisor_state == "CLOSED"
    assert runtime.state.session_status == "CLOSED" and runtime.state.ordered == []
    assert client.get("/public/live.json").json()["status"] == "CLOSED"
    assert client.get("/health/node").status_code == 200


def test_starting_twice_never_creates_a_second_loop_or_feed(make_runtime):
    runtime = make_runtime(0, when=ist(10, 0))
    runtime.start()
    first_thread = runtime._thread
    runtime.start()
    assert runtime._thread is first_thread
    assert _wait_for(lambda: runtime.feed is not None)
    runtime.tick()
    runtime.tick()
    mine = [f for f in FakeFeed.instances if f.state is runtime.state and f.started and not f.stopped]
    assert len(mine) == 1
    names = [t.name for t in threading.enumerate()]
    assert names.count("live-core-session") == 1
    runtime.stop()


# --------------------------------------------------------------------------- pre-open


def test_pre_open_packets_set_baselines_but_never_make_candles():
    instruments = [SimpleNamespace(symbol="A", security_id="1", exchange_segment="NSE_EQ")]
    now = {"t": float(ist(9, 8, day=DAY).timestamp())}
    state = NodeState(clock=lambda: now["t"])
    state.begin("2026-10-05", instruments, open_time="09:15")
    state.set_session_status("PRE_OPEN")
    auction = int(ist(9, 8, day=DAY).timestamp())
    assert _trade(state, "1", auction, 100.0, 50_000)  # pre-open auction volume
    series = state.instruments["1"]
    assert series.current is None and len(series.epochs) == 0 and series.last_received is None
    assert series.previous_cumulative_volume == 50_000 and state.preopen_packets == 1
    # The first packet at/after the open turns the session LIVE; the auction print is not a candle.
    now["t"] = float(ist(9, 15, 2, day=DAY).timestamp())
    assert _trade(state, "1", auction, 100.0, 50_000)  # the auction print repeated after the open
    assert state.session_status == "LIVE" and series.current is None
    opening = int(ist(9, 15, 1, day=DAY).timestamp())
    assert _trade(state, "1", opening, 101.0, 50_100)
    assert series.current[0] == opening - opening % 60 and series.current[5] == 100  # only post-open volume
    state.finalize_all()
    assert list(series.epochs) == [opening - opening % 60]  # nothing before 09:15


def test_readiness_is_true_only_when_every_check_passes(make_runtime):
    runtime = make_runtime(0, when=ist(9, 5))
    runtime.tick()
    readiness = runtime.readiness()
    assert readiness["checks"]["websocket_connected"] and readiness["checks"]["subscriptions_complete"]
    assert not readiness["checks"]["valid_packets_received"] and not readiness["ready_for_market"]
    assert any("pre-open: not ready" in r for r in runtime.node_health()["reasons"])
    runtime.test_clock.set(ist(9, 5, 3))
    runtime.state.record_feed_message("Previous Close")
    readiness = runtime.readiness()
    assert readiness["ready_for_market"], readiness["failing_checks"]
    assert runtime.lifecycle_phase() == "READY"
    assert runtime.node_health()["readiness"]["ready_for_market"] is True


def test_an_authentication_failure_at_0905_recovers_before_the_open(make_runtime):
    runtime = make_runtime(0, when=ist(9, 5))
    runtime.test_api.verify_error = RuntimeError("Dhan API HTTP error: data plan not active")
    runtime.tick()
    assert runtime.feed is None and runtime.state.session_status == "AUTH_ERROR"
    assert not runtime.readiness()["ready_for_market"]
    runtime.test_api.verify_error = None
    runtime.test_clock.set(ist(9, 5, 31))
    runtime.tick()
    assert runtime.state.session_status == "PRE_OPEN" and runtime.feed is not None  # fixed before 09:15


def test_a_peer_that_is_down_blocks_readiness(make_runtime):
    calls = []

    def peer_get(url, timeout):
        calls.append(url)
        raise ConnectionRefusedError("node 1 unreachable")

    runtime = make_runtime(0, when=ist(9, 5), peers={1: "http://node1:10000"}, peer_get=peer_get)
    runtime.tick()
    runtime.test_clock.set(ist(9, 5, 40))
    runtime.tick()  # first peer probe
    readiness = runtime.readiness()
    assert readiness["checks"]["peers_ready"] is False and readiness["peers"]["1"]["reachable"] is False


# --------------------------------------------------------------------------- one bad stock / many


def test_many_bad_packets_never_disturb_the_other_stocks(make_runtime):
    runtime, ids = _live_runtime(make_runtime)
    good, bad = ids[:20], ids[100:150]
    feed_before = runtime.feed
    minute = int(ist(10, 1).timestamp())
    runtime.test_clock.set(ist(10, 1, 30))
    for sid in bad:
        for packet in (
            {"LTT_EPOCH": minute, "LTP": float("nan"), "volume": 1},
            {"LTT_EPOCH": minute, "LTP": -1.0, "volume": 1},
            {"LTT_EPOCH": "x", "LTP": 1.0},
            {"LTT_EPOCH": 86400, "LTP": 1.0, "volume": 1},
        ):
            assert runtime.state.update_quote(sid, packet) is False
    for sid in good:
        assert _trade(runtime.state, sid, minute + 5, 100.5, 2 * 10**6)
    runtime.tick()
    assert runtime.feed is feed_before  # no global restart for stock-level errors
    assert all(runtime.state.instruments[sid].current is not None for sid in good)
    assert all(runtime.state.instruments[sid].current is None for sid in bad)
    rejected = runtime.state.snapshot()["rejected_packets"]
    assert rejected["non_finite"] == 50 and rejected["non_positive"] == 50 and rejected["outside_session_day"] == 50


def test_a_history_refill_failure_is_recorded_and_harmless(make_runtime):
    runtime, ids = _live_runtime(make_runtime)

    def broken(item, interval):
        raise RuntimeError("Dhan history HTTP 500")

    runtime.test_api.load_today_completed_intraday = broken
    before = runtime.state.memory_summary()["completed_candles_in_ram"]
    runtime.history.enqueue(ids[:3])
    assert _wait_for(lambda: runtime.history.status()["failures"] >= 3)
    assert runtime.state.memory_summary()["completed_candles_in_ram"] == before
    assert runtime.state.session_status == "LIVE" and runtime.feed is not None


# --------------------------------------------------------------------------- peers


def test_peer_backoff_follows_the_bounded_schedule():
    assert [backoff_after(n) for n in range(0, 9)] == [0.0, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 60.0, 60.0]
    now = {"t": 1000.0}
    calls = []

    def get(url, timeout):
        calls.append(now["t"])
        raise TimeoutError("peer timed out")

    peer = PeerClient(
        1,
        "http://node1:10000",
        expected_partition={"start_index": 495, "end_index": 989},
        stale_seconds=0.0,
        get=get,
        monotonic=lambda: now["t"],
    )
    for _ in range(600):  # ten minutes of requests, one per second
        with pytest.raises(PeerUnavailable):
            peer.snapshot()
        now["t"] += 1.0
    # 1 + 2 + 5 + 10 + 30 + 60 s gaps, then every 60 s: about 14 attempts in 10 minutes, never 600.
    assert 10 <= len(calls) <= 16
    assert peer.status()["backoff_seconds"] == 60.0


@pytest.mark.parametrize("failure", ["timeout", "http500", "disappears"])
def test_node0_reports_partial_while_node1_fails_and_recovers_to_complete(make_runtime, failure):
    from test_live_core_api import NODE1_URL, Router

    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get, peer_stale_seconds=0.0)
    node1 = make_runtime(1, when=ist(9, 15))
    for runtime in (node0, node1):
        runtime.tick()
        feed_minutes(runtime.state, [s.security_id for s in runtime.state.ordered], int(ist(9, 15).timestamp()), 2)
        runtime.test_clock.set(ist(9, 17, 5))
        runtime.tick()
    healthy_client = TestClient(create_app(node1, start_runtime=False))
    client0 = TestClient(create_app(node0, start_runtime=False))

    def broken_get(url, timeout):
        if failure == "timeout":
            raise TimeoutError("read timed out")
        if failure == "http500":
            return 500, b"internal error"
        raise ConnectionRefusedError("connection refused")

    router.get = broken_get
    node0.peers[1]._get = broken_get
    payload = client0.get("/public/live.json").json()
    assert payload["status"] == "PARTIAL" and payload["stock_count"] == 495
    assert payload["coverage"]["complete"] is False  # never a fake 989/989
    assert client0.get("/health/node").status_code == 200  # node 0 keeps serving

    def healthy_get(url, timeout):
        path = url.split("10000", 1)[1]
        response = healthy_client.get(path)
        return response.status_code, response.content

    node0.peers[1]._get = healthy_get
    node0.peers[1]._retry_at = 0.0  # the back-off has elapsed
    payload = client0.get("/public/live.json").json()
    assert payload["status"] == "OK" and payload["stock_count"] == 989 and payload["coverage"]["complete"] is True
    assert node0.peers[1].status()["circuit_open"] is False


# --------------------------------------------------------------------------- HTTP


def test_uncompressed_live_json_is_streamed_byte_identical_and_gzip_still_decodes(make_runtime):
    runtime, _ids = _live_runtime(make_runtime)
    client = TestClient(create_app(runtime, start_runtime=False))
    service_body = client.app.state.live_core.live()
    plain = client.get("/public/live.json", headers={"Accept-Encoding": "identity"})
    assert plain.status_code == 200 and "content-encoding" not in plain.headers
    assert int(plain.headers["content-length"]) == len(plain.content) == len(service_body)
    assert plain.content == service_body.head + b"".join(service_body.tail.parts)
    zipped = client.get("/public/live.json", headers={"Accept-Encoding": "gzip"})
    assert zipped.json() == orjson.loads(plain.content)
    raw = client.get("/public/live.json", headers={"Accept-Encoding": "gzip"}).request
    assert raw is not None
    assert gzip.decompress(client.app.state.live_core.live().gzipped()) == plain.content


def test_http_requests_are_counted_in_ram_with_bounded_endpoint_keys(make_runtime):
    runtime, _ids = _live_runtime(make_runtime)
    client = TestClient(create_app(runtime, start_runtime=False))
    client.get("/public/live.json")
    client.get("/public/live-a.json")  # a shard owned by node 0
    client.get(f"/public/stock/{runtime.universe.symbols[0]}.json")  # owned by node 0
    for i in range(50):
        client.get(f"/scanner/probe-{i}.php")
    snapshot = runtime.http_metrics.snapshot()
    assert snapshot["request_count"] == 53
    assert snapshot["client_errors"] == 50 and snapshot["request_errors"] == 0
    assert set(snapshot["endpoints"]) == {
        "/public/live.json",
        "/public/live-{shard}.json",
        "/public/stock/{symbol}.json",
        "other",
    }
    assert snapshot["response_bytes"] > 0 and snapshot["active_requests"] == 0
    assert endpoint_key("/public/live-z.json") == "/public/live-{shard}.json"
    metrics = client.get("/health/metrics").json()
    assert metrics["http"]["request_count"] >= 53 and metrics["storage"].startswith("RAM only")


def test_slow_requests_and_errors_are_counted():
    metrics = HttpMetrics(slow_seconds=0.5)
    metrics.started()
    metrics.finished("/public/live.json", 200, 1000, 0.7)
    metrics.started()
    metrics.finished("/health", 500, 10, 0.01)
    snap = metrics.snapshot()
    assert snap["slow_requests"] == 1 and snap["request_errors"] == 1 and snap["latency_p95_ms"] == 1000


def test_the_sampler_records_minutes_and_pins_the_opening_window(make_runtime):
    runtime = make_runtime(0, when=ist(9, 14, 30))
    runtime.tick()
    for minute in range(15, 32):
        runtime.test_clock.set(ist(9, minute, 1))
        runtime.tick()
    snap = runtime.sampler.snapshot()
    labels = [sample["minute"] for sample in snap["minutes"]]
    assert "09:15" in labels and "09:30" in labels
    window = snap["market_open_window"]["minutes"]
    assert [s["minute"] for s in window] == [f"09:{m}" for m in range(15, 31)]
    first = window[0]
    for key in (
        "cpu_percent_of_one_core",
        "rss_mb",
        "open_fds",
        "threads",
        "packets_per_second_max",
        "http_requests",
        "stale_stocks",
        "instruments",
        "supervisor_state",
    ):
        assert key in first


# --------------------------------------------------------------------------- real feeds


def _authority_runtime(make_runtime):
    runtime = make_runtime(0, when=ist(10, 0), feed_factory=LiveCoreFeed)
    return runtime


def test_token_renewal_during_live_replaces_the_real_feed_while_http_keeps_serving(make_runtime, streaming_marketfeed):
    """LIVE -> token renewal -> old socket closed and loop released -> new LiveFeed with the renewed
    token for the same partition -> valid packets -> LIVE. HTTP answers throughout; candles survive."""
    runtime = _authority_runtime(make_runtime)
    runtime.tick()
    old = runtime.feed
    assert _wait_for(lambda: runtime.state.feed_status == "CONNECTED")
    ids = [s.security_id for s in runtime.state.ordered]
    feed_minutes(runtime.state, ids[:20], int(ist(9, 50).timestamp()), 10)
    runtime.test_clock.set(ist(10, 0, 30))
    for sid in ids[:10]:
        _trade(runtime.state, sid, int(ist(10, 0, 25).timestamp()), 101.0, 10**6)
    runtime.tick()
    assert runtime.supervisor_state == "LIVE"
    client = TestClient(create_app(runtime, start_runtime=False))
    candles_before = runtime.state.memory_summary()["completed_candles_in_ram"]

    answers = []
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            answers.append(client.get("/public/live.json").status_code)
            answers.append(client.get("/health/node").status_code)

    poller = threading.Thread(target=poll)
    poller.start()
    try:
        runtime.settings.access_token = "renewed-token"
        assert runtime._replace_feed(
            "Dhan token renewed by the token authority", runtime.test_clock.epoch(), token=True
        )
    finally:
        time.sleep(0.05)
        stop.set()
        poller.join(10)
    new = runtime.feed
    assert new is not old and old.retired and not old.thread_alive()
    assert old._feed is None and streaming_marketfeed[0].loop.is_closed()
    assert new.settings.access_token == "renewed-token" and new.instruments == old.instruments
    assert answers and set(answers) == {200}  # HTTP never failed during the replacement
    assert _wait_for(lambda: runtime.state.feed_status == "CONNECTED")
    runtime.test_clock.set(ist(10, 1, 5))
    for sid in ids[:10]:
        _trade(runtime.state, sid, int(ist(10, 1, 2).timestamp()), 102.0, 2 * 10**6)
    runtime.tick()
    assert runtime.supervisor_state == "LIVE"
    assert runtime.state.memory_summary()["completed_candles_in_ram"] >= candles_before
    assert runtime.abandoned_feed_threads == 0
    life = new.lifecycle()
    assert life["event_loops_leaked"] == 0


def _replace_many(runtime, cycles):
    for _ in range(cycles):
        assert runtime._replace_feed("stress", runtime.test_clock.epoch(), token=True)
        assert _wait_for(lambda: runtime.state.feed_status == "CONNECTED", 5)


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
def test_100_feed_replacements_do_not_grow_fds_threads_loops_or_memory(make_runtime, streaming_marketfeed):
    runtime = _authority_runtime(make_runtime)
    runtime.tick()
    assert _wait_for(lambda: runtime.state.feed_status == "CONNECTED")
    _replace_many(runtime, 5)  # warm-up: imports, thread pools, allocator arenas
    gc.collect()
    readings = {}
    base = (open_fd_count(), threading.active_count(), rss_bytes())
    done = 0
    for checkpoint in (10, 25, 50, 100):
        _replace_many(runtime, checkpoint - done)
        done = checkpoint
        gc.collect()
        readings[checkpoint] = (open_fd_count(), threading.active_count(), rss_bytes())
    created = streaming_marketfeed
    current = runtime.feed._feed
    unclosed = [m for m in created if not m.loop.is_closed() and m is not current]
    assert unclosed == []  # every replaced feed's private event loop is closed
    for checkpoint, (fds, threads, _rss) in readings.items():
        assert fds - base[0] <= 4, (checkpoint, readings, base)
        assert threads - base[1] <= 2, (checkpoint, readings, base)
    # No linear growth: memory after 100 replacements is within a few MB of after 10.
    assert readings[100][2] - readings[10][2] < 12 * 1048576, readings
    assert runtime.abandoned_feed_threads == 0
    assert len(created) >= 105


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
def test_100_replacements_under_uvloop_do_not_leak_descriptors(make_runtime, streaming_marketfeed):
    uvloop = pytest.importorskip("uvloop")
    runtime = _authority_runtime(make_runtime)
    result = {}

    def run():
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        try:
            runtime.tick()
            _wait_for(lambda: runtime.state.feed_status == "CONNECTED")
            _replace_many(runtime, 5)
            before = open_fd_count()
            _replace_many(runtime, 100)
            result["growth"] = open_fd_count() - before
        finally:
            asyncio.set_event_loop_policy(None)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(240)
    assert result["growth"] <= 4, result


def _seconds(value):
    from datetime import timedelta

    return timedelta(seconds=value)


@pytest.fixture(autouse=True)
def _reset_fake_feeds():
    FakeFeed.instances.clear()
    yield
    FakeFeed.instances.clear()


def test_aggregate_module_exposes_the_schedule():
    assert aggregate.BACKOFF_SCHEDULE_SECONDS == (1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
