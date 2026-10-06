"""HTTP contract across two nodes: aggregation, shard ownership, node failure and feed failure."""

import gzip
import importlib.util
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import orjson
import pytest
from fastapi.testclient import TestClient
from live_core_helpers import feed_minutes, ist

import feed as feed_module
from live_core.api import create_app
from live_core.feed import LiveCoreFeed

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("check_live_universe", ROOT / "tools" / "check_live_universe.py")
check_live_universe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_live_universe)

NODE0_URL = "http://node0.internal:10000"
NODE1_URL = "http://node1.internal:10000"


class Router:
    """Routes a node's peer requests to the other node's in-process app (or fails like a dead VM)."""

    def __init__(self):
        self.clients: dict[str, TestClient] = {}
        self.down: set[str] = set()
        self.calls: list[str] = []

    def get(self, url, timeout):
        parts = urlsplit(url)
        base = f"{parts.scheme}://{parts.netloc}"
        self.calls.append(parts.path)
        if base in self.down or base not in self.clients:
            raise ConnectionRefusedError(f"connect to {base} refused")
        response = self.clients[base].get(parts.path + (f"?{parts.query}" if parts.query else ""))
        return response.status_code, response.content


@pytest.fixture
def cluster(make_runtime):
    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get)
    node1 = make_runtime(1, when=ist(9, 15), peers={0: NODE0_URL}, peer_get=router.get)
    clients = []
    for runtime, url in ((node0, NODE0_URL), (node1, NODE1_URL)):
        runtime.tick()
        start = int(ist(9, 15).timestamp())
        # Trades for 09:15-09:17, received "now" at 09:18:05, so the node is fresh and the minutes are closed.
        runtime.test_clock.set(ist(9, 18, 5))
        feed_minutes(runtime.state, [s.security_id for s in runtime.state.ordered], start, 3)
        for series in runtime.state.ordered:
            runtime.state.set_market_reference(series.security_id, previous_close=100.0, today_open=100.5)
        runtime.tick()
        client = TestClient(create_app(runtime, start_runtime=False))
        router.clients[url] = client
        clients.append(client)
    return node0, node1, clients[0], clients[1], router


def test_node0_serves_the_full_989_stock_live_json_from_both_partitions(cluster, universe):
    _node0, _node1, client0, client1, _router = cluster
    response = client0.get("/public/live.json")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "OK"
    assert payload["stock_count"] == 989 and len(payload["stocks"]) == 989
    assert list(payload["stocks"]) == sorted(universe.symbols)
    assert payload["coverage"]["complete"] is True
    assert {k: v["source"] for k, v in payload["coverage"]["nodes"].items()} == {"0": "local", "1": "peer"}
    check_live_universe.validate_payload_contract(payload, "FULL", 989)
    for symbol in ("RELIANCE", universe.symbols[494], universe.symbols[495], universe.symbols[-1]):
        assert len(payload["stocks"][symbol]["candles_1m"]) == 3
    # The peer's stock objects are spliced in unchanged.
    peer_symbol = universe.symbols[700]
    assert payload["stocks"][peer_symbol] == {
        k: v
        for k, v in client1.get(f"/public/stock/{peer_symbol}.json").json().items()
        if k not in {"service", "schema_version", "status"}
    }


def test_existing_universe_checker_passes_against_the_live_core_cluster(cluster, monkeypatch, capsys):
    _node0, _node1, client0, _client1, _router = cluster

    def fetch_json(_base_url, path):
        response = client0.get(path)
        assert response.status_code == 200, (path, response.text[:200])
        return response.json()

    monkeypatch.setattr(check_live_universe, "fetch_json", fetch_json)
    assert check_live_universe.main() == 0
    assert "RESULT: PASS" in capsys.readouterr().out


def test_shards_are_served_by_their_owning_node(cluster, universe):
    _node0, _node1, client0, _client1, router = cluster
    router.calls.clear()
    for name, start, end, owner in (("a", 0, 45, 0), ("k", 450, 495, 0), ("l", 495, 540, 1), ("v", 945, 989, 1)):
        payload = client0.get(f"/public/live-{name}.json").json()
        assert list(payload["stocks"]) == list(universe.symbols[start:end])  # canonical order, never re-sorted
        assert list(payload["coverage"]["nodes"]) == [str(owner)]
        check_live_universe.validate_payload_contract(payload, name, end - start)
    # Shards a..k never touch the peer; l..v fetch only their own slice.
    assert router.calls == [
        "/internal/live-core/fragments",
        "/internal/live-core/fragments",
    ]


def test_either_node_can_answer_for_the_whole_universe(cluster, universe):
    _node0, _node1, _client0, client1, _router = cluster
    payload = client1.get("/public/live.json").json()
    assert payload["status"] == "OK" and len(payload["stocks"]) == 989
    assert client1.get("/public/stock/RELIANCE.json").json()["symbol"] == "RELIANCE"
    assert client1.get("/public/live-a.json").json()["stock_count"] == 45


def test_stock_endpoint_routes_to_the_owner_and_handles_unknown_symbols(cluster, universe):
    _node0, _node1, client0, _client1, _router = cluster
    local = client0.get("/public/stock/RELIANCE.json").json()
    peer = client0.get(f"/public/stock/{universe.symbols[900]}.json").json()
    mm = client0.get("/public/stock/M&M.json")
    for payload in (local, peer, mm.json()):
        assert payload["status"] == "OK" and payload["schema_version"] == "4.0"
        assert set(payload) == {"service", "schema_version", "status"} | check_live_universe.EXPECTED_STOCK_KEYS
    assert client0.get("/public/stock/NOPE.json").json() == {
        "service": "PSYGRID",
        "symbol": "NOPE",
        "status": "NOT_FOUND",
    }


def test_peer_failure_is_reported_never_hidden(cluster, universe):
    node0, _node1, client0, _client1, router = cluster
    router.down.add(NODE1_URL)
    response = client0.get("/public/live.json")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "PARTIAL"
    assert payload["stock_count"] == 495 and set(payload["stocks"]) == set(universe.symbols[:495])
    assert payload["coverage"]["complete"] is False
    assert payload["coverage"]["nodes"]["1"]["available"] is False
    assert "refused" in payload["coverage"]["nodes"]["1"]["error"]

    shard = client0.get("/public/live-l.json")
    assert shard.status_code == 503 and shard.json()["status"] == "NODE_UNAVAILABLE"
    assert client0.get("/public/live-a.json").json()["status"] == "OK"  # node 0's own shards are unaffected
    stock = client0.get(f"/public/stock/{universe.symbols[600]}.json")
    assert stock.status_code == 503 and stock.json()["node_id"] == 1

    health = client0.get("/health").json()
    cluster_view = health["cluster"]
    assert cluster_view["nodes"]["0"]["healthy"] is True
    assert cluster_view["nodes"]["1"]["reachable"] is False and cluster_view["nodes"]["1"]["healthy"] is False
    assert cluster_view["partitions_covered"] is False and cluster_view["coverage_status"] == "LIVE_INCOMPLETE"
    assert cluster_view["covered_instrument_count"] == 495

    peer = node0.peers[1]
    assert peer.circuit_open()  # a failed peer is not hammered: it is retried after a back-off
    router.calls.clear()
    assert client0.get("/public/live.json").json()["status"] == "PARTIAL"
    assert router.calls == []
    router.down.clear()  # node 1 comes back: coverage recovers without restarting node 0
    peer._retry_at = 0.0  # the back-off has elapsed
    peer._health = None
    assert client0.get("/public/live.json").json()["status"] == "OK"
    assert client0.get("/health").json()["cluster"]["partitions_covered"] is True


def test_a_brief_peer_blip_reuses_recent_peer_state_then_expires(cluster):
    node0, _node1, client0, _client1, router = cluster
    peer = node0.peers[1]
    fake_now = {"t": 1000.0}
    peer._monotonic = lambda: fake_now["t"]
    peer.stale_seconds = 15.0
    assert client0.get("/public/live.json").json()["status"] == "OK"
    router.down.add(NODE1_URL)
    fake_now["t"] += 5
    blip = client0.get("/public/live.json").json()
    assert blip["status"] == "OK" and blip["stock_count"] == 989
    assert blip["coverage"]["nodes"]["1"]["stale"] is True
    fake_now["t"] += 20
    expired = client0.get("/public/live.json").json()
    assert expired["status"] == "PARTIAL" and expired["stock_count"] == 495


def test_a_peer_built_from_a_different_partition_is_refused(make_runtime):
    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get)
    wrong = make_runtime(1, node_count=3, when=ist(9, 15))
    for runtime in (node0, wrong):
        runtime.tick()
    router.clients[NODE1_URL] = TestClient(create_app(wrong, start_runtime=False))
    client0 = TestClient(create_app(node0, start_runtime=False))
    payload = client0.get("/public/live.json").json()
    assert payload["status"] == "PARTIAL" and payload["stock_count"] == 495
    assert "different partition" in payload["coverage"]["nodes"]["1"]["error"]
    health = client0.get("/health").json()["cluster"]
    assert health["nodes"]["1"]["healthy"] is False and health["partitions_covered"] is False


def test_off_market_endpoints_match_the_full_psygrid_closed_contract(make_runtime, monkeypatch, capsys):
    router = Router()
    node0 = make_runtime(0, when=ist(16, 0), peers={1: NODE1_URL}, peer_get=router.get)
    node1 = make_runtime(1, when=ist(16, 0), peers={0: NODE0_URL}, peer_get=router.get)
    for runtime, url in ((node0, NODE0_URL), (node1, NODE1_URL)):
        runtime.tick()
        router.clients[url] = TestClient(create_app(runtime, start_runtime=False))
    client0 = router.clients[NODE0_URL]
    payload = client0.get("/public/live.json").json()
    assert payload["status"] == "CLOSED" and payload["stocks"] == {} and payload["stock_count"] == 0
    assert client0.get("/public/live-q.json").json()["status"] == "CLOSED"
    assert client0.get("/public/stock/RELIANCE.json").json()["status"] == "NOT_FOUND"
    health = client0.get("/health").json()
    assert health["cluster"]["coverage_status"] == "IDLE_READY" and health["status"] == "OK"

    monkeypatch.setattr(check_live_universe, "fetch_json", lambda _base, path: client0.get(path).json())
    monkeypatch.setattr(check_live_universe, "MARKET_START", ist(23, 59).time())
    assert check_live_universe.main() == 0
    assert "OFF-MARKET" in capsys.readouterr().out


def test_node_health_reports_every_required_field(cluster):
    _node0, _node1, client0, _client1, _router = cluster
    health = client0.get("/health/node").json()
    assert health["status"] == "OK", health["reasons"]
    assert (health["node_id"], health["node_count"]) == (0, 2)
    assert health["session"]["session_status"] == "LIVE"
    feed = health["feed"]
    assert feed["feed_status"] == "CONNECTED"
    assert (feed["subscribed_instrument_count"], feed["expected_instrument_count"]) == (495, 495)
    for key in ("last_feed_message_at", "websocket_reconnects", "event_loops_leaked", "connection_cycles"):
        assert key in feed
    for key in ("open_fds", "fd_limit", "rss_mb"):
        assert key in health["process"]
    assert {"fresh", "last_tick_age_seconds", "live_stock_count", "stale_stock_count"} <= set(health["freshness"])
    assert isinstance(health["errors"], list)
    assert health["storage"] == {
        "market_data_on_disk": False,
        "archive_enabled": False,
        "microstructure_enabled": False,
    }
    assert health["partition"]["expected_instrument_count"] == 495
    cluster_health = client0.get("/health").json()["cluster"]
    assert cluster_health["all_nodes_healthy"] is True and cluster_health["partitions_covered"] is True
    assert cluster_health["coverage_status"] == "LIVE_COMPLETE"
    assert cluster_health["covered_instrument_count"] == 989
    assert client0.get("/public/health.json").json()["live_core"]["node_count"] == 2


def test_stale_feed_makes_the_node_degraded_but_still_serving(cluster):
    node0, _node1, client0, _client1, _router = cluster
    node0.test_clock.set(ist(9, 30))  # last trade at 09:17:50: far past the 30 s freshness limit
    health = client0.get("/health/node").json()
    assert health["status"] == "DEGRADED"
    assert health["freshness"]["stale"] is True
    assert any("stale" in reason for reason in health["reasons"])
    assert client0.get("/ready").status_code == 503
    assert client0.get("/public/live-a.json").status_code == 200


def test_large_responses_are_gzipped_and_cached(cluster):
    node0, _node1, client0, _client1, router = cluster
    raw = client0.get("/public/live.json", headers={"Accept-Encoding": "identity"})
    assert "content-encoding" not in raw.headers
    response = client0.get("/public/live.json", headers={"Accept-Encoding": "gzip"})
    assert response.headers["content-encoding"] == "gzip"
    assert response.headers["cache-control"].startswith("no-store")
    assert orjson.loads(response.content)["stock_count"] == 989  # httpx already decoded it
    entry = client0.app.state.live_core._cache[("live",)]
    assert orjson.loads(gzip.decompress(entry.gzipped()))["stock_count"] == 989
    assert len(entry.gzipped()) < len(entry.body) / 4
    object.__setattr__(node0.cfg, "render_cache_seconds", 60.0)
    client0.get("/public/live.json")  # renders once more under the long TTL
    router.calls.clear()
    client0.get("/public/live.json")
    client0.get("/public/live.json")
    assert router.calls == []  # served from the cached body: no peer fetch, no re-render


def test_http_stays_available_while_the_dhan_feed_keeps_failing(make_runtime, monkeypatch):
    async def refused(self):
        raise ConnectionRefusedError("dhan unreachable")

    monkeypatch.setattr(feed_module.MarketFeed, "connect", refused)

    def failing_feed(settings, state, instruments):
        feed = LiveCoreFeed(settings, state, instruments)
        feed.NORMAL_INITIAL_BACKOFF = feed.NORMAL_MAX_BACKOFF = feed._backoff = 0.001
        return feed

    runtime = make_runtime(0, node_count=1, when=ist(10, 0), feed_factory=failing_feed, history_bootstrap=False)
    runtime.tick()
    client = TestClient(create_app(runtime, start_runtime=False))
    deadline = time.monotonic() + 2.0
    answered = 0
    slowest = 0.0
    while time.monotonic() < deadline:
        for path in ("/health", "/health/node", "/public/live.json", "/public/live-c.json", "/public/stock/TCS.json"):
            started = time.perf_counter()
            response = client.get(path)
            slowest = max(slowest, time.perf_counter() - started)
            assert response.status_code == 200, (path, response.text[:200])
            answered += 1
    life = runtime.feed.lifecycle()
    assert life["connection_cycles"] > 20 and life["event_loops_leaked"] == 0
    assert answered > 50 and slowest < 1.0
    health = client.get("/health/node").json()
    assert health["status"] == "DEGRADED" and health["feed"]["websocket_reconnects"] > 20
    assert client.get("/public/live.json").json()["status"] == "OK"  # session is LIVE; candles just stop growing


def test_concurrent_requests_share_one_render(cluster):
    node0, _node1, client0, _client1, router = cluster
    object.__setattr__(node0.cfg, "render_cache_seconds", 60.0)
    router.calls.clear()
    errors = []

    def hit():
        try:
            assert client0.get("/public/live.json").status_code == 200
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    assert router.calls.count("/internal/live-core/fragments") == 1


class SizedRouter(Router):
    def __init__(self):
        super().__init__()
        self.sizes: list[int] = []

    def get(self, url, timeout):
        status, body = super().get(url, timeout)
        self.sizes.append(len(body))
        return status, body


def test_unchanged_peer_state_is_not_transferred_again(make_runtime, universe):
    router = SizedRouter()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get)
    node1 = make_runtime(1, when=ist(9, 15))
    for runtime in (node0, node1):
        runtime.tick()
    node1.test_clock.set(ist(9, 18, 5))
    feed_minutes(node1.state, [s.security_id for s in node1.state.ordered], int(ist(9, 15).timestamp()), 3)
    node1.tick()
    router.clients[NODE1_URL] = TestClient(create_app(node1, start_runtime=False))
    client0 = TestClient(create_app(node0, start_runtime=False))

    first = client0.get("/public/live.json").json()
    assert first["stock_count"] == 989
    full_transfer = router.sizes[-1]
    assert full_transfer > 100_000
    for _ in range(3):
        assert client0.get("/public/live.json").json()["stock_count"] == 989
    assert all(size < 2_000 for size in router.sizes[1:]), router.sizes  # header-only not_modified replies

    node1.test_clock.set(ist(9, 19, 5))
    feed_minutes(node1.state, [s.security_id for s in node1.state.ordered], int(ist(9, 18).timestamp()), 1)
    node1.tick()
    changed = client0.get("/public/live.json").json()
    assert router.sizes[-1] > full_transfer  # one more minute of candles: full refetch
    assert len(changed["stocks"][universe.symbols[600]]["candles_1m"]) == 4


def test_a_restarted_peer_is_never_mistaken_for_its_previous_state(make_runtime, universe):
    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get)
    old = make_runtime(1, when=ist(9, 15))
    for runtime in (node0, old):
        runtime.tick()
    old.test_clock.set(ist(9, 18, 5))
    feed_minutes(old.state, [s.security_id for s in old.state.ordered], int(ist(9, 15).timestamp()), 3)
    old.tick()
    router.clients[NODE1_URL] = TestClient(create_app(old, start_runtime=False))
    client0 = TestClient(create_app(node0, start_runtime=False))
    assert len(client0.get("/public/live.json").json()["stocks"][universe.symbols[600]]["candles_1m"]) == 3

    restarted = make_runtime(1, when=ist(9, 15))
    restarted.tick()
    restarted.state.version = old.state.version  # same counter value, different process
    router.clients[NODE1_URL] = TestClient(create_app(restarted, start_runtime=False))
    payload = client0.get("/public/live.json").json()
    assert payload["stocks"][universe.symbols[600]]["candles_1m"] == []


def test_unchanged_content_reuses_the_encoded_stocks_and_rebuilds_only_the_head(cluster, monkeypatch):
    node0, _node1, client0, _client1, _router = cluster
    service = client0.app.state.live_core
    import live_core.render as render_module

    client0.get("/public/live-a.json")
    first_tail = service._tails[("shard", 0, 45)][1]
    clock = iter(["2026-10-05 09:18:06 IST", "2026-10-05 09:18:07 IST"])

    class _Now:
        @staticmethod
        def now(tz):
            class _T:
                def strftime(self, fmt):
                    return next(clock)

            return _T()

    monkeypatch.setattr(render_module, "datetime", _Now)
    a = client0.get("/public/live-a.json", headers={"Accept-Encoding": "gzip"}).json()
    b = client0.get("/public/live-a.json", headers={"Accept-Encoding": "identity"}).json()
    assert service._tails[("shard", 0, 45)][1] is first_tail  # stocks not re-encoded or re-compressed
    assert (a["session"]["current_time_ist"], b["session"]["current_time_ist"]) == (
        "2026-10-05 09:18:06 IST",
        "2026-10-05 09:18:07 IST",
    )  # the head is fresh on every build
    assert a["stocks"] == b["stocks"]
    monkeypatch.undo()
    series = node0.state.ordered[0]
    node0.state.set_market_reference(series.security_id, previous_close=123.0)
    payload = client0.get("/public/live-a.json").json()
    assert service._tails[("shard", 0, 45)][1] is not first_tail
    assert payload["stocks"][series.symbol]["previous_close"] == 123.0


def test_gzip_and_identity_bodies_are_byte_identical_after_decoding(cluster):
    import gzip as gzip_module

    _node0, _node1, client0, _client1, _router = cluster
    service = client0.app.state.live_core
    entry = service.live()
    assert gzip_module.decompress(entry.gzipped()) == entry.body
    payload = orjson.loads(entry.body)
    assert payload["stock_count"] == 989 and len(payload["stocks"]) == 989


def test_session_end_drops_cached_bodies_and_peer_snapshots(cluster):
    node0, _node1, client0, _client1, _router = cluster
    assert client0.get("/public/live.json").json()["stock_count"] == 989
    service = client0.app.state.live_core
    assert service._cache and node0.peers[1]._snapshot is not None
    node0.test_clock.set(ist(15, 15))
    node0.tick()
    assert service._cache == {} and node0.peers[1]._snapshot is None
    assert client0.get("/public/live-a.json").json()["stocks"] == {}


def test_misconfigured_nodes_cannot_bounce_a_stock_request_between_them(make_runtime, universe):
    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_get=router.get)
    # Node 1 wrongly believes it is node 0 of 2, so it would forward node 1's stocks back to node 0.
    confused = make_runtime(0, when=ist(9, 15), peers={1: NODE0_URL}, peer_get=router.get)
    for runtime in (node0, confused):
        runtime.tick()
    router.clients[NODE0_URL] = TestClient(create_app(node0, start_runtime=False))
    router.clients[NODE1_URL] = TestClient(create_app(confused, start_runtime=False))
    response = router.clients[NODE0_URL].get(f"/public/stock/{universe.symbols[700]}.json")
    assert response.status_code == 503
    assert len(router.calls) == 1  # one hop, answered NOT_OWNER, never forwarded again
    assert router.clients[NODE0_URL].get("/").json()["live_endpoints"][-1] == "/public/live-v.json"


def test_every_response_is_one_coherent_snapshot_while_the_feed_writes(make_runtime):
    """While the feed completes a minute for every stock at once, a response never shows some
    stocks with the new minute and others without it."""
    runtime = make_runtime(0, node_count=1, when=ist(9, 15))
    runtime.tick()
    state = runtime.state
    ids = [series.security_id for series in state.ordered]
    start = int(ist(9, 15).timestamp())
    service = TestClient(create_app(runtime, start_runtime=False)).app.state.live_core
    stop = threading.Event()
    errors = []

    def writer():
        minute = 0
        volume = 1000
        while not stop.is_set() and minute < 300:
            volume += 1
            for security_id in ids:
                state.update_quote(security_id, {"LTT_EPOCH": start + minute * 60 + 5, "LTP": 100.0, "volume": volume})
            state.finalize_due(start + (minute + 2) * 60, grace_seconds=3)  # completes every stock in one sweep
            minute += 1

    def reader():
        from live_core.render import snapshot

        while not stop.is_set():
            snap = snapshot(state, 0, 989)
            counts = {orjson.loads(fragment)["candles_1m"].__len__() for _, _, fragment in snap.items}
            if len(counts) > 1:
                errors.append(counts)
                return

    threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(2)]
    for thread in threads:
        thread.start()
    threads[0].join(60)
    stop.set()
    for thread in threads[1:]:
        thread.join(10)
    assert not errors, f"a response mixed stocks from different moments: candle counts {errors[0]}"
    assert service is not None


def test_one_stock_that_fails_to_render_never_fails_the_endpoint(cluster, monkeypatch):
    node0, _node1, client0, _client1, _router = cluster
    import live_core.render as render_module

    original = render_module._candle_bytes
    target = node0.state.by_symbol["RELIANCE"]
    good = client0.get("/public/live.json").json()["stocks"]["RELIANCE"]

    def broken(series, position):
        if series is target:
            raise ValueError("corrupt column")
        return original(series, position)

    monkeypatch.setattr(render_module, "_candle_bytes", broken)
    node0.state.set_market_reference(target.security_id, previous_close=1.0)  # forces a re-render
    object.__setattr__(node0.cfg, "render_cache_seconds", 0.0)
    response = client0.get("/public/live.json")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "OK" and payload["stock_count"] == 989
    assert payload["stocks"]["RELIANCE"]["candles_1m"] == good["candles_1m"]  # last good encoding served
    assert node0.state.render_errors >= 1
    assert client0.get("/health/node").json()["data_quality"]["render_errors"] >= 1


def test_a_hung_peer_delays_at_most_one_request_per_back_off(make_runtime):
    router = Router()
    node0 = make_runtime(0, when=ist(9, 15), peers={1: NODE1_URL}, peer_timeout_seconds=1.0)
    node0.tick()
    hang = threading.Event()

    def hung_get(url, timeout):
        hang.wait(timeout)  # accepts, never answers, until the client's timeout
        raise TimeoutError("read timed out")

    node0.peers[1]._get = hung_get
    client0 = TestClient(create_app(node0, start_runtime=False))
    timings = []
    for _ in range(10):
        started = time.perf_counter()
        payload = client0.get("/public/live.json").json()
        timings.append(time.perf_counter() - started)
        assert payload["status"] == "PARTIAL" and payload["stock_count"] == 495
    assert timings[0] >= 0.9  # the first request waited for the timeout
    assert max(timings[1:]) < 0.5, timings  # the open circuit answers the rest immediately
    assert router.calls == []


def test_a_live_peer_missing_stocks_is_partial_not_complete(cluster, universe):
    _node0, node1, client0, _client1, _router = cluster
    victim = node1.state.ordered[10]
    with node1.state.lock:
        node1.state.ordered.remove(victim)  # the peer serves 493 of its 494 stocks
        node1.state.version += 1
    payload = client0.get("/public/live.json").json()
    assert payload["status"] == "PARTIAL"
    assert payload["stock_count"] == 988
    assert payload["coverage"]["complete"] is False
    assert payload["coverage"]["nodes"]["1"]["missing_stock_count"] == 1


def test_reachable_nodes_without_stocks_are_never_complete(make_runtime):
    """Both nodes up but in CONFIG_ERROR (no Dhan credentials): 0 stocks must not read as complete."""
    router = Router()
    node0 = make_runtime(0, when=ist(10, 0), peers={1: NODE1_URL}, peer_get=router.get)
    node1 = make_runtime(1, when=ist(10, 0), peers={0: NODE0_URL}, peer_get=router.get)

    def no_credentials():
        raise RuntimeError("Missing required environment variable: DHAN_CLIENT_ID")

    for runtime, url in ((node0, NODE0_URL), (node1, NODE1_URL)):
        runtime._settings_loader = no_credentials
        runtime.tick()
        router.clients[url] = TestClient(create_app(runtime, start_runtime=False))
    payload = router.clients[NODE0_URL].get("/public/live.json").json()
    assert payload["status"] == "CONFIG_ERROR"
    assert payload["stock_count"] == 0
    assert payload["coverage"]["complete"] is False
    assert {k: v["missing_stock_count"] for k, v in payload["coverage"]["nodes"].items()} == {"0": 495, "1": 494}


def _fresh_cluster_node(make_runtime, n_stale=0):
    runtime = make_runtime(0, node_count=1, when=ist(9, 15))
    runtime.tick()
    clock = runtime.test_clock
    start = int(ist(9, 15).timestamp())
    stale_ids = [s.security_id for s in runtime.state.ordered[:n_stale]]
    fresh_ids = [s.security_id for s in runtime.state.ordered[n_stale:]]
    clock.set(ist(9, 16, 0))
    feed_minutes(runtime.state, stale_ids, start, 1)  # last valid data at 09:16:00
    clock.set(ist(9, 18, 30))
    feed_minutes(runtime.state, fresh_ids, start + 120, 1)  # last valid data at 09:18:30
    runtime.state.record_feed_message("Full Data")
    clock.set(ist(9, 18, 31))  # stale stocks are now 151 s old, the rest 1 s
    runtime.tick()
    return runtime, TestClient(create_app(runtime, start_runtime=False))


def test_reconnecting_socket_with_fresh_data_is_a_valid_state(make_runtime):
    runtime, client = _fresh_cluster_node(make_runtime)
    runtime.state.mark_websocket_reconnecting("websocket closed by peer")
    health = client.get("/health/node").json()
    assert health["dimensions"]["http"] == "AVAILABLE"
    assert health["dimensions"]["feed"] == "RECONNECTING"
    assert health["dimensions"]["data"] == "FRESH"
    payload = client.get("/public/live.json").json()
    assert payload["status"] == "OK" and payload["stock_count"] == 989
    assert all(stock["candles_1m"] for stock in payload["stocks"].values())  # last valid state served


@pytest.mark.parametrize("n_stale", [1, 7])
def test_stale_stocks_never_fail_the_endpoint_or_the_node(make_runtime, n_stale):
    runtime, client = _fresh_cluster_node(make_runtime, n_stale)
    health = client.get("/health/node").json()
    assert health["status"] == "OK", health["reasons"]
    assert health["dimensions"] == {
        "http": "AVAILABLE",
        "feed": "CONNECTED",
        "data": "FRESH_WITH_STALE_STOCKS",
        "coverage": "COMPLETE",
        "stale_after_seconds": 120.0,
    }
    assert health["freshness"]["stale_stock_count"] == n_stale
    assert len(health["freshness"]["stale_symbols_sample"]) == n_stale
    payload = client.get("/public/live.json").json()
    assert payload["status"] == "OK" and payload["stock_count"] == 989
    stale_symbol = runtime.state.ordered[0].symbol
    assert payload["stocks"][stale_symbol]["candles_1m"]  # its last valid candles are still served


def test_a_wholly_stale_node_is_degraded_but_keeps_serving_its_last_state(make_runtime):
    runtime, client = _fresh_cluster_node(make_runtime)
    runtime.test_clock.set(ist(9, 20, 31))  # 121 s after the last packet for every stock
    health = client.get("/health/node").json()
    assert health["status"] == "DEGRADED" and health["dimensions"]["data"] == "STALE"
    payload = client.get("/public/live.json").json()
    assert payload["status"] == "OK" and payload["stock_count"] == 989


def test_live_latest_is_the_full_contract_with_only_the_newest_candles(cluster, universe):
    _node0, _node1, client0, _client1, _router = cluster
    full = client0.get("/public/live.json").json()
    for count in (1, 2, 5, 60):
        response = client0.get(f"/public/live-latest.json?candles={count}")
        assert response.status_code == 200
        light = response.json()
        assert light["status"] == full["status"] == "OK"
        assert light["stock_count"] == 989 and list(light["stocks"]) == sorted(universe.symbols)
        assert light["coverage"]["complete"] is True
        for symbol in ("RELIANCE", universe.symbols[494], universe.symbols[495], universe.symbols[-1]):
            whole = full["stocks"][symbol]
            sliced = light["stocks"][symbol]
            # Identical stock object, only the candle list cut to its newest `count` entries.
            assert {k: v for k, v in sliced.items() if k != "candles_1m"} == {
                k: v for k, v in whole.items() if k != "candles_1m"
            }
            assert sliced["candles_1m"] == whole["candles_1m"][-count:]
    none = client0.get("/public/live-latest.json?candles=0").json()
    assert all(stock["candles_1m"] == [] for stock in none["stocks"].values())
    assert client0.get("/public/live-latest.json?candles=61").status_code == 422
    # Even with only 3 candles per stock in this fixture, the light body is smaller.
    assert len(client0.get("/public/live-latest.json?candles=1").content) < len(
        client0.get("/public/live.json").content
    )


def test_fragment_last_matches_a_parse_for_every_shape():
    import orjson

    from live_core.render import fragment_last

    def fragment(candles):
        return orjson.dumps(
            {"symbol": "M&M", "security_id": "1", "previous_close": 1.5, "today_open": None, "candles_1m": candles}
        )

    candles = [
        {"timestamp": f"2026-10-06 09:{15 + i}:00 IST", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": i}
        for i in range(7)
    ]
    for total in (0, 1, 3, 7):
        whole = fragment(candles[:total])
        for count in (0, 1, 2, 3, 10):
            expected = candles[:total][-count:] if count else []
            assert orjson.loads(fragment_last(whole, count))["candles_1m"] == expected
    # An unexpected layout falls back to a real parse, never a wrong slice.
    odd = orjson.dumps({"candles_1m": candles[:3], "symbol": "X"})
    assert orjson.loads(fragment_last(odd, 1))["candles_1m"] == candles[2:3]
