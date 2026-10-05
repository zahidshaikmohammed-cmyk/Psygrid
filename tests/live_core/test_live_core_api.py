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

    router.down.clear()  # node 1 comes back: coverage recovers without restarting node 0
    node0.peers[1]._health = None
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
    assert health["storage"] == {"market_data_on_disk": False, "archive_enabled": False}
    assert health["partition"]["expected_instrument_count"] == 495
    cluster_health = client0.get("/health").json()["cluster"]
    assert cluster_health["all_nodes_healthy"] is True and cluster_health["partitions_covered"] is True
    assert cluster_health["coverage_status"] == "LIVE_COMPLETE"
    assert cluster_health["covered_instrument_count"] == 989
    assert client0.get("/public/health.json").json()["cluster"]["node_count"] == 2


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


def test_unchanged_content_is_served_without_reassembly(cluster):
    node0, _node1, client0, _client1, _router = cluster
    object.__setattr__(node0.cfg, "render_max_age_seconds", 60.0)
    service = client0.app.state.live_core
    client0.get("/public/live-a.json")
    first = service._cache[("shard", 0, 45)]
    client0.get("/public/live-a.json")
    assert service._cache[("shard", 0, 45)] is first  # same body object: nothing re-assembled
    series = node0.state.ordered[0]
    node0.state.set_market_reference(series.security_id, previous_close=123.0)
    payload = client0.get("/public/live-a.json").json()
    assert service._cache[("shard", 0, 45)] is not first
    assert payload["stocks"][series.symbol]["previous_close"] == 123.0


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
