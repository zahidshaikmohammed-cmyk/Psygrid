"""Old-vs-new contract: Live Core responses against the full PSYGRID's committed golden shapes.

``tests/contracts/api_shapes.json`` pins the shape (keys and value types) of every route of the
full PSYGRID, rendered from a fixed 989-stock fixture. Here the same fixture data - 989 stocks with
30 one-minute candles and reference prices - is loaded into a two-node Live Core cluster, and every
route the two systems share must be shape-compatible with the golden:

* the market-data routes (/public/live.json, every shard, /public/stock) must match exactly, apart
  from one documented additive key, ``coverage``;
* the health routes (/health, /public/health.json, /ready, /) must contain every key of the old
  shape with a compatible type; extra keys are allowed (they are additive).
"""

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from live_core_helpers import ist

from live_core.api import create_app
from live_core.state import SOURCE_HISTORICAL
from tests.contracts.shapes import shape

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = json.loads((ROOT / "tests" / "contracts" / "api_shapes.json").read_text())
IST = ZoneInfo("Asia/Kolkata")
SHARDS = "abcdefghijklmnopqrstuv"
ADDITIVE_LIVE_KEYS = {"coverage"}


def compatible(golden, ours, path="$") -> list[str]:
    """Differences that would break a consumer of the golden shape (missing keys, changed types)."""
    if isinstance(golden, str):
        allowed = set(golden.split("|"))
        if "null" in allowed and len(allowed) == 1:
            return []  # the fixture had no value here; any type is acceptable
        if isinstance(ours, str):
            got = set(ours.split("|")) - {"null"}
            want = allowed - {"null"}
            numeric = {"int", "float"}
            if got <= want or (got <= numeric and want <= numeric):
                return []
            return [f"{path}: type {ours} where the old contract has {golden}"]
        return [f"{path}: {type(ours).__name__} where the old contract has {golden}"]
    if isinstance(golden, list):
        if not golden or not isinstance(ours, list) or not ours:
            return [] if isinstance(ours, list) else [f"{path}: not a list"]
        return compatible(golden[0], ours[0], path + "[]")
    if isinstance(golden, dict):
        if not isinstance(ours, dict):
            return [f"{path}: not an object"]
        problems = []
        if "<each value>" in golden:
            values = ours.get("<each value>")
            if values is None:  # ours is small enough not to be collapsed: check each value
                return [
                    p for key, value in ours.items() for p in compatible(golden["<each value>"], value, f"{path}.{key}")
                ]
            return compatible(golden["<each value>"], values, path + ".*")
        for key, value in golden.items():
            optional = key.endswith("?")
            name = key.rstrip("?")
            if name not in ours and name + "?" not in ours:
                if not optional:
                    problems.append(f"{path}.{name}: missing")
                continue
            problems += compatible(value, ours.get(name, ours.get(name + "?")), f"{path}.{name}")
        return problems
    return []


@pytest.fixture(scope="module")
def contract_cluster(request, universe, instruments):
    """989 stocks, 30 historical one-minute bars each, split over two live nodes."""
    from live_core_helpers import Clock, FakeDhanAPI, FakeFeed, FakeSettings

    from live_core.config import LiveCoreConfig
    from live_core.partition import build_partition
    from live_core.runtime import LiveCoreRuntime

    clients: dict[str, TestClient] = {}

    def get(url, timeout):
        parts = urlsplit(url)
        response = clients[f"{parts.scheme}://{parts.netloc}"].get(
            parts.path + (f"?{parts.query}" if parts.query else "")
        )
        return response.status_code, response.content

    runtimes = []
    start = int(datetime(2026, 10, 5, 9, 15, tzinfo=IST).timestamp())
    for node in (0, 1):
        clock = Clock(ist(9, 45, 5))
        cfg = LiveCoreConfig(node_id=node, node_count=2, peers={1 - node: f"http://node{1 - node}"},
                             render_cache_seconds=0.0, peer_cache_seconds=0.0)  # fmt: skip
        runtime = LiveCoreRuntime(
            cfg, universe, build_partition(universe, node, 2), settings_loader=FakeSettings,
            instrument_loader=lambda: list(instruments), api_factory=lambda settings: FakeDhanAPI(),
            feed_factory=FakeFeed, token_refresher=lambda settings, force=False: None,
            now=clock.now, clock=clock.epoch, peer_get=get,
        )  # fmt: skip
        runtime.tick()
        runtime.history.stop()
        for series in runtime.state.ordered:
            base = 100.0 + series.index
            runtime.state.set_market_reference(series.security_id, previous_close=base - 1, today_open=base)
            for minute in range(30):
                close = base + (minute % 7) - 3
                row = (start + 60 * minute, base, max(base, close) + 1, min(base, close) - 1, close, 1000 + minute)
                series.put_completed(row, SOURCE_HISTORICAL)
            series.last_received = clock.epoch()
        runtime.state.last_tick_received_epoch = clock.epoch()
        runtime.state.record_feed_message("Full Data")
        runtimes.append(runtime)
        clients[f"http://node{node}"] = TestClient(create_app(runtime, start_runtime=False))
    yield clients["http://node0"]
    for runtime in runtimes:
        runtime.stop()


def _golden(path):
    return GOLDEN[path]


@pytest.mark.parametrize("path", ["/public/live.json", *[f"/public/live-{name}.json" for name in SHARDS]])
def test_market_data_routes_match_the_old_shape_exactly_apart_from_coverage(contract_cluster, path):
    response = contract_cluster.get(path)
    golden = _golden(path)
    assert response.status_code == golden["status_code"]
    payload = response.json()
    assert set(payload) - set(golden["shape"]) == ADDITIVE_LIVE_KEYS
    for key in ADDITIVE_LIVE_KEYS:
        payload.pop(key)
    assert shape(payload) == golden["shape"]


def test_stock_route_matches_the_old_shape_exactly(contract_cluster):
    golden = _golden("/public/stock/{symbol}.json")
    for symbol in ("RELIANCE", "MEESHO"):  # one stock on each node
        response = contract_cluster.get(f"/public/stock/{symbol}.json")
        assert response.status_code == golden["status_code"]
        assert shape(response.json()) == golden["shape"]


@pytest.mark.parametrize("path", ["/health", "/public/health.json", "/"])
def test_health_and_root_routes_keep_every_old_field(contract_cluster, path):
    response = contract_cluster.get(path)
    golden = _golden(path)
    assert response.status_code == golden["status_code"]
    ours = shape(response.json())
    old = dict(golden["shape"])
    if path == "/":
        # The root lists the endpoint families a server actually serves: derivative, breadth and
        # context families are not part of the Live Core and are deliberately not advertised.
        not_served = {"breadth_endpoints", "context_endpoints", "derivatives_endpoints", "derivatives_symbols",
                      "futures_endpoints", "not_available", "stock_depth", "stock_options",
                      "underlying_indicator_endpoints"}  # fmt: skip
        old = {k: v for k, v in old.items() if k not in not_served}
    if path == "/health":
        # index_layer.feed_status counts index feeds by status; its keys are data, not schema, and
        # the Live Core runs no index layer, so the map is empty ({} - still an object).
        old = {**old, "index_layer": {k: v for k, v in old["index_layer"].items() if k != "feed_status"}}
        assert ours["index_layer"]["feed_status"] == {}
    problems = compatible(old, ours)
    assert problems == [], problems


def test_public_health_carries_the_old_equity_component(contract_cluster):
    payload = contract_cluster.get("/public/health.json").json()
    assert {"service", "market_status", "overall_status", "components", "archive"} <= set(payload)
    equity = payload["components"]["equity_990"]
    assert equity["expected_record_count"] == 989
    assert {"live_core_node_0", "live_core_node_1"} <= set(payload["components"])


def test_ready_keeps_every_old_field(contract_cluster):
    response = contract_cluster.get("/ready")
    golden = _golden("/ready")
    assert compatible(golden["shape"], shape(response.json())) == []
    assert response.status_code in {200, 503}


def test_live_json_values_match_the_full_psygrid_rendering_of_the_same_data(contract_cluster, universe):
    """Not only the shape: the bytes of each stock equal what output.py renders for the same bars."""
    from types import SimpleNamespace

    from output import market_live_json
    from state import PsygridState

    full = PsygridState(SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=30))
    from config import Instrument

    full.begin(
        "2026-10-05", [Instrument(symbol=s, security_id=str(100_000 + i)) for i, s in enumerate(universe.symbols)]
    )
    start = int(datetime(2026, 10, 5, 9, 15, tzinfo=IST).timestamp())
    for index in range(universe.size):
        security_id = str(100_000 + index)
        base = 100.0 + index
        full.set_market_reference(security_id, previous_close=base - 1, today_open=base)
        rows = []
        for minute in range(30):
            close = base + (minute % 7) - 3
            rows.append({"timestamp": start + 60 * minute, "open": base, "high": max(base, close) + 1,
                         "low": min(base, close) - 1, "close": close, "volume": 1000 + minute,
                         "source": "DHAN_HISTORICAL_API", "complete": True})  # fmt: skip
        full.merge_today_1m_history(security_id, rows)
    old = market_live_json(full)
    new = contract_cluster.get("/public/live.json").json()
    assert list(new["stocks"]) == list(old["stocks"])  # same symbol order
    assert new["stocks"] == old["stocks"]
    assert (new["status"], new["stock_count"], new["universe_size"]) == ("OK", 989, 989)
