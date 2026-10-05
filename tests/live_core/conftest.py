"""Shared Live Core fixtures: the real canonical universe and runtimes wired to fake Dhan endpoints."""

from __future__ import annotations

import pytest
from live_core_helpers import Clock, FakeDhanAPI, FakeFeed, FakeSettings, ist

from config import Instrument
from live_core.config import LiveCoreConfig
from live_core.partition import build_partition, load_universe
from live_core.runtime import LiveCoreRuntime


@pytest.fixture(scope="session")
def universe():
    return load_universe()


@pytest.fixture(scope="session")
def instruments(universe):
    return [
        Instrument(symbol=symbol, security_id=str(100_000 + index)) for index, symbol in enumerate(universe.symbols)
    ]


@pytest.fixture
def make_runtime(universe, instruments):
    created: list[LiveCoreRuntime] = []

    def factory(node_id=0, node_count=2, *, when=None, peers=None, peer_get=None, feed_factory=FakeFeed, **overrides):
        clock = Clock(when or ist(10, 0))
        api = FakeDhanAPI()
        cfg_kwargs = {
            "node_id": node_id,
            "node_count": node_count,
            "peers": peers or {},
            "history_interval_seconds": 0.0,
            "render_cache_seconds": 0.0,
            "peer_cache_seconds": 0.0,
        }
        cfg_kwargs.update(overrides)
        cfg = LiveCoreConfig(**cfg_kwargs)
        refresh_calls: list[bool] = []
        runtime = LiveCoreRuntime(
            cfg,
            universe,
            build_partition(universe, node_id, node_count),
            settings_loader=FakeSettings,
            instrument_loader=lambda: list(instruments),
            api_factory=lambda settings: api,
            feed_factory=feed_factory,
            token_refresher=lambda settings, force=False: refresh_calls.append(force),
            now=clock.now,
            clock=clock.epoch,
            peer_get=peer_get,
        )
        runtime.test_clock = clock
        runtime.test_api = api
        runtime.test_refresh_calls = refresh_calls
        created.append(runtime)
        return runtime

    yield factory
    for runtime in created:
        runtime.stop()
