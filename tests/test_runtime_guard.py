"""Process guards: MarketFeed loop cleanup, trading-day gating, watchdog and the real /health payload."""

import asyncio
import os
import threading
from datetime import date, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

import app as app_module
import runtime_guard
from runtime_guard import ServiceWatchdog, close_market_feed, is_trading_day, open_fd_count


class _LoopOwningFeed:
    """Mimics dhanhq.MarketFeed: the constructor creates and installs a private event loop."""

    def __init__(self, loop_factory=asyncio.new_event_loop):
        self.loop = loop_factory()
        asyncio.set_event_loop(self.loop)
        self.closed_connection = False

    def close_connection(self):
        self.closed_connection = True


def test_close_market_feed_closes_the_loop_and_tolerates_none():
    feed = _LoopOwningFeed()
    close_market_feed(feed)
    assert feed.closed_connection
    assert feed.loop.is_closed()
    close_market_feed(None)
    close_market_feed(feed)  # idempotent


def test_close_market_feed_survives_a_failing_disconnect():
    feed = _LoopOwningFeed()
    feed.close_connection = lambda: (_ for _ in ()).throw(RuntimeError("socket gone"))
    close_market_feed(feed)
    assert feed.loop.is_closed()


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
def test_reconnect_cycles_do_not_leak_descriptors():
    uvloop = pytest.importorskip("uvloop")
    result = {}

    def cycles():
        for _ in range(5):  # warm-up
            close_market_feed(_LoopOwningFeed(uvloop.new_event_loop))
        before = open_fd_count()
        for _ in range(200):
            close_market_feed(_LoopOwningFeed(uvloop.new_event_loop))
        result["growth"] = open_fd_count() - before

    worker = threading.Thread(target=cycles)
    worker.start()
    worker.join()
    assert result["growth"] <= 5


def test_trading_day_gating_and_overrides():
    assert is_trading_day(date(2026, 10, 5), {})  # Monday
    assert not is_trading_day(date(2026, 10, 3), {})  # Saturday
    assert not is_trading_day(date(2026, 10, 4), {})  # Sunday
    env = {"PSYGRID_MARKET_HOLIDAYS": "2026-10-02, 2026-10-20", "PSYGRID_SPECIAL_SESSIONS": "2026-11-08"}
    assert not is_trading_day(date(2026, 10, 2), env)
    assert is_trading_day(date(2026, 11, 8), env)  # Sunday special session


def test_session_managers_never_open_on_a_weekend():
    from index_layer import IndexLayerManager
    from session import SessionManager

    tz = ZoneInfo("Asia/Kolkata")
    settings = SimpleNamespace(market_start="09:15", market_end="15:15")
    saturday_noon = datetime(2026, 10, 3, 12, 0, tzinfo=tz)
    monday_noon = datetime(2026, 10, 5, 12, 0, tzinfo=tz)
    session = SimpleNamespace(settings=settings, now=lambda: saturday_noon)
    assert not SessionManager.in_market(session, saturday_noon)
    assert SessionManager.in_market(session, monday_noon)
    index = SimpleNamespace(settings=settings)
    assert not IndexLayerManager._in_market(index, saturday_noon)
    assert IndexLayerManager._in_market(index, monday_noon)


def _watchdog(probe_ok=True, stats=None, grace=0.0):
    pings = []
    stats = stats or {"open_fds": 50, "fd_limit": 65536, "fd_usage_ratio": 0.001, "rss_mb": 200.0}
    dog = ServiceWatchdog(10000, startup_grace=grace, probe=lambda: probe_ok, notify=pings.append, stats=lambda: stats)
    return dog, pings


def test_watchdog_pings_only_when_healthy():
    dog, pings = _watchdog()
    assert dog.tick() and pings == ["WATCHDOG=1"]

    dog, pings = _watchdog(probe_ok=False)
    assert not dog.tick() and pings == []
    assert "not answering" in dog.last_check["reasons"][0]

    dog, pings = _watchdog(stats={"open_fds": 1000, "fd_limit": 1024, "fd_usage_ratio": 0.977, "rss_mb": 1.0})
    assert not dog.tick() and pings == []

    dog, pings = _watchdog(stats={"open_fds": 1, "fd_limit": 1024, "fd_usage_ratio": 0.001, "rss_mb": 9999.0})
    assert not dog.tick() and pings == []


def test_watchdog_startup_grace_keeps_pinging():
    dog, pings = _watchdog(probe_ok=False, grace=600.0)
    assert not dog.tick()
    assert pings == ["WATCHDOG=1"]


def test_sd_notify_is_a_noop_without_systemd():
    assert runtime_guard.sd_notify("WATCHDOG=1", {}) is False


def _snapshot(**overrides):
    snap = {
        "session_status": "LIVE",
        "session_date": "2026-10-05",
        "feed_status": "CONNECTED",
        "stream_health": "FULL_LIVE",
        "last_tick_at": "2026-10-05T12:00:00+05:30",
        "last_tick_age_seconds": 1.0,
        "max_live_age_seconds": 120,
        "stock_count": 989,
        "subscribed_count": 989,
        "live_stock_count": 989,
        "websocket_reconnects": 0,
    }
    snap.update(overrides)
    return snap


def _health(monkeypatch, snap, when, thread_alive=True):
    monkeypatch.setattr(app_module, "config_error", "")
    monkeypatch.setattr(app_module, "state", SimpleNamespace(snapshot=lambda: snap))
    thread = SimpleNamespace(is_alive=lambda: thread_alive)
    monkeypatch.setattr(app_module, "manager", SimpleNamespace(feed=SimpleNamespace(_thread=thread)))
    monkeypatch.setattr(app_module, "index_manager", None)
    monkeypatch.setattr(app_module, "service_watchdog", None)

    class _Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return when.astimezone(tz) if tz else when

    monkeypatch.setattr(app_module, "datetime", _Clock)
    response = TestClient(app_module.app).get("/health")
    assert response.status_code == 200
    return response.json()


TZ = ZoneInfo("Asia/Kolkata")


def test_health_ok_when_live_and_fresh(monkeypatch):
    body = _health(monkeypatch, _snapshot(), datetime(2026, 10, 5, 12, 0, tzinfo=TZ))
    assert body["service"] == "PSYGRID"
    assert body["status"] == "OK", body["reasons"]
    assert body["data"]["fresh"] is True
    assert body["session"]["market_window"] == "OPEN"
    assert body["process"]["open_fds"] > 0


def test_health_degraded_when_stale_or_disconnected_in_market(monkeypatch):
    snap = _snapshot(feed_status="RECONNECTING", last_tick_age_seconds=900.0)
    body = _health(monkeypatch, snap, datetime(2026, 10, 5, 12, 0, tzinfo=TZ), thread_alive=False)
    assert body["status"] == "DEGRADED"
    joined = " ".join(body["reasons"])
    assert "RECONNECTING" in joined and "stale" in joined and "thread" in joined
    assert body["data"]["fresh"] is False


def test_health_ok_when_idle_on_a_weekend(monkeypatch):
    snap = _snapshot(session_status="CLOSED", feed_status="STOPPED", last_tick_age_seconds=None)
    body = _health(monkeypatch, snap, datetime(2026, 10, 4, 12, 0, tzinfo=TZ))
    assert body["status"] == "OK"
    assert body["session"]["market_window"] == "NON_TRADING_DAY"
    assert body["data"]["fresh"] is False
