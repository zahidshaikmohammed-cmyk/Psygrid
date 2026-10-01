"""Derivatives feeds pause outside market hours; health reports after-hours quiet as CLOSED."""

import threading
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import app
from health_monitor import build_health, component_health
from index_depth import DepthContract, IndexDepthManager
from index_options import MARKET_CLOSED_STATUS, NIFTY, IndexOptionsManager
from stock_depth import StockDepthManager
from stock_options import StockOptionsManager

IST = ZoneInfo("Asia/Kolkata")
SETTINGS = SimpleNamespace(timezone="Asia/Kolkata", access_token="t", client_id="c")


def _run_loop_once(manager, loop) -> None:
    """Run a manager loop until it reaches its first wait, then stop it."""
    waited = threading.Event()
    real_wait = manager.stop_event.wait

    def wait(timeout=None):
        # Helper threads the loop spawns (e.g. depth quote pollers) also wait;
        # only the loop's own first wait ends the run.
        if threading.current_thread() is thread:
            waited.set()
            manager.stop_event.set()
        return real_wait(0 if manager.stop_event.is_set() else 0.01)

    manager.stop_event.wait = wait
    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    assert waited.wait(5), "loop never reached its wait"
    thread.join(5)
    assert not thread.is_alive()


# --- feeds pause outside market hours -------------------------------------------------


def test_index_options_pause_without_calling_dhan_and_keep_last_chain():
    api = MagicMock()
    manager = IndexOptionsManager(SETTINGS, api, NIFTY)
    manager.state.set_snapshot({"last_price": 25000.0, "oc": [{"strike": 25000.0}]}, ["2026-10-07"], "2026-10-07")

    with patch.object(IndexOptionsManager, "_market_open", return_value=False):
        _run_loop_once(manager, manager._loop)

    api.option_expiry_list.assert_not_called()
    api.option_chain.assert_not_called()
    snap = manager.state.snapshot()
    assert snap["status"] == MARKET_CLOSED_STATUS
    assert snap["strikes"] == [{"strike": 25000.0}]


def test_index_depth_pauses_and_closes_its_socket():
    manager = IndexDepthManager(SETTINGS, MagicMock(), MagicMock(), NIFTY)
    manager.ws = MagicMock()

    with (
        patch.object(IndexDepthManager, "_market_open", return_value=False),
        patch("index_depth.websocket.create_connection") as connect,
    ):
        _run_loop_once(manager, manager._loop)

    connect.assert_not_called()
    manager.ws.close.assert_called()
    assert manager.state.snapshot()["status"] == MARKET_CLOSED_STATUS


def test_index_depth_socket_stops_reading_when_the_market_closes():
    manager = IndexDepthManager(SETTINGS, MagicMock(), MagicMock(), NIFTY)
    manager._contracts = [DepthContract("1", 25000.0, "CE", "2026-10-07")]
    ws = MagicMock()
    ws.recv.return_value = b""
    open_checks = iter([True, True, False])

    with (
        patch.object(IndexDepthManager, "_market_open", side_effect=lambda: next(open_checks)),
        patch("index_depth.websocket.create_connection", return_value=ws),
    ):
        manager._run_socket()

    assert ws.recv.call_count == 2
    ws.close.assert_called_once()


def test_stock_options_pause_every_resolved_symbol():
    with patch("stock_options.fetch_nse_equity_security_ids", return_value={"RELIANCE": "2885", "TCS": "11536"}):
        manager = StockOptionsManager(SETTINGS, MagicMock())

    with patch.object(StockOptionsManager, "_market_open", return_value=False):
        _run_loop_once(manager, manager._loop)

    manager.dhan_api.option_chain.assert_not_called()
    assert manager.states["RELIANCE"].status == MARKET_CLOSED_STATUS
    assert manager.states["TCS"].status == MARKET_CLOSED_STATUS
    assert manager.states["INFY"].status == "UNRESOLVED"  # unresolved symbols keep their own status


def test_stock_depth_pauses_without_opening_a_socket():
    options = SimpleNamespace(states={"RELIANCE": None, "TCS": None}, instruments={})
    manager = StockDepthManager(SETTINGS, MagicMock(), options)
    manager.states["RELIANCE"].status = "LIVE"
    manager.states["RELIANCE"].rotation_status = "ACTIVE"

    with (
        patch.object(StockDepthManager, "_market_open", return_value=False),
        patch("stock_depth.websocket.create_connection") as connect,
    ):
        _run_loop_once(manager, manager._loop)

    connect.assert_not_called()
    assert manager.states["RELIANCE"].status == MARKET_CLOSED_STATUS
    assert manager.states["RELIANCE"].rotation_status == "IDLE"
    assert manager.states["TCS"].status == "PENDING"  # never-fetched symbols stay pending


def test_paused_feed_still_serves_its_last_data():
    manager = IndexOptionsManager(SETTINGS, MagicMock(), NIFTY)
    manager.state.set_snapshot({"last_price": 1.0, "oc": []}, ["2026-10-07"], "2026-10-07")
    manager.state.set_market_closed()
    with patch.object(app, "_error_response", return_value=None), patch.object(app, "nifty_options_manager", manager):
        response = app._derivatives_endpoint("nifty_options_manager", app.index_options_json, "NIFTY", "X")
    assert response.status_code == 200


# --- market-aware health -------------------------------------------------------------


def _component(name, status, age=None, now=None):
    now = now or datetime(2026, 10, 1, 19, 30, tzinfo=IST)
    updated = None if age is None else now.timestamp() - age
    return component_health(name=name, status=status, updated_at=updated, expected_refresh_seconds=2.0, now=now)


def test_after_hours_quiet_is_closed_not_degraded():
    components = [
        _component("equity_990", "CLOSED"),
        _component("nifty_depth", "ERROR"),
        _component("nifty_futures", "LIVE", age=600),
        _component("global_context", "LIVE", age=1),
    ]
    health = build_health(components, "CLOSED", frozenset({"equity_990", "nifty_depth", "nifty_futures"}))
    assert health["overall_status"] == "HEALTHY"
    assert health["closed_count"] == 3
    assert health["components"]["nifty_depth"]["status"] == "CLOSED"
    assert health["components"]["global_context"]["status"] == "FRESH"


def test_everything_closed_reads_market_closed():
    components = [_component("equity_990", "CLOSED"), _component("nifty_options", "MARKET_CLOSED", age=9000)]
    health = build_health(components, "CLOSED", frozenset({"equity_990", "nifty_options"}))
    assert health["overall_status"] == "MARKET_CLOSED"


def test_always_on_failures_still_degrade_after_hours():
    components = [
        _component("equity_990", "CLOSED"),
        _component("global_context", "LIVE", age=1),
        _component("rbi_news", "ERROR"),
    ]
    health = build_health(components, "CLOSED", frozenset({"equity_990"}))
    assert health["overall_status"] == "DEGRADED"
    assert health["components"]["rbi_news"]["status"] == "ERROR"


def test_in_session_failures_are_never_hidden():
    components = [_component("equity_990", "AUTH_ERROR"), _component("nifty_options", "LIVE", age=1)]
    health = build_health(components, "CLOSED", frozenset())
    assert health["components"]["equity_990"]["status"] == "ERROR"
    assert health["overall_status"] == "DEGRADED"


NAMES = ["equity_990", "index_nifty", "nifty_indicators", "nifty_options", "sensex_depth", "nifty_futures",
         "stock_options_nifty50", "stock_depth_nifty50", "global_context", "rbi_news"]  # fmt: skip
EQUITY_GROUP = {"equity_990", "index_nifty", "nifty_indicators"}
DERIVATIVES_GROUP = {"nifty_options", "sensex_depth", "nifty_futures", "stock_options_nifty50", "stock_depth_nifty50"}


def _closed_at(*when):
    with patch.object(app, "settings", None):
        return app._out_of_session(NAMES, datetime(*when, tzinfo=IST))


def test_session_windows_follow_the_clock():
    assert _closed_at(2026, 10, 1, 10, 0) == frozenset()  # Thursday, mid-session
    assert _closed_at(2026, 10, 1, 15, 20) == EQUITY_GROUP  # equity closed, derivatives still open
    assert _closed_at(2026, 10, 1, 19, 30) == EQUITY_GROUP | DERIVATIVES_GROUP
    assert _closed_at(2026, 10, 3, 11, 0) == EQUITY_GROUP | DERIVATIVES_GROUP  # Saturday
    assert _closed_at(2026, 10, 1, 9, 14) == EQUITY_GROUP | DERIVATIVES_GROUP
    assert _closed_at(2026, 10, 1, 9, 15) == frozenset()
