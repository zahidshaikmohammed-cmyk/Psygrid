"""End-to-end verification that every endpoint's HTTP handler produces a
meaningful payload (not just a 200 status) once its manager is healthy, and
that /public/health.json accurately reflects the true state of every feed,
including one broken secondary feed not dragging down the whole picture.

This exercises the real FastAPI route functions via TestClient (bare, so
the lifespan/startup() is never triggered - every manager global is
monkeypatched directly instead), fed simulated data since this sandbox has
no live Dhan credentials.
"""

import threading
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

import app as app_module
from futures_layer import FuturesContract, FuturesState
from global_context import GlobalContextState


def _fresh_manager_state_only(status="LIVE"):
    """For components health.json reads via st.status/st.updated_at/last_error
    directly, without ever calling .snapshot()."""
    state = SimpleNamespace(status=status, updated_at=datetime.now(timezone.utc).isoformat(), last_error="")
    return SimpleNamespace(state=state)


def _setup_all_healthy(monkeypatch):
    equity_state = SimpleNamespace(
        session_status="LIVE", feed_status="CONNECTED",
        last_message_at=datetime.now(timezone.utc).isoformat(),
        last_feed_error="", subscribed_count=990, websocket_reconnects=0,
    )
    monkeypatch.setattr(app_module, "config_error", "")
    monkeypatch.setattr(app_module, "state", equity_state)

    index_state = SimpleNamespace(feed_status="CONNECTED", last_tick_received_epoch=datetime.now(timezone.utc).timestamp(), last_feed_error="")
    fake_index_manager = SimpleNamespace(states={route: index_state for route in app_module.INDEX_ROUTES}, resolution_errors={})
    monkeypatch.setattr(app_module, "index_manager", fake_index_manager)
    monkeypatch.setattr(app_module, "index_error", "")

    for name in ("nifty_options_manager", "nifty_depth_manager", "banknifty_options_manager", "banknifty_depth_manager",
                 "midcpnifty_options_manager", "midcpnifty_depth_manager", "midcpnifty_underlying_manager",
                 "sensex_options_manager", "sensex_depth_manager"):
        monkeypatch.setattr(app_module, name, _fresh_manager_state_only())

    contract = FuturesContract(symbol="NIFTY", security_id="49081", exchange_segment="NSE_FNO", instrument="FUTIDX", trading_symbol="NIFTY-FUT", expiry_date="2026-09-26", lot_size=75, tick_size=0.05)
    nifty_futures_state = FuturesState("NIFTY", SimpleNamespace(timezone="Asia/Kolkata"))
    nifty_futures_state.set_contract(contract)
    nifty_futures_state.set_quote({"last_price": 25100.0, "volume": 1000, "oi": 500000, "ohlc": {"open": 25000.0, "high": 25200.0, "low": 24950.0, "close": 25050.0}})
    monkeypatch.setattr(app_module, "nifty_futures_manager", SimpleNamespace(state=nifty_futures_state))
    monkeypatch.setattr(app_module, "banknifty_futures_manager", _fresh_manager_state_only())
    monkeypatch.setattr(app_module, "sensex_futures_manager", _fresh_manager_state_only())

    healthy_context = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    healthy_context.set_series({"sp500": {"series_id": "SP500", "value": 6500.0, "source_date": "2026-09-18", "source": "FRED"}})
    monkeypatch.setattr(app_module, "global_context_manager", SimpleNamespace(state=healthy_context))

    from rbi_news import RbiNewsState
    healthy_rbi = RbiNewsState(SimpleNamespace(timezone="Asia/Kolkata"))
    healthy_rbi.set_items([{"id": "1", "headline": "x"}], {})
    monkeypatch.setattr(app_module, "rbi_news_manager", SimpleNamespace(state=healthy_rbi))

    for name in ("nifty_underlying_indicators", "banknifty_underlying_indicators", "midcpnifty_underlying_indicators", "sensex_underlying_indicators"):
        monkeypatch.setattr(app_module, name, SimpleNamespace(
            snapshot=lambda: {"status": "OK"},
            last_updated_at=lambda: datetime.now(timezone.utc).isoformat(),
        ))
    monkeypatch.setattr(app_module, "indicator_runtime", SimpleNamespace())
    monkeypatch.setattr(app_module, "indicator_error", "")

    return equity_state, fake_index_manager, nifty_futures_state


def test_health_reports_everything_fresh_when_all_managers_healthy(monkeypatch):
    _setup_all_healthy(monkeypatch)
    client = TestClient(app_module.app)
    resp = client.get("/public/health.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["overall_status"] == "HEALTHY"
    assert payload["error_count"] == 0
    assert payload["components"]["equity_990"]["status"] == "FRESH"
    assert payload["components"]["index_nifty"]["status"] == "FRESH"
    assert payload["components"]["nifty_futures"]["status"] == "FRESH"
    assert payload["components"]["sensex_options"]["status"] == "FRESH"
    assert payload["components"]["sensex_futures"]["status"] == "FRESH"


def test_one_broken_secondary_feed_does_not_take_down_core_dhan_infrastructure(monkeypatch):
    _setup_all_healthy(monkeypatch)
    # Simulate FRED being unconfigured - a Tier 2, non-Dhan feed.
    broken_global_context = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    broken_global_context.set_error("FRED_API_KEY environment variable is not set; global-context is unavailable")
    monkeypatch.setattr(app_module, "global_context_manager", SimpleNamespace(state=broken_global_context))

    client = TestClient(app_module.app)
    health = client.get("/public/health.json").json()
    futures = client.get("/public/nifty-futures.json").json()

    assert health["components"]["global_context"]["status"] == "ERROR"
    # Core Dhan-sourced feeds must remain individually FRESH...
    assert health["components"]["equity_990"]["status"] == "FRESH"
    assert health["components"]["index_nifty"]["status"] == "FRESH"
    assert health["components"]["nifty_options"]["status"] == "FRESH"
    assert health["components"]["nifty_futures"]["status"] == "FRESH"
    # ...and the endpoints themselves must keep serving real data, unaffected.
    assert futures["status"] == "LIVE"
    assert futures["last_price"] == 25100.0
    # Overall picture is DEGRADED (something is broken), never DOWN (core is fine).
    assert health["overall_status"] == "DEGRADED"


def test_global_context_missing_api_key_does_not_crash_process(monkeypatch):
    _setup_all_healthy(monkeypatch)
    broken_global_context = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    broken_global_context.set_error("FRED_API_KEY environment variable is not set; global-context is unavailable")
    monkeypatch.setattr(app_module, "global_context_manager", SimpleNamespace(state=broken_global_context))

    client = TestClient(app_module.app)
    resp = client.get("/public/global-context.json")
    health_resp = client.get("/public/health.json")

    assert resp.status_code == 503  # explicit failure code, not silently 200 with fake data
    payload = resp.json()
    assert payload["status"] == "ERROR"
    assert "FRED_API_KEY" in payload["error"]
    assert payload["series"] == {}  # no cached/fake values presented as current
    assert health_resp.status_code == 200  # health endpoint itself never breaks


def test_futures_endpoint_serves_meaningful_data_not_just_200(monkeypatch):
    _setup_all_healthy(monkeypatch)
    client = TestClient(app_module.app)
    resp = client.get("/public/nifty-futures.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "LIVE"
    assert payload["security_id"] == "49081"
    assert payload["last_price"] == 25100.0
    assert payload["oi"] == 500000
    assert payload["lot_size"] == 75
    assert payload["expiry"] == "2026-09-26"


def test_market_breadth_and_sectors_serve_real_numbers(monkeypatch):
    equity_state, _, _ = _setup_all_healthy(monkeypatch)
    equity_state.instruments = {
        "1": {"symbol": "TCS", "security_id": "1"},
        "2": {"symbol": "INFY", "security_id": "2"},
    }
    equity_state.last_ltp_by_security = {"1": 3900.0, "2": 1500.0}
    equity_state.market_reference = {"1": {"previous_close": 3800.0}, "2": {"previous_close": 1550.0}}
    equity_state.live_candles = {"1": [{"high": 3950.0, "low": 3850.0}], "2": [{"high": 1560.0, "low": 1490.0}]}
    equity_state.current_1m = {}
    equity_state.lock = threading.RLock()
    equity_state.settings = SimpleNamespace(timezone="Asia/Kolkata")

    client = TestClient(app_module.app)
    breadth = client.get("/public/market-breadth.json").json()
    sectors = client.get("/public/sectors.json").json()

    assert breadth["advancing"] == 1
    assert breadth["declining"] == 1
    assert breadth["universe_size"] == 2
    assert sectors["sector_count"] > 0


def test_rbi_news_and_global_context_serve_meaningful_data_when_healthy(monkeypatch):
    _setup_all_healthy(monkeypatch)
    from rbi_news import RbiNewsState
    rbi_state = RbiNewsState(SimpleNamespace(timezone="Asia/Kolkata"))
    rbi_state.set_items([{"id": "1", "headline": "RBI test release", "source": "RBI_OFFICIAL", "category": "press_releases", "url": "https://rbi.org.in/x", "summary": "s", "published_at": "2026-09-19T10:00:00+00:00", "country": "IN"}], {})
    monkeypatch.setattr(app_module, "rbi_news_manager", SimpleNamespace(state=rbi_state))

    healthy_context = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    healthy_context.set_series({"sp500": {"series_id": "SP500", "value": 6500.0, "source_date": "2026-09-18", "source": "FRED_FEDERAL_RESERVE_BANK_OF_ST_LOUIS"}})
    monkeypatch.setattr(app_module, "global_context_manager", SimpleNamespace(state=healthy_context))

    client = TestClient(app_module.app)
    rbi = client.get("/public/rbi-news.json").json()
    ctx = client.get("/public/global-context.json").json()

    assert rbi["status"] == "LIVE"
    assert rbi["item_count"] == 1
    assert rbi["items"][0]["headline"] == "RBI test release"
    assert ctx["status"] == "LIVE"
    assert ctx["market_data_status"] == "DELAYED"
    assert ctx["series"]["sp500"]["value"] == 6500.0
