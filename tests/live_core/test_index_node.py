"""Index node: NIFTY/BANKNIFTY/SENSEX candles, previous close, today's open and endpoints (no network)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from live_core import index_node
from live_core.index_node import IndexNode, create_app

IST = ZoneInfo("Asia/Kolkata")
TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjQxMDI0NDQ4MDB9.c2lnbmF0dXJlLW5vdC1yZWFs"


def _ts(h, m, day=16):
    return int(datetime(2026, 10, day, h, m, tzinfo=IST).timestamp())


class FakeAPI:
    def __init__(self, settings):
        self.settings = settings
        self.calls = []

    def daily(self, item, start, end):
        self.calls.append(("daily", item.security_id))
        return [
            {"timestamp": _ts(0, 0, 14), "open": 1, "high": 1, "low": 1, "close": 24900.5, "volume": 0},
            {"timestamp": _ts(0, 0, 15), "open": 1, "high": 1, "low": 1, "close": 25010.25, "volume": 0},
            {"timestamp": _ts(0, 0, 16), "open": 1, "high": 1, "low": 1, "close": 99999.0, "volume": 0},  # today
        ]

    def load_today_completed_intraday(self, item, interval):
        self.calls.append(("intraday", item.security_id, item.exchange_segment, item.instrument))
        return [
            {"timestamp": _ts(9, 15), "open": 25050.0, "high": 25070.0, "low": 25040.0, "close": 25060.0, "volume": 0},
            {"timestamp": _ts(9, 16), "open": 25060.0, "high": 25080.0, "low": 25055.0, "close": 25075.0, "volume": 0},
        ]


def _node():
    def token_get(url, timeout):
        assert url == "http://10.0.0.215:10000/internal/dhan-token"
        return 200, {"status": "OK", "client_id": "1100000001", "access_token": TOKEN}

    now = datetime(2026, 10, 16, 9, 17, 5, tzinfo=IST)
    node = IndexNode(
        "http://10.0.0.215:10000", token_get=token_get, api_factory=FakeAPI, now=lambda: now, start_managers=False
    )
    assert node._ensure_started()
    node.poll_candles(now)
    return node


def test_candles_previous_close_and_today_open_per_index():
    node = _node()
    live = node.live_payload()
    assert set(live["indices"]) == {"NIFTY", "BANKNIFTY", "SENSEX"}
    nifty = live["indices"]["NIFTY"]
    assert nifty["previous_close"] == 25010.25 and nifty["previous_close_date"] == "2026-10-15"
    assert nifty["today_open"] == 25050.0 and nifty["ltp"] == 25075.0
    assert [c["timestamp"][11:16] for c in nifty["candles_1m"]] == ["09:15", "09:16"]
    intraday = [c for c in node.api.calls if c[0] == "intraday"]
    assert {c[1] for c in intraday} == {"13", "25", "51"}
    assert all(c[2] == "IDX_I" and c[3] == "INDEX" for c in intraday)


def test_endpoints_and_no_secret_in_health():
    node = _node()
    client = TestClient(create_app(node))
    assert client.get("/public/live.json").status_code == 200
    assert client.get("/public/index/banknifty.json").json()["symbol"] == "BANKNIFTY"
    assert client.get("/public/options/sensex.json").json()["symbol"] == "SENSEX"
    assert client.get("/public/depth/nifty.json").json()["symbol"] == "NIFTY"
    assert client.get("/public/options/reliance.json").status_code == 404
    everything = client.get("/public/all.json").json()
    assert {"option_chain", "depth", "candles_1m"} <= set(everything["indices"]["NIFTY"])
    health = client.get("/health").text
    assert TOKEN not in health and "1100000001" not in health
    assert client.get("/health").json()["indices"]["NIFTY"]["candles"] == 2


def test_new_day_resets_candles(monkeypatch):
    node = _node()
    state = node.candles["NIFTY"]
    assert state.session_date == "2026-10-16"
    state.reset("2026-10-19")
    assert state.payload()["candles_1m"] == [] and state.payload()["previous_close"] is None


def test_token_rotation_reaches_shared_settings():
    node = _node()
    rotated = TOKEN[:-4] + "NEWW"
    node.token_source._get = lambda url, timeout: (
        200,
        {"status": "OK", "client_id": "1100000001", "access_token": rotated},
    )
    node._last_token_poll = -1e9
    node._poll_token()
    assert node.settings.access_token == rotated and node.api.settings is node.settings
    assert node.token_rotations == 1


def test_alternate_universe_file_sets_universe_size(tmp_path, monkeypatch):
    import importlib
    import json

    import config

    path = tmp_path / "stocks_fno.json"
    path.write_text(
        json.dumps({"universe": "X", "exchange": "NSE", "instrument": "EQUITY", "symbols": ["A", "B", "C"]})
    )
    monkeypatch.setenv("PSYGRID_STOCKS_FILE", str(path))
    try:
        assert importlib.reload(config).UNIVERSE_SIZE == 3
    finally:
        monkeypatch.delenv("PSYGRID_STOCKS_FILE")
        assert importlib.reload(config).UNIVERSE_SIZE == 989


def test_small_universe_has_no_negative_shards():
    from live_core.partition import shard_ranges

    assert all(start <= end for _, start, end in shard_ranges(214))
    assert sum(end - start for _, start, end in shard_ranges(214)) == 214
    assert index_node.INDICES[0].symbol == "NIFTY"


def test_fresh_candles_are_not_reported_stale():
    node = _node()  # last candle 09:16 (complete at 09:17), checked at 09:17:05
    health = node.health()
    assert health["indices"]["NIFTY"]["last_candle_age_minutes"] < 1
    assert not any("stale" in r for r in health["reasons"])


def test_sensex_depth_comes_from_five_level_quotes():
    from index_depth import IndexDepthManager, IndexDepthState, _quote_depth
    from index_options import NIFTY, SENSEX

    settings = type("S", (), {"timezone": "Asia/Kolkata"})()
    assert IndexDepthManager(settings, None, None, NIFTY).twenty_level
    assert not IndexDepthManager(settings, None, None, SENSEX).twenty_level
    book = _quote_depth(
        {"buy": [{"price": 101.5, "quantity": 20, "orders": 2}], "sell": [{"price": 102, "quantity": 40, "orders": 1}]}
    )
    assert book == (
        [{"level": 1, "price": 101.5, "quantity": 20, "orders": 2}],
        [{"level": 1, "price": 102.0, "quantity": 40, "orders": 1}],
    )
    state = IndexDepthState(settings, SENSEX)
    state.contracts = {"9001": {"security_id": "9001", "bid": [], "ask": []}}
    state.update_quotes(
        {"9001": {"last_price": 101.7, "depth": {"buy": [{"price": 101.5, "quantity": 20, "orders": 2}]}}},
        depth_from_quote=True,
    )
    snap = state.snapshot()
    assert snap["status"] == "LIVE" and snap["contracts"][0]["depth_source"] == "DHAN_QUOTE_5_LEVEL"
    assert snap["contracts"][0]["bid"][0]["price"] == 101.5
