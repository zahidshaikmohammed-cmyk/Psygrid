from types import SimpleNamespace

from output import market_live_json, stock_json
from state import PsygridState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=60)


def _instrument():
    return SimpleNamespace(symbol="AAA", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY")


def test_public_market_payload_is_1m_only():
    state = PsygridState(_settings())
    state.begin("2026-09-16", [_instrument()])
    state.set_market_reference("1", previous_close=100.0, today_open=101.0)
    state.live_candles["1"].append({
        "timestamp": 1778989500,
        "epoch": 1778989500,
        "open": 101.0,
        "high": 102.0,
        "low": 100.5,
        "close": 101.5,
        "volume": 123,
        "complete": True,
    })

    payload = market_live_json(state)
    stock = payload["stocks"]["AAA"]
    assert payload["schema_version"] == "4.0"
    assert set(stock) == {"symbol", "security_id", "previous_close", "today_open", "candles_1m"}
    assert len(stock["candles_1m"]) == 1
    assert set(stock["candles_1m"][0]) == {"timestamp", "open", "high", "low", "close", "volume"}
    assert stock["previous_close"] == 100.0
    assert stock["today_open"] == 101.0
    assert "5m" not in payload
    assert "15m" not in payload
    assert "1h" not in payload
    assert "depth" not in payload
    assert "indicators" not in payload


def test_stock_endpoint_matches_1m_contract():
    state = PsygridState(_settings())
    state.begin("2026-09-16", [_instrument()])
    state.set_market_reference("1", previous_close=99.5, today_open=100.0)
    payload = stock_json(state, "AAA")
    assert payload["symbol"] == "AAA"
    assert payload["previous_close"] == 99.5
    assert payload["today_open"] == 100.0
    assert payload["candles_1m"] == []
    assert set(payload) == {"service", "schema_version", "status", "symbol", "security_id", "previous_close", "today_open", "candles_1m"}


def test_completed_minute_is_unique_when_history_and_websocket_overlap():
    state = PsygridState(_settings())
    state.begin("2026-09-16", [_instrument()])
    minute = 1778989500
    state.merge_today_1m_history("1", [{
        "timestamp": minute,
        "epoch": minute,
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 500,
        "source": "DHAN_HISTORICAL_API",
        "complete": True,
    }])
    state.current_1m["1"] = {
        "timestamp": minute,
        "epoch": minute,
        "open": 100.0,
        "high": 103.0,
        "low": 99.0,
        "close": 102.0,
        "volume": 25,
        "source": "DHAN_WEBSOCKET_QUOTE",
        "complete": False,
    }
    payload = stock_json(state, "AAA")
    candles = payload["candles_1m"]
    assert len(candles) == 1
    assert candles[0]["timestamp"] == "2026-05-16 16:05:00 IST" or candles[0]["timestamp"]
    assert candles[0]["volume"] == 500


def test_merge_history_replaces_duplicate_minutes_and_keeps_them_sorted():
    state = PsygridState(_settings())
    state.begin("2026-09-16", [_instrument()])
    base = 1778989500
    rows = [
        {"timestamp": base + 120, "epoch": base + 120, "open": 102, "high": 103, "low": 101, "close": 102, "volume": 20, "source": "DHAN_HISTORICAL_API", "complete": True},
        {"timestamp": base, "epoch": base, "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10, "source": "DHAN_HISTORICAL_API", "complete": True},
        {"timestamp": base, "epoch": base, "open": 100, "high": 102, "low": 98, "close": 101, "volume": 15, "source": "DHAN_HISTORICAL_API", "complete": True},
    ]
    state.merge_today_1m_history("1", rows)
    candles = state.live_candles["1"]
    assert len(candles) == 2
    assert [c["epoch"] for c in candles] == [base, base + 120]
    assert candles[0]["volume"] == 15
