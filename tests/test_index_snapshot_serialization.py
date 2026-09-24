"""Regression coverage for IndexLayerManager.snapshot()'s historical-candle
serialization. A production instance returned INDEX_ENDPOINT_ERROR with
"name '_normalize_ohlcv' is not defined" as soon as the 5m/15m/1h history
bootstrap actually started succeeding (after the auth-retry fix) - the
serializer called a function that was never imported and no longer exists
in output.py. It went unnoticed for a long time because an empty
historical[tf] list never evaluates the list-comprehension body, so the
bug only surfaces once real historical candles are present.
"""

from types import SimpleNamespace

from index_layer import IndexLayerManager, IndexInstrument, IndexState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata", market_start="09:15", market_end="15:15")


def _manager_with_populated_history():
    manager = IndexLayerManager.__new__(IndexLayerManager)
    manager.settings = _settings()
    manager.dhan_api = None
    manager.resolution_errors = {}
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    state = IndexState(manager.settings, "nifty", "NIFTY", instrument)
    state.session_status = "LIVE"
    state.session_date = "2026-09-24"
    state.feed_status = "CONNECTED"
    candle = {
        "timestamp": 1758700200, "epoch": 1758700200,
        "open": 25000.0, "high": 25050.0, "low": 24980.0, "close": 25020.0,
        "volume": 0, "source": "DHAN_HISTORICAL_API", "complete": True,
    }
    state.historical = {"5m": [dict(candle)], "15m": [dict(candle)], "1h": [dict(candle)]}
    state.live_candles = [dict(candle, source="DHAN_WEBSOCKET_FULL")]
    manager.states = {"nifty": state}
    return manager


def test_snapshot_serializes_non_empty_5m_15m_1h_history_without_crashing():
    manager = _manager_with_populated_history()

    snap = manager.snapshot("nifty")

    assert len(snap["5m"]) == 1
    assert len(snap["15m"]) == 1
    assert len(snap["1h"]) == 1
    for timeframe in ("5m", "15m", "1h"):
        row = snap[timeframe][0]
        assert row["open"] == 25000.0
        assert row["close"] == 25020.0
        assert "timestamp" in row and row["timestamp"] is not None


def test_snapshot_5m_15m_1h_candles_have_the_same_shape_as_1m():
    manager = _manager_with_populated_history()

    snap = manager.snapshot("nifty")

    assert set(snap["1m"][0]) == set(snap["5m"][0]) == set(snap["15m"][0]) == set(snap["1h"][0])
