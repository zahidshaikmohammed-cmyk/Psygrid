from datetime import datetime, timedelta

from indicator_runtime import IndicatorRuntime


def _payload():
    start = datetime(2026, 9, 17, 9, 15)
    candles = []
    for i in range(40):
        t = start + timedelta(minutes=i)
        p = 100.0 + i * 0.1
        candles.append({
            "timestamp": t.strftime("%Y-%m-%d %H:%M:%S IST"),
            "open": p,
            "high": p + 0.2,
            "low": p - 0.1,
            "close": p + 0.1,
            "volume": 1000 + i,
        })
    return {
        "service": "PSYGRID",
        "schema_version": "4.0",
        "status": "OK",
        "session": {"current_time_ist": "2026-09-17 09:54:00 IST"},
        "universe_size": 450,
        "data_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN",
        "synthetic_candles": False,
        "stocks": {
            "TEST": {
                "symbol": "TEST",
                "security_id": "1",
                "previous_close": 99.0,
                "today_open": 100.0,
                "candles_1m": candles,
            }
        },
    }


class FakeState:
    session_status = "LIVE"

    def snapshot(self):
        return {
            "session_status": "LIVE",
            "feed_status": "CONNECTED",
            "stock_count": 450,
            "subscribed_count": 450,
            "live_stock_count": 450,
            "stream_health": "FULL_LIVE",
        }


def test_runtime_syncs_from_canonical_source_and_exposes_stock():
    payload = _payload()
    runtime = IndicatorRuntime(FakeState(), lambda _state: payload, interval_seconds=0.25)
    runtime._sync_once()

    snapshot = runtime.snapshot()
    assert snapshot["source"]["source_endpoint"] == "/public/live.json"
    assert snapshot["timeframe"] == "1m"
    assert snapshot["stock_count"] == 1
    assert snapshot["results"]["TEST"]["indicators"]["ema_20"] is not None

    stock = runtime.stock("test")
    assert stock["status"] == "OK"
    assert stock["result"]["symbol"] == "TEST"


def test_runtime_does_not_publish_partial_full_universe_as_ok():
    payload = _payload()
    runtime = IndicatorRuntime(FakeState(), lambda _state: payload, interval_seconds=0.25)
    runtime._sync_once()
    assert runtime.snapshot()["status"] == "PARTIAL"
