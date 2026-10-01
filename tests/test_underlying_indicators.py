from datetime import datetime, timedelta
from types import SimpleNamespace

from underlying_indicators import UnderlyingIndicatorRuntime


def _candles(n=25, start_price=25000.0):
    base = datetime(2026, 9, 21, 9, 15)
    candles = []
    price = start_price
    for i in range(n):
        ts = (base + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S IST")
        o = price
        c = price + (1 if i % 2 == 0 else -1) * 3.5
        h = max(o, c) + 1.0
        low = min(o, c) - 1.0
        candles.append(
            {
                "timestamp": ts,
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(low, 2),
                "close": round(c, 2),
                "volume": 1000 + i * 10,
            }
        )
        price = c
    return candles


def test_no_candles_leaves_runtime_starting():
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: None, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    snap = rt.snapshot()
    assert snap["status"] == "STARTING"


def test_valid_candles_produce_ok_result_with_full_indicator_suite():
    candles = _candles()
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: candles, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    snap = rt.snapshot()
    assert snap["status"] == "OK"
    result = snap["result"]
    assert result["symbol"] == "NIFTY"
    assert result["bar_count"] == len(candles)
    assert "indicators" in result
    for key in ("rsi_14", "macd_line", "atr_14", "ema_9", "sma_20", "adx_14"):
        assert key in result["indicators"]


def test_unfingerprinted_repeat_does_not_recompute():
    candles = _candles()
    calls = {"n": 0}

    def source():
        calls["n"] += 1
        return candles

    rt = UnderlyingIndicatorRuntime("NIFTY", source, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    first_sync_count = rt._sync_count
    rt._sync_once()
    assert rt._sync_count == first_sync_count  # unchanged candles -> no recompute
    assert calls["n"] == 2  # source is still polled each cycle


def test_bad_candles_report_error_not_crash():
    bad_candles = [{"timestamp": "not-a-timestamp", "open": 1, "high": 2, "low": 1, "close": 1.5, "volume": 10}]
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: bad_candles, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    snap = rt.snapshot()
    assert snap["status"] == "ERROR"
    assert "error" in snap


def test_duplicate_and_unordered_candles_do_not_freeze_indicators():
    candles = _candles()
    messy = [*candles[:10], dict(candles[5]), *candles[12:], candles[11], candles[10]]
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: messy, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    snap = rt.snapshot()
    assert snap["status"] == "OK"
    assert snap["result"]["bar_count"] == len(candles)


def test_freshness_is_evaluated_when_served_not_when_computed():
    candles = _candles()  # 2026-09-21 09:15-09:39, long in the past
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: candles, SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    with rt._lock:
        rt._result["freshness"] = {"status": "FRESH", "age_seconds": 0, "reason": None}
    snap = rt.snapshot()
    assert snap["result"]["freshness"]["status"] == "STALE"


def test_error_after_a_good_result_is_exposed():
    good = _candles()
    state = {"candles": good}
    rt = UnderlyingIndicatorRuntime("NIFTY", lambda: state["candles"], SimpleNamespace(timezone="Asia/Kolkata"))
    rt._sync_once()
    state["candles"] = [
        *good,
        {"timestamp": "2026-09-21 09:40:00 IST", "open": 1, "high": 0, "low": 2, "close": 1, "volume": 1},
    ]
    rt._sync_once()
    snap = rt.snapshot()
    assert snap["status"] == "OK" and "invalid OHLC" in snap["last_error"]["message"]
