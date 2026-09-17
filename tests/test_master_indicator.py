from datetime import datetime, timedelta

import pytest

from psygrid_master_indicator import run_psygrid


def _payload(*, current_time="2026-09-17 09:54:30 IST", candles=None):
    if candles is None:
        start = datetime(2026, 9, 17, 9, 15)
        candles = []
        for i in range(40):
            ts = start + timedelta(minutes=i)
            price = 100.0 + i * 0.1
            candles.append({
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S IST"),
                "open": price,
                "high": price + 0.2,
                "low": price - 0.1,
                "close": price + 0.1,
                "volume": 100 + i,
            })

    return {
        "service": "PSYGRID",
        "schema_version": "4.0",
        "status": "OK",
        "session": {
            "timezone": "Asia/Kolkata",
            "current_time_ist": current_time,
        },
        "universe_size": 450,
        "stock_count": 1,
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


def test_master_indicator_snapshot_and_warmup():
    out = run_psygrid(_payload())
    row = out["results"]["TEST"]

    assert out["processed_count"] == 1
    assert out["error_count"] == 0
    assert row["timeframe"] == "1m"
    assert row["synthetic_candles"] is False
    assert row["freshness"]["status"] == "FRESH"
    assert row["indicators"]["ema_9"] is not None
    assert row["indicator_status"]["ema_9"]["ready"] is True
    assert row["indicators"]["macd_signal"] is None
    assert row["indicator_status"]["macd_signal"]["ready"] is False


def test_stale_stock_suppresses_numeric_indicators():
    out = run_psygrid(_payload(current_time="2026-09-17 10:10:00 IST"))
    row = out["results"]["TEST"]

    assert row["freshness"]["status"] == "STALE"
    assert row["latest_bar"] is not None
    assert row["indicators"]["ema_9"] is None
    assert row["indicators"]["vwap"] is None


def test_duplicate_minute_is_rejected_for_that_stock():
    payload = _payload()
    payload["stocks"]["TEST"]["candles_1m"].append(
        dict(payload["stocks"]["TEST"]["candles_1m"][-1])
    )

    out = run_psygrid(payload)

    assert out["processed_count"] == 0
    assert out["error_count"] == 1
    assert out["errors"]["TEST"]["error_type"] == "ValueError"
    assert "duplicate 1-minute timestamps" in out["errors"]["TEST"]["message"]


def test_endpoint_policy_is_strict():
    payload = _payload()
    payload["synthetic_candles"] = True

    with pytest.raises(ValueError, match="synthetic_candles"):
        run_psygrid(payload)
