from types import SimpleNamespace

from midcpnifty_underlying import MidcapNiftyUnderlyingState


def test_merge_candles_is_ram_only_and_non_synthetic():
    state = MidcapNiftyUnderlyingState(SimpleNamespace(timezone="Asia/Kolkata"))
    rows = [
        {"timestamp": 1758265500, "open": 12000.0, "high": 12010.0, "low": 11990.0, "close": 12005.0, "volume": 100, "complete": True, "source": "DHAN_HISTORICAL_API"},
        {"timestamp": 1758265560, "open": 12005.0, "high": 12020.0, "low": 12000.0, "close": 12015.0, "volume": 120, "complete": True, "source": "DHAN_HISTORICAL_API"},
    ]
    state.merge_candles(rows)
    snap = state.snapshot()
    assert snap["status"] == "LIVE"
    assert snap["symbol"] == "MIDCPNIFTY"
    assert snap["synthetic_data"] if "synthetic_data" in snap else snap["synthetic_candles"] is False
    assert snap["storage"] == "RAM_ONLY"
    assert len(snap["candles_1m"]) == 2
    assert snap["candles_1m"][0]["close"] == 12005.0


def test_merge_candles_deduplicates_by_minute_and_sorts():
    state = MidcapNiftyUnderlyingState(SimpleNamespace(timezone="Asia/Kolkata"))
    state.merge_candles([
        {"timestamp": 1758265560, "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10, "complete": True},
    ])
    state.merge_candles([
        {"timestamp": 1758265500, "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10, "complete": True},
        {"timestamp": 1758265560, "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.8, "volume": 20, "complete": True},
    ])
    snap = state.snapshot()
    timestamps = [c["timestamp"] for c in snap["candles_1m"]]
    assert timestamps == sorted(timestamps)
    assert len(snap["candles_1m"]) == 2


def test_incomplete_candle_is_never_merged():
    state = MidcapNiftyUnderlyingState(SimpleNamespace(timezone="Asia/Kolkata"))
    state.merge_candles([
        {"timestamp": 1758265500, "open": 1.0, "high": 2.0, "low": 1.0, "close": 1.5, "volume": 10, "complete": False},
    ])
    snap = state.snapshot()
    assert snap["candles_1m"] == []
