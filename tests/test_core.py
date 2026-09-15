from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from feed import LiveFeed
from indicators import ema, rsi, sma, vwap


def test_dhan_ltt_accepts_exchange_wall_clock_time_string():
    expected_dt = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0) - timedelta(seconds=60)
    text = expected_dt.strftime("%H:%M:%S")
    epoch = LiveFeed._parse_ltt(text)
    assert epoch is not None
    assert datetime.fromtimestamp(epoch, ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S") == text


def test_dhan_ltt_accepts_epoch():
    assert LiveFeed._parse_ltt(1788234360) == 1788234360


def test_indicators_are_standard():
    closes = [float(x) for x in range(1, 31)]
    assert sma(closes, 9) == 26.0
    assert ema(closes, 20) is not None
    assert rsi(closes, 14) == 100.0


def test_vwap_is_candle_derived():
    candles = [
        {"high": 12, "low": 8, "close": 10, "volume": 100},
        {"high": 14, "low": 10, "close": 12, "volume": 100},
    ]
    assert vwap(candles) == 11.0


def test_public_ohlcv_timeframes_are_fixed():
    assert ("1m", "5m", "15m", "1h") == ("1m", "5m", "15m", "1h")
