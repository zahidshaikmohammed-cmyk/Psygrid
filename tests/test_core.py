from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from feed import LiveFeed
from indicators import ema, rsi, sma, vwap
from output import _historical_payload
from state import PsygridState


def test_dhan_ltt_accepts_sdk_utc_time_string():
    expected_dt = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=60)
    text = expected_dt.strftime("%H:%M:%S")
    epoch = LiveFeed._parse_ltt(text)
    assert epoch is not None
    assert datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%M:%S") == text


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


def test_weekly_is_never_synthesized():
    settings = SimpleNamespace(
        weekly_lookback=7,
        daily_lookback=7,
        timezone="Asia/Kolkata",
        ma_period=9,
        ema_period=20,
        rsi_period=14,
    )
    state = PsygridState(settings)
    result = _historical_payload(state, "1333", "1w")
    assert result["status"] == "UNAVAILABLE_NATIVE_DHAN_WEEKLY_CANDLE"
    assert result["synthetic_candles"] is False
