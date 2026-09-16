from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from feed import LiveFeed


def test_dhan_ltt_accepts_exchange_wall_clock_time_string():
    expected_dt = datetime.now(ZoneInfo("Asia/Kolkata")).replace(microsecond=0) - timedelta(seconds=60)
    text = expected_dt.strftime("%H:%M:%S")
    epoch = LiveFeed._parse_ltt(text)
    assert epoch is not None
    assert datetime.fromtimestamp(epoch, ZoneInfo("Asia/Kolkata")).strftime("%H:%M:%S") == text


def test_dhan_ltt_accepts_epoch():
    assert LiveFeed._parse_ltt(1788234360) == 1788234360
