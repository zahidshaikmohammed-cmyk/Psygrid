from datetime import datetime
from zoneinfo import ZoneInfo

from tools.profile_host import ist_lag_seconds, parse_meminfo

IST = ZoneInfo("Asia/Kolkata")


def test_parse_meminfo_reads_kilobyte_fields_as_bytes():
    values = parse_meminfo("MemTotal:       24576000 kB\nMemAvailable:   12288000 kB\nHugePages_Total:       0\n")
    assert values["MemTotal"] == 24576000 * 1024
    assert values["MemAvailable"] == 12288000 * 1024
    assert values["HugePages_Total"] == 0


def test_indicator_lag_is_measured_from_the_ist_stamp():
    now = datetime(2026, 10, 2, 10, 0, 30, tzinfo=IST)
    assert ist_lag_seconds("2026-10-02 10:00:00 IST", now) == 30.0
    assert ist_lag_seconds(None, now) is None
    assert ist_lag_seconds("not a time", now) is None
