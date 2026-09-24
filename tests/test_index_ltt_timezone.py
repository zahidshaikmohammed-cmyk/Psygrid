"""Regression coverage for IndexLayerFeed._parse_ltt.

Real production evidence: /public/nifty.json reported current_time_ist as
"2026-09-24 14:16:45 IST" (correct - independently computed straight from
the server clock) while feed.ltp_timestamp showed "2026-09-24 19:46:45 IST"
- exactly 5:30:00 ahead, the IST/UTC offset. Dhan reports LTT as a bare
HH:MM:SS string already in Asia/Kolkata wall-clock time, but _parse_ltt
mislabeled it as UTC (tzinfo=ZoneInfo("UTC")) before converting to an
epoch, adding a spurious +5:30 to every tick and every 1m candle built
from it. The sealed equity feed.py._parse_ltt already tags the same kind
of string with Asia/Kolkata, not UTC - this ports that exact pattern,
including its future-timestamp rejection safety net.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from index_layer import IndexLayerFeed, IndexState, IndexInstrument


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def _feed():
    instrument = IndexInstrument(security_id="13", exchange_segment="IDX_I")
    state = IndexState(_settings(), "nifty", "NIFTY", instrument)
    return IndexLayerFeed(_settings(), {"nifty": state})


def test_bare_hhmmss_string_is_interpreted_as_ist_not_utc():
    feed = _feed()
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
    ltt_string = ist_now.strftime("%H:%M:%S")

    epoch = feed._parse_ltt(ltt_string)

    assert epoch is not None
    # Must land within a few seconds of the real current instant - not
    # 5:30:00 in the future, which is what the UTC-mislabeling bug produced.
    assert abs(epoch - ist_now.timestamp()) < 5


def test_reproduces_the_exact_reported_offset_when_mislabeled_as_utc():
    """Directly demonstrates the bug's magnitude: tagging a genuine IST
    HH:MM:SS string as UTC shifts the resulting epoch forward by exactly
    the zone's UTC offset (5:30:00 for Asia/Kolkata) - matching the
    reported current_time_ist=14:16:45 vs ltp_timestamp=19:46:45."""
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
    text = ist_now.strftime("%H:%M:%S")

    buggy = datetime.strptime(text, "%H:%M:%S").replace(
        year=ist_now.year, month=ist_now.month, day=ist_now.day, tzinfo=timezone.utc,
    )
    ist_offset_seconds = ZoneInfo("Asia/Kolkata").utcoffset(ist_now).total_seconds()
    assert int(buggy.timestamp()) - int(ist_now.timestamp()) == int(ist_offset_seconds) == 19800  # 5:30:00


def test_numeric_epoch_passes_through_unchanged():
    feed = _feed()
    now = int(datetime.now(timezone.utc).timestamp())

    assert feed._parse_ltt(now) == now
    assert feed._parse_ltt(str(now)) == now


def test_far_future_epoch_is_rejected_not_silently_accepted():
    feed = _feed()
    far_future = int(datetime.now(timezone.utc).timestamp()) + 3600  # 1 hour ahead, not a tz-offset artifact

    assert feed._parse_ltt(far_future) is None


def test_none_and_empty_values_are_rejected():
    feed = _feed()
    assert feed._parse_ltt(None) is None
    assert feed._parse_ltt("") is None
