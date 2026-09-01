import time
from types import SimpleNamespace

from feed import LiveFeed
from state_runtime import RuntimeFreshnessState


def _settings():
    return SimpleNamespace(
        timezone="Asia/Kolkata",
        max_live_age_seconds=60,
        ma_period=9,
        ema_period=20,
        rsi_period=14,
    )


def _instruments():
    return [
        SimpleNamespace(symbol="AAA", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY"),
        SimpleNamespace(symbol="BBB", security_id="2", exchange_segment="NSE_EQ", instrument="EQUITY"),
    ]


def test_runtime_freshness_uses_packet_receipt_not_ltt():
    state = RuntimeFreshnessState(_settings())
    state.begin("2026-09-01", _instruments())

    # Deliberately use an old market-trade timestamp. Receipt time must still
    # make the accepted quote fresh immediately after it is received.
    old_ltt = int(time.time()) - 3600
    state.record_live_quote("1", old_ltt)

    assert state.freshness("1")["status"] == "LIVE"
    assert state.freshness("1")["live_data_valid"] is True


def test_runtime_freshness_blocks_old_packet_receipt():
    state = RuntimeFreshnessState(_settings())
    state.begin("2026-09-01", _instruments())
    state.record_live_quote("1", int(time.time()))

    state.last_tick_received_by_security["1"] = time.time() - 61
    state.last_tick_received_epoch = time.time() - 61

    assert state.freshness("1")["status"] == "STALE"
    assert state.freshness("1")["live_data_valid"] is False
    assert state.snapshot()["live_stock_count"] == 0
    assert state.all_live_stale() is True


def test_future_wall_clock_epoch_is_corrected_only_by_exchange_offset():
    now = int(time.time())
    future_wall_clock = now + 19_800
    corrected = LiveFeed._normalize_future_epoch(
        future_wall_clock,
        "Asia/Kolkata",
        float(now),
    )
    assert corrected is not None
    assert abs(corrected - now) <= 5


def test_genuinely_far_future_epoch_is_rejected():
    now = int(time.time())
    assert LiveFeed._normalize_future_epoch(
        now + 40_000,
        "Asia/Kolkata",
        float(now),
    ) is None
