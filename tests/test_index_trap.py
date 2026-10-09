"""The NIFTY opening-range trap test script: rules, no look-ahead, exits, expiry calendar, costs."""

import importlib.util
from datetime import date, time, timedelta
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "index_trap_test", Path(__file__).resolve().parents[1] / "deploy" / "live-core" / "index_trap_test.py"
)
T = importlib.util.module_from_spec(spec)
spec.loader.exec_module(T)


def day(closes, start=(9, 15), spread=2.0):
    """5-minute bars from closes; open = previous close."""
    out, prev = [], closes[0]
    for i, c in enumerate(closes):
        m = start[0] * 60 + start[1] + 5 * i
        out.append((time(m // 60, m % 60), prev, max(prev, c) + spread, min(prev, c) - spread, c))
        prev = c
    return out


# OR 09:15-09:25 = [24000-ish .. 24060]; 09:30 closes above, 09:35 back inside -> short at 09:40 open
TRAP_UP = [23950, 24050, 24040, 24080, 24030] + [24020 - 3 * i for i in range(60)]


def test_failed_up_breakout_is_a_short_with_stop_above_the_extreme():
    bars = day(TRAP_UP)
    sig = T.signal(bars)
    assert sig["side"] == -1 and sig["entry_i"] == 5 and sig["entry"] == bars[5][1]
    assert sig["stop"] == pytest.approx(max(b[2] for b in bars[3:5]) * 1.0003)
    assert sig["target"] == min(b[3] for b in bars[:3])


def test_no_look_ahead():
    bars = day(TRAP_UP)
    future = bars[:6] + [(t, 1.0, 99999.0, 0.5, 5000.0) for t, *_ in bars[6:]]
    assert T.signal(bars) == T.signal(future)


def test_no_trade_cases():
    assert T.signal(day([24000] * 3 + [24001] * 60, spread=0.01)) is None  # OR too narrow, no breakout
    assert T.signal(day([24010, 24050, 24040] + [24100] * 60)) is None  # breakout never fails
    late = day([24010, 24050, 24040] + [24045] * 56 + [24100, 24030] + [24030] * 4)
    assert T.signal(late) is None  # trap after 14:30


def test_stop_before_target_and_time_exit():
    bars = day(TRAP_UP)
    sig = T.signal(bars)
    t, o, _h, _lo, c = bars[6]
    bars[6] = (t, o, sig["stop"] + 5, sig["target"] - 5, c)  # both in one candle
    assert T.manage(bars, sig)[2] == "STOP"
    flat = day([23950, 24050, 24040, 24080, 24030] + [24025] * 60, spread=0.5)
    s2 = T.signal(flat)
    _price, i, reason = T.manage(flat, s2)
    assert reason == "TIME" and i == s2["entry_i"] + T.HOLD_BARS - 1


def test_expiry_calendar_with_switch_and_holidays():
    sessions = {
        date(2025, 8, 25) + timedelta(days=i)
        for i in range(30)
        if (date(2025, 8, 25) + timedelta(days=i)).weekday() < 5
    }
    assert T.expiry_for(date(2025, 8, 25), sessions) == date(2025, 8, 28)  # Thursday before the switch
    assert T.expiry_for(date(2025, 8, 29), sessions) == date(2025, 9, 2)  # first Tuesday after the switch
    assert T.expiry_for(date(2025, 9, 3), sessions) == date(2025, 9, 9)
    sessions.discard(date(2025, 9, 9))  # a holiday on the Tuesday
    assert T.expiry_for(date(2025, 9, 3), sessions) == date(2025, 9, 8)


def test_lot_sizes_by_contract_expiry():
    assert T.lot_size(date(2024, 4, 25)) == 50
    assert T.lot_size(date(2024, 6, 6)) == 25
    assert T.lot_size(date(2025, 3, 6)) == 75
    assert T.lot_size(date(2026, 1, 13)) == 65
    assert T.uncertain(date(2024, 12, 5)) and not T.uncertain(date(2025, 3, 6))


def test_charges_follow_the_october_2024_revision():
    old = T.charges(date(2024, 9, 30), 7500, 7500, 0.0, 75)
    new = T.charges(date(2024, 10, 1), 7500, 7500, 0.0, 75)
    assert new > old  # STT on sold premium 0.0625% -> 0.1% outweighs the lower exchange charge
    assert T.charges(date(2025, 1, 1), 7500, 7500, 0.5, 75) - T.charges(date(2025, 1, 1), 7500, 7500, 0.0, 75) == 75.0


def test_option_estimate_costs_and_time_decay():
    bars = day(TRAP_UP)
    sig = T.signal(bars)
    exp = date(2026, 10, 13)
    pnl_flat, _, p0 = T.option_trade(date(2026, 10, 8), bars, sig, sig["entry"], sig["entry_i"], 13.0, exp, 65, 0.10)
    assert pnl_flat < -40  # no move: brokerage, taxes, slippage and decay
    win, risk, _ = T.option_trade(
        date(2026, 10, 8), bars, sig, sig["entry"] - 60, sig["entry_i"] + 6, 13.0, exp, 65, 0.10
    )
    assert win > 900  # 60 points x 0.5 delta x 65 minus costs
    assert risk > 0.5 * sig["risk_pts"] * 65
    assert 50 < p0 < 400
