"""Support/resistance levels: fixed formulas, checked against hand-computed values (no network)."""

from __future__ import annotations

import itertools
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import levels

IST = ZoneInfo("Asia/Kolkata")
TODAY = date(2026, 10, 16)  # a Friday


def _daily(day: date, o: float, h: float, low: float, c: float) -> dict:
    return {
        "timestamp": int(datetime(day.year, day.month, day.day, tzinfo=IST).timestamp()),
        "open": o,
        "high": h,
        "low": low,
        "close": c,
    }


def _rows():
    rows = []
    day = date(2026, 9, 1)
    price = 100.0
    while day < TODAY + timedelta(days=1):
        if day.weekday() < 5:
            rows.append(_daily(day, price, price + 2, price - 2, price + 0.5))
            price += 0.5
        day += timedelta(days=1)
    # Make last week (Oct 5-9) and the previous session (Oct 15) distinctive.
    for r in rows:
        d = datetime.fromtimestamp(r["timestamp"], IST).date()
        if d == date(2026, 10, 7):
            r.update(high=150.0)
        if d == date(2026, 10, 6):
            r.update(low=90.0)
        if d == date(2026, 10, 15):
            r.update(high=130.0, low=110.0, close=120.0)
    return rows


def test_daily_rows_before_excludes_today_and_sorts():
    prior = levels.daily_rows_before(reversed(_rows()), TODAY, IST)
    assert prior[-1]["date"] == date(2026, 10, 15)
    assert all(a["date"] < b["date"] for a, b in itertools.pairwise(prior))


def test_major_levels_formulas():
    prior = levels.daily_rows_before(_rows(), TODAY, IST)
    major = levels.major_levels(prior, TODAY)
    ref = major["reference"]
    assert (ref["PDH"], ref["PDL"], ref["PDC"]) == (130.0, 110.0, 120.0)
    assert ref["DAILY_PP"] == pytest.approx(120.0)
    assert ref["DAILY_R1"] == pytest.approx(130.0) and ref["DAILY_S1"] == pytest.approx(110.0)
    assert ref["DAILY_R2"] == pytest.approx(140.0) and ref["DAILY_S2"] == pytest.approx(100.0)
    assert ref["CPR_TC"] == pytest.approx(120.0) and ref["CPR_BC"] == pytest.approx(120.0)
    assert ref["PWH"] == 150.0 and ref["PWL"] == 90.0
    oct9 = next(r for r in prior if r["date"] == date(2026, 10, 9))
    assert ref["PWC"] == oct9["close"]
    sept = [r for r in prior if r["date"].month == 9]
    assert ref["PMH"] == max(r["high"] for r in sept) and ref["PMC"] == sept[-1]["close"]
    # This week before today: Mon 12 .. Thu 15
    week = [r for r in prior if r["date"] >= date(2026, 10, 12)]
    assert major["week_prior_days"] == (max(r["high"] for r in week), min(r["low"] for r in week))
    names = {lv["name"] for lv in major["levels"]}
    assert {"PDH", "PWL", "PMH", "HIGH_20D", "WEEKLY_PP", "DAILY_SWING_HIGH"} <= names
    swing = [lv for lv in major["levels"] if lv["name"] == "DAILY_SWING_HIGH"]
    assert any(lv["price"] == 150.0 for lv in swing)


def _minute(h: int, m: int) -> int:
    return int(datetime(2026, 10, 16, h, m, tzinfo=IST).timestamp())


def _intraday(volume: float = 0.0):
    """09:15-10:14: up to 110 at 09:40, down to 95 at 10:00, back to 100."""
    candles = []
    path = [100 + i * 0.4 for i in range(26)] + [110 - i * 0.75 for i in range(20)] + [95 + i * 0.35 for i in range(14)]
    for i, p in enumerate(path):
        t = datetime(2026, 10, 16, 9, 15, tzinfo=IST) + timedelta(minutes=i)
        candles.append((int(t.timestamp()), p, p + 0.2, p - 0.2, p, volume))
    return candles


def test_intraday_levels_opening_range_day_extremes_and_swings():
    candles = _intraday()
    out = levels.intraday_levels(candles, (125.0, 99.0), IST)
    ref = out["reference"]
    assert ref["DAY_OPEN"] == 100.0
    assert ref["DAY_HIGH"] == pytest.approx(110.2) and ref["DAY_LOW"] == pytest.approx(94.8)
    assert ref["ORH"] == pytest.approx(105.6 + 0.2) and ref["ORL"] == pytest.approx(99.8)
    assert ref["OR_PROVISIONAL"] is False
    assert ref["WEEK_HIGH"] == 125.0 and ref["WEEK_LOW"] == pytest.approx(94.8)
    assert "VWAP" not in ref  # no traded volume (an index)
    names = {lv["name"]: lv for lv in out["levels"]}
    assert names["INTRADAY_SWING_HIGH"]["price"] == pytest.approx(110.2)
    assert names["INTRADAY_SWING_LOW"]["price"] == pytest.approx(94.8, abs=0.5)


def test_opening_range_is_provisional_before_0930_and_vwap_with_volume():
    out = levels.intraday_levels(_intraday(volume=10)[:5], None, IST)
    assert out["reference"]["OR_PROVISIONAL"] is True
    assert out["reference"]["VWAP"] == pytest.approx(sum(100 + i * 0.4 for i in range(5)) / 5)


def test_build_levels_sorts_supports_and_resistances_around_the_price():
    prior = levels.daily_rows_before(_rows(), TODAY, IST)
    major = levels.major_levels(prior, TODAY)
    intraday = levels.intraday_levels(_intraday(), major["week_prior_days"], IST)
    options = levels.option_levels({"resistance_strikes": [110, 115], "support_strikes": [95], "max_pain_strike": 100})
    out = levels.build_levels("NIFTY", 100.0, major, intraday, options, as_of="x")
    assert all(lv["price"] <= 100.0 for lv in out["supports"])
    assert all(lv["price"] > 100.0 for lv in out["resistances"])
    assert [lv["price"] for lv in out["supports"]] == sorted((lv["price"] for lv in out["supports"]), reverse=True)
    assert [lv["price"] for lv in out["resistances"]] == sorted(lv["price"] for lv in out["resistances"])
    assert out["nearest"]["support"] == out["supports"][0]
    assert out["nearest"]["resistance"] == out["resistances"][0]
    assert out["nearest"]["intraday_resistance"]["scope"] == "INTRADAY"
    assert out["nearest"]["major_support"]["scope"] == "MAJOR"
    wall = next(lv for lv in out["resistances"] if lv["name"] == "CALL_OI_WALL_1")
    assert wall["price"] == 110.0 and wall["distance_points"] == 10.0 and wall["distance_pct"] == 10.0
    assert wall["confluence"] >= 1  # the day high / swing high near 110


def test_no_data_gives_empty_but_valid_payload():
    out = levels.build_levels("X", None, None, levels.intraday_levels([], None, IST))
    assert out["supports"] == [] and out["resistances"] == [] and out["nearest"]["support"] is None


def test_stock_node_levels_worker_and_view():
    from types import SimpleNamespace

    from live_core.levels_worker import LevelsView, LevelsWorker
    from live_core.state import NodeState

    class API:
        def __init__(self):
            self.calls = 0

        def daily(self, item, start, end):
            self.calls += 1
            if item.symbol == "BAD":
                raise RuntimeError("Dhan API HTTP error: 500")
            return _rows()

    items = [SimpleNamespace(symbol="RELIANCE", security_id="2885"), SimpleNamespace(symbol="BAD", security_id="1")]
    api = API()
    now = datetime(2026, 10, 16, 10, 15, tzinfo=IST)
    worker = LevelsWorker(api, items, "Asia/Kolkata", 0.0, now=lambda: now)
    worker.run_pass()
    assert set(worker.major) == {"RELIANCE"} and worker.failures == 1 and "BAD" in worker.last_error
    worker.run_pass()  # only the failed stock is asked again
    assert api.calls == 3

    state = NodeState("Asia/Kolkata")
    state.begin("2026-10-16", items)
    series = state.by_symbol["RELIANCE"]
    for c in _intraday(volume=5):
        series.put_completed(c, 0)
    runtime = SimpleNamespace(state=state, levels=worker)
    view = LevelsView(runtime)
    one = view.one("reliance")
    assert one["symbol"] == "RELIANCE" and one["reference"]["PDH"] == 130.0 and "VWAP" in one["reference"]
    assert view.one("NOPE") is None
    everything = view.everything()
    assert everything["stock_count"] == 2 and everything["major_levels_status"]["stocks_with_major_levels"] == 1
    assert everything["levels"]["BAD"]["reference"].get("PDH") is None
