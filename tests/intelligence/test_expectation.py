"""Derivatives expectation engine: IV, straddle, variance budget, skew, basis, with no look-ahead."""

import math

import numpy as np
import pytest

from intelligence.derivatives import ChainRecorder, DerivativesRecorder, chain_rows
from intelligence.expectation import expectation, index_returns, variance_profile
from intelligence.frame import as_of_time
from tests.intelligence.conftest import TODAY


def chain(spot, iv=15.0, deltas=True):
    strikes = []
    base = int(round(spot / 50) * 50)
    for strike in range(base - 1000, base + 1001, 50):
        m = (strike - spot) / spot
        legs = {}
        for side in ("ce", "pe"):
            legs[side] = {
                "last_price": max(1.0, 120 - 3000 * abs(m)), "oi": 1000.0, "previous_oi": 800.0, "volume": 10.0,
                "implied_volatility": iv + (60 * -m if side == "pe" else 20 * m),
                "top_bid_price": max(1.0, 119 - 3000 * abs(m)), "top_ask_price": max(1.5, 121 - 3000 * abs(m)),
                "greeks": {"delta": (0.5 - 10 * m if side == "ce" else -0.5 - 10 * m)} if deltas else {},
            }  # fmt: skip
        strikes.append({"strike": strike, **legs})
    return {"expiry": "2026-10-08", "underlying_ltp": spot, "strikes": strikes}


@pytest.fixture
def store(tmp_path):
    rec = ChainRecorder(tmp_path)
    for hhmm, spot in (("10:00", 25000.0), ("10:30", 25010.0), ("11:00", 25020.0)):
        rec.append(TODAY, chain_rows(int(as_of_time(TODAY, hhmm).timestamp()), "nifty", chain(spot)))
    fut = DerivativesRecorder(tmp_path)
    for i in range(20):
        minute = int(as_of_time(TODAY, "10:00").timestamp()) + 60 * i
        basis = 30.0 + i % 3 if i < 19 else 60.0
        fut.append(TODAY, {"minute": minute, "spot": {"nifty": 25000.0},
                           "futures": {"nifty": {"last_price": 25000.0 + basis}}, "options": {}})  # fmt: skip
    return tmp_path


def test_values_from_the_latest_snapshot_known_at_as_of(market_root, store):
    at = as_of_time(TODAY, "10:45")
    e = expectation(market_root, store, TODAY, "nifty", at)
    assert e.snapshot_minute == "10:30" and e.snapshot_age_s == 900 and e.uncertainty["stale_snapshot"]
    assert e.spot == 25010.0 and e.atm_strike == 25000.0
    assert e.atm_iv == pytest.approx(0.15, abs=0.005)
    assert e.implied_daily_move_pct == pytest.approx(e.atm_iv / math.sqrt(252) * 100)
    assert e.straddle == pytest.approx(2 * (120 - 3000 * 10 / 25010), rel=1e-6)  # mids
    assert e.skew_method == "risk_reversal_25d" and e.skew > 0  # puts richer than calls
    assert e.put_call_oi == pytest.approx(1.0) and e.ce_oi_change > 0
    assert e.days_to_expiry == pytest.approx((as_of_time("2026-10-08", "15:30") - at).total_seconds() / 86400)
    assert e.basis_bps == pytest.approx(24.0)  # 10:00 .. 10:19 known; the last reading is the jump
    later = expectation(market_root, store, TODAY, "nifty", as_of_time(TODAY, "10:20"))
    assert later.snapshot_minute == "10:00"


def test_basis_z_and_skew_fallback(market_root, store, tmp_path):
    e = expectation(market_root, store, TODAY, "nifty", as_of_time(TODAY, "10:19"))
    assert e.basis_bps == pytest.approx(24.0) and e.basis_z > 3
    ChainRecorder(tmp_path / "nod").append(TODAY, chain_rows(int(as_of_time(TODAY, "10:00").timestamp()), "nifty",
                                                             chain(25000.0, deltas=False)))  # fmt: skip
    f = expectation(market_root, tmp_path / "nod", TODAY, "nifty", as_of_time(TODAY, "10:05"))
    assert f.skew_method == "iv_3pct_otm" and f.skew > 0


def test_variance_budget(market_root, store):
    at = as_of_time(TODAY, "12:00")
    r = index_returns(market_root, TODAY, "nifty", int(at.timestamp()))
    assert len(r) == 165  # 09:15 .. 11:59, nothing after as_of
    profile = variance_profile(market_root, "nifty", TODAY)
    assert profile is not None and profile.shape[1] == 375 and np.all(np.diff(profile, axis=1) >= -1e-12)
    e = expectation(market_root, store, TODAY, "nifty", at, profile=profile)
    assert e.realised_var == pytest.approx(float((r**2).sum()))
    assert e.budget_consumed == pytest.approx(e.realised_var / (e.atm_iv**2 / 252))
    assert e.budget_ratio == pytest.approx(e.budget_consumed / e.expected_share)
    lo, hi = e.expected_share_band
    assert lo <= e.expected_share <= hi and e.uncertainty["profile_sessions"] == profile.shape[0]


def test_nothing_recorded(market_root, tmp_path):
    e = expectation(market_root, tmp_path, TODAY, "banknifty", as_of_time(TODAY, "09:16"))
    assert e.atm_iv is None and "no option-chain snapshot recorded by as_of" in e.notes
