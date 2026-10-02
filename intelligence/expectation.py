"""Derivatives expectation engine: what the option market prices for the day, and how much of it has happened.

For each recorded underlying (NIFTY, BANKNIFTY, MIDCPNIFTY) at ``as_of``, from
the latest full option-chain snapshot taken by then
(``<store>/chains/<date>.csv.gz``), the index's 1m bars and the per-minute
futures snapshots:

- **ATM IV** (mean of call and put IV at the strike nearest spot) and the
  implied daily move ``IV / sqrt(252)``;
- **ATM straddle** (call + put, mid where both quotes exist) and the move it
  prices to expiry as a share of spot; ``straddle / spot / sqrt(2/pi)`` is the
  implied standard deviation to expiry under a normal approximation;
- **realised variance** of the index since the open (sum of squared 1m log
  returns) and the **variance budget consumed**: realised variance over the
  implied daily variance. Because variance does not arrive evenly through
  the day, it is compared with the share of a day's realised variance that
  had arrived by this time of day in earlier sessions (median and 10th/90th
  percentiles). ``budget_ratio`` > 1 means the day has used more of its
  implied budget than usual by this time;
- **skew**: 25-delta risk reversal (put IV - call IV) when deltas are served,
  else the IV difference of strikes about 3% out of the money on each side;
- **put/call OI** and the session change in OI on each side;
- **basis**: futures minus spot in bps, and its z-score against the session's
  earlier basis readings;
- **tail z**: the latest 1m index return in units of the 1m move implied by
  ATM IV.

Uncertainty is reported with every value: snapshot age, the ATM legs'
bid-ask width relative to the straddle, the number of strikes with an IV,
and the number of sessions behind the time-of-day profile. Nothing here
models dealer positioning or hedging flows; IV, OI and prices are reported
as served by Dhan.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np

from daily_archive import INDEX_FILE
from intelligence.archive import IST, _read_rows, parse_timestamp, session_days
from intelligence.derivatives import load_chains, load_derivatives

TRADING_DAYS = 252
SESSION_MINUTES = 375
SKEW_DISTANCE = 0.03
MAX_SNAPSHOT_AGE = 180  # seconds; older snapshots are reported but flagged stale
SQRT_2_OVER_PI = math.sqrt(2 / math.pi)


def _f(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _iv(value) -> float | None:
    """IV as a fraction; Dhan serves percent (13.2), tolerate fractions (0.132)."""
    number = _f(value)
    if number is None or number <= 0:
        return None
    return number / 100 if number > 3 else number


@dataclass
class Expectation:
    underlying: str
    as_of: str
    snapshot_minute: str | None = None
    snapshot_age_s: int | None = None
    expiry: str | None = None
    days_to_expiry: float | None = None
    spot: float | None = None
    atm_strike: float | None = None
    atm_iv: float | None = None
    implied_daily_move_pct: float | None = None
    straddle: float | None = None
    straddle_pct: float | None = None
    implied_sd_to_expiry_pct: float | None = None
    realised_var: float | None = None
    realised_move_pct: float | None = None
    budget_consumed: float | None = None
    expected_share: float | None = None
    expected_share_band: tuple | None = None
    budget_ratio: float | None = None
    skew: float | None = None
    skew_method: str | None = None
    put_call_oi: float | None = None
    ce_oi_change: float | None = None
    pe_oi_change: float | None = None
    basis_bps: float | None = None
    basis_z: float | None = None
    tail_z: float | None = None
    uncertainty: dict = field(default_factory=dict)
    notes: list = field(default_factory=list)

    def view(self) -> dict:
        out = {}
        for k, v in self.__dict__.items():
            out[k] = round(v, 6) if isinstance(v, float) else v
        return out


def _latest_snapshot(rows: list[dict], key: str, as_of_epoch: int) -> tuple[int | None, list[dict]]:
    minutes = [int(r["minute"]) for r in rows if r.get("underlying") == key and r.get("minute", "").isdigit()]
    known = [m for m in minutes if m <= as_of_epoch]
    if not known:
        return None, []
    latest = max(known)
    return latest, [r for r in rows if r.get("underlying") == key and r.get("minute") == str(latest)]


def _mid(leg: dict) -> tuple[float | None, float | None]:
    """(price, bid-ask width): mid when both quotes are positive, else last price with unknown width."""
    bid, ask = _f(leg.get("top_bid_price")), _f(leg.get("top_ask_price"))
    if bid and ask and ask >= bid > 0:
        return (bid + ask) / 2, ask - bid
    return _f(leg.get("last_price")), None


def chain_view(snapshot: list[dict]) -> dict:
    """Strike -> {"CE": row, "PE": row} for one snapshot."""
    strikes: dict[float, dict] = {}
    for row in snapshot:
        strike = _f(row.get("strike"))
        if strike is not None:
            strikes.setdefault(strike, {})[row.get("side")] = row
    return strikes


def _skew(strikes: dict, spot: float) -> tuple[float | None, str | None]:
    puts = [(abs(_f(s["PE"].get("delta")) + 0.25), _iv(s["PE"].get("implied_volatility")))
            for s in strikes.values() if "PE" in s and _f(s["PE"].get("delta")) is not None]  # fmt: skip
    calls = [(abs(_f(s["CE"].get("delta")) - 0.25), _iv(s["CE"].get("implied_volatility")))
             for s in strikes.values() if "CE" in s and _f(s["CE"].get("delta")) is not None]  # fmt: skip
    puts = [p for p in puts if p[1] is not None and p[0] < 0.1]
    calls = [c for c in calls if c[1] is not None and c[0] < 0.1]
    if puts and calls:
        return min(puts)[1] - min(calls)[1], "risk_reversal_25d"
    low = min(strikes, key=lambda k: abs(k - spot * (1 - SKEW_DISTANCE)), default=None)
    high = min(strikes, key=lambda k: abs(k - spot * (1 + SKEW_DISTANCE)), default=None)
    if low is None or high is None or low == high:
        return None, None
    put_iv = _iv((strikes[low].get("PE") or {}).get("implied_volatility"))
    call_iv = _iv((strikes[high].get("CE") or {}).get("implied_volatility"))
    if put_iv is None or call_iv is None:
        return None, None
    return put_iv - call_iv, f"iv_{int(SKEW_DISTANCE * 100)}pct_otm"


def index_returns(archive_root: Path, session_date: str, key: str, as_of_epoch: int | None = None) -> np.ndarray:
    """1m log returns (close over open) of an index for bars closed by ``as_of``, from the archived index file."""
    path = Path(archive_root) / session_date / INDEX_FILE
    if not path.exists():
        return np.zeros(0)
    out = []
    for row in _read_rows(path):
        if row.get("index") != key:
            continue
        try:
            minute = parse_timestamp(row["timestamp"])
            o, c = float(row["open"]), float(row["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if (as_of_epoch is None or minute + 60 <= as_of_epoch) and o > 0 and c > 0:
            out.append((minute, math.log(c / o)))
    out.sort()
    return np.array([r for _, r in out])


def variance_profile(archive_root: Path, key: str, before: str, lookback: int = 60) -> np.ndarray | None:
    """(sessions, SESSION_MINUTES) cumulative share of each earlier session's realised variance by minute."""
    rows = []
    for session in [d for d in session_days(archive_root) if d < before][-lookback:]:
        r = index_returns(archive_root, session, key)
        if len(r) < SESSION_MINUTES * 0.9:
            continue
        sq = np.cumsum(r[:SESSION_MINUTES] ** 2)
        if sq[-1] <= 0:
            continue
        share = np.full(SESSION_MINUTES, 1.0)
        share[: len(sq)] = sq / sq[-1]
        rows.append(share)
    return np.array(rows) if rows else None


def expectation(archive_root: Path, store_root: Path, session_date: str, key: str, as_of: datetime,
                returns: np.ndarray | None = None, profile: np.ndarray | None = None) -> Expectation:  # fmt: skip
    """The derivatives expectation for ``key`` at ``as_of``, from data recorded by then."""
    epoch = int(as_of.timestamp())
    out = Expectation(key, as_of.strftime("%Y-%m-%d %H:%M:%S IST"))
    minute, snapshot = _latest_snapshot(load_chains(store_root, session_date), key, epoch)
    if minute is None:
        out.notes.append("no option-chain snapshot recorded by as_of")
    else:
        out.snapshot_minute = datetime.fromtimestamp(minute, IST).strftime("%H:%M")
        out.snapshot_age_s = epoch - minute
        strikes = chain_view(snapshot)
        spot = _f(snapshot[0].get("underlying_ltp"))
        out.spot, out.expiry = spot, snapshot[0].get("expiry") or None
        try:
            expiry = datetime.strptime(out.expiry, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=IST)
            out.days_to_expiry = max((expiry - as_of).total_seconds() / 86400, 0.0)
        except (TypeError, ValueError):
            pass
        ivs = sum(1 for s in strikes.values() for leg in s.values() if _iv(leg.get("implied_volatility")))
        if spot and strikes:
            atm = min(strikes, key=lambda k: abs(k - spot))
            out.atm_strike = atm
            legs = strikes[atm]
            iv = [_iv(legs[s].get("implied_volatility")) for s in ("CE", "PE") if s in legs]
            iv = [v for v in iv if v is not None]
            out.atm_iv = sum(iv) / len(iv) if iv else None
            if out.atm_iv:
                out.implied_daily_move_pct = out.atm_iv / math.sqrt(TRADING_DAYS) * 100
            prices = [_mid(legs[s]) for s in ("CE", "PE") if s in legs]
            if len(prices) == 2 and all(p[0] for p in prices):
                out.straddle = prices[0][0] + prices[1][0]
                out.straddle_pct = out.straddle / spot * 100
                out.implied_sd_to_expiry_pct = out.straddle_pct / SQRT_2_OVER_PI
                widths = [p[1] for p in prices]
                out.uncertainty["atm_spread_share"] = (sum(widths) / out.straddle) if None not in widths else None
            out.skew, out.skew_method = _skew(strikes, spot)
            ce_oi = sum(_f(s["CE"].get("oi")) or 0 for s in strikes.values() if "CE" in s)
            pe_oi = sum(_f(s["PE"].get("oi")) or 0 for s in strikes.values() if "PE" in s)
            out.put_call_oi = pe_oi / ce_oi if ce_oi else None
            out.ce_oi_change = ce_oi - sum(_f(s["CE"].get("previous_oi")) or 0 for s in strikes.values() if "CE" in s)
            out.pe_oi_change = pe_oi - sum(_f(s["PE"].get("previous_oi")) or 0 for s in strikes.values() if "PE" in s)
        out.uncertainty.update({"strikes_with_iv": ivs, "stale_snapshot": out.snapshot_age_s > MAX_SNAPSHOT_AGE})
    # realised variance and the budget
    r = index_returns(archive_root, session_date, key, epoch) if returns is None else returns
    if len(r):
        out.realised_var = float((r**2).sum())
        out.realised_move_pct = math.sqrt(out.realised_var) * 100
        if out.atm_iv:
            daily_var = out.atm_iv**2 / TRADING_DAYS
            out.budget_consumed = out.realised_var / daily_var
            out.tail_z = float(r[-1] / (out.atm_iv / math.sqrt(TRADING_DAYS * SESSION_MINUTES)))
    profile = variance_profile(archive_root, key, session_date) if profile is None else profile
    if profile is not None and len(r):
        at = min(len(r), SESSION_MINUTES) - 1
        share = profile[:, at]
        out.expected_share = float(np.median(share))
        out.expected_share_band = (float(np.quantile(share, 0.1)), float(np.quantile(share, 0.9)))
        out.uncertainty["profile_sessions"] = int(profile.shape[0])
        if out.budget_consumed is not None and out.expected_share > 0:
            out.budget_ratio = out.budget_consumed / out.expected_share
    elif len(r):
        out.notes.append("no earlier sessions for the time-of-day variance profile")
    # basis from the per-minute futures snapshots
    basis = []
    for snap in load_derivatives(store_root, session_date).known_at(epoch):
        fut = ((snap.get("futures") or {}).get(key) or {}).get("last_price")
        spot = (snap.get("spot") or {}).get(key)
        if fut and spot:
            basis.append((fut - spot) / spot * 1e4)
    if basis:
        out.basis_bps = basis[-1]
        if len(basis) >= 10 and np.std(basis[:-1]) > 0:
            out.basis_z = float((basis[-1] - np.mean(basis[:-1])) / np.std(basis[:-1]))
    return out
