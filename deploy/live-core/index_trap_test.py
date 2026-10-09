"""NIFTY opening-range TRAP: one pre-registered test on multi-year Dhan index candles. Read-only.

Run by the deploy workflow's `index-test` action on a Live Core node, after hours, with the
node's own environment: it takes the account's EXISTING token from the token authority (never
mints one), downloads 5-minute candles for NIFTY 50, BANKNIFTY and INDIA VIX from Dhan's
/charts/intraday in 90-day pieces into RAM, prints the result and exits. Nothing is written to disk
and no order is ever placed.

THE RULES (fixed before any data was seen; nothing here is tuned):
  Range       OR = high/low of the 09:15, 09:20 and 09:25 five-minute candles (first 15 minutes).
              No trade when the OR is narrower than 0.15% or wider than 1.0% of the index.
  Breakout    The FIRST 5-minute close outside the OR, from the 09:30 candle on.
  Trap        Within the next 3 candles a candle closes back inside the OR. Only the first breakout
              of the day counts; no re-entry within 3 candles = no trade that day.
  Entry       Open of the candle after the trap candle, at or before 14:30. Failed up-breakout ->
              SHORT the index (buy the ATM PUT); failed down-breakout -> LONG (buy the ATM CALL).
  Stop        Beyond the extreme reached since the breakout, plus 0.03% of the price.
  Target      The opposite side of the OR. No trade when it is less than 1R away.
  Time        Exit at the close 60 minutes after entry, or 15:15, whichever is first.
              Stop is checked before target when one candle touches both.
  Filters     NIFTY: no trade on the weekly expiry day (Thursday before 2025-09-01, Tuesday after;
              exchange holidays that move the expiry are ignored -- approximation).
              One trade per index per day.
  Size        1 lot. Skip the trade when the estimated option loss at the stop exceeds Rs 950
              (5% of a Rs 19,000 account).

OPTION P&L IS MODELLED, NOT OBSERVED: there is no historical option price data here. Premium =
0.4 * S * VIX/100 * sqrt(T) (ATM approximation), delta 0.5, time decay = premium(T0) - premium(T1),
gamma ignored. Costs per lot: brokerage Rs 20 x 2, STT 0.1% of sell premium, NSE 0.03503% of
premium both sides, SEBI 0.0001%, GST 18%, stamp 0.003% of buy premium, slippage Rs 0.10/unit/side
(BASE) or Rs 0.50 (ADVERSE). The INDEX-POINTS result needs no option model at all.
"""

from __future__ import annotations

import math
import os
import sys
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
INDICES = {"NIFTY": ("13", 75), "BANKNIFTY": ("25", 35)}  # Dhan IDX_I security id, lot size
VIX_ID = "21"
OR_BARS = 3
MAX_ENTRY = time(14, 30)
HOLD_BARS = 12
LAST_EXIT = time(15, 10)  # its close is 15:15
MIN_OR, MAX_OR = 0.15, 1.0
BUFFER = 0.0003
MAX_RISK_RUPEES = 950.0
SLIP = {"BASE": 0.10, "ADVERSE": 0.50}


# --------------------------------------------------------------------------- data


def fetch(api, security_id: str, years: float, say=print):
    """5-minute IDX_I candles in 90-day pieces -> {date: [(time, o, h, l, c)]} (IST)."""
    item = SimpleNamespace(security_id=security_id, exchange_segment="IDX_I", instrument="INDEX")
    end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    start = end - timedelta(days=int(365 * years))
    days: dict = defaultdict(dict)
    cur = start
    while cur < end:
        stop = min(cur + timedelta(days=89), end)
        try:
            rows = api.intraday(item, 5, cur, stop)
        except Exception as exc:
            say(f"  {security_id} {cur:%Y-%m-%d}..{stop:%Y-%m-%d}: {type(exc).__name__}: {str(exc)[:120]}")
            rows = []
        for r in rows:
            ts = datetime.fromtimestamp(int(r["timestamp"]), IST)
            if time(9, 15) <= ts.time() < time(15, 30):
                days[ts.date()][ts.time()] = (r["open"], r["high"], r["low"], r["close"])
        cur = stop + timedelta(days=1)
    return {d: [(t, *v) for t, v in sorted(bars.items())] for d, bars in sorted(days.items())}


# --------------------------------------------------------------------------- rules


def weekly_expiry(d: date) -> bool:
    return d.weekday() == (3 if d < date(2025, 9, 1) else 1)


def next_expiry(d: date) -> date:
    wd = 3 if d < date(2025, 9, 1) else 1
    return d + timedelta(days=(wd - d.weekday()) % 7)


def signal(bars):
    """bars: [(time, o, h, l, c)] of ONE day. -> dict or None. Reads bars in time order only."""
    if len(bars) < OR_BARS + 2 or [b[0] for b in bars[:OR_BARS]] != [time(9, 15), time(9, 20), time(9, 25)]:
        return None
    hi = max(b[2] for b in bars[:OR_BARS])
    lo = min(b[3] for b in bars[:OR_BARS])
    width = (hi - lo) / lo * 100
    if not MIN_OR <= width <= MAX_OR:
        return None
    first = None
    for i in range(OR_BARS, len(bars)):
        if bars[i][4] > hi or bars[i][4] < lo:
            first = i
            break
    if first is None:
        return None
    up = bars[first][4] > hi
    for k in range(first + 1, min(first + 4, len(bars))):
        back = bars[k][4] <= hi if up else bars[k][4] >= lo
        if not back:
            continue
        if k + 1 >= len(bars) or bars[k + 1][0] > MAX_ENTRY:
            return None
        entry = bars[k + 1][1]
        side = -1 if up else 1
        ext = max(b[2] for b in bars[first : k + 1]) if up else min(b[3] for b in bars[first : k + 1])
        stop = ext * (1 + BUFFER) if up else ext * (1 - BUFFER)
        target = lo if up else hi
        risk = side * (entry - stop)
        if risk <= 0 or side * (target - entry) < risk:
            return None
        return {
            "entry_i": k + 1,
            "side": side,
            "entry": entry,
            "stop": stop,
            "target": target,
            "or_hi": hi,
            "or_lo": lo,
            "risk_pts": risk,
        }
    return None


def manage(bars, sig):
    """Exit price, exit index and reason; stop before target; 60 minutes or 15:15."""
    e, side = sig["entry_i"], sig["side"]
    last = e
    for i in range(e, min(e + HOLD_BARS, len(bars))):
        t, o, h, lo, _c = bars[i]
        if t > LAST_EXIT:
            break
        last = i
        stop_hit = lo <= sig["stop"] if side > 0 else h >= sig["stop"]
        tgt_hit = h >= sig["target"] if side > 0 else lo <= sig["target"]
        if stop_hit:
            gapped = o <= sig["stop"] if side > 0 else o >= sig["stop"]
            return (o if gapped else sig["stop"]), i, "STOP"
        if tgt_hit:
            gapped = o >= sig["target"] if side > 0 else o <= sig["target"]
            return (o if gapped else sig["target"]), i, "TARGET"
    return bars[last][4], last, "TIME"


# --------------------------------------------------------------------------- option model


def premium(spot: float, vix: float, years: float) -> float:
    return 0.4 * spot * vix / 100 * math.sqrt(max(years, 1 / 365 / 6.25))


def option_trade(d: date, bars, sig, exit_price, exit_i, vix: float, lot: int, slip: float):
    """Modelled rupee P&L for 1 lot of the ATM option bought in the trade's direction."""
    t0 = datetime.combine(d, bars[sig["entry_i"]][0], IST)
    t1 = datetime.combine(d, bars[exit_i][0], IST) + timedelta(minutes=5)
    exp = datetime.combine(next_expiry(d), time(15, 30), IST)

    def yrs(t):
        return max((exp - t).total_seconds(), 0) / (365 * 24 * 3600)

    p0 = premium(sig["entry"], vix, yrs(t0))
    p1 = premium(sig["entry"], vix, yrs(t1))
    move = sig["side"] * (exit_price - sig["entry"])
    unit = 0.5 * move - (p0 - p1)
    exit_prem = max(p0 + unit, 0.05)
    buy_v, sell_v = p0 * lot, exit_prem * lot
    brokerage = 40.0
    exch = 0.0003503 * (buy_v + sell_v)
    sebi = 0.000001 * (buy_v + sell_v)
    costs = (
        brokerage + 0.001 * sell_v + exch + sebi + 0.18 * (brokerage + exch + sebi) + 0.00003 * buy_v + 2 * slip * lot
    )
    risk_rupees = (0.5 * sig["risk_pts"]) * lot + costs
    return (exit_prem - p0) * lot - costs, risk_rupees, p0


# --------------------------------------------------------------------------- report


def stats(rows, key):
    xs = [r[key] for r in rows]
    if not xs:
        return "no trades"
    wins = [x for x in xs if x > 0]
    losses = [x for x in xs if x <= 0]
    eq = peak = dd = 0.0
    streak = worst = 0
    for x in xs:
        eq += x
        peak = max(peak, eq)
        dd = min(dd, eq - peak)
        streak = streak + 1 if x <= 0 else 0
        worst = max(worst, streak)
    pf = sum(wins) / -sum(losses) if losses and sum(losses) < 0 else float("inf")
    return (
        f"n {len(xs):>4}  win {len(wins) / len(xs):>4.0%}  avg {sum(xs) / len(xs):>+8.2f}  "
        f"avgWin {sum(wins) / max(1, len(wins)):>+8.2f}  avgLoss {sum(losses) / max(1, len(losses)):>+8.2f}  "
        f"PF {pf:>5.2f}  total {sum(xs):>+10.1f}  maxDD {dd:>+9.1f}  worst losing streak {worst}"
    )


def run(data, vix, name, lot):
    rows = []
    for d, bars in data.items():
        if name == "NIFTY" and weekly_expiry(d):
            continue
        sig = signal(bars)
        if not sig:
            continue
        exit_price, exit_i, reason = manage(bars, sig)
        v = None
        for t, *ohlc in vix.get(d, []):
            if t <= bars[sig["entry_i"] - 1][0]:
                v = ohlc[3]
        row = {
            "day": d,
            "points": sig["side"] * (exit_price - sig["entry"]),
            "reason": reason,
            "R": sig["side"] * (exit_price - sig["entry"]) / sig["risk_pts"],
            "vix": v,
        }
        if v and name == "NIFTY":  # the expiry calendar below is NIFTY's weekly one
            for label, slip in SLIP.items():
                pnl, risk_rs, _p0 = option_trade(d, bars, sig, exit_price, exit_i, v, lot, slip)
                row[label] = pnl
                row["risk_rs"] = risk_rs
        rows.append(row)
    return rows


def report(name, rows, lot):
    print(f"\n=== {name}  (1 lot = {lot}; trades {len(rows)} on {len({r['day'] for r in rows})} days)")
    if not rows:
        return False
    print("  INDEX POINTS (no option model):  " + stats(rows, "points"))
    print("  R multiples:                     " + stats(rows, "R"))
    opt = [r for r in rows if "BASE" in r and r["risk_rs"] <= MAX_RISK_RUPEES]
    if name != "NIFTY":
        print("  OPTION P&L not modelled: BANKNIFTY has monthly expiries only (weekly ended Nov 2024).")
    skipped = sum(1 for r in rows if "BASE" in r and r["risk_rs"] > MAX_RISK_RUPEES)
    print(f"  OPTION Rs/lot, risk <= Rs {MAX_RISK_RUPEES:.0f} ({skipped} skipped as too risky; MODELLED):")
    for label in SLIP:
        print(f"    {label:<8} " + stats(opt, label))
    half = len(rows) // 2
    print("  Chronological halves (INDEX POINTS): early " + stats(rows[:half], "points"))
    print("                                       late  " + stats(rows[half:], "points"))
    by_year = defaultdict(list)
    for r in rows:
        by_year[r["day"].year].append(r)
    for y, rs in sorted(by_year.items()):
        print(f"  {y}: " + stats(rs, "points"))
    vixs = sorted(r["vix"] for r in rows if r["vix"])
    if vixs:
        lo_c, hi_c = vixs[len(vixs) // 3], vixs[2 * len(vixs) // 3]
        for lab, fn in (
            ("VIX low", lambda v: v < lo_c),
            ("VIX mid", lambda v: lo_c <= v < hi_c),
            ("VIX high", lambda v: v >= hi_c),
        ):
            print(f"  {lab:<9}: " + stats([r for r in rows if r["vix"] and fn(r["vix"])], "points"))
    exits = defaultdict(int)
    for r in rows:
        exits[r["reason"]] += 1
    print(f"  exits: {dict(exits)}")
    late = rows[half:]
    adv = [r["ADVERSE"] for r in opt]
    years = [sum(r["points"] for r in rs) for rs in by_year.values()]
    adv_ok = (sum(adv) > 0) if name == "NIFTY" else True  # BANKNIFTY: points only
    ok = (
        len(rows) >= 100
        and sum(r["points"] for r in late) > 0
        and adv_ok
        and sum(1 for y in years if y > 0) >= 0.75 * len(years)
    )
    print(
        f"  VERDICT: {'PASSES the pre-set bar' if ok else 'FAILS the pre-set bar'} "
        "(>= 100 trades, late half positive in points, modelled option P&L positive at ADVERSE "
        "slippage, >= 75% of years positive)"
    )
    return ok


def main() -> int:
    if (
        datetime.now(IST).weekday() < 5
        and time(9, 0) <= datetime.now(IST).time() < time(15, 40)
        and not os.getenv("RUN_ANYWAY")
    ):
        print("Market hours: this test runs only after the close.")
        return 1
    years = float(os.getenv("YEARS", "4"))
    sys.path.insert(0, os.getcwd())
    from dhan_api import DhanAPI
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    rt = build_runtime()
    try:
        settings = rt._settings_loader()
    except Exception as exc:
        print("Cannot get the token from the token authority:", redact(f"{type(exc).__name__}: {exc}"))
        return 1
    api = DhanAPI(settings)
    print(f"Downloading {years:g} years of 5-minute candles (RAM only): NIFTY, BANKNIFTY, INDIA VIX ...")
    vix = fetch(api, VIX_ID, years, say=lambda m: print(redact(m)))
    passed = {}
    for name, (sid, lot) in INDICES.items():
        data = fetch(api, sid, years, say=lambda m: print(redact(m)))
        if data:
            print(f"{name}: {len(data)} sessions {min(data)} .. {max(data)}")
        passed[name] = report(name, run(data, vix, name, lot), lot)
    print("\nRESULT:", ", ".join(f"{k} {'PASS' if v else 'FAIL'}" for k, v in passed.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
