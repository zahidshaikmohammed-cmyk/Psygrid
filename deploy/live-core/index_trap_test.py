"""NIFTY opening-range TRAP: one pre-registered test on multi-year Dhan index candles. Read-only.

Run by the deploy workflow's `index-test` action on Live Core node 0, after hours, with the node's
own environment: it takes the account's EXISTING token from the token authority (never mints one),
downloads 5-minute candles for NIFTY 50, BANKNIFTY and INDIA VIX from Dhan /charts/intraday into
RAM, prints the result and exits. Nothing is written to disk; no order is placed; the running
Live Core service is not touched.

DATA LIMITS (verified, not assumed): Dhan documents intraday OHLC for the last 5 years in 1/5/15/
25/60-minute candles. The per-request range is NOT documented officially (a third-party guide says
~90 days), so the script asks for 90-day pieces, falls back to 30-day pieces when a request fails,
and reports every request and the coverage it actually got.

THE RULES (fixed before any data was seen; nothing here is tuned):
  Range     OR = high/low of the 09:15, 09:20 and 09:25 five-minute candles. No trade when the OR is
            narrower than 0.15% or wider than 1.0% of the index.
  Breakout  The FIRST 5-minute close outside the OR, from the 09:30 candle on.
  Trap      Within the next 3 candles a candle closes back inside the OR (only the first breakout
            of the day counts).
  Entry     Open of the candle after the trap candle, at or before 14:30. Failed up-breakout ->
            SHORT the index (buy the ATM PUT); failed down-breakout -> LONG (buy the ATM CALL).
  Stop      Beyond the extreme since the breakout, plus 0.03% of the price.
  Target    The opposite side of the OR; no trade when it is less than 1R away.
  Time      Exit at the close 60 minutes after entry, or 15:15. Stop before target in one candle.
  Filters   NIFTY: no trade on its weekly expiry day. One trade per index per day.
  Size      1 lot; skip when the estimated option loss at the stop exceeds Rs 950 (5% of Rs 19,000).

CONTRACT FACTS (from NSE circulars as reported by brokers; transition windows are EXCLUDED from the
option estimate rather than guessed):
  NIFTY weekly expiry   Thursday for expiries up to 2025-08-31, Tuesday from 2025-09-01. When that
                        day is not a trading session in the data (holiday) the expiry is the
                        previous session.
  NIFTY lot size        50 -> 25 (contracts expiring from 2024-05-02) -> 75 (contracts listed from
                        2024-11-20; full from 2024-12-26) -> 65 (weeklies from 2026-01-06).
  Option charges        STT on the sold premium 0.0625% before 2024-10-01, 0.1% from it; NSE
                        transaction charge 0.0495% before 2024-10-01, 0.03503% from it; brokerage
                        Rs 20/order; SEBI Rs 10/crore; GST 18%; stamp 0.003% of the buy premium.
  Slippage              Rs 0.10 (BASE), 0.50 (ADVERSE), 1.00 (SEVERE) per unit per side.

OPTION P&L IS AN ESTIMATE, NOT OBSERVED: no historical option prices are used. Premium =
0.4 x S x VIX/100 x sqrt(T), delta 0.5, time decay = premium(T0) - premium(T1), gamma ignored.
The INDEX-POINTS result needs no option model and is the primary evidence.
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
INDICES = {"NIFTY": "13", "BANKNIFTY": "25"}  # Dhan IDX_I security ids (checked in index_layer.py)
VIX_ID = "21"
OR_BARS = 3
MAX_ENTRY = time(14, 30)
HOLD_BARS = 12
LAST_EXIT = time(15, 10)  # its close is 15:15
MIN_OR, MAX_OR = 0.15, 1.0
BUFFER = 0.0003
MAX_RISK_RUPEES = 950.0
SLIP = {"BASE": 0.10, "ADVERSE": 0.50, "SEVERE": 1.00}
HOLDOUT_DAYS = 365  # the last 12 months are reported on their own (rules are not tuned on them)
EXPIRY_SWITCH = date(2025, 9, 1)
# Windows where the lot size or expiry weekday of the traded contract is uncertain (excluded from the estimate).
UNCERTAIN = [
    (date(2024, 4, 25), date(2024, 5, 2)),
    (date(2024, 11, 20), date(2024, 12, 26)),
    (date(2025, 8, 25), date(2025, 9, 9)),
    (date(2025, 12, 23), date(2026, 1, 6)),
]


# --------------------------------------------------------------------------- data


def _rows(api, item, start, stop, say):
    try:
        rows = api.intraday(item, 5, start, stop)
        return rows, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {str(exc)[:120]}"


def fetch(api, security_id: str, years: float, say=print):
    """5-minute IDX_I candles -> ({date: [(time, o, h, l, c)]}, request log)."""
    item = SimpleNamespace(security_id=security_id, exchange_segment="IDX_I", instrument="INDEX")
    end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    start = end - timedelta(days=int(365 * years))
    days: dict = defaultdict(dict)
    log = []
    cur = start
    while cur < end:
        for span in (89, 29):
            stop = min(cur + timedelta(days=span), end)
            rows, err = _rows(api, item, cur, stop, say)
            if rows is not None:
                break
        log.append((cur.date(), stop.date(), span + 1, len(rows) if rows else 0, err if rows is None else None))
        for r in rows or []:
            ts = datetime.fromtimestamp(int(r["timestamp"]), IST)
            if time(9, 15) <= ts.time() < time(15, 30):
                days[ts.date()][ts.time()] = (r["open"], r["high"], r["low"], r["close"])
        cur = stop + timedelta(days=1)
    return {d: [(t, *v) for t, v in sorted(bars.items())] for d, bars in sorted(days.items())}, log


def coverage(name, data, log, say=print):
    fails = [x for x in log if x[4]]
    say(
        f"{name}: {len(log)} requests ({sum(1 for x in log if x[2] == 90)} x 90-day, "
        f"{sum(1 for x in log if x[2] == 30)} x 30-day), {len(fails)} failed"
    )
    for a, b, _span, _n, err in fails[:5]:
        say(f"  FAILED {a}..{b}: {err}")
    if not data:
        say("  NO DATA returned")
        return
    full = sum(1 for bars in data.values() if len(bars) >= 75)
    by_year = defaultdict(int)
    for d in data:
        by_year[d.year] += 1
    say(
        f"  sessions {len(data)} from {min(data)} to {max(data)}; {full} with all 75 candles; "
        f"per year {dict(sorted(by_year.items()))}"
    )


# --------------------------------------------------------------------------- contract calendar


def expiry_for(d: date, sessions: set) -> date:
    """NIFTY weekly expiry on/after d: Thursday up to 2025-08-31, Tuesday after; a holiday moves it
    to the previous trading session (holidays are read from the sessions in the data)."""
    wd = 3 if d < EXPIRY_SWITCH else 1
    exp = d + timedelta(days=(wd - d.weekday()) % 7)
    if exp >= EXPIRY_SWITCH > d:  # crossing the switch: the first Tuesday on/after the switch
        exp = EXPIRY_SWITCH + timedelta(days=(1 - EXPIRY_SWITCH.weekday()) % 7)
    probe = exp
    while probe not in sessions and probe > d and sessions and probe <= max(sessions):
        probe -= timedelta(days=1)
    return probe if probe >= d else exp


def lot_size(expiry: date) -> int:
    if expiry < date(2024, 5, 2):
        return 50
    if expiry < date(2024, 12, 26):
        return 25 if expiry < date(2024, 11, 21) else 75
    if expiry < date(2026, 1, 6):
        return 75
    return 65


def uncertain(d: date) -> bool:
    return any(a <= d <= b for a, b in UNCERTAIN)


def charges(d: date, buy_v: float, sell_v: float, slip: float, lot: int) -> float:
    stt, exch = (0.000625, 0.000495) if d < date(2024, 10, 1) else (0.001, 0.0003503)
    brokerage = 40.0
    ex = exch * (buy_v + sell_v)
    sebi = 0.000001 * (buy_v + sell_v)
    return brokerage + stt * sell_v + ex + sebi + 0.18 * (brokerage + ex + sebi) + 0.00003 * buy_v + 2 * slip * lot


# --------------------------------------------------------------------------- rules


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


# --------------------------------------------------------------------------- option estimate


def premium(spot: float, vix: float, years: float) -> float:
    return 0.4 * spot * vix / 100 * math.sqrt(max(years, 1 / 365 / 6.25))


def option_trade(d: date, bars, sig, exit_price, exit_i, vix: float, expiry: date, lot: int, slip: float):
    """ESTIMATED rupee P&L for 1 lot of the ATM option bought in the trade's direction."""
    t0 = datetime.combine(d, bars[sig["entry_i"]][0], IST)
    t1 = datetime.combine(d, bars[exit_i][0], IST) + timedelta(minutes=5)
    exp = datetime.combine(expiry, time(15, 30), IST)

    def yrs(t):
        return max((exp - t).total_seconds(), 0) / (365 * 24 * 3600)

    p0 = premium(sig["entry"], vix, yrs(t0))
    p1 = premium(sig["entry"], vix, yrs(t1))
    unit = 0.5 * sig["side"] * (exit_price - sig["entry"]) - (p0 - p1)
    exit_prem = max(p0 + unit, 0.05)
    costs = charges(d, p0 * lot, exit_prem * lot, slip, lot)
    stop_unit = 0.5 * sig["risk_pts"] + (p0 - p1)
    risk_rupees = stop_unit * lot + charges(d, p0 * lot, max(p0 - stop_unit, 0.05) * lot, slip, lot)
    return (exit_prem - p0) * lot - costs, risk_rupees, p0


# --------------------------------------------------------------------------- report


def stats(rows, key):
    xs = [r[key] for r in rows if r.get(key) is not None]
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
        f"PF {pf:>5.2f}  total {sum(xs):>+10.1f}  maxDD {dd:>+9.1f}  worst losing run {worst}"
    )


def run(data, vix, name):
    sessions = set(data)
    rows, skipped = [], defaultdict(int)
    for d, bars in data.items():
        expiry = expiry_for(d, sessions) if name == "NIFTY" else None
        if name == "NIFTY" and expiry == d:
            skipped["expiry day"] += 1
            continue
        sig = signal(bars)
        if not sig:
            continue
        exit_price, exit_i, reason = manage(bars, sig)
        v = None
        for t, *ohlc in vix.get(d, []):
            if t <= bars[sig["entry_i"] - 1][0]:  # VIX known at the signal candle's close
                v = ohlc[3]
        pts = sig["side"] * (exit_price - sig["entry"])
        row = {"day": d, "points": pts, "R": pts / sig["risk_pts"], "reason": reason, "vix": v, "risk_rs": None}
        if name == "NIFTY":
            if not v:
                skipped["no VIX at signal (option estimate skipped)"] += 1
            elif uncertain(d) or uncertain(expiry):
                skipped["lot/expiry transition window (option estimate skipped)"] += 1
            else:
                lot = lot_size(expiry)
                for label, slip in SLIP.items():
                    pnl, risk_rs, _p0 = option_trade(d, bars, sig, exit_price, exit_i, v, expiry, lot, slip)
                    row[label] = pnl
                    row["risk_rs"] = risk_rs
                row["lot"] = lot
        rows.append(row)
    return rows, skipped


def report(name, rows, skipped):
    print(f"\n{'=' * 100}\n{name}: {len(rows)} signals" + (f"; skipped {dict(skipped)}" if skipped else ""))
    if not rows:
        return
    cut = rows[-1]["day"] - timedelta(days=HOLDOUT_DAYS)
    early, late = [r for r in rows if r["day"] <= cut], [r for r in rows if r["day"] > cut]
    print("A. INDEX POINTS per trade, gross (verified from Dhan candles; no option model)")
    print(f"   ALL                    {stats(rows, 'points')}")
    print(f"   up to {cut}       {stats(early, 'points')}")
    print(f"   LAST 12 MONTHS        {stats(late, 'points')}")
    by_year = defaultdict(list)
    for r in rows:
        by_year[r["day"].year].append(r)
    for y, rs in sorted(by_year.items()):
        print(f"   {y}                   {stats(rs, 'points')}")
    vixs = sorted(r["vix"] for r in rows if r["vix"])
    if vixs:
        lo_c, hi_c = vixs[len(vixs) // 3], vixs[2 * len(vixs) // 3]
        for lab, fn in (
            ("VIX low ", lambda v: v < lo_c),
            ("VIX mid ", lambda v: lo_c <= v < hi_c),
            ("VIX high", lambda v: v >= hi_c),
        ):
            print(f"   {lab}               {stats([r for r in rows if r['vix'] and fn(r['vix'])], 'points')}")
    print(f"   R multiples (ALL)      {stats(rows, 'R')}")
    exits = defaultdict(int)
    for r in rows:
        exits[r["reason"]] += 1
    print(f"   exits {dict(exits)}")
    if name != "NIFTY":
        print("B. OPTION ESTIMATE not produced: BANKNIFTY has monthly expiries only since Nov 2024.")
        return
    est = [r for r in rows if r.get("BASE") is not None]
    ok = [r for r in est if r["risk_rs"] <= MAX_RISK_RUPEES]
    print(
        f"B. ESTIMATED OPTION P&L, Rs per 1 lot -- AN ESTIMATE, NOT VERIFIED PROFITABILITY "
        f"({len(est)} estimable; {len(est) - len(ok)} skipped: risk above Rs {MAX_RISK_RUPEES:.0f})"
    )
    for label in SLIP:
        print(f"   {label:<8} every trade, 1 lot      {stats(est, label)}")
        print(f"   {label:<8} within Rs {MAX_RISK_RUPEES:.0f} cap       {stats(ok, label)}")
        print(f"   {label:<8} cap, LAST 12 MONTHS     {stats([r for r in ok if r['day'] > cut], label)}")
    risks = sorted(r["risk_rs"] for r in est)
    if risks:
        print(
            f"   Estimated loss at the stop per lot: median Rs {risks[len(risks) // 2]:,.0f}, "
            f"{sum(1 for x in risks if x <= MAX_RISK_RUPEES)} of {len(risks)} within Rs {MAX_RISK_RUPEES:.0f}"
        )


def main() -> int:
    now = datetime.now(IST)
    if now.weekday() < 5 and time(9, 0) <= now.time() < time(15, 40) and not os.getenv("RUN_ANYWAY"):
        print("Market hours: this test runs only after the close.")
        return 1
    years = float(os.getenv("YEARS", "4"))
    sys.path.insert(0, os.getcwd())
    from dhan_api import DhanAPI
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    try:
        settings = build_runtime()._settings_loader()
    except Exception as exc:
        print("Cannot get the token from the token authority:", redact(f"{type(exc).__name__}: {exc}"))
        return 1
    api = DhanAPI(settings)

    def say(m):
        print(redact(m), flush=True)

    say(f"Requesting {years:g} years of 5-minute candles (RAM only): INDIA VIX, NIFTY, BANKNIFTY")
    vix, vlog = fetch(api, VIX_ID, years, say)
    coverage("INDIA VIX", vix, vlog, say)
    for name, sid in INDICES.items():
        data, log = fetch(api, sid, years, say)
        coverage(name, data, log, say)
        rows, skipped = run(data, vix, name)
        report(name, rows, skipped)
    print("\nNo thresholds are applied here: these are results for review, not a trading recommendation.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
