"""Per-stock pattern scan: which intraday pattern does each liquid F&O stock (and index) repeat?

Read-only research, RAM only, no orders. Runs on a Live Core node with the token authority's token.

Universe: NIFTY, BANKNIFTY, SENSEX and the 99 F&O stocks ranked by traded value on 2026-10-12.
Data: 5-minute candles from Dhan, ~4 years. Each instrument is fetched, scanned and discarded
before the next one (no market data is stored).

A FIXED library of 7 patterns x 2 directions is tested on every instrument (rules written before
any result; nothing is tuned per stock):
  ORB        first close beyond the 09:15-09:25 range (by 11:00), stop at the other side
  ORB_TRAP   that breakout closes back inside within 3 candles -> fade it, stop at the extreme
  GAP_FADE   open gaps >= 1% from the previous close -> trade toward the previous close
  GAP_GO     gap >= 1% and the first candle closes in the gap direction -> go with the gap
  PDHL       first close beyond the previous day's high/low (09:30-13:00), stop at that candle
  HOUR1      by 10:15 the stock is >= 0.7% from the open -> continue, stop at the day's extreme
  LATE       by 14:15 the stock is >= 0.5% from the open -> continue to 15:15, no stop
Entry is the next candle's open; every trade exits at the 15:10 candle's close (15:15) or its stop.
Returns are % of the underlying, net of a round-trip cost (COST, default 0.10%).

Honest testing: the last 12 months are a HOLDOUT. A stock's pattern counts only if it is strong in
the earlier "discovery" years (n >= 20, mean > 0, t >= 2) AND still positive in the holdout.
With ~1,400 tests about 2-3% pass discovery by luck alone and half of those stay positive by luck,
so the report compares the holdout confirmation rate with that 50% coin-flip baseline.
"""

import csv
import io
import math
import os
import sys
import time as clock
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
BASE = "https://api.dhan.co/v2"
YEARS = float(os.getenv("YEARS", "4"))
COST = float(os.getenv("COST", "0.10"))
GAP_SPACING = 0.6  # seconds between data requests: keeps live PSYGRID's request budget free
HOLDOUT_DAYS = 365
LAST_EXIT = time(15, 10)

INDICES = {"NIFTY": ("13", "IDX_I", "INDEX"), "BANKNIFTY": ("25", "IDX_I", "INDEX"), "SENSEX": ("51", "IDX_I", "INDEX")}
TOP99 = [
    "DMART",
    "BSE",
    "HDFCBANK",
    "ICICIBANK",
    "RELIANCE",
    "BHARTIARTL",
    "TCS",
    "TECHM",
    "INFY",
    "TATASTEEL",
    "MCX",
    "SUZLON",
    "AXISBANK",
    "DELHIVERY",
    "IDEA",
    "APOLLOHOSP",
    "SBIN",
    "ATHERENERG",
    "INDUSINDBK",
    "SAIL",
    "POLYCAB",
    "M&M",
    "ETERNAL",
    "KOTAKBANK",
    "TATAELXSI",
    "BHEL",
    "BAJFINANCE",
    "ADANIPORTS",
    "TRENT",
    "NTPC",
    "ITC",
    "PAYTM",
    "VEDL",
    "ADANIPOWER",
    "ADANIENT",
    "TVSMOTOR",
    "HCLTECH",
    "PERSISTENT",
    "LAURUSLABS",
    "ANANDRATHI",
    "DIVISLAB",
    "POWERINDIA",
    "MARUTI",
    "DIXON",
    "ADANIGREEN",
    "ANGELONE",
    "SOLARINDS",
    "LT",
    "EICHERMOT",
    "COFORGE",
    "KAYNES",
    "MAZDOCK",
    "POLICYBZR",
    "INDIGO",
    "JIOFIN",
    "HINDZINC",
    "KALYANKJIL",
    "HINDALCO",
    "AUROPHARMA",
    "PNBHOUSING",
    "BEL",
    "PREMIERENE",
    "RECLTD",
    "TMPV",
    "BANDHANBNK",
    "TIINDIA",
    "ASHOKLEY",
    "HAL",
    "IRFC",
    "TORNTPHARM",
    "SUNPHARMA",
    "CGPOWER",
    "PFC",
    "RBLBANK",
    "HEROMOTOCO",
    "CHOLAFIN",
    "ENRIN",
    "TITAN",
    "MANAPPURAM",
    "UJJIVANSFB",
    "COCHINSHIP",
    "YESBANK",
    "HINDUNILVR",
    "INOXWIND",
    "ABB",
    "GMRAIRPORT",
    "KEI",
    "HDFCLIFE",
    "WAAREEENER",
    "VBL",
    "ONGC",
    "LICI",
    "SWIGGY",
    "LICHSGFIN",
    "DRREDDY",
    "CANBK",
    "LTM",
    "POWERGRID",
    "UPL",
]
PATTERNS = ("ORB", "ORB_TRAP", "GAP_FADE", "GAP_GO", "PDHL", "HOUR1", "LATE")


class RateLimited(RuntimeError):
    pass


# --------------------------------------------------------------------------- data


def fetch(session, headers, sid, segment, instrument, say):
    """{date: [(time, o, h, l, c)]} of 5-minute candles. Stops the whole scan on HTTP 429."""
    end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    cur = end - timedelta(days=int(365 * YEARS))
    days: dict = defaultdict(dict)
    fails = 0
    while cur < end:
        got = None
        for span in (89, 29):
            stop = min(cur + timedelta(days=span), end)
            payload = {
                "securityId": sid,
                "exchangeSegment": segment,
                "instrument": instrument,
                "interval": "5",
                "oi": False,
                "fromDate": cur.strftime("%Y-%m-%d %H:%M:%S"),
                "toDate": stop.strftime("%Y-%m-%d %H:%M:%S"),
            }
            clock.sleep(GAP_SPACING)
            try:
                r = session.post(BASE + "/charts/intraday", headers=headers, json=payload, timeout=40)
            except Exception as exc:
                say(f"    request error {type(exc).__name__}")
                continue
            if r.status_code == 429:
                raise RateLimited("Dhan HTTP 429: stopping so live PSYGRID keeps its request budget")
            if r.status_code == 200:
                got = r.json()
                break
            say(f"    HTTP {r.status_code} for {cur:%Y-%m-%d}+{span}d: {r.text[:120]}")
        if got is None:
            fails += 1
            if fails >= 3:
                say("    3 failed windows: skipping this instrument")
                return {}
        else:
            ts = got.get("timestamp") or []
            o, h, lo, c = (got.get(k) or [] for k in ("open", "high", "low", "close"))
            for i, t in enumerate(ts):
                when = datetime.fromtimestamp(int(t), IST)
                if time(9, 15) <= when.time() < time(15, 30) and i < len(c):
                    days[when.date()][when.time()] = (float(o[i]), float(h[i]), float(lo[i]), float(c[i]))
        cur = stop + timedelta(days=1)
    return {d: [(t, *v) for t, v in sorted(bars.items())] for d, bars in sorted(days.items())}


# --------------------------------------------------------------------------- patterns


def _exit(bars, entry_i, side, stop):
    """(exit price, exit index): stop first, else the 15:10 candle's close."""
    for j in range(entry_i, len(bars)):
        t, o, h, lo, c = bars[j]
        if stop is not None:
            if side > 0 and lo <= stop:
                return min(o, stop), j
            if side < 0 and h >= stop:
                return max(o, stop), j
        if t >= LAST_EXIT:
            return c, j
    return bars[-1][4], len(bars) - 1


def _at(bars, when):
    for i, b in enumerate(bars):
        if b[0] == when:
            return i
    return None


def signals(bars, prev):
    """[(pattern, side, entry_index, stop)] for one day; prev = (high, low, close) of the previous session."""
    out = []
    if len(bars) < 60 or bars[0][0] != time(9, 15):
        return out
    o0 = bars[0][1]
    orh = max(b[2] for b in bars[:3])
    orl = min(b[3] for b in bars[:3])
    # ORB and ORB_TRAP
    for i in range(3, len(bars) - 1):
        t, _o, _h, _l, c = bars[i]
        if t > time(11, 0):
            break
        side = 1 if c > orh else -1 if c < orl else 0
        if not side:
            continue
        out.append(("ORB", side, i + 1, orl if side > 0 else orh))
        for k in range(i + 1, min(i + 4, len(bars) - 1)):
            if bars[k][0] > time(14, 30):
                break
            if orl <= bars[k][4] <= orh:
                ext = max(b[2] for b in bars[i : k + 1]) if side > 0 else min(b[3] for b in bars[i : k + 1])
                out.append(("ORB_TRAP", -side, k + 1, ext))
                break
        break
    if prev:
        ph, pl, pc = prev
        gap = o0 / pc - 1
        if abs(gap) >= 0.01:
            g = 1 if gap > 0 else -1
            out.append(("GAP_FADE", -g, 1, bars[0][2] if g > 0 else bars[0][3]))
            if (bars[0][4] - bars[0][1]) * g > 0:
                out.append(("GAP_GO", g, 1, bars[0][3] if g > 0 else bars[0][2]))
        if pl < o0 < ph:
            for i in range(3, len(bars) - 1):
                t, _o, h, lo, c = bars[i]
                if t > time(13, 0):
                    break
                if c > ph:
                    out.append(("PDHL", 1, i + 1, lo))
                    break
                if c < pl:
                    out.append(("PDHL", -1, i + 1, h))
                    break
    i = _at(bars, time(10, 10))
    if i is not None and i + 1 < len(bars):
        move = bars[i][4] / o0 - 1
        if abs(move) >= 0.007:
            s = 1 if move > 0 else -1
            ext = min(b[3] for b in bars[: i + 1]) if s > 0 else max(b[2] for b in bars[: i + 1])
            out.append(("HOUR1", s, i + 1, ext))
    i = _at(bars, time(14, 10))
    if i is not None and i + 1 < len(bars):
        move = bars[i][4] / o0 - 1
        if abs(move) >= 0.005:
            out.append(("LATE", 1 if move > 0 else -1, i + 1, None))
    return out


def scan(data):
    """{(pattern, side): [(date, net %)]}"""
    res = defaultdict(list)
    prev = None
    for d, bars in data.items():
        for pat, side, ei, stop in signals(bars, prev):
            if ei >= len(bars) or bars[ei][0] >= LAST_EXIT:
                continue
            entry = bars[ei][1]
            px, _ = _exit(bars, ei, side, stop)
            res[(pat, side)].append((d, side * (px / entry - 1) * 100 - COST))
        if bars:
            prev = (max(b[2] for b in bars), min(b[3] for b in bars), bars[-1][4])
    return res


def stat(xs):
    n = len(xs)
    if not n:
        return 0, 0.0, 0.0, 0.0
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1)) if n > 1 else 0.0
    t = m / (sd / math.sqrt(n)) if sd > 0 else 0.0
    return n, 100 * sum(1 for x in xs if x > 0) / n, m, t


# --------------------------------------------------------------------------- main


def main() -> int:
    sys.path.insert(0, os.getcwd())
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    try:
        settings = build_runtime()._settings_loader()
    except Exception as exc:
        print("Cannot get the token from the token authority:", redact(f"{type(exc).__name__}: {exc}"))
        return 1
    headers = {"Accept": "application/json", "Content-Type": "application/json", "access-token": settings.access_token}
    session = requests.Session()

    def say(m):
        print(redact(str(m)), flush=True)

    text = requests.get(MASTER, timeout=60).text
    ids = {}
    for row in csv.DictReader(io.StringIO(text)):
        if (
            row.get("SEM_EXM_EXCH_ID", "").strip().upper() == "NSE"
            and row.get("SEM_INSTRUMENT_NAME", "").strip().upper() == "EQUITY"
        ):
            ids[row.get("SEM_TRADING_SYMBOL", "").strip().upper()] = row.get("SEM_SMST_SECURITY_ID", "").strip()
    del text
    universe = [(n, *v) for n, v in INDICES.items()] + [(s, ids.get(s), "NSE_EQ", "EQUITY") for s in TOP99]
    cut = date.today() - timedelta(days=HOLDOUT_DAYS)
    say(
        f"Pattern scan: {len(universe)} instruments, {YEARS:g} years of 5-minute candles, cost {COST}% round trip; "
        f"discovery < {cut}, holdout >= {cut}. RAM only, no orders."
    )

    results = {}  # name -> {(pat, side): (disc list, hold list)}
    coverage = {}
    stopped = None
    for k, (name, sid, seg, inst) in enumerate(universe, 1):
        if not sid:
            say(f"[{k}/{len(universe)}] {name}: no security id, skipped")
            continue
        try:
            data = fetch(session, headers, sid, seg, inst, say)
        except RateLimited as exc:
            stopped = str(exc)
            say(f"[{k}/{len(universe)}] {name}: {exc}")
            break
        if not data:
            say(f"[{k}/{len(universe)}] {name}: no data")
            continue
        coverage[name] = (min(data), max(data), len(data))
        per = {}
        for key, rows in scan(data).items():
            per[key] = ([x for d, x in rows if d < cut], [x for d, x in rows if d >= cut])
        results[name] = per
        del data
        say(f"[{k}/{len(universe)}] {name}: {coverage[name][2]} sessions {coverage[name][0]} .. {coverage[name][1]}")

    if not results:
        say("No results.")
        return 1
    if stopped:
        say(f"\nSTOPPED EARLY ({stopped}); results below cover {len(results)} instruments.")

    # pooled view per pattern
    say("\n== EACH PATTERN POOLED OVER ALL INSTRUMENTS (net % per trade) ==")
    say(
        f"{'pattern':<9}{'side':>5} | {'disc n':>7} {'win%':>5} {'avg':>7} {'t':>6} | {'hold n':>7} {'win%':>5} {'avg':>7} {'t':>6}"
    )
    for pat in PATTERNS:
        for side in (1, -1):
            d = [x for per in results.values() for x in per.get((pat, side), ([], []))[0]]
            h = [x for per in results.values() for x in per.get((pat, side), ([], []))[1]]
            dn, dw, dm, dt = stat(d)
            hn, hw, hm, ht = stat(h)
            lbl = "long" if side > 0 else "short"
            say(
                f"{pat:<9}{lbl:>5} | {dn:>7} {dw:>5.0f} {dm:>+7.3f} {dt:>+6.1f} | {hn:>7} {hw:>5.0f} {hm:>+7.3f} {ht:>+6.1f}"
            )

    # per instrument: discovery passes and their holdout result
    tests = passes = confirmed = 0
    found = []
    best = {}
    for name, per in results.items():
        for (pat, side), (d, h) in per.items():
            tests += 1
            dn, dw, dm, dt = stat(d)
            if dn >= 20 and (name not in best or dt > best[name][3]):
                best[name] = (pat, side, dn, dt, dm, dw, stat(h))
            if dn >= 20 and dm > 0 and dt >= 2.0:
                passes += 1
                hn, hw, hm, ht = stat(h)
                ok = hn >= 8 and hm > 0
                confirmed += ok
                found.append((name, pat, side, dn, dw, dm, dt, hn, hw, hm, ht, ok))

    say("\n== STOCK-SPECIFIC PATTERNS THAT PASSED DISCOVERY (n>=20, avg>0, t>=2) AND THEIR HOLDOUT ==")
    say(
        f"{'instrument':<12}{'pattern':<9}{'side':>6} | {'n':>4} {'win%':>5} {'avg':>7} {'t':>5} | {'hold n':>6} {'win%':>5} {'avg':>7} | holds?"
    )
    for r in sorted(found, key=lambda r: -r[6]):
        name, pat, side, dn, dw, dm, dt, hn, hw, hm, _ht, ok = r
        lbl = "long" if side > 0 else "short"
        say(
            f"{name:<12}{pat:<9}{lbl:>6} | {dn:>4} {dw:>5.0f} {dm:>+7.3f} {dt:>5.1f} | {hn:>6} {hw:>5.0f} {hm:>+7.3f} | "
            f"{'YES' if ok else 'no'}"
        )
    rate = 100 * confirmed / passes if passes else 0.0
    say(
        f"\nTests: {tests} (instrument x pattern x side). Passed discovery: {passes} "
        f"(~{0.023 * tests:.0f} expected by luck alone). Still positive in holdout: {confirmed}/{passes} = {rate:.0f}% "
        f"(coin-flip baseline ~50%)."
    )

    say("\n== EACH INSTRUMENT'S STRONGEST DISCOVERY PATTERN (n>=20) AND WHAT IT DID IN THE HOLDOUT ==")
    for name in results:
        if name not in best:
            say(f"{name:<12} no pattern with 20+ discovery trades")
            continue
        pat, side, dn, dt, dm, dw, (hn, hw, hm, _ht) = best[name]
        lbl = "long" if side > 0 else "short"
        say(
            f"{name:<12}{pat:<9}{lbl:>6}  disc n {dn:>4} win {dw:>3.0f}% avg {dm:>+6.3f}% t {dt:>+5.1f}  ->  "
            f"holdout n {hn:>3} win {hw:>3.0f}% avg {hm:>+6.3f}%"
        )
    say("\nResults are % moves of the underlying after a flat cost; option P&L is not modelled here. No data stored.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
