"""Open-ended per-stock pattern mining: what does each stock repeat, by itself, in its own data?

Read-only research, RAM only, no orders. Runs on a Live Core node with the token authority's token.

No pattern is chosen in advance. Each instrument's 4 years of 5-minute candles are rolled into
15-minute bars (25 per day) and EVERY context below is measured for that stock on its own:
  SLOT      each 15-minute slot of the day (does this stock usually rise/fall at 11:15?)
  DOW       each weekday, open -> 15:15
  DOWSLOT   weekday x 15-minute slot
  MOTIF     every 3-bar candle sequence (5 candle types: big down, down, flat, up, big up, scaled to
            the stock's own typical 15-minute move) x part of the day -> next 30 minutes
  OPEN      overnight gap bucket x first 15-minute candle -> rest of the day
  HOUR1     gap bucket x first-hour move bucket x first-hour range bucket -> rest of the day
That is ~650 contexts per stock. For each, the direction (long/short) is whatever the stock did
more often in the DISCOVERY years; one trade per context per day; entry at the next bar's open.
Returns are % of the underlying net of a round-trip cost (COST, default 0.10%).

Because ~65,000 contexts are searched, many will look strong by luck. A context is reported as a
stock's pattern only if it is strong in discovery (n >= 30, net mean > 0, t >= 3) AND we then show
exactly what it did in the last 12 months (HOLDOUT), which the search never saw.
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
SPACING = 0.6
HOLDOUT_DAYS = 365
N_MIN, T_MIN = 30, 3.0
SLOTS = 25  # 15-minute bars 09:15 .. 15:15; bar 23 closes at 15:15, bar 24 (15:15-15:30) is never traded
LAST = 23
DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri")

INDICES = {"NIFTY": ("13", "IDX_I", "INDEX"), "BANKNIFTY": ("25", "IDX_I", "INDEX"), "SENSEX": ("51", "IDX_I", "INDEX")}
TOP99 = [
    "ADANIPORTS",
    "RELIANCE",
    "HDFCBANK",
    "ICICIBANK",
    "BSE",
    "DMART",
    "SBIN",
    "INFY",
    "BHARTIARTL",
    "APOLLOHOSP",
    "KOTAKBANK",
    "TCS",
    "AXISBANK",
    "TECHM",
    "MCX",
    "SUZLON",
    "M&M",
    "ATHERENERG",
    "BAJFINANCE",
    "SAIL",
    "TATASTEEL",
    "DELHIVERY",
    "INDUSINDBK",
    "LT",
    "IDEA",
    "COALINDIA",
    "NTPC",
    "ETERNAL",
    "ITC",
    "POLYCAB",
    "INDIGO",
    "PAYTM",
    "MARUTI",
    "BHEL",
    "DIVISLAB",
    "TITAN",
    "BEL",
    "POLICYBZR",
    "HCLTECH",
    "ADANIENT",
    "LAURUSLABS",
    "KALYANKJIL",
    "OFSS",
    "VEDL",
    "TVSMOTOR",
    "HINDUNILVR",
    "EICHERMOT",
    "TMPV",
    "TATAELXSI",
    "SOLARINDS",
    "HAL",
    "ADANIPOWER",
    "ULTRACEMCO",
    "COFORGE",
    "HINDALCO",
    "PFC",
    "DIXON",
    "INDUSTOWER",
    "PNBHOUSING",
    "BHARATFORG",
    "DRREDDY",
    "KAYNES",
    "RECLTD",
    "ONGC",
    "AUROPHARMA",
    "SUNPHARMA",
    "POWERINDIA",
    "LICHSGFIN",
    "ANGELONE",
    "TORNTPHARM",
    "ADANIGREEN",
    "ANANDRATHI",
    "PERSISTENT",
    "TRENT",
    "CHOLAFIN",
    "SHRIRAMFIN",
    "ASHOKLEY",
    "NESTLEIND",
    "MAXHEALTH",
    "VBL",
    "HDFCAMC",
    "HINDPETRO",
    "FEDERALBNK",
    "JIOFIN",
    "SONACOMS",
    "GMRAIRPORT",
    "MANAPPURAM",
    "GLENMARK",
    "HEROMOTOCO",
    "UPL",
    "POWERGRID",
    "INDHOTEL",
    "TIINDIA",
    "GAIL",
    "DLF",
    "RBLBANK",
    "ENRIN",
    "ABB",
    "LICI",
]


class RateLimited(RuntimeError):
    pass


# --------------------------------------------------------------------------- data


def fetch(session, headers, sid, segment, instrument, say):
    """{date: [(o, h, l, c)] x 25} of 15-minute bars built from Dhan 5-minute candles."""
    end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    cur = end - timedelta(days=int(365 * YEARS))
    raw: dict = defaultdict(dict)
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
            clock.sleep(SPACING)
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
            say(f"    HTTP {r.status_code}: {r.text[:120]}")
        if got is None:
            fails += 1
            if fails >= 3:
                return {}
        else:
            ts = got.get("timestamp") or []
            o, h, lo, c = (got.get(k) or [] for k in ("open", "high", "low", "close"))
            for i, t in enumerate(ts):
                w = datetime.fromtimestamp(int(t), IST)
                if time(9, 15) <= w.time() < time(15, 30) and i < len(c):
                    raw[w.date()][w.time()] = (float(o[i]), float(h[i]), float(lo[i]), float(c[i]))
        cur = stop + timedelta(days=1)
    out = {}
    for d in sorted(raw):
        buckets = defaultdict(list)
        for t, v in sorted(raw[d].items()):
            buckets[((t.hour * 60 + t.minute) - 555) // 15].append(v)
        if any(k not in buckets for k in range(LAST + 1)):
            continue  # incomplete day
        out[d] = [
            (b[0][0], max(x[1] for x in b), min(x[2] for x in b), b[-1][3])
            for b in (buckets[k] for k in range(LAST + 1))
        ]
    return out


# --------------------------------------------------------------------------- contexts


def slot_name(k):
    m = 555 + 15 * k
    return f"{m // 60:02d}:{m % 60:02d}"


def bucket(x, edges):
    i = 0
    while i < len(edges) and x >= edges[i]:
        i += 1
    return i


def sym(r, s):
    return "D2" if r < -1.5 * s else "D" if r < -0.5 * s else "U2" if r > 1.5 * s else "U" if r > 0.5 * s else "F"


def part(k):
    return "morning" if k < 7 else "midday" if k < 17 else "afternoon"


GAP_NAMES = ("gap<=-1%", "gap -1..-0.3%", "flat open", "gap +0.3..1%", "gap>=+1%")


def events(data, sigma):
    """{context: [(date, gross % in the long direction)]}, one entry per context per day."""
    ev = defaultdict(list)
    prev_close = None
    for d, bars in data.items():
        seen = set()

        def add(key, entry, exit_, d=d, seen=seen):
            if key not in seen and entry > 0:
                seen.add(key)
                ev[key].append((d, (exit_ / entry - 1) * 100))

        dow = DAYS[d.weekday()] if d.weekday() < 5 else None
        rets = [c / o - 1 if o else 0.0 for o, _h, _l, c in bars]
        for k in range(LAST + 1):
            add(("SLOT", slot_name(k)), bars[k][0], bars[k][3])
            if dow:
                add(("DOWSLOT", f"{dow} {slot_name(k)}"), bars[k][0], bars[k][3])
        if dow:
            add(("DOW", dow), bars[0][0], bars[LAST][3])
        syms = [sym(r * 100, sigma) for r in rets]
        for k in range(2, LAST - 1):
            add(("MOTIF", f"{'-'.join(syms[k - 2 : k + 1])} {part(k)}"), bars[k + 1][0], bars[min(k + 2, LAST)][3])
        if prev_close:
            gap = (bars[0][0] / prev_close - 1) * 100
            g = GAP_NAMES[bucket(gap, (-1.0, -0.3, 0.3, 1.0))]
            add(("OPEN", f"{g}, first 15m {syms[0]}"), bars[1][0], bars[LAST][3])
            move = (bars[3][3] / bars[0][0] - 1) * 100
            rng = (max(b[1] for b in bars[:4]) / min(b[2] for b in bars[:4]) - 1) * 100
            mv = ("hour1 down big", "hour1 down", "hour1 flat", "hour1 up", "hour1 up big")[
                bucket(move, (-2 * sigma, -sigma, sigma, 2 * sigma))
            ]
            rg = ("narrow", "normal", "wide")[bucket(rng, (2.5 * sigma, 5 * sigma))]
            add(("HOUR1", f"{g}, {mv}, range {rg}"), bars[4][0], bars[LAST][3])
        prev_close = bars[-1][3]
    return ev


def stat(xs):
    n = len(xs)
    if n < 2:
        return n, 0.0, 0.0, 0.0
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return n, 100 * sum(1 for x in xs if x > 0) / n, m, (m / (sd / math.sqrt(n)) if sd > 0 else 0.0)


def describe(kind, key, side):
    s = "LONG" if side > 0 else "SHORT"
    if kind == "SLOT":
        return f"{s} the {key} 15-min candle (every day)"
    if kind == "DOWSLOT":
        return f"{s} the {key} 15-min candle"
    if kind == "DOW":
        return f"{s} every {key}, open -> 15:15"
    if kind == "MOTIF":
        return f"after candles {key}: {s} for 30 min"
    if kind == "OPEN":
        return f"{key}: {s} from 09:30 to 15:15"
    return f"{key}: {s} from 10:15 to 15:15"


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
        f"Open-ended pattern mining: {len(universe)} instruments, {YEARS:g} years, 15-minute bars, cost {COST}% "
        f"round trip; discovery < {cut} <= holdout. Selection: n>={N_MIN}, net mean>0, t>={T_MIN}. RAM only, no orders."
    )

    tests = 0
    found = []  # (name, kind, key, side, disc stats, hold stats)
    per_stock_best = {}
    stopped = None
    for i, (name, sid, seg, inst) in enumerate(universe, 1):
        if not sid:
            say(f"[{i}/{len(universe)}] {name}: no security id")
            continue
        try:
            data = fetch(session, headers, sid, seg, inst, say)
        except RateLimited as exc:
            stopped = str(exc)
            say(f"[{i}/{len(universe)}] {name}: {exc}")
            break
        disc_days = [d for d in data if d < cut]
        if len(disc_days) < 150:
            say(f"[{i}/{len(universe)}] {name}: only {len(disc_days)} discovery days, skipped")
            continue
        moves = sorted(abs(c / o - 1) * 100 for d in disc_days for o, _h, _l, c in data[d] if o)
        sigma = moves[len(moves) // 2] or 0.05
        n_ctx = 0
        for (kind, key), rows in events(data, sigma).items():
            d_raw = [x for d, x in rows if d < cut]
            if len(d_raw) < N_MIN:
                continue
            n_ctx += 1
            tests += 1
            side = 1 if sum(d_raw) >= 0 else -1
            disc = stat([side * x - COST for x in d_raw])
            hold = stat([side * x - COST for d, x in rows if d >= cut])
            gross_h = stat([side * x for d, x in rows if d >= cut])
            if name not in per_stock_best or disc[3] > per_stock_best[name][4][3]:
                per_stock_best[name] = (kind, key, side, None, disc, hold, gross_h)
            if disc[2] > 0 and disc[3] >= T_MIN:
                found.append((name, kind, key, side, disc, hold, gross_h))
        say(
            f"[{i}/{len(universe)}] {name}: {len(data)} days ({min(data)}..{max(data)}), "
            f"typical 15m move {sigma:.2f}%, {n_ctx} contexts tested"
        )
        del data

    if stopped:
        say(f"\nSTOPPED EARLY: {stopped}")
    say(f"\nContexts tested: {tests}. Strong in discovery (t>={T_MIN}, net>0): {len(found)}.")
    say(f"Up to {0.0027 * tests:.0f} would reach t>={T_MIN} in the profitable direction by luck alone.")

    if found:
        ok = [f for f in found if f[5][0] >= 10 and f[5][2] > 0]
        held_n = [f for f in found if f[5][0] >= 10]
        rate = 100 * len(ok) / len(held_n) if held_n else 0.0
        pooled = stat([f[5][2] for f in held_n])
        say(
            f"Of those with 10+ holdout trades ({len(held_n)}): still profitable after costs in the last 12 months: "
            f"{len(ok)} = {rate:.0f}% (coin flip ~50%). Mean holdout net per trade across them: {pooled[2]:+.3f}%."
        )
        say("\n== EVERY PATTERN THE DATA PRODUCED (discovery strong), WITH ITS LAST-12-MONTH RESULT ==")
        say(
            f"{'instrument':<12} {'pattern':<62} | {'n':>4} {'win%':>4} {'net':>7} {'t':>4} | "
            f"{'hold n':>6} {'win%':>4} {'net':>7} {'t':>5} | real?"
        )
        for name, kind, key, side, disc, hold, _g in sorted(found, key=lambda f: (-f[5][3], -f[4][3])):
            verdict = "YES" if hold[0] >= 10 and hold[2] > 0 and hold[3] >= 2 else "weak" if hold[2] > 0 else "no"
            say(
                f"{name:<12} {describe(kind, key, side)[:62]:<62} | {disc[0]:>4} {disc[1]:>4.0f} {disc[2]:>+7.3f} "
                f"{disc[3]:>4.1f} | {hold[0]:>6} {hold[1]:>4.0f} {hold[2]:>+7.3f} {hold[3]:>+5.1f} | {verdict}"
            )
    say("\n== EACH INSTRUMENT'S SINGLE STRONGEST DISCOVERY PATTERN -> LAST 12 MONTHS ==")
    for name, (kind, key, side, _x, disc, hold, gross_h) in per_stock_best.items():
        say(
            f"{name:<12} {describe(kind, key, side)[:62]:<62} disc n {disc[0]:>4} win {disc[1]:>3.0f}% "
            f"net {disc[2]:>+6.3f}% t {disc[3]:>+4.1f} -> hold n {hold[0]:>3} win {hold[1]:>3.0f}% "
            f"net {hold[2]:>+6.3f}% (gross {gross_h[2]:>+6.3f}%)"
        )
    say("\n'real?' YES = still profitable after costs in the unseen last 12 months with t>=2. No data stored.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
