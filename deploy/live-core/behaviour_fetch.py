"""Stream historical intraday candles for the behaviour-discovery universe to stdout (read-only).

Runs on a Live Core node with the token authority's token (the token never leaves the node).
Writes a gzip stream in the format of research/behaviour/data.py to stdout and progress to stderr.
Nothing is written to disk on the node; no orders; the Live Core service is untouched.

Interval: 1-minute in 89-day requests when Dhan accepts it, otherwise 5-minute (decided once, on
the first instrument). Requests are spaced and the whole fetch stops on the first HTTP 429 so the
live PSYGRID keeps its request budget.

Span: START/END (YYYY-MM-DD, inclusive) when set, otherwise the last YEARS years. Each request
starts OVERLAP_DAYS before the first new day and rows before that day are dropped here, because Dhan
omits the 09:15 candle on the first day of a request (probes 38351078556 and 38351580582).

Safety (any trigger writes STOP <reason> and ends the stream cleanly; the analysis then makes no
verdict):
  - refuses to start, and stops, inside 08:45-15:45 IST on weekdays (the live service's market hours);
  - the first HTTP 429, MAX_REQUESTS requests, or MAX_CONSECUTIVE_FAILURES failed requests in a row;
  - node memory: MemAvailable below MIN_MEM_AVAILABLE_MB;
  - the live service: /health/node on this node unreachable, slower than HEALTH_MAX_SECONDS, in
    CONFIG_ERROR, or reporting a new hard reset, on two checks in a row (checked before each instrument).
Only the health status word, supervisor state and timings are printed, never the payload.

PREFLIGHT=1: makes no Dhan request (no token is read); prints the live service's health word, this
node's memory, CPUs and load, whether it is inside market hours, and the planned request count.
"""

import csv
import gzip
import os
import sys
import time as clock
from datetime import datetime, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo

import numpy as np
import requests

IST = ZoneInfo("Asia/Kolkata")
BASE = "https://api.dhan.co/v2"
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
YEARS = float(os.getenv("YEARS", "4"))
START, END = os.getenv("START", ""), os.getenv("END", "")
SPACING = 0.6
SPAN_DAYS = 89  # Dhan's per-request maximum for intraday candles
OVERLAP_DAYS = 4
MAX_REQUESTS = int(os.getenv("MAX_REQUESTS", "2600"))
MAX_CONSECUTIVE_FAILURES = 10
MIN_MEM_AVAILABLE_MB = 300
HEALTH_URL = os.getenv("HEALTH_URL", "http://127.0.0.1:10000/health/node")
HEALTH_MAX_SECONDS = 3.0
QUIET_WEEKDAY_HOURS = ("08:45", "15:45")  # IST; no fetching while the market service is busy

INDICES = [("NIFTY", "13"), ("BANKNIFTY", "25"), ("SENSEX", "51")]
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


class Stop(RuntimeError):
    """Any safety stop: the stream ends with STOP <reason>."""


class RateLimited(Stop):
    pass


def market_busy(now: datetime) -> bool:
    a, b = QUIET_WEEKDAY_HOURS
    return now.weekday() < 5 and a <= now.strftime("%H:%M") < b


def mem_available_mb() -> float | None:
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        return None
    return None


class HealthGuard:
    """Stops the fetch if the live service on this node looks stressed (two bad checks in a row)."""

    def __init__(self, say):
        self.say, self.bad, self.resets = say, 0, None

    def check(self) -> str:
        t = clock.time()
        problem, word = None, "?"
        try:
            r = requests.get(HEALTH_URL, timeout=HEALTH_MAX_SECONDS + 2)
            dt = clock.time() - t
            h = r.json() if r.status_code == 200 else {}
            word = str(h.get("status"))
            sup = h.get("supervisor") or {}
            resets = sup.get("hard_resets")
            if r.status_code != 200:
                problem = f"HTTP {r.status_code}"
            elif word == "CONFIG_ERROR":
                problem = "CONFIG_ERROR"
            elif dt > HEALTH_MAX_SECONDS:
                problem = f"slow ({dt:.1f}s)"
            elif self.resets is not None and resets is not None and resets > self.resets:
                problem = "hard reset during the fetch"
            if self.resets is None:
                self.resets = resets
            word = f"{word}/{sup.get('state')} {dt:.2f}s"
        except Exception as exc:
            problem = type(exc).__name__
        self.bad = self.bad + 1 if problem else 0
        if self.bad >= 2:
            raise Stop(f"live service health: {problem}")
        return word if not problem else f"{word} WARN {problem}"


def windows(start: datetime, end: datetime, interval: int = 1) -> list[tuple]:
    """Request plan: (first new day, last day, request from, request to) per request.

    New days tile [start, end] exactly once; each request starts OVERLAP_DAYS early (1-minute only)
    and spans at most SPAN_DAYS days, Dhan's per-request maximum."""
    out, cur = [], start.date()
    lead = timedelta(days=OVERLAP_DAYS if interval == 1 else 0)
    while cur <= end.date():
        stop = min(cur + timedelta(days=SPAN_DAYS - OVERLAP_DAYS), end.date())
        out.append(
            (cur, stop, datetime.combine(cur - lead, dtime(9, 15), IST), datetime.combine(stop, dtime(15, 30), IST))
        )
        cur = stop + timedelta(days=1)
    return out


def fetch_span() -> tuple[datetime, datetime]:
    if END:
        end = datetime.combine(datetime.strptime(END, "%Y-%m-%d").date(), dtime(15, 30), IST)
    else:
        end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    if START:
        start = datetime.combine(datetime.strptime(START, "%Y-%m-%d").date(), dtime(9, 15), IST)
    else:
        start = end - timedelta(days=int(365 * YEARS))
    return start, end


def preflight() -> int:
    start, end = fetch_span()
    guard = HealthGuard(lambda m: None)
    try:
        health = guard.check()
    except Stop as exc:
        health = f"FAIL {exc}"
    w = len(windows(start, end))
    n = len(INDICES) + len(TOP99)
    with open("/proc/loadavg") as fh:
        load = " ".join(fh.read().split()[:3])
    print(f"span {start:%Y-%m-%d} .. {end:%Y-%m-%d}: {w} requests per instrument x {n} instruments + 1 interval probe")
    print(
        f"planned requests {w * n + 1} (cap {MAX_REQUESTS}); spacing {SPACING}s -> at most {1 / SPACING:.2f} requests/s"
    )
    print(f"live service on this node: {health}")
    print(f"node: {os.cpu_count()} CPUs, load {load}, MemAvailable {mem_available_mb() or 0:.0f} MB")
    print(f"inside 08:45-15:45 IST weekday now: {market_busy(datetime.now(IST))} (the fetch refuses to run then)")
    print("preflight only: no Dhan request, no token read, nothing stored")
    return 0


def main() -> int:
    if os.getenv("PREFLIGHT") == "1":
        return preflight()
    sys.path.insert(0, os.getcwd())
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    def say(m):
        print(redact(str(m)), file=sys.stderr, flush=True)

    try:
        settings = build_runtime()._settings_loader()
    except Exception as exc:
        say("Cannot get the token from the token authority: " + redact(f"{type(exc).__name__}: {exc}"))
        return 1
    headers = {"Accept": "application/json", "Content-Type": "application/json", "access-token": settings.access_token}
    session = requests.Session()
    ids = {}
    # streamed line by line: the master is tens of MB and the fetch runs under a 150 MB cap
    with requests.get(MASTER, timeout=60, stream=True) as resp:
        lines = (line.decode("utf-8", "replace") for line in resp.iter_lines())
        for row in csv.DictReader(lines):
            if (
                row.get("SEM_EXM_EXCH_ID", "").strip() == "NSE"
                and row.get("SEM_INSTRUMENT_NAME", "").strip() == "EQUITY"
            ):
                ids[row.get("SEM_TRADING_SYMBOL", "").strip().upper()] = row.get("SEM_SMST_SECURITY_ID", "").strip()
    universe = [(n, sid, "IDX_I", "INDEX", "INDEX") for n, sid in INDICES]
    universe += [(s, ids.get(s), "NSE_EQ", "EQUITY", "-") for s in TOP99]

    if market_busy(datetime.now(IST)):
        say("refusing to start inside 08:45-15:45 IST on a weekday (live service hours)")
        return 2
    start, end = fetch_span()
    guard = HealthGuard(say)
    say(f"live service before the fetch: {guard.check()}; node MemAvailable {mem_available_mb():.0f} MB")
    count = {"requests": 0, "failed_in_a_row": 0}

    def request(sid, seg, inst, a, b, interval):
        payload = {
            "securityId": sid,
            "exchangeSegment": seg,
            "instrument": inst,
            "interval": str(interval),
            "oi": False,
            "fromDate": a.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": b.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if count["requests"] >= MAX_REQUESTS:
            raise Stop(f"request cap {MAX_REQUESTS} reached")
        clock.sleep(SPACING)
        count["requests"] += 1
        try:
            r = session.post(BASE + "/charts/intraday", headers=headers, json=payload, timeout=60)
        except requests.RequestException as exc:
            r = None
            err = type(exc).__name__
        if r is not None and r.status_code == 429:
            raise RateLimited("Dhan HTTP 429")
        if r is None or r.status_code != 200:
            count["failed_in_a_row"] += 1
            if count["failed_in_a_row"] >= MAX_CONSECUTIVE_FAILURES:
                raise Stop(f"{MAX_CONSECUTIVE_FAILURES} failed requests in a row")
            return None, err if r is None else f"HTTP {r.status_code}: {redact(r.text[:100])}"
        count["failed_in_a_row"] = 0
        j = r.json()
        ts = j.get("timestamp") or []
        cols = [j.get(k) or [] for k in ("open", "high", "low", "close", "volume")]
        n = min([len(ts)] + [len(c) for c in cols[:4]])
        vol = cols[4] if len(cols[4]) >= n else [0.0] * n
        rows = np.column_stack(
            [np.asarray(ts[:n], float)] + [np.asarray(c[:n], float) for c in cols[:4]] + [np.asarray(vol[:n], float)]
        )
        return rows, None

    # decide the interval once: 1-minute in 89-day windows if Dhan serves it, else 5-minute
    interval = 5
    probe, err = request("13", "IDX_I", "INDEX", end - timedelta(days=SPAN_DAYS), end, 1)
    if probe is not None and len(probe) > 1000:
        interval = 1
    say(
        f"interval: {interval}-minute in {SPAN_DAYS}-day requests (1-minute probe: {'ok' if interval == 1 else err or 'too few rows'})"
    )

    out = gzip.GzipFile(fileobj=sys.stdout.buffer, mode="wb", compresslevel=3)
    out.write(f"META interval={interval} years={YEARS:g} start={start:%Y-%m-%d} end={end:%Y-%m-%d}\n".encode())
    t0 = clock.time()
    name = "-"
    try:
        for k, (name, sid, seg, inst, sector) in enumerate(universe, 1):
            if market_busy(datetime.now(IST)):
                raise Stop("reached 08:45 IST on a weekday (live service hours)")
            mem = mem_available_mb()
            if mem is not None and mem < MIN_MEM_AVAILABLE_MB:
                raise Stop(f"node MemAvailable {mem:.0f} MB < {MIN_MEM_AVAILABLE_MB} MB")
            health = guard.check()
            out.write(f"INST {name} {inst} {sector}\n".encode())
            if not sid:
                out.write(f"DONE {name} NO_SECURITY_ID\n".encode())
                say(f"[{k}/{len(universe)}] {name}: no security id")
                continue
            rows_total, fails = 0, 0
            for cur, stop, frm, to in windows(start, end, interval):
                rows, err = request(sid, seg, inst, frm, to, interval)
                if rows is None:
                    rows, err = request(sid, seg, inst, frm, to, interval)  # one retry
                if rows is None:
                    fails += 1
                    say(f"    {name} {cur:%Y-%m-%d}: {err}")
                    continue
                # keep only the new days [cur, stop]; the lead-in only completes cur's first minute
                lo = datetime.combine(cur, dtime(0, 0), IST).timestamp()
                hi = datetime.combine(stop + timedelta(days=1), dtime(0, 0), IST).timestamp()
                rows = rows[(rows[:, 0] >= lo) & (rows[:, 0] < hi)]
                if len(rows):
                    out.write(f"ROWS {len(rows)}\n".encode())
                    out.write(np.ascontiguousarray(rows, dtype="<f8").tobytes())
                    rows_total += len(rows)
            status = "OK" if fails == 0 else f"PARTIAL_{fails}_WINDOWS_FAILED"
            out.write(f"DONE {name} {status}\n".encode())
            say(
                f"[{k}/{len(universe)}] {name}: {rows_total} rows, {status}, {count['requests']} requests, "
                f"{clock.time() - t0:.0f}s; live service {health}; node MemAvailable {mem_available_mb() or 0:.0f} MB"
            )
    except Stop as exc:
        out.write(f"DONE {name} STOPPED\nSTOP {str(exc).replace(chr(10), ' ')}\n".encode())
        say(f"STOPPED on {name}: {exc} (live PSYGRID keeps its request budget)")
    out.write(b"END\n")
    out.close()
    sys.stdout.buffer.flush()
    say(f"fetch finished: {count['requests']} requests; nothing stored on the node, no orders placed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
