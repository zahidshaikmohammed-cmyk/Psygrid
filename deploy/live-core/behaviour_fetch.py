"""Stream historical intraday candles for the behaviour-discovery universe to stdout (read-only).

Runs on a Live Core node with the token authority's token (the token never leaves the node).
Writes a gzip stream in the format of research/behaviour/data.py to stdout and progress to stderr.
Nothing is written to disk on the node; no orders; the Live Core service is untouched.

Interval: 1-minute in 89-day requests when Dhan accepts it, otherwise 5-minute (decided once, on
the first instrument). Requests are spaced and the whole fetch stops on the first HTTP 429 so the
live PSYGRID keeps its request budget.
"""

import csv
import gzip
import io
import os
import sys
import time as clock
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import requests

IST = ZoneInfo("Asia/Kolkata")
BASE = "https://api.dhan.co/v2"
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
YEARS = float(os.getenv("YEARS", "4"))
SPACING = 0.6

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


class RateLimited(RuntimeError):
    pass


def main() -> int:
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
    for row in csv.DictReader(io.StringIO(requests.get(MASTER, timeout=60).text)):
        if row.get("SEM_EXM_EXCH_ID", "").strip() == "NSE" and row.get("SEM_INSTRUMENT_NAME", "").strip() == "EQUITY":
            ids[row.get("SEM_TRADING_SYMBOL", "").strip().upper()] = row.get("SEM_SMST_SECURITY_ID", "").strip()
    universe = [(n, sid, "IDX_I", "INDEX", "INDEX") for n, sid in INDICES]
    universe += [(s, ids.get(s), "NSE_EQ", "EQUITY", "-") for s in TOP99]

    end = datetime.now(IST).replace(hour=15, minute=30, second=0, microsecond=0)
    start = end - timedelta(days=int(365 * YEARS))

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
        clock.sleep(SPACING)
        r = session.post(BASE + "/charts/intraday", headers=headers, json=payload, timeout=60)
        if r.status_code == 429:
            raise RateLimited("Dhan HTTP 429")
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}: {r.text[:100]}"
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
    interval, span = 5, 89
    probe, err = request("13", "IDX_I", "INDEX", end - timedelta(days=89), end, 1)
    if probe is not None and len(probe) > 1000:
        interval = 1
    say(
        f"interval: {interval}-minute in {span}-day windows (1-minute probe: {'ok' if interval == 1 else err or 'too few rows'})"
    )

    out = gzip.GzipFile(fileobj=sys.stdout.buffer, mode="wb", compresslevel=3)
    out.write(f"META interval={interval} years={YEARS:g} start={start:%Y-%m-%d} end={end:%Y-%m-%d}\n".encode())
    t0 = clock.time()
    try:
        for k, (name, sid, seg, inst, sector) in enumerate(universe, 1):
            out.write(f"INST {name} {inst} {sector}\n".encode())
            if not sid:
                out.write(f"DONE {name} NO_SECURITY_ID\n".encode())
                say(f"[{k}/{len(universe)}] {name}: no security id")
                continue
            cur, rows_total, fails = start, 0, 0
            while cur < end:
                stop = min(cur + timedelta(days=span), end)
                rows, err = request(sid, seg, inst, cur, stop, interval)
                if rows is None:
                    rows, err = request(sid, seg, inst, cur, stop, interval)  # one retry
                if rows is None:
                    fails += 1
                    say(f"    {name} {cur:%Y-%m-%d}: {err}")
                elif len(rows):
                    out.write(f"ROWS {len(rows)}\n".encode())
                    out.write(np.ascontiguousarray(rows, dtype="<f8").tobytes())
                    rows_total += len(rows)
                cur = stop + timedelta(days=1)
            status = "OK" if fails == 0 else f"PARTIAL_{fails}_WINDOWS_FAILED"
            out.write(f"DONE {name} {status}\n".encode())
            say(f"[{k}/{len(universe)}] {name}: {rows_total} rows, {status}, {clock.time() - t0:.0f}s")
    except RateLimited as exc:
        out.write(f"DONE {name} STOPPED\nSTOP {exc}\n".encode())
        say(f"STOPPED on {name}: {exc} (live PSYGRID keeps its request budget)")
    out.write(b"END\n")
    out.close()
    sys.stdout.buffer.flush()
    say("fetch finished; nothing stored on the node, no orders placed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
