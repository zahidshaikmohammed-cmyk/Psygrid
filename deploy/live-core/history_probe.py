"""ONE read-only Dhan request: is 1-minute data available for the start of the confirmation year?

Requests RELIANCE (NSE_EQ) 1-minute candles for 2021-10-01 .. 2021-12-28 (89 days, Dhan's per-request
maximum) and prints the HTTP status, any error body, and the coverage of what came back. Runs on a
Live Core node with the token authority's token; prints nothing secret; stores nothing; no orders.
"""

import csv
import io
import os
import sys
from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
SYMBOL, FROM, TO = "RELIANCE", "2021-10-01 09:15:00", "2021-12-28 15:30:00"


def main() -> int:
    sys.path.insert(0, os.getcwd())
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    settings = build_runtime()._settings_loader()
    sid = None
    for row in csv.DictReader(
        io.StringIO(requests.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=60).text)
    ):
        if (
            row.get("SEM_EXM_EXCH_ID") == "NSE"
            and row.get("SEM_INSTRUMENT_NAME") == "EQUITY"
            and row.get("SEM_TRADING_SYMBOL") == SYMBOL
        ):
            sid = row["SEM_SMST_SECURITY_ID"].strip()
            break
    payload = {
        "securityId": sid,
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "interval": "1",
        "oi": False,
        "fromDate": FROM,
        "toDate": TO,
    }
    r = requests.post(
        "https://api.dhan.co/v2/charts/intraday",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": settings.access_token,
        },
        json=payload,
        timeout=60,
    )
    print(f"request: {SYMBOL} (securityId {sid}) 1-minute {FROM} .. {TO}")
    print(f"HTTP {r.status_code}")
    if r.status_code != 200:
        print("body:", redact(r.text[:500]))
        return 0
    j = r.json()
    ts = j.get("timestamp") or []
    print(f"rows: {len(ts)}; fields: {sorted(j)}")
    if ts:
        t = [datetime.fromtimestamp(int(x), IST) for x in ts]
        per = Counter(x.date() for x in t)
        counts = sorted(per.values())
        print(f"first candle: {t[0]:%Y-%m-%d %H:%M}  last candle: {t[-1]:%Y-%m-%d %H:%M}")
        print(
            f"sessions: {len(per)}; minutes per session min/median/max: {counts[0]}/{counts[len(counts) // 2]}/{counts[-1]}"
        )
        print(
            f"first session's first/last minute: {min(x for x in t if x.date() == t[0].date()):%H:%M} / {max(x for x in t if x.date() == t[0].date()):%H:%M}"
        )
        print(f"sessions with >= 356 of 375 minutes: {sum(1 for v in per.values() if v >= 356)}")
        vol = j.get("volume") or []
        print(f"volume present: {bool(vol) and any(v for v in vol)}")
    print("one request made; nothing stored; no orders")
    return 0


if __name__ == "__main__":
    sys.exit(main())
