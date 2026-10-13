"""ONE read-only Dhan request: which timestamp convention do 1-minute candles use, and does it change?

Requests RELIANCE (NSE_EQ) 1-minute candles for 2022-09-01 .. 2022-11-28 (89 days, Dhan's per-request
maximum), a window that crosses 2022-10-13 (the first session of the data already processed), and
prints, per session, the first and last minute label and how many minutes fall before 09:15,
inside 09:15-15:29 and at or after 15:30. Timestamps and counts only: no prices are printed.
(The previous probe, run 38351078556, covered 2021-10-01 .. 2021-12-28.) Runs on a
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
SYMBOL, FROM, TO = "RELIANCE", "2022-09-01 09:15:00", "2022-11-28 15:30:00"


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
        print("session     first last  n    <09:15 09:15-15:29 >=15:30  has_09:15 has_15:29")
        layouts = {}
        for day in sorted(per):
            ms = sorted(x.hour * 60 + x.minute for x in t if x.date() == day)
            pre = sum(1 for m in ms if m < 555)
            core = sum(1 for m in ms if 555 <= m < 930)
            post = sum(1 for m in ms if m >= 930)
            f, la = f"{ms[0] // 60:02d}:{ms[0] % 60:02d}", f"{ms[-1] // 60:02d}:{ms[-1] % 60:02d}"
            print(f"{day}  {f} {la} {len(ms):4d} {pre:6d} {core:11d} {post:8d}  {555 in ms!s:9} {929 in ms!s}")
            layouts.setdefault((f, la, 555 in ms, 929 in ms), []).append(day)
        print("layouts (first, last, has 09:15, has 15:29) -> session range:")
        for k, v in layouts.items():
            print(f"  {k}: {len(v)} sessions, {v[0]} .. {v[-1]}")
    print("one request made; nothing stored; no orders")
    return 0


if __name__ == "__main__":
    sys.exit(main())
