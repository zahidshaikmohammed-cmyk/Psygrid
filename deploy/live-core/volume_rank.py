"""Read-only ranking of NSE F&O stocks by traded value and volume (one Dhan market quote, RAM only).

Takes every NSE stock that has stock options (from Dhan's instrument master), asks Dhan's
/marketfeed/quote once for all of them, and ranks them by traded value (volume x average traded
price) and by volume. During market hours the numbers are today's session so far. It makes a
single data request, so it can run alongside live PSYGRID. Nothing is stored; no orders.
"""

import csv
import io
import os
import sys
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
TOP = 99


def main() -> int:
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

    text = requests.get(MASTER, timeout=60).text
    ids, fno = {}, set()
    for row in csv.DictReader(io.StringIO(text)):
        if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != "NSE":
            continue
        kind = row.get("SEM_INSTRUMENT_NAME", "").strip().upper()
        sym = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
        if kind == "EQUITY" and sym and row.get("SEM_SMST_SECURITY_ID", "").strip():
            ids[sym] = row["SEM_SMST_SECURITY_ID"].strip()
        elif kind == "OPTSTK" and sym:
            fno.add(sym.split("-")[0])
    del text
    symbols = sorted(s for s in fno if s in ids)
    print(f"{len(fno)} NSE stocks have stock options; {len(symbols)} matched to an equity security id.", flush=True)

    by_id = {ids[s]: s for s in symbols}
    try:
        quotes = api.quote_snapshot([SimpleNamespace(security_id=ids[s], exchange_segment="NSE_EQ") for s in symbols])
    except Exception as exc:
        print("Quote request failed:", redact(f"{type(exc).__name__}: {str(exc)[:200]}"))
        return 1
    rows = []
    for sid, q in quotes.items():
        sym = by_id.get(str(sid))
        vol = q.get("volume") or 0
        avg = q.get("average_price") or q.get("last_price") or 0
        if sym and vol:
            rows.append((sym, float(vol), float(vol) * float(avg), float(q.get("last_price") or 0)))
    if not rows:
        print("No data.", str(list(quotes.items())[:2])[:300])
        return 1
    print(f"Quote at {datetime.now(IST):%Y-%m-%d %H:%M} IST for {len(rows)} F&O stocks (today's session so far).")

    ranked = sorted(rows, key=lambda r: r[2], reverse=True)
    print(f"\nF&O STOCKS BY TRADED VALUE (volume x average traded price); top {TOP} marked *")
    print(f"{'#':>4}   {'SYMBOL':<14} {'traded value Rs cr':>19} {'volume (shares)':>16} {'last price':>11}")
    for i, (sym, vol, value, price) in enumerate(ranked, 1):
        print(f"{i:>4} {'*' if i <= TOP else ' '} {sym:<14} {value / 1e7:>19,.1f} {vol:>16,.0f} {price:>11,.2f}")
    print(f"\nTOP {TOP} (copy-ready): " + ", ".join(r[0] for r in ranked[:TOP]))
    print("\nNo data stored, no orders placed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
