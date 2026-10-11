"""Read-only ranking of NSE stocks by traded volume and traded value (Dhan market quote, RAM only).

Universe: PSYGRID's 989 NSE equities (stocks.json). One Dhan /marketfeed/quote request returns the
latest session's volume and average traded price for all of them; stocks are ranked by traded
value (volume x average price) and by volume (shares). Stocks with NSE stock options (F&O) are
marked from Dhan's instrument master. Nothing is stored; no orders.
"""

import csv
import io
import os
import sys
from datetime import datetime, time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> int:
    now = datetime.now(IST)
    if now.weekday() < 5 and time(9, 0) <= now.time() < time(15, 40) and not os.getenv("RUN_ANYWAY"):
        print("Market hours: this runs only after the close.")
        return 1
    sys.path.insert(0, os.getcwd())
    from dhan_api import DhanAPI
    from live_core.partition import load_universe
    from live_core.redact import redact
    from live_core.runtime import build_runtime

    try:
        settings = build_runtime()._settings_loader()
    except Exception as exc:
        print("Cannot get the token from the token authority:", redact(f"{type(exc).__name__}: {exc}"))
        return 1
    api = DhanAPI(settings)
    symbols = load_universe().symbols

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
    print(f"Universe {len(symbols)} stocks; {len(fno)} NSE stocks have stock options (F&O).", flush=True)

    items = [SimpleNamespace(security_id=ids[s], exchange_segment="NSE_EQ") for s in symbols if s in ids]
    missing = [s for s in symbols if s not in ids]
    by_id = {ids[s]: s for s in symbols if s in ids}
    try:
        quotes = api.quote_snapshot(items)
    except Exception as exc:
        print("Quote request failed:", redact(f"{type(exc).__name__}: {str(exc)[:200]}"))
        return 1
    rows = []
    for sid, q in quotes.items():
        sym = by_id.get(str(sid))
        vol = q.get("volume") or 0
        avg = q.get("average_price") or q.get("last_price") or 0
        if sym and vol:
            rows.append((sym, float(vol), float(vol) * float(avg), float(q.get("last_price") or 0), sym in fno))
    if not rows:
        print("No data.", str(list(quotes.items())[:2])[:300])
        return 1
    sample = next(iter(quotes.values()))
    print(
        f"Latest session quote for {len(rows)} stocks ({len(missing)} without a security id); fields: {sorted(sample)}"
    )

    def table(title, key, top):
        print(f"\n{title}")
        print(
            f"{'#':>4} {'SYMBOL':<14} {'F&O':<4} {'traded value Rs cr':>20} {'volume (shares)':>20} {'last price':>11}"
        )
        for i, r in enumerate(sorted(rows, key=key, reverse=True)[:top], 1):
            sym, vol, value, close, f = r
            print(f"{i:>4} {sym:<14} {'yes' if f else '':<4} {value / 1e7:>20,.1f} {vol:>20,.0f} {close:>11,.2f}")

    table("BY TRADED VALUE (volume x average traded price), top 100", lambda r: r[2], 100)
    table("BY VOLUME (shares), top 50", lambda r: r[1], 50)
    print("\nNo data stored, no orders placed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
