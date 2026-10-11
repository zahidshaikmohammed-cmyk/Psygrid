"""Read-only ranking of NSE stocks by traded volume and traded value (Dhan daily candles, RAM only).

Universe: PSYGRID's 989 NSE equities (stocks.json). For each, the last 20 completed sessions of
daily candles are fetched and ranked by average daily traded value (volume x close, an
approximation of turnover) and by average daily volume (shares). Stocks with NSE stock options
(F&O) are marked from Dhan's instrument master. Nothing is stored; no orders.
"""

import csv
import io
import os
import sys
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"
SESSIONS = 20


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

    rows, failed, spans = [], [], set()
    start = now - timedelta(days=45)
    for n, sym in enumerate(symbols, 1):
        sid = ids.get(sym)
        if not sid:
            failed.append((sym, "no security id"))
            continue
        item = SimpleNamespace(security_id=sid, exchange_segment="NSE_EQ", instrument="EQUITY")
        try:
            candles = api.daily(item, start, now)
        except Exception as exc:
            failed.append((sym, redact(f"{type(exc).__name__}: {str(exc)[:80]}")))
            continue
        candles = [c for c in candles if datetime.fromtimestamp(c["timestamp"], IST).date() <= now.date()]
        candles.sort(key=lambda c: c["timestamp"])
        last = candles[-SESSIONS:]
        if not last:
            failed.append((sym, "no candles"))
            continue
        vol = sum(c["volume"] for c in last) / len(last)
        value = sum(c["volume"] * c["close"] for c in last) / len(last)
        rows.append((sym, vol, value, last[-1]["close"], last[-1]["volume"], len(last), sym in fno))
        spans.add(
            (
                datetime.fromtimestamp(last[0]["timestamp"], IST).date(),
                datetime.fromtimestamp(last[-1]["timestamp"], IST).date(),
            )
        )
        if n % 100 == 0:
            print(f"  fetched {n}/{len(symbols)}", flush=True)

    if not rows:
        print("No data.")
        return 1
    common = max(spans, key=lambda s: sum(1 for x in spans if x == s))
    print(
        f"\nRanked {len(rows)} stocks on their last {SESSIONS} sessions (most stocks: {common[0]} .. {common[1]}); {len(failed)} failed"
    )
    for sym, err in failed[:10]:
        print(f"  FAILED {sym}: {err}")

    def table(title, key, top):
        print(f"\n{title}")
        print(
            f"{'#':>4} {'SYMBOL':<14} {'F&O':<4} {'avg value Rs cr/day':>20} {'avg volume (shares)':>20} {'last close':>11}"
        )
        for i, r in enumerate(sorted(rows, key=key, reverse=True)[:top], 1):
            sym, vol, value, close, _lv, _n, f = r
            print(f"{i:>4} {sym:<14} {'yes' if f else '':<4} {value / 1e7:>20,.1f} {vol:>20,.0f} {close:>11,.2f}")

    table("BY AVERAGE DAILY TRADED VALUE (volume x close), top 100", lambda r: r[2], 100)
    table("BY AVERAGE DAILY VOLUME (shares), top 50", lambda r: r[1], 50)
    print("\nNo data stored, no orders placed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
