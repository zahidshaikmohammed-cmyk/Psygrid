"""Read-only probe: can this Dhan Data API plan fetch EXPIRED NIFTY option candles?

Calls POST /charts/rollingoption (Dhan v2 expired-options endpoint, as used by the official
dhanhq library) for NIFTY weekly ATM and ATM+2 calls over a few windows going back 4 years, and
prints what comes back: HTTP status, candle count, first/last timestamp, a sample row, and the
largest date range accepted. Nothing is stored; no orders; the running service is untouched.
"""

import os
import sys
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BASE = "https://api.dhan.co/v2"


def main() -> int:
    now = datetime.now(IST)
    if now.weekday() < 5 and time(9, 0) <= now.time() < time(15, 40) and not os.getenv("RUN_ANYWAY"):
        print("Market hours: this probe runs only after the close.")
        return 1
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
    headers = {"Accept": "application/json", "Content-Type": "application/json", "access-token": settings.access_token}

    def say(m):
        print(redact(str(m)), flush=True)

    def probe(label, start, stop, strike="ATM", interval=5, option="CALL", code=1):
        payload = {
            "exchangeSegment": "NSE_FNO",
            "interval": str(interval),
            "securityId": 13,
            "instrument": "OPTIDX",
            "expiryFlag": "WEEK",
            "expiryCode": code,
            "strike": strike,
            "drvOptionType": option,
            "requiredData": ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"],
            "fromDate": start.isoformat(),
            "toDate": stop.isoformat(),
        }
        try:
            r = api.session.post(BASE + "/charts/rollingoption", headers=headers, json=payload, timeout=60)
        except Exception as exc:
            say(f"{label}: request error {type(exc).__name__}: {str(exc)[:200]}")
            return None
        if r.status_code != 200:
            say(f"{label}: HTTP {r.status_code} {r.text[:300]}")
            return None
        try:
            body = r.json()
        except ValueError:
            say(f"{label}: HTTP 200 but not JSON: {r.text[:200]}")
            return None
        data = body.get("data") if isinstance(body, dict) else None
        side = None
        if isinstance(data, dict):
            side = data.get("ce") or data.get("pe") or data.get("CE") or data.get("PE")
        if not isinstance(side, dict) or not side.get("timestamp"):
            say(f"{label}: HTTP 200, no candles. keys={list(body)[:6]} data={str(data)[:300]}")
            return 0
        ts = side["timestamp"]
        first = datetime.fromtimestamp(int(ts[0]), IST)
        last = datetime.fromtimestamp(int(ts[-1]), IST)
        days = len({datetime.fromtimestamp(int(t), IST).date() for t in ts})
        i = min(5, len(ts) - 1)
        sample = {k: (v[i] if isinstance(v, list) and len(v) > i else v) for k, v in side.items()}
        say(f"{label}: {len(ts)} candles over {days} sessions, {first:%Y-%m-%d %H:%M} .. {last:%Y-%m-%d %H:%M}")
        say(f"   fields={sorted(side)}  sample(bar {i})={sample}")
        return len(ts)

    today = date.today()
    say("Probing Dhan /charts/rollingoption (expired options), NIFTY weekly, 5-minute, RAM only")
    for years_back in (0, 1, 2, 3, 4):
        end = today - timedelta(days=365 * years_back + 3)
        probe(f"ATM CALL, {years_back}y back, 30 days to {end}", end - timedelta(days=29), end)
    end = today - timedelta(days=370)
    probe(f"ATM+2 CALL (sold leg of a 100-pt spread), 30 days to {end}", end - timedelta(days=29), end, strike="ATM+2")
    probe(f"ATM PUT, 30 days to {end}", end - timedelta(days=29), end, option="PUT")
    probe(f"ATM CALL, 1-minute, 5 days to {end}", end - timedelta(days=4), end, interval=1)
    for span in (31, 60, 90):
        probe(f"range test: {span} days to {end}", end - timedelta(days=span - 1), end)
    say("Done. No data stored, no orders placed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
