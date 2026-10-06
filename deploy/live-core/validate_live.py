"""Validate a live /public/live.json against the Live Core's data rules (standard library only).

Checks every stock and every candle: 989 unique symbols matching stocks.json, unique security
ids, today's session date only, minute-aligned timestamps inside 09:15-15:15, strictly increasing,
no forming minute (every candle's minute has ended), finite positive prices, OHLC geometry,
non-negative integer volume. Prints a summary and every violation (first 20 per rule).

    python3 deploy/live-core/validate_live.py http://129.225.112.47:10000
"""

from __future__ import annotations

import gzip
import json
import math
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def canonical_symbols() -> list[str]:
    data = json.loads((Path(__file__).resolve().parents[2] / "stocks.json").read_text(encoding="utf-8"))
    rows = data if isinstance(data, list) else data.get("stocks", data.get("symbols", []))
    return [str(r["symbol"] if isinstance(r, dict) else r).strip().upper() for r in rows]


def main() -> int:
    base = sys.argv[1].rstrip("/")
    request = urllib.request.Request(base + "/public/live.json", headers={"Accept-Encoding": "gzip"})
    fetched_at = time.time()
    with urllib.request.urlopen(request, timeout=120) as response:
        body = response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
    payload = json.loads(body)
    problems: dict[str, list[str]] = defaultdict(list)
    stocks = payload.get("stocks") or {}
    session_date = (payload.get("session") or {}).get("date")
    print(
        f"status={payload.get('status')} session={payload.get('session', {}).get('status')} date={session_date} "
        f"universe_size={payload.get('universe_size')} stock_count={payload.get('stock_count')} "
        f"coverage.complete={payload.get('coverage', {}).get('complete')} bytes={len(body)}"
    )
    if payload.get("stock_count") != len(stocks):
        problems["stock_count"].append(f"declared {payload.get('stock_count')} != {len(stocks)}")
    canonical = canonical_symbols()
    missing = sorted(set(canonical) - set(stocks))
    extra = sorted(set(stocks) - set(canonical))
    if missing:
        problems["missing_symbols"].append(f"{len(missing)}: {missing[:20]}")
    if extra:
        problems["unknown_symbols"].append(f"{len(extra)}: {extra[:20]}")
    ids = defaultdict(list)
    totals = {"candles": 0, "zero_volume_candles": 0, "stocks_without_candles": 0}
    for symbol, stock in stocks.items():
        if stock.get("symbol") != symbol:
            problems["symbol_mismatch"].append(symbol)
        ids[str(stock.get("security_id"))].append(symbol)
        candles = stock.get("candles_1m") or []
        totals["candles"] += len(candles)
        if not candles:
            totals["stocks_without_candles"] += 1
        previous = None
        for candle in candles:
            ts = candle.get("timestamp", "")
            try:
                moment = datetime.strptime(ts.replace(" IST", ""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
            except ValueError:
                problems["bad_timestamp"].append(f"{symbol} {ts!r}")
                continue
            if session_date and moment.date().isoformat() != session_date:
                problems["other_day_candle"].append(f"{symbol} {ts}")
            if moment.second or not ("09:15" <= moment.strftime("%H:%M") < "15:15"):
                problems["outside_session_or_unaligned"].append(f"{symbol} {ts}")
            if moment.timestamp() + 60 > fetched_at + 1:
                problems["forming_or_future_minute"].append(f"{symbol} {ts}")
            if previous is not None and moment <= previous:
                problems["not_strictly_increasing"].append(f"{symbol} {ts}")
            previous = moment
            prices = [candle.get(k) for k in ("open", "high", "low", "close")]
            if not all(isinstance(p, (int, float)) and math.isfinite(p) and p > 0 for p in prices):
                problems["bad_price"].append(f"{symbol} {ts} {prices}")
                continue
            o, h, low, c = prices
            if h < max(o, c) or low > min(o, c) or low > h:
                problems["ohlc_geometry"].append(f"{symbol} {ts} {prices}")
            volume = candle.get("volume")
            if not isinstance(volume, int) or volume < 0:
                problems["bad_volume"].append(f"{symbol} {ts} {volume!r}")
            elif volume == 0:
                totals["zero_volume_candles"] += 1
    duplicates = {k: v for k, v in ids.items() if len(v) > 1}
    if duplicates:
        problems["duplicate_security_ids"].append(str(list(duplicates.items())[:20]))
    print(
        f"stocks={len(stocks)} canonical={len(canonical)} candles={totals['candles']} "
        f"stocks_without_candles={totals['stocks_without_candles']} zero_volume_candles={totals['zero_volume_candles']}"
    )
    if not problems:
        print("DATA VALIDATION: PASS (0 violations)")
        return 0
    for rule, items in problems.items():
        print(f"VIOLATION {rule}: {len(items)}  e.g. {items[:20]}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
