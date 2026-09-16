from __future__ import annotations

import json
import os
import sys
from datetime import datetime, time
from itertools import combinations
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

EXPECTED = 450
SHARDS = tuple("abcdefghij")
IST = ZoneInfo("Asia/Kolkata")
MARKET_START = time(9, 15)
MARKET_END = time(15, 15)


def fetch_json(base_url: str, path: str) -> dict:
    url = base_url.rstrip("/") + path
    request = Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "psygrid-universe-check/1.0"})
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {url}")
        return json.loads(response.read().decode("utf-8"))


def symbols_from(payload: dict, label: str, expected_count: int) -> list[str]:
    stocks = payload.get("stocks")
    if not isinstance(stocks, dict):
        raise AssertionError(f"{label}: missing stocks object")
    symbols = list(stocks.keys())
    if len(symbols) != expected_count:
        raise AssertionError(f"{label}: expected {expected_count} records, got {len(symbols)}")
    if len(symbols) != len(set(symbols)):
        raise AssertionError(f"{label}: duplicate symbol references inside endpoint")
    return symbols


def main() -> int:
    base_url = os.getenv("PSYGRID_BASE_URL", "http://140.245.226.102:10000")
    print("PSYGRID // LIVE UNIVERSE INTEGRITY CHECKPOINT")
    print(f"BASE URL: {base_url}")

    root_payload = fetch_json(base_url, "/")
    root_status = str(root_payload.get("status", ""))
    now_ist = datetime.now(IST)
    in_market = MARKET_START <= now_ist.time() < MARKET_END

    full_probe = fetch_json(base_url, "/public/live.json")
    endpoint_status = str(full_probe.get("status", ""))
    off_market = endpoint_status == "CLOSED" and not in_market
    expected_shard_count = 45 if not off_market else 0
    print(f"MARKET STATE: {'OPEN' if in_market else 'CLOSED'} ({now_ist.strftime('%Y-%m-%d %H:%M:%S IST')})")
    print(f"ENDPOINT STATE: {endpoint_status}")
    print(f"CHECK MODE: {'LIVE 450-STOCK' if not off_market else 'OFF-MARKET SCHEMA'}")

    if off_market:
        if root_status not in {"ONLINE", "CONFIG_ERROR"}:
            raise AssertionError(f"ROOT: unexpected status {root_status!r}")
    elif endpoint_status != "OK":
        raise AssertionError(f"FULL ENDPOINT: expected OK during market hours, got {endpoint_status!r}")

    shard_symbols: dict[str, list[str]] = {}
    for shard in SHARDS:
        payload = fetch_json(base_url, f"/public/live-{shard}.json")
        shard_status = str(payload.get("status", ""))
        if off_market and shard_status != "CLOSED":
            raise AssertionError(f"SHARD {shard.upper()}: expected CLOSED off-market, got {shard_status!r}")
        shard_symbols[shard] = symbols_from(payload, f"SHARD {shard.upper()}", expected_shard_count)
        print(f"SHARD {shard.upper():>1}: OK {len(shard_symbols[shard])}/{expected_shard_count}")

    failures: list[str] = []
    for left, right in combinations(SHARDS, 2):
        overlap = sorted(set(shard_symbols[left]) & set(shard_symbols[right]))
        if overlap:
            failures.append(f"OVERLAP {left.upper()}-{right.upper()}: {','.join(overlap)}")

    flattened = [symbol for shard in SHARDS for symbol in shard_symbols[shard]]
    unique = set(flattened)
    print(f"RAW RECORDS: {len(flattened)}")
    print(f"UNIQUE SYMBOLS: {len(unique)}")
    print(f"DUPLICATES: {len(flattened) - len(unique)}")

    full_payload = full_probe
    full_stocks = full_payload.get("stocks")
    if not isinstance(full_stocks, dict):
        failures.append("FULL ENDPOINT: missing stocks object")
    else:
        full_symbols = list(full_stocks.keys())
        print(f"FULL ENDPOINT: {len(full_symbols)} records")
        expected_full = EXPECTED if not off_market else 0
        if len(full_symbols) != expected_full:
            failures.append(f"FULL ENDPOINT COUNT: expected {expected_full}, got {len(full_symbols)}")
        if len(full_symbols) != len(set(full_symbols)):
            failures.append("FULL ENDPOINT: duplicate symbol references")
        if set(full_symbols) != unique:
            failures.append("FULL ENDPOINT != UNION(A..J)")

    expected_total = EXPECTED if not off_market else 0
    if len(flattened) != expected_total:
        failures.append(f"TOTAL RECORDS: expected {expected_total}, got {len(flattened)}")
    if len(unique) != expected_total:
        failures.append(f"TOTAL UNIQUE: expected {expected_total}, got {len(unique)}")
    if failures:
        print("\nRESULT: FAIL")
        for failure in failures:
            print(f" - {failure}")
        return 1

    if off_market:
        print("OFF-MARKET: 0 live stock records expected; endpoint schema and shard family verified.")
    else:
        print("CROSS-SHARD OVERLAPS: 0")
        print("MISSING SYMBOLS: 0")
        print("FULL VS SHARDS: MATCH")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
