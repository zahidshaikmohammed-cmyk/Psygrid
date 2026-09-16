from __future__ import annotations

import json
import os
import sys
from itertools import combinations
from urllib.request import Request, urlopen

EXPECTED = 450
SHARDS = tuple("abcdefghij")


def fetch_json(base_url: str, path: str) -> dict:
    url = base_url.rstrip("/") + path
    request = Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "psygrid-universe-check/1.0"})
    with urlopen(request, timeout=15) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}: {url}")
        return json.loads(response.read().decode("utf-8"))


def symbols_from(payload: dict, label: str) -> list[str]:
    stocks = payload.get("stocks")
    if not isinstance(stocks, dict):
        raise AssertionError(f"{label}: missing stocks object")
    symbols = list(stocks.keys())
    if len(symbols) != 45:
        raise AssertionError(f"{label}: expected 45 records, got {len(symbols)}")
    if len(symbols) != len(set(symbols)):
        raise AssertionError(f"{label}: duplicate symbol references inside endpoint")
    return symbols


def main() -> int:
    base_url = os.getenv("PSYGRID_BASE_URL", "http://140.245.226.102:10000")
    print("PSYGRID // LIVE UNIVERSE INTEGRITY CHECKPOINT")
    print(f"BASE URL: {base_url}")

    shard_symbols: dict[str, list[str]] = {}
    for shard in SHARDS:
        payload = fetch_json(base_url, f"/public/live-{shard}.json")
        shard_symbols[shard] = symbols_from(payload, f"SHARD {shard.upper()}")
        print(f"SHARD {shard.upper():>1}: OK {len(shard_symbols[shard])}/45")

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

    full_payload = fetch_json(base_url, "/public/live.json")
    full_stocks = full_payload.get("stocks")
    if not isinstance(full_stocks, dict):
        failures.append("FULL ENDPOINT: missing stocks object")
    else:
        full_symbols = list(full_stocks.keys())
        print(f"FULL ENDPOINT: {len(full_symbols)} records")
        if len(full_symbols) != EXPECTED:
            failures.append(f"FULL ENDPOINT COUNT: expected {EXPECTED}, got {len(full_symbols)}")
        if len(full_symbols) != len(set(full_symbols)):
            failures.append("FULL ENDPOINT: duplicate symbol references")
        if set(full_symbols) != unique:
            failures.append("FULL ENDPOINT != UNION(A..J)")

    if len(flattened) != EXPECTED:
        failures.append(f"TOTAL RECORDS: expected {EXPECTED}, got {len(flattened)}")
    if len(unique) != EXPECTED:
        failures.append(f"TOTAL UNIQUE: expected {EXPECTED}, got {len(unique)}")
    if failures:
        print("\nRESULT: FAIL")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("CROSS-SHARD OVERLAPS: 0")
    print("MISSING SYMBOLS: 0")
    print("FULL VS SHARDS: MATCH")
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
