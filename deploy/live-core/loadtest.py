"""Bounded HTTP load test for a Live Core node (standard library only).

Each level runs ``concurrency`` worker threads for ``seconds``, each worker requesting its URLs
back to back (gzip, like a real client) and recording latency, bytes and HTTP status. Levels are
run in order and the test stops early once a level's error rate exceeds 5 %, so it never keeps
hammering a node that is already failing.

    python3 deploy/live-core/loadtest.py http://129.225.112.47:10000 --plan shards:1,5,10,25,50
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time
import urllib.error
import urllib.request

SHARDS = [f"/public/live-{chr(c)}.json" for c in range(ord("a"), ord("v") + 1)]
TARGETS = {
    "live": ["/public/live.json"],
    "latest": ["/public/live-latest.json?candles=5"],
    "shards": SHARDS,
    "stock": ["/public/stock/RELIANCE.json", "/public/stock/MEESHO.json", "/public/stock/TCS.json"],
    "health": ["/health"],
}


def fetch(base: str, path: str, timeout: float) -> tuple[int, float, int]:
    started = time.perf_counter()
    request = urllib.request.Request(
        base + path, headers={"Accept-Encoding": "gzip", "User-Agent": "live-core-loadtest"}
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            size = len(response.read())
            return response.status, time.perf_counter() - started, size
    except urllib.error.HTTPError as exc:
        return exc.code, time.perf_counter() - started, 0
    except Exception:
        return 0, time.perf_counter() - started, 0


def run_level(base: str, paths: list[str], concurrency: int, seconds: float, timeout: float) -> dict:
    results: list[tuple[int, float, int]] = []
    lock = threading.Lock()
    deadline = time.monotonic() + seconds

    def worker(offset: int) -> None:
        index = offset
        while time.monotonic() < deadline:
            outcome = fetch(base, paths[index % len(paths)], timeout)
            index += 1
            with lock:
                results.append(outcome)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(seconds + timeout + 5)
    latencies = sorted(r[1] for r in results)
    errors = sum(1 for r in results if r[0] != 200)

    def pct(p: float) -> float:
        return latencies[min(len(latencies) - 1, int(p * len(latencies)))] if latencies else float("nan")

    return {
        "requests": len(results),
        "errors": errors,
        "error_rate": errors / len(results) if results else 1.0,
        "rps": len(results) / seconds,
        "mbit_s": sum(r[2] for r in results) * 8 / seconds / 1e6,
        "p50": pct(0.50),
        "p95": pct(0.95),
        "p99": pct(0.99),
        "max": latencies[-1] if latencies else float("nan"),
        "mean": statistics.fmean(latencies) if latencies else float("nan"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("base")
    parser.add_argument("--plan", action="append", required=True, help="target:c1,c2,... (e.g. shards:1,5,10)")
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--pause", type=float, default=10.0, help="rest between levels")
    args = parser.parse_args()
    base = args.base.rstrip("/")
    for plan in args.plan:
        target, _, levels = plan.partition(":")
        for concurrency in [int(x) for x in levels.split(",") if x]:
            r = run_level(base, TARGETS[target], concurrency, args.seconds, args.timeout)
            print(
                f"{time.strftime('%H:%M:%S')} {target:7s} c={concurrency:3d} req={r['requests']:5d} "
                f"rps={r['rps']:7.1f} err={r['errors']}({r['error_rate']:.1%}) {r['mbit_s']:6.1f} Mbit/s "
                f"p50={r['p50']:.3f}s p95={r['p95']:.3f}s p99={r['p99']:.3f}s max={r['max']:.3f}s",
                flush=True,
            )
            if r["error_rate"] > 0.05:
                print(f"STOP: error rate {r['error_rate']:.1%} at {target} c={concurrency}", flush=True)
                return 1
            time.sleep(args.pause)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
