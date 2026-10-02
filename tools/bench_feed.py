"""Live-feed safety benchmark: tick processing with and without the recorder, with and without a backfill.

    python tools/bench_feed.py                 # 989 instruments, 60 s of packets per scenario
    python tools/bench_feed.py --seconds 20

For each scenario the real ingestion path runs on Full packets parsed by
dhanhq's own ``MarketFeed.process_full`` from binary frames:
``LiveFeed._handle_packet`` -> ``PsygridState.update_quote`` (+ the
microstructure recorder when enabled). It reports per-packet latency
(p50/p99/max), sustained packets per second, and minute-bar completion delay
(the time from a minute's end to the flush that writes it).

Scenarios: baseline (recorder off), recorder on, and recorder on while a
history bootstrap runs in a separate process (against the offline Dhan fake,
at full speed, so it is a CPU stress well above the real 2 requests/second).
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import random
import statistics
import struct
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import Instrument, Settings
from feed import LiveFeed
from microstructure import MicrostructureRecorder, state_bars_for_minute
from state import PsygridState


def full_frame(security_id: int, ltp: float, volume: int, ltt: int, rng: random.Random) -> bytes:
    """A binary Full packet in Dhan's v2 layout (162 bytes), parsed by dhanhq exactly as in production."""
    depth = b"".join(
        struct.pack(
            "<IIHHff",
            rng.randint(1, 5000),
            rng.randint(1, 5000),
            rng.randint(1, 20),
            rng.randint(1, 20),
            ltp - 0.05 * (i + 1),
            ltp + 0.05 * (i + 1),
        )
        for i in range(5)
    )
    header = struct.pack("<BHBIfHIfIIIIIIffff", 8, 162, 1, security_id, ltp, rng.randint(1, 100), ltt, ltp,
                         volume, rng.randint(1, 10**6), rng.randint(1, 10**6), 0, 0, 0, ltp, ltp, ltp, ltp)  # fmt: skip
    return header + depth


def backfill_load(stop_at: float) -> None:
    """A CPU-heavy history bootstrap against the offline Dhan fake (no network, no rate limit)."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from datetime import datetime

    import tests.intelligence.fake_dhan as fake
    from intelligence.archive import IST
    from intelligence.history_bootstrap import Bootstrap, HistoryClient, RateLimiter

    fake.SYMBOLS = {f"S{i:03d}": str(20000 + i) for i in range(300)}
    while time.time() < stop_at:
        root = Path(tempfile.mkdtemp())
        client = HistoryClient("c", "t", rate=4, post=fake.FakeDhan(fail_once=()), sleep=lambda s: None,
                               get=fake.FakeDhan().profile)  # fmt: skip
        client.limiter = RateLimiter(4, sleep=lambda s: None)
        Bootstrap(root, client, sessions=10, clock=lambda: datetime(2026, 10, 2, 20, tzinfo=IST), log=lambda m: None,
                  master_lines=fake.master_lines(), symbols=sorted(fake.SYMBOLS)).run()  # fmt: skip


def scenario(name: str, seconds: float, recorder_on: bool, backfill: bool, instruments: int = 989) -> dict:
    from dhanhq import MarketFeed

    rng = random.Random(7)
    settings = Settings(client_id="c", access_token="t")
    live = PsygridState(settings)
    items = [Instrument(f"S{i:04d}", str(10_000 + i)) for i in range(instruments)]
    for item in items:
        live.instruments[item.security_id] = {"symbol": item.symbol}
    live.session_status = "LIVE"
    feed = LiveFeed(settings, live, items)
    parser = MarketFeed.__new__(MarketFeed)
    symbols = {item.security_id: item.symbol for item in items}
    tmp = Path(tempfile.mkdtemp())
    recorder = None
    if recorder_on:
        recorder = MicrostructureRecorder(tmp, symbols, min_free_bytes=0,
                                          bars_for_minute=lambda m: state_bars_for_minute(live, symbols, m))  # fmt: skip
        feed.observer = recorder.observe
        recorder.start()
    loader = None
    if backfill:
        loader = multiprocessing.Process(target=backfill_load, args=(time.time() + seconds + 5,), daemon=True)
        loader.start()
        time.sleep(2)
    prices = {item.security_id: 100.0 + i % 500 for i, item in enumerate(items)}
    volumes = dict.fromkeys(prices, 0)
    parse_ns, handle_ns = [], []
    end = time.time() + seconds
    while time.time() < end:
        item = items[rng.randrange(instruments)]
        prices[item.security_id] *= 1 + rng.gauss(0, 0.0005)
        volumes[item.security_id] += rng.randint(0, 50)
        frame = full_frame(int(item.security_id), prices[item.security_id], volumes[item.security_id],
                           int(time.time()), rng)  # fmt: skip
        t0 = time.perf_counter_ns()
        data = parser.process_full(frame)
        t1 = time.perf_counter_ns()
        feed._handle_packet(data)
        t2 = time.perf_counter_ns()
        parse_ns.append(t1 - t0)
        handle_ns.append(t2 - t1)
    flush_delay = None
    if recorder is not None:
        recorder.stop()
        status = recorder.status()
        flush_delay = status.get("last_written_at")
    if loader is not None:
        loader.terminate()
        loader.join()

    def pct(values, q):
        return round(statistics.quantiles(values, n=1000)[int(q * 10) - 1] / 1000, 2)

    total = len(handle_ns)
    return {
        "scenario": name,
        "packets": total,
        "packets_per_second": round(total / seconds),
        "parse_us": {"p50": pct(parse_ns, 50), "p99": pct(parse_ns, 99)},
        "handle_us": {
            "p50": pct(handle_ns, 50),
            "p99": pct(handle_ns, 99),
            "p999": pct(handle_ns, 99.9),
            "max": round(max(handle_ns) / 1000, 1),
        },
        "recorder": recorder.status() if recorder else None,
        "last_flush": flush_delay,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    results = [
        scenario("baseline: recorder off", args.seconds, False, False),
        scenario("recorder on", args.seconds, True, False),
        scenario("recorder on + history backfill in another process", args.seconds, True, True),
        scenario("recorder off + history backfill in another process", args.seconds, False, True),
    ]
    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
