"""Benchmark the intelligence layer at production scale.

    python tools/bench_intelligence.py                       # 21 synthetic sessions x the 989-stock universe
    python tools/bench_intelligence.py --days 8 --every 5    # quicker
    python tools/bench_intelligence.py --archive-dir ~/psygrid-data --date 2026-10-20   # a real archive

Measures what the live service does each day: building baselines (cold and
cached), stepping the engine through a full session minute by minute, the
event volume, similarity searches (cold and cached), API latency, store size
and peak memory. Prints JSON; ``docs/intelligence/performance.md`` records a run.
"""

from __future__ import annotations

import argparse
import json
import resource
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from intelligence.archive import available_days, load_day
from intelligence.engine import IntelligenceEngine, replay_session
from intelligence.event_store import EventStore
from intelligence.frame import as_of_time, frame_at
from intelligence.history import build_baselines
from intelligence.similarity import instrument_matches, market_matches
from intelligence.synthetic import Injection, SyntheticMarket, trading_dates


def peak_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)


def timed(fn):
    started = time.perf_counter()
    result = fn()
    return result, round(time.perf_counter() - started, 3)


def synthetic_archive(root: Path, days: int) -> list[str]:
    symbols = tuple(json.loads((Path(__file__).resolve().parents[1] / "stocks.json").read_text())["symbols"])
    dates = trading_dates("2026-09-01", days)
    injections = [
        Injection(dates[-1], "TCS", 120, volume_multiplier=12.0),
        Injection(dates[-1], "HDFCBANK", 150, extra_return=0.02),
        Injection(dates[-1], "SUNPHARMA", 200, length=30, extra_return=0.0012),
    ]
    SyntheticMarket(symbols=symbols, injections=injections).write_days(root, dates)
    return dates


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--archive-dir", type=Path, help="a real archive; default: generate a synthetic one")
    parser.add_argument("--date", help="session to benchmark (default: the latest archived)")
    parser.add_argument("--days", type=int, default=21, help="synthetic sessions to generate")
    parser.add_argument("--every", type=int, default=1, help="minutes between replay steps")
    args = parser.parse_args(argv)

    work = Path(tempfile.mkdtemp(prefix="psygrid-bench-"))
    report: dict = {}
    try:
        if args.archive_dir:
            archive = args.archive_dir
        else:
            archive = work / "archive"
            _, report["generate_archive_s"] = timed(lambda: synthetic_archive(archive, args.days))
        date = args.date or available_days(archive)[-1]
        store = work / "store"
        day, report["load_day_s"] = timed(lambda: load_day(archive, date))
        report["instruments"] = len(day.equity.keys)
        report["sessions_in_archive"] = len(available_days(archive))
        _, report["baselines_cold_s"] = timed(lambda: build_baselines(archive, date, day.equity.keys, cache_root=store))
        _, report["baselines_cached_s"] = timed(
            lambda: build_baselines(archive, date, day.equity.keys, cache_root=store)
        )
        report["peak_rss_after_baselines_mb"] = peak_mb()

        engine = IntelligenceEngine(archive, store, event_store=EventStore(store / "events.db"))
        engine.baselines_for(date, day.equity.keys)
        totals = []
        summary, report["replay_s"] = timed(
            lambda: replay_session(
                engine, day, every=args.every, on_step=lambda s: totals.append(s.timings_ms["total"])
            )
        )
        report["replay"] = {
            "steps": summary.steps,
            "events": summary.events,
            "events_by_type": summary.events_by_type,
            "step_ms": {
                "mean": round(statistics.mean(totals), 1),
                "p50": round(summary.step_ms["p50"], 1),
                "p95": round(summary.step_ms["p95"], 1),
                "max": round(summary.step_ms["max"], 1),
            },
            "stage_ms_mean": summary.stage_ms_mean,
        }
        report["peak_rss_after_replay_mb"] = peak_mb()

        frame = frame_at(day, as_of_time(date, "13:00"))
        _, report["similarity_market_cold_s"] = timed(lambda: market_matches(frame, archive, store))
        _, report["similarity_market_cached_s"] = timed(lambda: market_matches(frame, archive, store))
        _, report["similarity_instrument_s"] = timed(lambda: instrument_matches(frame, "TCS", archive, store))
        report["peak_rss_mb"] = peak_mb()

        from fastapi.testclient import TestClient

        from intelligence.api import create_app
        from intelligence.keys import KeyStore
        from intelligence.live import LiveRunner
        from intelligence.settings import Settings

        settings = Settings(archive, store, "http://127.0.0.1:9", "127.0.0.1", 10001, True, 100_000, 100_000,
                            10, 10, False, False, 60, 2)  # fmt: skip
        runner = LiveRunner(settings)
        runner._publish(engine.step(frame))
        _, key = KeyStore(settings.keys_file).create("bench")
        client = TestClient(create_app(settings, runner))
        client.headers["X-API-Key"] = key
        api = {}
        for path in ("/v2/market", "/v2/observations?limit=1000", "/v2/instruments/TCS", "/v2/anomalies",
                     "/v2/relationships", "/v2/events?limit=100", "/v2/historical-matches/market"):  # fmt: skip
            samples = []
            for _ in range(20):
                started = time.perf_counter()
                assert client.get(path).status_code == 200, path
                samples.append((time.perf_counter() - started) * 1000)
            api[path] = {"p50_ms": round(statistics.median(samples), 1), "max_ms": round(max(samples), 1)}
        report["api"] = api
        report["store_bytes"] = {
            "events_db": (store / "events.db").stat().st_size,
            "summaries": sum(p.stat().st_size for p in (store / "summaries").glob("*")),
            "states": sum(p.stat().st_size for p in (store / "states").rglob("*") if p.is_file()),
        }
        report["archive_bytes_per_day"] = sum(p.stat().st_size for p in (Path(archive) / date).glob("*"))
        report["peak_rss_mb"] = peak_mb()
    finally:
        shutil.rmtree(work, ignore_errors=True)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
