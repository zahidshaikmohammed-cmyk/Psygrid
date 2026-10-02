"""End-to-end latency of the minute stream: bar close -> Intelligence snapshot, at the 989-stock scale.

    python tools/bench_stream.py               # 21 synthetic sessions x the 989-stock universe
    python tools/bench_stream.py --days 8

The last session's archive is cut at 10:30 (as if PSYGRID had last written it
then) and each later minute is appended to the stream exactly as the PSYGRID
recorder writes it. For each minute the benchmark measures how long the live
runner takes to see the block and publish the new snapshot (merge of archive
and stream, then one engine step), and the research views for that minute
(response state, market state, one stock's full view). The end-to-end delay
from a bar's close is::

    recorder grace (3 s) + recorder flush tick (<= 1 s) + runner poll (<= 2 s) + processing (measured)
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench_intelligence import peak_mb, synthetic_archive

from daily_archive import EQUITY_FILE, INDEX_FILE
from intelligence.archive import IST, parse_timestamp
from intelligence.event_store import EventStore
from intelligence.frame import as_of_time
from intelligence.live import STREAM_POLL_SECONDS, LiveRunner
from intelligence.research import ResearchViews
from intelligence.settings import Settings
from intelligence.stream import stream_path
from microstructure import BAR_COLUMNS, GRACE_SECONDS, MicrostructureRecorder


class Clock:
    def __init__(self, moment):
        self.moment = moment

    def __call__(self):
        return self.moment


def split_day(root: Path, day: str, cut: float) -> dict[int, list[tuple]]:
    """Keep bars opening before ``cut`` in the archive; return later bars as stream rows by minute."""
    later: dict[int, list[tuple]] = {}
    for name, index in ((EQUITY_FILE, False), (INDEX_FILE, True)):
        path = root / day / name
        with gzip.open(path, "rt", newline="") as handle:
            reader = csv.DictReader(handle)
            columns, rows = reader.fieldnames, list(reader)
        keep = []
        for r in rows:
            minute = parse_timestamp(r["timestamp"])
            if minute < cut:
                keep.append(r)
            else:
                sid = f"IDX:{r['index']}" if index else r.get("security_id", "")
                bar = (r["symbol"], sid, r["timestamp"], r["open"], r["high"], r["low"], r["close"], r["volume"])
                later.setdefault(minute, []).append(bar)
        with gzip.open(path, "wt", newline="") as handle:
            writer = csv.DictWriter(handle, columns)
            writer.writeheader()
            writer.writerows(keep)
    return later


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--minutes", type=int, default=20, help="stream minutes to measure after 10:30")
    args = parser.parse_args(argv)
    work = Path(tempfile.mkdtemp(prefix="psygrid-bench-stream-"))
    try:
        archive, store = work / "archive", work / "store"
        dates = synthetic_archive(archive, args.days)
        today = dates[-1]
        cut = as_of_time(today, "10:30")
        later = split_day(archive, today, cut.timestamp())
        os.utime(archive / today / EQUITY_FILE, (cut.timestamp() + 20, cut.timestamp() + 20))
        settings = Settings(archive, store, "http://127.0.0.1:9", "127.0.0.1", 18101, False, 600, 100, 3, 2,
                            False, False, 60, 2)  # fmt: skip
        clock = Clock(cut + timedelta(seconds=25))
        runner = LiveRunner(settings, fetch=lambda url: {}, clock=clock)
        started = time.perf_counter()
        catch_up_steps = runner.follow_archive(clock())
        catch_up = time.perf_counter() - started
        research = ResearchViews(settings, runner)
        started = time.perf_counter()
        research.stock(runner.snapshot, "TCS", runner.store)
        research.wait_for_model(today, timeout=600)
        model_build = time.perf_counter() - started
        target = stream_path(archive, today)
        step_s, research_s, stock_s, ranked_s = [], [], [], []
        for minute in sorted(later)[: args.minutes]:
            MicrostructureRecorder._append_block(None, target, BAR_COLUMNS, later[minute], minute)
            clock.moment = datetime.fromtimestamp(minute + 60 + GRACE_SECONDS + 1, IST)
            t0 = time.perf_counter()
            steps = runner.follow_archive(clock())
            step_s.append(time.perf_counter() - t0)
            assert steps == 1, steps
            t0 = time.perf_counter()
            research._minute(runner.snapshot)
            research_s.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            research.stock(runner.snapshot, "TCS", runner.store)
            stock_s.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            research.ranked(runner.snapshot, "response_gap_sigma", 50)
            ranked_s.append(time.perf_counter() - t0)

        def stats(values):
            values = sorted(values)
            return {"median_s": round(statistics.median(values), 3), "max_s": round(values[-1], 3)}

        processing = statistics.median(step_s)
        result = {
            "stocks": len(runner.snapshot.features.keys),
            "sessions": args.days,
            "catch_up": {"steps": catch_up_steps, "seconds": round(catch_up, 2)},
            "response_model_build_s": round(model_build, 2),
            "per_minute_snapshot": stats(step_s),
            "per_minute_research": stats(research_s),
            "stock_view_cached": stats(stock_s),
            "ranked_view_cached": stats(ranked_s),
            "end_to_end_seconds_after_bar_close": {
                "best": round(GRACE_SECONDS + processing, 2),
                "worst": round(GRACE_SECONDS + 1 + STREAM_POLL_SECONDS + max(step_s), 2),
                "with_research": round(GRACE_SECONDS + 1 + STREAM_POLL_SECONDS + max(step_s) + max(research_s), 2),
            },
            "archive_only_worst_seconds": 5 * 60 + 60,
            "peak_rss_mb": peak_mb(),
            "events": EventStore(settings.events_db).stats()["events"],
        }
        print(json.dumps(result, indent=2))
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
