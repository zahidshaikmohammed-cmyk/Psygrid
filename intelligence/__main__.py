"""Command line for the intelligence layer.

    python -m intelligence days
    python -m intelligence replay 2026-10-02 --until 10:17
    python -m intelligence replay 2026-10-02 --until 10:17 --json

``replay`` shows the market as PSYGRID knew it at ``--until``: how many bars
had completed, coverage, and the instruments with the worst data quality.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from daily_archive import archive_dir_from_environment
from intelligence.archive import available_days, load_day
from intelligence.frame import as_of_time, frame_at
from intelligence.quality import frame_quality


def _replay(args) -> int:
    try:
        day = load_day(args.archive_dir, args.date)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    frame = frame_at(day, as_of_time(args.date, args.until))
    quality = frame_quality(frame)
    if args.json:
        print(json.dumps(asdict(quality), indent=2))
        return 0
    print(f"PSYGRID replay  {quality.session_date}  as of {quality.as_of}")
    print(f"completed bars per instrument: {quality.equity.expected_bars_each}")
    for label, block in (("equity", quality.equity), ("indices", quality.indices)):
        if block is None:
            print(f"{label}: not archived")
            continue
        coverage = f"{block.coverage:.1%}" if block.coverage is not None else "n/a (before the first bar)"
        print(
            f"{label}: {block.instruments} instruments, coverage {coverage}, "
            f"{block.complete} complete, {block.with_gaps} with gaps, {block.no_data} no data, "
            f"{block.rejected_bars} rejected bars"
        )
        for item in block.worst(args.worst):
            print(
                f"  {item.key:<14} {item.status:<8} missing {item.missing_bars:>3}  "
                f"last bar {item.last_bar or '-'}  rejected {item.rejected_bars}"
            )
    return 0


def _days(args) -> int:
    days = available_days(args.archive_dir)
    print("\n".join(days) if days else f"no archived days under {args.archive_dir}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m intelligence", description="PSYGRID intelligence layer")
    parser.add_argument("--archive-dir", type=Path, default=archive_dir_from_environment())
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("days", help="list archived session dates").set_defaults(run=_days)
    replay = commands.add_parser("replay", help="show the market as known at one minute of an archived day")
    replay.add_argument("date", help="session date, YYYY-MM-DD")
    replay.add_argument("--until", default="15:15", help="IST time HH:MM (default: the close)")
    replay.add_argument("--worst", type=int, default=5, help="how many worst-quality instruments to list")
    replay.add_argument("--json", action="store_true", help="print the full data-quality record as JSON")
    replay.set_defaults(run=_replay)
    args = parser.parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
