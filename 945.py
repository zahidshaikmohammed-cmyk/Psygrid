"""PSYGRID 945: the 09:45 IST one-stock selector.

    python 945.py show [DATE]                 # the stored live decision (and outcome) for a session
    python 945.py decide DATE                 # decide an archived session as of 09:45 (replay namespace)
    python 945.py replay [--from D] [--to D]  # reconstruct every archived session's 09:45 decision, then report
    python 945.py report [--namespace NS]     # out-of-sample performance of stored decisions

The live service (``psygrid-intelligence``) makes the real decision each session at 09:45 from the minute
stream; this tool reads it, and reconstructs history with exactly the same code
(``intelligence/pipeline945.py``). Decisions are immutable: a stored decision is never rewritten.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from daily_archive import archive_dir_from_environment
from intelligence.archive import load_day, session_days
from intelligence.history import store_dir
from intelligence.pipeline945 import decide_day, finish_day, reconstruct, summarise_decisions
from intelligence.selector945 import SELECTOR_VERSION, DecisionStore, render


def _show(args) -> int:
    store = DecisionStore(args.store_dir, args.namespace)
    date = args.date or (store.decisions() or [None])[-1]
    decision = store.load_decision(date) if date else None
    if decision is None:
        print(f"no {args.namespace} decision stored{' for ' + date if date else ''}", file=sys.stderr)
        return 2
    outcome = store.load_outcome(date)
    print(json.dumps({"decision": decision, "outcome": outcome}, indent=2) if args.json else render(decision, outcome))
    return 0


def _decide(args) -> int:
    day = load_day(args.archive_dir, args.date)
    earlier = [d for d in session_days(args.archive_dir) if d < args.date]
    store = DecisionStore(args.store_dir, args.namespace)
    decision, matrix = decide_day(args.archive_dir, args.store_dir, day, store, earlier=earlier,
                                  computed_at="historical decision")  # fmt: skip
    outcome = finish_day(store, decision, matrix, day)
    print(json.dumps({"decision": decision.payload, "outcome": outcome}, indent=2) if args.json
          else render(decision.payload, outcome))  # fmt: skip
    return 0


def _replay(args) -> int:
    report = reconstruct(args.archive_dir, args.store_dir, args.namespace, first=args.first, last=args.last,
                         log_fn=lambda m: print(m, file=sys.stderr, flush=True))  # fmt: skip
    _write(args, report)
    return 0


def _report(args) -> int:
    _write(args, summarise_decisions(DecisionStore(args.store_dir, args.namespace)))
    return 0


def _write(args, report: dict) -> None:
    text = json.dumps(report, indent=2)
    if getattr(args, "out", None):
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
    print(text)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="945.py", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--archive-dir", type=Path, default=archive_dir_from_environment())
    parser.add_argument(
        "--store-dir", type=Path, default=None, help="intelligence store (default PSYGRID_INTELLIGENCE_DIR)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    show = commands.add_parser("show", help="print a stored decision and its outcome")
    show.add_argument("date", nargs="?")
    show.add_argument("--namespace", default="live")
    show.add_argument("--json", action="store_true")
    show.set_defaults(run=_show)
    decide = commands.add_parser("decide", help="decide one archived session as of 09:45")
    decide.add_argument("date")
    decide.add_argument("--namespace", default=f"replay-{SELECTOR_VERSION}")
    decide.add_argument("--json", action="store_true")
    decide.set_defaults(run=_decide)
    replay = commands.add_parser("replay", help="historical 09:45 reconstruction of every archived session")
    replay.add_argument("--from", dest="first")
    replay.add_argument("--to", dest="last")
    replay.add_argument("--namespace", default=f"replay-{SELECTOR_VERSION}")
    replay.add_argument("--out", type=Path)
    replay.set_defaults(run=_replay)
    report = commands.add_parser("report", help="out-of-sample performance of stored decisions")
    report.add_argument("--namespace", default=f"replay-{SELECTOR_VERSION}")
    report.add_argument("--out", type=Path)
    report.set_defaults(run=_report)
    args = parser.parse_args(argv)
    args.store_dir = args.store_dir or store_dir()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
