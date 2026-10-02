"""Command line for the intelligence layer.

    python -m intelligence days
    python -m intelligence replay 2026-10-02 --until 10:17
    python -m intelligence replay 2026-10-02 --until 10:17 --json
    python -m intelligence run 2026-10-02 [--every 1] [--start 09:15 --end 15:15]

``run`` replays a whole session through every engine (features, anomalies,
relationships, events) exactly as the live process would, storing events in
the intelligence store, and prints what fired and how long each step took.

``replay`` shows the market as PSYGRID knew it at ``--until``: how many bars
had completed, coverage, and the instruments with the worst data quality.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
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


def _run(args) -> int:
    from intelligence.engine import IntelligenceEngine, replay_session
    from intelligence.event_store import EventStore
    from intelligence.history import store_dir

    try:
        day = load_day(args.archive_dir, args.date)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    root = args.store_dir or store_dir()
    engine = IntelligenceEngine(args.archive_dir, root, event_store=EventStore(root / "events.db"))
    summary = replay_session(engine, day, args.start, args.end, args.every)
    print(json.dumps(asdict(summary), indent=2))
    return 0


def _serve(args) -> int:
    import logging

    import uvicorn

    from intelligence.api import create_app
    from intelligence.settings import Settings

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_environment()
    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.host,
        port=settings.port,
        log_level="warning",
        access_log=False,  # the app writes its own access log, without query strings
        timeout_graceful_shutdown=20,
        limit_concurrency=200,
        ws_max_size=65536,
    )
    return 0


def _keys(args) -> int:
    from intelligence.keys import KeyStore
    from intelligence.settings import Settings

    store = KeyStore(Settings.from_environment().keys_file)
    if args.action == "create":
        key_id, key = store.create(args.name, args.rate_per_minute)
        print(f"created key {key_id} for {args.name!r}. Store it now; it cannot be shown again:\n{key}")
    elif args.action == "revoke":
        if not store.revoke(args.id):
            print(f"no active key {args.id}", file=sys.stderr)
            return 1
        print(f"revoked {args.id}")
    else:
        print(json.dumps(store.list(), indent=2))
    return 0


def _backup(args) -> int:
    from datetime import datetime

    from intelligence.archive import IST
    from intelligence.live import LiveRunner
    from intelligence.settings import Settings

    runner = LiveRunner(Settings.from_environment())
    print(runner.backup(datetime.now(IST).strftime("%Y-%m-%dT%H%M%S")))
    return 0


def _safe_preflight(client) -> dict:
    from intelligence.history_bootstrap import BootstrapBlocked

    try:
        return client.preflight()
    except BootstrapBlocked as exc:
        return {"blocked": str(exc)}


def _bootstrap_history(args) -> int:
    from datetime import datetime

    from dhan_auth import DhanTokenRateLimited
    from intelligence.archive import IST
    from intelligence.history_bootstrap import (
        DEFAULT_RATE,
        Bootstrap,
        BootstrapBlocked,
        HistoryClient,
        credentials,
        read_env_file,
    )

    def log(message: str) -> None:
        print(f"{datetime.now(IST):%H:%M:%S} {message}", flush=True)

    bootstrap = Bootstrap(args.archive_dir, sessions=args.sessions, workers=args.workers, log=log)
    if args.action == "plan":
        print(json.dumps(bootstrap.plan(args.rate), indent=2))
        return 0
    if args.action == "status":
        print(json.dumps(bootstrap.status(), indent=2))
        return 0
    if args.action == "verify":
        report = bootstrap.verify()
        bad = {day: r["problems"] for day, r in report.items() if not r["ok"]}
        print(json.dumps({"days": len(report), "ok": len(report) - len(bad), "failed": bad}, indent=2))
        return 1 if bad else 0
    env = dict(os.environ)
    if args.env_file:
        env.update(read_env_file(args.env_file))
    try:
        for attempt in range(4):
            # Dhan allows one token generation per ~2 minutes and rejects a TOTP code already used (PSYGRID
            # generates its own token at every restart): wait for the cooldown or the next 30 s TOTP window.
            try:
                client_id, token, how = credentials(env, allow_generate=args.generate_token)
                break
            except DhanTokenRateLimited as exc:
                wait = min(exc.retry_after + 5, 300)
            except RuntimeError as exc:
                if "TOTP" not in str(exc) or attempt == 3:
                    raise
                wait = 35
            if attempt == 3:
                raise BootstrapBlocked("could not obtain a Dhan token after 4 attempts")
            log(f"Dhan token not available yet; waiting {wait}s")
            time.sleep(wait)
        log(f"credentials: {how}; rate {args.rate}/s, {args.workers} workers")
        bootstrap.client = HistoryClient(client_id, token, rate=args.rate or DEFAULT_RATE)
        if args.action == "probe":
            print(json.dumps({"profile": _safe_preflight(bootstrap.client), "checks": bootstrap.client.probe()},
                             indent=2))  # fmt: skip
            return 0
        result = bootstrap.run(allow_market_hours=args.allow_market_hours, keep_staging=args.keep_staging)
    except BootstrapBlocked as exc:
        print(f"blocked: {exc}", file=sys.stderr)
        return 3
    except DhanTokenRateLimited as exc:
        print(f"blocked: Dhan token generation is cooling down; retry in {exc.retry_after}s", file=sys.stderr)
        return 3
    summary = {k: v for k, v in result.items() if k != "verified"}
    summary["verified_ok"] = sum(r["ok"] for r in result.get("verified", {}).values())
    summary["verified_failed"] = {d: r["problems"] for d, r in result.get("verified", {}).items() if not r["ok"]}
    print(json.dumps(summary, indent=2))
    return 0 if not result.get("failures") and not summary["verified_failed"] else 1


def _validate_replay(args) -> int:
    from intelligence.history import store_dir
    from intelligence.validation import validate_replay

    date = args.date or (available_days(args.archive_dir) or [None])[-1]
    if date is None:
        print(f"no archived days under {args.archive_dir}", file=sys.stderr)
        return 2
    report = validate_replay(args.archive_dir, date, args.store_dir or store_dir(), every=args.every)
    print(json.dumps(report, indent=2))
    return 0 if report["ok"] else 1


def _evaluate(args) -> int:
    from intelligence.evaluation import evaluate
    from intelligence.history import store_dir

    report = evaluate(args.archive_dir, args.store_dir or store_dir(), train_sessions=args.train_sessions,
                      test_days=args.test_days, every=args.every, fdr_q=args.fdr,
                      log=lambda m: print(m, file=sys.stderr, flush=True))  # fmt: skip
    text = json.dumps(report, indent=2)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(text)
    print(text)
    return 0 if report.get("status") == "OK" else 2


def _research(args) -> int:
    """Run the research engines on one archived minute and print what they measure (real-data check)."""
    import numpy as np

    from intelligence.archive import session_days
    from intelligence.expectation import expectation
    from intelligence.history import store_dir
    from intelligence.market_state import measure, with_history
    from intelligence.response import align, evaluate_state, model_for, session_returns

    store = args.store_dir or store_dir()
    date = args.date or (session_days(args.archive_dir) or [None])[-1]  # the latest real session
    if date is None:
        print(f"no archived days under {args.archive_dir}", file=sys.stderr)
        return 2
    at = as_of_time(date, args.at)
    timings, out = {}, {"date": date, "as_of": args.at}
    started = time.perf_counter()
    day = load_day(args.archive_dir, date)
    sr = session_returns(day, at)
    timings["load_s"] = round(time.perf_counter() - started, 2)
    started = time.perf_counter()
    model = model_for(args.archive_dir, date, cache_root=store)
    timings["model_s"] = round(time.perf_counter() - started, 2)
    if model is None:
        out["response"] = "fewer than 5 earlier qualified sessions"
    else:
        started = time.perf_counter()
        sr = align(sr, model.keys)
        state = evaluate_state(model, sr)
        timings["response_state_s"] = round(time.perf_counter() - started, 3)
        delay = model.delay_profile()
        order = np.argsort(-np.nan_to_num(np.abs(state.gap_sigma), nan=-1))[: args.top]
        out["response"] = {
            "trained_on": [model.trained_on[0], model.trained_on[-1], len(model.trained_on)],
            "market_factor": sr.market_source,
            "stocks": len(sr.keys),
            "median_delay_index": float(np.nanmedian(delay["delay_index"])),
            "median_total_market_beta": float(np.nanmedian(model.total_market_beta)),
            "largest_gaps": [state.of(sr.keys[i], model) | {"key": sr.keys[i]} for i in order],
        }
    started = time.perf_counter()
    out["market_state"] = with_history(measure(day, at), args.archive_dir, store).view()
    timings["market_state_s"] = round(time.perf_counter() - started, 2)
    out["expectation"] = expectation(args.archive_dir, store, date, "nifty", at).view()
    out["timings"] = timings
    print(json.dumps(out, indent=2, default=float))
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
    run = commands.add_parser("run", help="replay a session through every engine and store its events")
    run.add_argument("date", help="session date, YYYY-MM-DD")
    run.add_argument("--start", default="09:15")
    run.add_argument("--end", default="15:15")
    run.add_argument("--every", type=int, default=1, help="minutes between steps (events depend on the cadence)")
    run.add_argument(
        "--store-dir", type=Path, default=None, help="intelligence store (default PSYGRID_INTELLIGENCE_DIR)"
    )
    run.set_defaults(run=_run)
    commands.add_parser(
        "serve", help="run the live engine and the /v2 API (the psygrid-intelligence service)"
    ).set_defaults(run=_serve)
    keys = commands.add_parser("keys", help="manage API keys for /v2")
    keys.add_argument("action", choices=("create", "list", "revoke"))
    keys.add_argument("--name", help="who the key is for (create)")
    keys.add_argument("--id", help="key id (revoke)")
    keys.add_argument("--rate-per-minute", type=int, default=None, help="override the default rate limit (create)")
    keys.set_defaults(run=_keys)
    history = commands.add_parser(
        "bootstrap-history",
        help="fill the archive with genuine Dhan 1m history (plan, run, status, verify); resumable",
    )
    history.add_argument("action", choices=("plan", "run", "status", "verify", "probe"))
    history.add_argument("--sessions", type=int, default=20, help="completed sessions to hold (default 20)")
    history.add_argument("--rate", type=float, default=2.0, help="Dhan requests per second (max 4; default 2)")
    history.add_argument("--workers", type=int, default=2, help="concurrent downloads (max 4; default 2)")
    history.add_argument("--env-file", type=Path, help="read Dhan credentials from this KEY=VALUE file")
    history.add_argument("--generate-token", action="store_true", help="allow generating a token from PIN + TOTP")
    history.add_argument("--allow-market-hours", action="store_true", help="run even during market hours")
    history.add_argument("--keep-staging", action="store_true", help="keep downloaded staging files after success")
    history.set_defaults(run=_bootstrap_history)
    validate = commands.add_parser("validate-replay", help="check determinism and no look-ahead on an archived day")
    validate.add_argument("date", nargs="?", help="session date (default: the latest archived)")
    validate.add_argument("--every", type=int, default=5)
    validate.add_argument("--store-dir", type=Path, default=None, help="baseline cache (default: the service store)")
    validate.set_defaults(run=_validate_replay)
    evaluation = commands.add_parser("evaluate", help="walk-forward out-of-sample test of the response signals")
    evaluation.add_argument("--train-sessions", type=int, default=20)
    evaluation.add_argument("--test-days", type=int, default=None, help="evaluate only the latest N eligible days")
    evaluation.add_argument("--every", type=int, default=5, help="minutes between evaluation points")
    evaluation.add_argument("--fdr", type=float, default=0.05, help="Benjamini-Hochberg false discovery rate")
    evaluation.add_argument("--out", type=Path, default=None, help="also write the JSON report here")
    evaluation.add_argument("--store-dir", type=Path, default=None)
    evaluation.set_defaults(run=_evaluate)
    research = commands.add_parser("research", help="run the research engines on one archived minute")
    research.add_argument("date", nargs="?", help="session date (default: the latest archived)")
    research.add_argument("--at", default="11:00", help="IST time HH:MM")
    research.add_argument("--top", type=int, default=5)
    research.add_argument("--store-dir", type=Path, default=None)
    research.set_defaults(run=_research)
    commands.add_parser("backup", help="copy the event store and key file to <store>/backups").set_defaults(run=_backup)
    args = parser.parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
