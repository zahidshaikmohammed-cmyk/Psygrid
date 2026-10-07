"""Off-hours dress rehearsal of one node's pre-open path against the real Dhan account. Read-only.

Run by the deploy workflow's ``rehearse`` action on a node VM, from the deployed release with the
node's environment loaded, while the market is closed. It runs in its own short-lived process next
to the running service (it binds no port and never touches the service, its unit or its state) and
walks through exactly what the service does at 08:55-09:05, using the production code:

1. token        take the Dhan token from the token authority (the full PSYGRID); never mint one
2. data access  Dhan profile check (token accepted, data plan active)
3. instruments  resolve the 989-stock universe and this node's partition
4. snapshot     one REST quote snapshot for the partition
5. feed         open the Dhan WebSocket, subscribe the whole partition, wait for CONNECTED
6. replacement  stop that feed (bounded teardown) and build a completely new one, as a token
                renewal or hard reset does, then stop it too
7. cleanup      no event loop leaked, no feed thread abandoned, threads back to the baseline
8. service      the running service's /health/node readiness block

Market data packets after hours are reported but not required: Dhan may send nothing once the
market is closed. Nothing is written to disk. No secret is printed (logs and errors are redacted).
Exit status 0 means every required step passed.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.getcwd())

from live_core.feed import LiveCoreFeed
from live_core.redact import redact
from live_core.runtime import build_runtime
from live_core.state import NodeState

CONNECT_WAIT_SECONDS = 60.0
PACKET_WAIT_SECONDS = 20.0
results: list[tuple[str, bool, str]] = []


def step(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, ok, redact(detail)[:400]))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {redact(detail)[:400]}", flush=True)
    return ok


def run_feed(runtime, label: str) -> bool:
    """One full feed lifecycle on a throw-away state: start, connect, subscribe, stop."""
    tz = ZoneInfo("Asia/Kolkata")
    state = NodeState("Asia/Kolkata", 120)
    state.begin(datetime.now(tz).date().isoformat(), runtime._instruments, runtime.partition.start)
    # PRE_OPEN with the open far ahead: packets are counted and only set baselines, never candles.
    state.market_open_epoch = int(time.time()) + 30 * 86400
    state.set_session_status("PRE_OPEN")
    feed = LiveCoreFeed(runtime.settings, state, runtime._instruments)
    started = time.monotonic()
    feed.start()
    connected = False
    while time.monotonic() - started < CONNECT_WAIT_SECONDS:
        if state.feed_status == "CONNECTED":
            connected = True
            break
        time.sleep(0.5)
    connect_seconds = time.monotonic() - started
    subscribed = state.subscribed_count
    if connected:
        deadline = time.monotonic() + PACKET_WAIT_SECONDS
        while time.monotonic() < deadline and state.quote_packets == 0:
            time.sleep(0.5)
    packets, messages = state.quote_packets, state.feed_messages
    error = state.last_feed_error
    stop_started = time.monotonic()
    feed.stop()
    stop_seconds = time.monotonic() - stop_started
    life = feed.lifecycle()
    expected = len(runtime._instruments)
    ok = step(
        f"{label}: websocket connected and subscribed",
        connected and subscribed == expected,
        f"status {state.feed_status if not connected else 'CONNECTED'} after {connect_seconds:.1f}s, "
        f"subscribed {subscribed}/{expected}" + (f", last error: {error}" if error and not connected else ""),
    )
    step(
        f"{label}: market packets (informational after hours)",
        True,
        f"{packets} quote packets, {messages} frames in {PACKET_WAIT_SECONDS:.0f}s",
    )
    ok &= step(
        f"{label}: bounded teardown",
        not life["abandoned"] and not life["feed_thread_alive"] and life["event_loops_leaked"] == 0,
        f"stopped in {stop_seconds:.1f}s, cycles {life['connection_cycles']}, loops closed "
        f"{life['event_loops_closed']}, leaked {life['event_loops_leaked']}, abandoned {life['abandoned']}",
    )
    return ok


def service_health(port: str) -> None:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health/node", timeout=5) as response:
            health = json.loads(response.read())
    except Exception as exc:
        step("service: /health/node answers", False, f"{type(exc).__name__}: {exc}")
        return
    readiness = health.get("readiness") or {}
    lifecycle = health.get("lifecycle") or {}
    step(
        "service: /health/node answers",
        True,
        f"session {health.get('session_status')}, phase {lifecycle.get('phase') if isinstance(lifecycle, dict) else lifecycle}, "
        f"release {health.get('release') or health.get('version', '')}",
    )
    checks = readiness.get("checks") or {}
    if checks:
        print("        readiness checks now (most only turn true from 08:55-09:05 IST):", flush=True)
        for name, value in checks.items():
            print(f"          {name}: {value}", flush=True)


def main() -> int:
    tz = ZoneInfo("Asia/Kolkata")
    now = datetime.now(tz)
    if now.weekday() < 5 and "09:00" <= now.strftime("%H:%M") < "15:20" and not os.getenv("REHEARSE_ANYWAY"):
        print(f"{now:%Y-%m-%d %H:%M} IST is inside the session; the rehearsal runs only off-hours")
        return 1
    runtime = build_runtime()
    print(f"node {runtime.partition.node_id}: stocks [{runtime.partition.start}, {runtime.partition.end})", flush=True)
    baseline_threads = threading.active_count()

    try:
        runtime.settings = runtime._settings_loader()
        source = "token authority" if runtime.token_source is not None else "shared environment token"
        step("token", True, f"taken from the {source}")
    except Exception as exc:
        step("token", False, f"{type(exc).__name__}: {exc}")
        return report()

    try:
        runtime.dhan_api = runtime._api_factory(runtime.settings)
        profile = runtime._authenticate()
        step("data access", True, f"Dhan accepted the token; data plan {profile.get('dataPlan')}")
    except Exception as exc:
        step("data access", False, f"{type(exc).__name__}: {exc}")
        return report()

    try:
        instruments = runtime._resolve_instruments(now.date().isoformat())
        step(
            "instruments",
            len(instruments) == runtime.partition.size,
            f"{len(instruments)} resolved for the partition (expected {runtime.partition.size}), "
            f"universe {len(runtime.universe.symbols)}",
        )
    except Exception as exc:
        step("instruments", False, f"{type(exc).__name__}: {exc}")
        return report()

    try:
        snapshot = runtime.dhan_api.quote_snapshot(instruments)
        step("quote snapshot", True, f"{len(snapshot)}/{len(instruments)} stocks returned by Dhan REST")
    except Exception as exc:  # the service treats this as non-fatal too
        step("quote snapshot", True, f"not available (non-fatal in the service): {type(exc).__name__}: {exc}")

    run_feed(runtime, "feed #1")
    time.sleep(2.0)
    run_feed(runtime, "feed #2 (replacement)")
    time.sleep(1.0)
    after = threading.active_count()
    step("threads back to baseline", after <= baseline_threads + 1, f"baseline {baseline_threads}, after {after}")
    service_health(os.getenv("LIVE_CORE_PORT", "10000"))
    return report()


def report() -> int:
    failed = [name for name, ok, _ in results if not ok]
    print("REHEARSAL " + ("PASSED" if not failed else f"FAILED: {', '.join(failed)}"), flush=True)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
