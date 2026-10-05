"""Post-deploy checks for PSYGRID Live Core nodes (stdlib only, runs on the VM or a CI runner).

python3 deploy/live-core/check_health.py node http://127.0.0.1:10000 --expect-node 0
python3 deploy/live-core/check_health.py cluster http://129.225.112.47:10000 [--require-coverage]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request


def fetch(url: str, timeout: float = 60.0) -> dict:
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": "live-core-check"}), timeout=timeout
    ) as r:
        return json.loads(r.read().decode("utf-8"))


def check_node(health: dict, expect_node: int, node_count: int, max_rss_mb: float) -> list[str]:
    process = health.get("process", {})
    print(
        f"node {health.get('node_id')}/{health.get('node_count')} status {health.get('status')} {health.get('reasons')}"
    )
    partition = health.get("partition", {})
    print(
        f"partition [{partition.get('start_index')}, {partition.get('end_index')}) expected {partition.get('expected_instrument_count')}"
    )
    print(f"fds {process.get('open_fds')}/{process.get('fd_limit')} rss_mb {process.get('rss_mb')}")
    failures = []
    if health.get("node_id") != expect_node or health.get("node_count") != node_count:
        failures.append(
            f"node identity is {health.get('node_id')}/{health.get('node_count')}, expected {expect_node}/{node_count}"
        )
    if health.get("status") == "CONFIG_ERROR":
        failures.append(
            "configuration error (fill /etc/psygrid-live-core.env): " + "; ".join(health.get("reasons", []))
        )
    if health.get("storage", {}).get("market_data_on_disk") is not False:
        failures.append("node does not report RAM-only market data")
    ratio = process.get("fd_usage_ratio")
    if ratio is not None and ratio >= 0.2:
        failures.append(f"descriptor usage {ratio} right after start")
    rss = process.get("rss_mb")
    if rss is not None and rss >= max_rss_mb:
        failures.append(f"rss {rss}MB right after start")
    if health.get("feed", {}).get("event_loops_leaked"):
        failures.append("feed event loops leaked")
    return failures


def check_cluster(health: dict, require_coverage: bool, require_reachable: bool = False) -> list[str]:
    cluster = health.get("cluster", {})
    for node, summary in sorted(cluster.get("nodes", {}).items()):
        print(
            f"NODE {node}: reachable={summary.get('reachable')} healthy={summary.get('healthy')} "
            f"session={summary.get('session_status')} feed={summary.get('feed_status')} "
            f"subscribed={summary.get('subscribed_instrument_count')}/{summary.get('expected_instrument_count')} "
            f"partition=[{summary.get('partition')}, {summary.get('partition_end')}) "
            f"status={summary.get('status')} reasons={summary.get('reasons')} error={summary.get('error', '')}"
        )
    print(
        f"COVERAGE: {cluster.get('coverage_status')} "
        f"{cluster.get('covered_instrument_count')}/{cluster.get('expected_instrument_count')}"
    )
    failures = []
    if require_coverage and not cluster.get("partitions_covered"):
        failures.append(f"partitions not covered: {cluster.get('coverage_status')}")
    nodes = cluster.get("nodes", {})
    if require_reachable and (
        len(nodes) != cluster.get("node_count") or any(not n.get("reachable") or n.get("error") for n in nodes.values())
    ):
        failures.append("every node must be reachable from this node with a matching partition")
    return failures


def check_live(payload: dict, require_live: bool) -> list[str]:
    """Summarise /public/live.json: status, coverage and how many real candles it carries."""
    stocks = payload.get("stocks") or {}
    candles = sum(len(stock.get("candles_1m") or []) for stock in stocks.values())
    with_candles = sum(1 for stock in stocks.values() if stock.get("candles_1m"))
    coverage = payload.get("coverage") or {}
    print(
        f"live.json: status={payload.get('status')} session={payload.get('session', {}).get('status')} "
        f"date={payload.get('session', {}).get('date')} stock_count={payload.get('stock_count')} "
        f"universe_size={payload.get('universe_size')} stocks_with_candles={with_candles} candles={candles}"
    )
    for node, info in sorted((coverage.get("nodes") or {}).items()):
        print(
            f"  node {node}: available={info.get('available')} session={info.get('session_status')} "
            f"stocks={info.get('stock_count')}/{info.get('expected_stock_count')} error={info.get('error', '')}"
        )
    failures = []
    if require_live:
        if payload.get("status") != "OK" or payload.get("stock_count") != payload.get("universe_size"):
            failures.append(f"live.json is {payload.get('status')} with {payload.get('stock_count')} stocks")
        if not with_candles:
            failures.append("live.json carries no candles yet")
    return failures


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["node", "cluster", "live"])
    parser.add_argument("base_url")
    parser.add_argument("--expect-node", type=int, default=0)
    parser.add_argument("--node-count", type=int, default=2)
    parser.add_argument("--max-rss-mb", type=float, default=400.0)
    parser.add_argument("--require-coverage", action="store_true")
    parser.add_argument("--require-reachable", action="store_true")
    parser.add_argument("--require-live", action="store_true")
    parser.add_argument("--wait-seconds", type=float, default=60.0)
    args = parser.parse_args(argv)
    path = {"node": "/health/node", "cluster": "/health", "live": "/public/live.json"}[args.mode]
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            health = fetch(args.base_url.rstrip("/") + path)
            break
        except Exception as exc:
            if time.monotonic() >= deadline:
                print(f"{args.base_url}{path} did not answer: {exc}", file=sys.stderr)
                return 1
            time.sleep(2)
    if args.mode == "node":
        failures = check_node(health, args.expect_node, args.node_count, args.max_rss_mb)
    elif args.mode == "cluster":
        failures = check_cluster(health, args.require_coverage, args.require_reachable)
    else:
        failures = check_live(health, args.require_live)
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
