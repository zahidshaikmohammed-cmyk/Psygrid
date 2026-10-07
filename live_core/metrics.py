"""RAM-only operational metrics for a Live Core node: HTTP request counters and a per-minute sampler.

Nothing here is market data and nothing is written to disk. Counters live for the process; the
per-minute samples are a bounded ring (``MINUTE_SAMPLES``) plus a pinned copy of the opening
window (09:15-09:30 IST by default) so the market-open burst can be read after the fact.

* ``HttpMetrics`` + ``MetricsMiddleware`` count requests, errors, active requests, response
  bytes, latency buckets and slow requests per endpoint family. Endpoint keys come from a fixed
  list, so a scanner requesting random paths cannot grow the table.
* ``MinuteSampler`` is driven by the session loop (once a second). Each second it notes the
  packet-rate; at every minute boundary it records CPU, RSS, cgroup memory, descriptors,
  threads, packets, HTTP traffic, reconnects, stale stocks and coverage for the minute just ended.
"""

from __future__ import annotations

import os
import re
import threading
import time
from collections import deque
from datetime import datetime

LATENCY_BUCKETS_MS = (10, 50, 100, 250, 500, 1000, 2500, 5000)
SLOW_REQUEST_SECONDS = 1.0
MINUTE_SAMPLES = 480  # 8 hours of one-minute samples
_SHARD = re.compile(r"^/public/live-[a-z]\.json$")


def endpoint_key(path: str) -> str:
    """A bounded endpoint family for ``path`` (never the raw path)."""
    if path in {
        "/",
        "/health",
        "/health/node",
        "/health/metrics",
        "/ready",
        "/public/health.json",
        "/public/live.json",
        "/public/live-latest.json",
        "/internal/live-core/fragments",
    }:
        return path
    if _SHARD.match(path):
        return "/public/live-{shard}.json"
    if path.startswith("/public/stock/"):
        return "/public/stock/{symbol}.json"
    return "other"


class HttpMetrics:
    def __init__(self, slow_seconds: float = SLOW_REQUEST_SECONDS):
        self.slow_seconds = slow_seconds
        self._lock = threading.Lock()
        self.request_count = 0
        self.request_errors = 0  # 5xx answers and exceptions
        self.client_errors = 0  # 4xx answers
        self.active_requests = 0
        self.max_active_requests = 0
        self.response_bytes = 0
        self.slow_requests = 0
        self.max_latency_ms = 0.0
        self.latency_buckets = [0] * (len(LATENCY_BUCKETS_MS) + 1)
        self.endpoints: dict[str, dict] = {}
        # The busiest concurrency seen since the sampler last asked (per-minute peak).
        self._window_max_active = 0

    def started(self) -> None:
        with self._lock:
            self.active_requests += 1
            if self.active_requests > self.max_active_requests:
                self.max_active_requests = self.active_requests
            if self.active_requests > self._window_max_active:
                self._window_max_active = self.active_requests

    def finished(self, endpoint: str, status: int, sent_bytes: int, seconds: float) -> None:
        ms = seconds * 1000.0
        bucket = next((i for i, limit in enumerate(LATENCY_BUCKETS_MS) if ms <= limit), len(LATENCY_BUCKETS_MS))
        with self._lock:
            self.active_requests = max(0, self.active_requests - 1)
            self.request_count += 1
            error = status >= 500 or status == 0
            if error:
                self.request_errors += 1
            elif status >= 400:
                self.client_errors += 1
            self.response_bytes += sent_bytes
            if seconds >= self.slow_seconds:
                self.slow_requests += 1
            if ms > self.max_latency_ms:
                self.max_latency_ms = ms
            self.latency_buckets[bucket] += 1
            entry = self.endpoints.setdefault(endpoint, {"count": 0, "errors": 0, "bytes": 0, "slow": 0, "max_ms": 0.0})
            entry["count"] += 1
            entry["errors"] += 1 if error else 0
            entry["bytes"] += sent_bytes
            entry["slow"] += 1 if seconds >= self.slow_seconds else 0
            if ms > entry["max_ms"]:
                entry["max_ms"] = round(ms, 1)

    def take_window_max_active(self) -> int:
        with self._lock:
            value, self._window_max_active = self._window_max_active, self.active_requests
            return value

    def snapshot(self) -> dict:
        with self._lock:
            buckets = list(self.latency_buckets)
            return {
                "request_count": self.request_count,
                "request_errors": self.request_errors,
                "client_errors": self.client_errors,
                "active_requests": self.active_requests,
                "max_active_requests": self.max_active_requests,
                "response_bytes": self.response_bytes,
                "slow_requests": self.slow_requests,
                "slow_request_seconds": self.slow_seconds,
                "max_latency_ms": round(self.max_latency_ms, 1),
                "latency_p50_ms": _percentile(buckets, 0.50),
                "latency_p95_ms": _percentile(buckets, 0.95),
                "latency_p99_ms": _percentile(buckets, 0.99),
                "endpoints": {key: dict(value) for key, value in sorted(self.endpoints.items())},
            }


def _percentile(buckets: list[int], fraction: float):
    """Upper bound (ms) of the latency bucket holding ``fraction`` of requests; None without data."""
    total = sum(buckets)
    if not total:
        return None
    target = fraction * total
    running = 0
    for index, count in enumerate(buckets):
        running += count
        if running >= target:
            return LATENCY_BUCKETS_MS[index] if index < len(LATENCY_BUCKETS_MS) else f">{LATENCY_BUCKETS_MS[-1]}"
    return None


class MetricsMiddleware:
    """Pure ASGI middleware: counts every HTTP request without touching its body."""

    def __init__(self, app, metrics: HttpMetrics):
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        endpoint = endpoint_key(scope.get("path", ""))
        status = [0]
        sent = [0]
        started = time.perf_counter()
        self.metrics.started()

        async def counting_send(message):
            kind = message.get("type")
            if kind == "http.response.start":
                status[0] = int(message.get("status", 0))
            elif kind == "http.response.body":
                sent[0] += len(message.get("body", b"") or b"")
            await send(message)

        try:
            await self.app(scope, receive, counting_send)
        except BaseException:
            status[0] = status[0] if status[0] >= 500 else 500
            raise
        finally:
            self.metrics.finished(endpoint, status[0], sent[0], time.perf_counter() - started)


def _cgroup_dir() -> str | None:
    try:
        with open("/proc/self/cgroup", encoding="ascii") as handle:
            for line in handle:
                if line.startswith("0::"):
                    return "/sys/fs/cgroup" + line.strip()[3:]
    except OSError:
        return None
    return None


def _read_int(path: str) -> int | None:
    try:
        with open(path, encoding="ascii") as handle:
            raw = handle.read().strip()
        return None if raw == "max" else int(raw)
    except (OSError, ValueError):
        return None


class MinuteSampler:
    """Per-minute resource and traffic samples, driven once a second by the session loop."""

    def __init__(self, runtime, *, opening_window=("09:15", "09:30"), clock=time.time, cpu_times=None):
        self.runtime = runtime
        self.opening_window = opening_window
        self._clock = clock
        self._cpu_times = cpu_times or (lambda: sum(os.times()[:2]))
        self.samples: deque[dict] = deque(maxlen=MINUTE_SAMPLES)
        self.market_open: list[dict] = []
        self._market_open_date: str | None = None
        self._lock = threading.Lock()
        self._cgroup = _cgroup_dir()
        self._minute: int | None = None
        self._start_cpu = 0.0
        self._start_wall = 0.0
        self._last_second_packets: int | None = None
        self._last_second: float | None = None
        self._max_rate = 0.0
        self._base: dict = {}

    def _counters(self) -> dict:
        runtime = self.runtime
        state = runtime.state
        with state.lock:
            counters = {
                "packets": state.quote_packets,
                "messages": state.feed_messages,
                "accepted": state.live_quotes,
                "reconnects": state.websocket_reconnects,
            }
        http = runtime.http_metrics.snapshot() if getattr(runtime, "http_metrics", None) is not None else {}
        counters["http_requests"] = http.get("request_count", 0)
        counters["http_errors"] = http.get("request_errors", 0)
        counters["http_bytes"] = http.get("response_bytes", 0)
        counters["http_slow"] = http.get("slow_requests", 0)
        counters["feed_replacements"] = getattr(runtime, "feed_replacements", 0)
        return counters

    def tick(self, now: datetime) -> dict | None:
        """Call about once a second. Returns the sample recorded when a minute ended, else None."""
        epoch = self._clock()
        minute = int(epoch // 60)
        packets = self.runtime.state.quote_packets
        if self._last_second is not None and epoch > self._last_second:
            rate = max(0, packets - (self._last_second_packets or 0)) / (epoch - self._last_second)
            if rate > self._max_rate:
                self._max_rate = rate
        self._last_second, self._last_second_packets = epoch, packets
        if self._minute is None:
            self._open_window(minute, epoch)
            return None
        if minute == self._minute:
            return None
        sample = self._close_window(now, epoch)
        self._open_window(minute, epoch)
        return sample

    def _open_window(self, minute: int, epoch: float) -> None:
        self._minute = minute
        self._start_cpu = self._cpu_times()
        self._start_wall = epoch
        self._max_rate = 0.0
        self._base = self._counters()
        metrics = getattr(self.runtime, "http_metrics", None)
        if metrics is not None:
            metrics.take_window_max_active()

    def _close_window(self, now: datetime, epoch: float) -> dict:
        from runtime_guard import process_stats

        runtime = self.runtime
        wall = max(1e-6, epoch - self._start_wall)
        cpu = max(0.0, self._cpu_times() - self._start_cpu)
        counters = self._counters()
        delta = {key: counters[key] - self._base.get(key, 0) for key in counters}
        process = process_stats()
        freshness = runtime.state.freshness(epoch)
        metrics = getattr(runtime, "http_metrics", None)
        window_label = datetime.fromtimestamp(self._minute * 60, now.tzinfo).strftime("%H:%M") if now.tzinfo else ""
        cg = self._cgroup
        sample = {
            "minute": window_label,
            "session_status": runtime.state.session_status,
            "feed_status": runtime.state.feed_status,
            "supervisor_state": getattr(runtime, "supervisor_state", None),
            "cpu_percent_of_one_core": round(100.0 * cpu / wall, 2),
            "rss_mb": process.get("rss_mb"),
            "cgroup_memory_mb": _mb(_read_int(f"{cg}/memory.current")) if cg else None,
            "cgroup_memory_peak_mb": _mb(_read_int(f"{cg}/memory.peak")) if cg else None,
            "open_fds": process.get("open_fds"),
            "threads": process.get("threads"),
            "packets": delta["packets"],
            "packets_per_second_avg": round(delta["packets"] / wall, 1),
            "packets_per_second_max": round(self._max_rate, 1),
            "accepted_packets": delta["accepted"],
            "feed_messages": delta["messages"],
            "reconnects": delta["reconnects"],
            "feed_replacements": delta["feed_replacements"],
            "http_requests": delta["http_requests"],
            "http_errors": delta["http_errors"],
            "http_bytes": delta["http_bytes"],
            "http_slow": delta["http_slow"],
            "http_max_active": metrics.take_window_max_active() if metrics is not None else None,
            "live_stocks": freshness["live_stock_count"],
            "stale_stocks": freshness["stale_stock_count"],
            "no_quote_stocks": freshness["no_quote_stock_count"],
            "instruments": len(runtime.state.ordered),
        }
        with self._lock:
            self.samples.append(sample)
            start, end = self.opening_window
            if window_label and start <= window_label <= end:
                day = now.date().isoformat()
                if self._market_open_date != day:
                    self.market_open = []
                    self._market_open_date = day
                self.market_open.append(sample)
        return sample

    def snapshot(self, limit: int = MINUTE_SAMPLES) -> dict:
        with self._lock:
            samples = list(self.samples)[-limit:]
            opening = list(self.market_open)
            opening_date = self._market_open_date
        return {
            "sample_seconds": 60,
            "retained": len(samples),
            "minutes": samples,
            "market_open_window": {"date": opening_date, "minutes": opening},
            "storage": "RAM only; resets when the process restarts",
        }


def _mb(value: int | None):
    return round(value / 1048576, 1) if value is not None else None
