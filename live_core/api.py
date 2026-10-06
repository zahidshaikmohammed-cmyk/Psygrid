"""Live Core HTTP API: the PSYGRID equity endpoints, served from RAM across the node partitions.

Every node serves the same routes. A route whose stocks live on this node is answered from
local RAM; stocks owned by a peer are fetched from that peer's current state (never history)
and spliced in as bytes. Node 0 is the designated public entry point, but node 1 can answer the
same URLs, so either node can stand in for the other's consumers.

Endpoint handlers only read RAM and never wait on Dhan, so the API keeps answering while the
feed is reconnecting, failing or stopped.
"""

from __future__ import annotations

import contextlib
import gzip
import threading
import time
from contextlib import asynccontextmanager

import orjson
from fastapi import FastAPI, Query, Request, Response

from live_core import SERVICE_NAME
from live_core.aggregate import FRAGMENTS_PATH, PeerUnavailable, encode_fragments
from live_core.gzipjoin import CompressedTail, gzip_join
from live_core.partition import nodes_for_range, shard_ranges
from live_core.render import (
    MAX_LATEST_CANDLES,
    MAX_SYMBOL_LENGTH,
    assemble_head,
    clear_caches,
    fragment_last,
    payload_status,
    snapshot,
    stock_body,
    tail_parts,
)
from runtime_guard import ServiceWatchdog, http_probe

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
    "Vary": "Accept-Encoding",
}
GZIP_MINIMUM_BYTES = 1024
GZIP_LEVEL = 3


class _Body:
    """A response body: ``head`` alone, or a small per-response ``head`` plus a shared large ``tail``."""

    __slots__ = ("_gzip", "_lock", "created", "head", "status_code", "tail")

    def __init__(self, body: bytes, status_code: int, created: float, tail: CompressedTail | None = None):
        self.head = body
        self.tail = tail
        self.status_code = status_code
        self.created = created
        self._gzip: bytes | None = None
        self._lock = threading.Lock()

    @property
    def body(self) -> bytes:
        return self.head if self.tail is None else self.head + self.tail.data

    def __len__(self) -> int:
        return len(self.head) + (self.tail.length if self.tail is not None else 0)

    def gzipped(self) -> bytes:
        with self._lock:
            if self._gzip is None:
                if self.tail is None:
                    self._gzip = gzip.compress(self.head, compresslevel=GZIP_LEVEL, mtime=0)
                else:
                    # The stocks tail was deflated once; only this response's head is compressed now.
                    self._gzip = gzip_join(self.head, self.tail)
            return self._gzip


def _respond(request: Request, entry: _Body) -> Response:
    headers = dict(NO_CACHE_HEADERS)
    if len(entry) >= GZIP_MINIMUM_BYTES and "gzip" in request.headers.get("accept-encoding", "").lower():
        headers["Content-Encoding"] = "gzip"
        body = entry.gzipped()
    else:
        body = entry.body
    return Response(content=body, media_type="application/json", status_code=entry.status_code, headers=headers)


def json_body(payload: dict, status_code: int = 200) -> _Body:
    return _Body(orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE), status_code, time.monotonic())


class LiveCoreService:
    """Builds endpoint bodies from the runtime's RAM state and the peers' current state."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.universe = runtime.universe
        self.partition = runtime.partition
        self.state = runtime.state
        self._cache: dict[tuple, _Body] = {}
        self._tails: dict[tuple, tuple] = {}
        self._cache_lock = threading.Lock()
        self._key_locks: dict[tuple, threading.Lock] = {}

    TAIL_IDLE_SECONDS = 120.0

    def _cached(self, key: tuple, start: int, end: int, sort_by_symbol: bool, last: int | None = None) -> _Body:
        """Serve a live body: a fresh head on every build, the stocks tail rebuilt only on change.

        Within ``render_cache_seconds`` a whole body is reused outright (a polling flood costs
        nothing). After that the body is rebuilt: the head (status, session time, coverage) always,
        the stocks tail - by far the largest part, and the only costly one to encode and compress -
        only when a node's content version changed, i.e. when a minute completed. The head's
        ``current_time_ist`` is therefore never older than ``render_cache_seconds``.
        """
        cfg = self.runtime.cfg
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is not None and time.monotonic() - entry.created <= cfg.render_cache_seconds:
                return entry
            key_lock = self._key_locks.setdefault(key, threading.Lock())
        # Single flight: concurrent requests for the same body wait for one build and share it.
        with key_lock:
            with self._cache_lock:
                entry = self._cache.get(key)
            if entry is not None and time.monotonic() - entry.created <= cfg.render_cache_seconds:
                return entry
            items, nodes = self.collect(start, end)
            if last is not None:
                # The light view: each stock keeps only its newest candles (a byte slice per stock).
                items = [(index, symbol, fragment_last(fragment, last)) for index, symbol, fragment in items]
            content_key = tuple(
                (node_id, node.get("available"), node.get("version"), node.get("stale"), node.get("session_status"))
                for node_id, node in sorted(nodes.items())
            )
            now = time.monotonic()
            with self._cache_lock:
                cached_tail = self._tails.get(key)
            if cached_tail is not None and cached_tail[0] == content_key:
                tail = cached_tail[1]
            else:
                tail = CompressedTail(tail_parts(items, sort_by_symbol=sort_by_symbol))
            entry = self.live_body(start, end, sort_by_symbol=sort_by_symbol, collected=(items, nodes), tail=tail)
            del items
            with self._cache_lock:
                self._cache[key] = entry
                self._tails[key] = (content_key, tail, now)
                # Bodies and tails nobody asked for recently are dropped instead of pinning RAM.
                for other in [k for k, v in self._cache.items() if now - v.created > cfg.render_cache_seconds]:
                    if other != key:
                        del self._cache[other]
                for other in [k for k, v in self._tails.items() if now - v[2] > self.TAIL_IDLE_SECONDS]:
                    del self._tails[other]
            return entry

    def clear(self) -> None:
        with self._cache_lock:
            self._cache.clear()
            self._tails.clear()

    # ------------------------------------------------------------------ live payloads

    def collect(self, start: int, end: int) -> tuple[list[tuple[int, str, bytes]], dict]:
        items: list[tuple[int, str, bytes]] = []
        nodes: dict[str, dict] = {}
        for node_id, sub_start, sub_end in nodes_for_range(self.universe.size, self.partition.node_count, start, end):
            expected = sub_end - sub_start
            if node_id == self.partition.node_id:
                local = snapshot(self.state, sub_start, sub_end)
                items.extend(local.items)
                nodes[str(node_id)] = {
                    "source": "local",
                    "available": True,
                    "version": local.version,
                    "session_status": local.session_status,
                    "session_date": local.session_date,
                    "stock_count": len(local.items),
                    "expected_stock_count": expected,
                    "data_age_seconds": 0.0,
                    "stale": False,
                }
                continue
            peer = self.runtime.peers.get(node_id)
            if peer is None:
                nodes[str(node_id)] = {
                    "source": "peer",
                    "available": False,
                    "expected_stock_count": expected,
                    "error": "peer URL not configured",
                }
                continue
            try:
                fetched = peer.fragments(sub_start, sub_end)
            except PeerUnavailable as exc:
                nodes[str(node_id)] = {
                    "source": "peer",
                    "available": False,
                    "expected_stock_count": expected,
                    "error": str(exc)[:300],
                }
                continue
            items.extend(fetched.items)
            nodes[str(node_id)] = {
                "source": "peer",
                "available": True,
                "version": fetched.header.get("version"),
                "session_status": fetched.header.get("session_status"),
                "session_date": fetched.header.get("session_date"),
                "stock_count": len(fetched.items),
                "expected_stock_count": expected,
                "data_age_seconds": fetched.age_seconds(time.monotonic()),
                "stale": fetched.stale,
            }
        return items, nodes

    def live_body(self, start: int, end: int, *, sort_by_symbol: bool, collected=None, tail=None) -> _Body:
        items, nodes = collected if collected is not None else self.collect(start, end)
        expected = max(0, min(end, self.universe.size) - max(0, start))
        available = [node for node in nodes.values() if node.get("available")]
        for node in available:
            # Complete means every expected stock is present: a reachable node that serves fewer
            # stocks (not live yet, CONFIG_ERROR, missing instruments) is never complete.
            node["missing_stock_count"] = max(0, node["expected_stock_count"] - node["stock_count"])
        coverage = {
            "complete": bool(nodes)
            and len(available) == len(nodes)
            and not any(node["missing_stock_count"] for node in available),
            "expected_stock_count": expected,
            "nodes": nodes,
        }
        if not available:
            return json_body(
                {
                    "service": "PSYGRID",
                    "status": "NODE_UNAVAILABLE",
                    "error": "no node owning these stocks is reachable",
                    "coverage": coverage,
                },
                503,
            )
        local = nodes.get(str(self.partition.node_id))
        reference = local if local is not None else available[0]
        session_status, session_date = reference.get("session_status"), reference.get("session_date")
        statuses = [node.get("session_status") for node in available]
        if coverage["complete"] and all(status == "LIVE" for status in statuses):
            status = "OK"
        elif "LIVE" in statuses:
            # Some partitions are live and some are missing: say so instead of serving a silent subset.
            status = "PARTIAL"
        else:
            status = payload_status(str(session_status))
        if tail is None:
            tail = CompressedTail(tail_parts(items, sort_by_symbol=sort_by_symbol))
        head = assemble_head(
            status=status,
            session_status=session_status,
            session_date=session_date,
            universe_size=self.universe.size,
            stock_count=len(items),
            extra={"coverage": coverage},
        )
        return _Body(head, 200, time.monotonic(), tail=tail)

    def live(self) -> _Body:
        return self._cached(("live",), 0, self.universe.size, True)

    def latest(self, candles: int) -> _Body:
        """``/public/live-latest.json``: every stock, only its last ``candles`` completed candles."""
        return self._cached(("latest", candles), 0, self.universe.size, True, last=candles)

    def shard(self, start: int, end: int) -> _Body:
        # Never sort a shard independently: it is a slice of the canonical universe order.
        return self._cached(("shard", start, end), start, end, False)

    def stock(self, symbol: str, local_only: bool = False) -> _Body:
        symbol = symbol.strip().upper()[:MAX_SYMBOL_LENGTH]
        index = self.universe.index_of(symbol)
        if index is None or self.partition.owns_index(index):
            return _Body(stock_body(self.state, symbol), 200, time.monotonic())
        owner = (index * self.partition.node_count) // self.universe.size
        if local_only:
            # A proxied request is never proxied again: two misconfigured nodes cannot ping-pong.
            return json_body({"service": "PSYGRID", "symbol": symbol, "status": "NOT_OWNER", "node_id": owner}, 503)
        peer = self.runtime.peers.get(owner)
        if peer is None:
            return json_body(
                {"service": "PSYGRID", "symbol": symbol, "status": "NODE_UNAVAILABLE", "node_id": owner}, 503
            )
        try:
            status, body = peer.stock(symbol)
        except PeerUnavailable as exc:
            return json_body(
                {
                    "service": "PSYGRID",
                    "symbol": symbol,
                    "status": "NODE_UNAVAILABLE",
                    "node_id": owner,
                    "error": str(exc)[:300],
                },
                503,
            )
        return _Body(body, status, time.monotonic())

    def fragments(self, start: int, end: int, if_version: str = "") -> bytes:
        start = max(0, start)
        end = min(self.universe.size, end)
        header = {
            "service": SERVICE_NAME,
            "partition": self.partition.describe(),
            "feed_status": self.state.feed_status,
            "generated_at_epoch": round(time.time(), 3),
        }
        version = self.state.content_version()
        if if_version and if_version == version:
            return encode_fragments(
                {
                    **header,
                    "version": version,
                    "session_status": self.state.session_status,
                    "session_date": self.state.session_date,
                    "not_modified": True,
                    "item_count": 0,
                },
                [],
            )
        # The version, session state and stocks are captured together (see render.snapshot).
        local = snapshot(self.state, max(start, self.partition.start), min(end, self.partition.end))
        return encode_fragments(
            {
                **header,
                "version": local.version,
                "session_status": local.session_status,
                "session_date": local.session_date,
                "item_count": len(local.items),
            },
            local.items,
        )


def root_payload(runtime) -> dict:
    return {
        "service": "PSYGRID",
        "runtime": SERVICE_NAME,
        "status": "ONLINE" if not runtime.config_error else "CONFIG_ERROR",
        "data_source": "DHAN",
        "output_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN",
        "synthetic_candles": False,
        "market_data_storage": "RAM_ONLY",
        "universe_size": runtime.universe.size,
        "node_id": runtime.partition.node_id,
        "node_count": runtime.partition.node_count,
        "partition": runtime.partition.describe(),
        "live_endpoint": "/public/live.json",
        "canonical_shard_family": "live-a-through-live-v",
        "live_endpoints": ["/public/live.json"]
        + [f"/public/live-{name}.json" for name, _, _ in shard_ranges(runtime.universe.size)],
        "stock_endpoint": "/public/stock/{SYMBOL}.json",
        "latest_endpoint": "/public/live-latest.json?candles=5",
        "live_timeframes": ["1m"],
        "depth_enabled": False,
        "indicators_enabled": False,
        "derivatives_enabled": False,
        "health_endpoint": "/public/health.json",
        "service_health_endpoint": "/health",
        "node_health_endpoint": "/health/node",
    }


def create_app(runtime, *, start_runtime: bool = True) -> FastAPI:
    service = LiveCoreService(runtime)
    runtime.session_end_hooks.append(service.clear)
    runtime.session_end_hooks.append(clear_caches)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        with contextlib.suppress(Exception):
            from anyio import to_thread

            to_thread.current_default_thread_limiter().total_tokens = runtime.cfg.http_threads
        if start_runtime:
            port = runtime.cfg.port
            runtime.watchdog = ServiceWatchdog(
                port,
                interval=15.0,
                startup_grace=180.0,
                max_fd_ratio=0.85,
                max_rss_mb=runtime.cfg.max_rss_mb,
                probe=lambda: http_probe(port, "/health/node"),
            )
            runtime.watchdog.start()
            runtime.start()
        try:
            yield
        finally:
            if start_runtime:
                runtime.watchdog.stop()
                runtime.stop()

    app = FastAPI(title="PSYGRID Live Core", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.live_core = service

    @app.get("/", response_class=Response)
    def root(request: Request) -> Response:
        return _respond(request, json_body(root_payload(runtime)))

    @app.get("/health/node", response_class=Response)
    def node_health(request: Request) -> Response:
        # Local only and never waits on a peer: this is what the systemd watchdog and peers probe.
        return _respond(request, json_body(runtime.node_health()))

    @app.get("/health", response_class=Response)
    def health(request: Request) -> Response:
        # Always HTTP 200 while the process answers; read status, reasons and cluster for the truth.
        return _respond(request, json_body(runtime.cluster_health()))

    @app.get("/public/health.json", response_class=Response)
    def public_health(request: Request) -> Response:
        # The full PSYGRID's /public/health.json schema; the cluster view rides along under "live_core".
        return _respond(request, json_body(runtime.public_health()))

    @app.get("/ready", response_class=Response)
    def ready(request: Request) -> Response:
        payload, ready_now = runtime.ready_payload()
        return _respond(request, json_body(payload, 200 if ready_now else 503))

    @app.get("/public/live.json", response_class=Response)
    def public_live(request: Request) -> Response:
        return _respond(request, service.live())

    @app.get("/public/live-latest.json", response_class=Response)
    def public_live_latest(request: Request, candles: int = Query(5, ge=0, le=MAX_LATEST_CANDLES)) -> Response:
        # Same contract as /public/live.json (all stocks, status, coverage) but only each stock's
        # newest candles: a few hundred KB instead of tens of MB, light enough for any browser.
        return _respond(request, service.latest(candles))

    def shard_route(start: int, end: int):
        def route(request: Request) -> Response:
            return _respond(request, service.shard(start, end))

        return route

    for name, start, end in shard_ranges(runtime.universe.size):
        app.add_api_route(
            f"/public/live-{name}.json",
            shard_route(start, end),
            methods=["GET"],
            response_class=Response,
            name=f"public_live_{name}",
        )

    @app.get("/public/stock/{symbol}.json", response_class=Response)
    def public_stock(symbol: str, request: Request, local: int = Query(0, ge=0, le=1)) -> Response:
        return _respond(request, service.stock(symbol, local_only=bool(local)))

    @app.get(FRAGMENTS_PATH, response_class=Response)
    def internal_fragments(
        start: int = Query(0, ge=0), end: int = Query(10_000, ge=0), if_version: str = Query("", max_length=64)
    ) -> Response:
        return Response(
            content=service.fragments(start, end, if_version), media_type="text/plain", headers=dict(NO_CACHE_HEADERS)
        )

    return app
