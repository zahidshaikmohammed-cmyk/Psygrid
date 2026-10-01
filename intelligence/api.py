"""The ``/v2`` intelligence API: a separate FastAPI app on its own port.

It serves what the live runner computed (the latest ``Snapshot``) and the event
store. It never touches PSYGRID's process, routes or data; PSYGRID's ``/public``
API is unchanged. Every data route needs an API key (``X-API-Key`` header or
``Authorization: Bearer``), is rate-limited per key, and validates its inputs.
``/v2/health`` and ``/v2/ready`` are public so monitors need no key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Path, Query, Request, Response, WebSocket
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketDisconnect

from intelligence.anomaly import CLASSES, EXTREME, UNUSUAL
from intelligence.event_store import SEVERITY_RANK, EventStore
from intelligence.events import CATEGORIES, ENGINE_VERSION, EVENT_TYPES, SCHEMA_VERSION
from intelligence.features import CATALOGUE, FEATURE_VERSION, MARKET_FEATURES
from intelligence.keys import KeyStore, RateLimiter
from intelligence.live import LiveRunner
from intelligence.settings import Settings
from intelligence.similarity import instrument_matches, market_matches

API_VERSION = "2.0.0"
KEY_RE = r"^[A-Za-z0-9&._-]{1,40}$"
DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
EVENT_ID_RE = r"^evt_[0-9a-f]{16}$"
STREAM_POLL_SECONDS = 1.0
STREAM_HEARTBEAT_SECONDS = 20.0
SIMILARITY_CONCURRENCY = 2
SIMILARITY_WAIT_SECONDS = 20.0

access_log = logging.getLogger("psygrid.intelligence.access")


class _LRU:
    def __init__(self, size: int):
        self.size, self.data, self.lock = size, OrderedDict(), threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.data:
                self.data.move_to_end(key)
                return self.data[key]
        return None

    def put(self, key, value) -> None:
        with self.lock:
            self.data[key] = value
            self.data.move_to_end(key)
            while len(self.data) > self.size:
                self.data.popitem(last=False)


def _csv(value: str | None, pattern: str = KEY_RE, limit: int = 200) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    if len(items) > limit or any(not re.match(pattern, v) for v in items):
        raise HTTPException(422, f"expected up to {limit} comma-separated values matching {pattern}")
    return items


def create_app(settings: Settings, runner: LiveRunner | None = None, start_live: bool | None = None) -> FastAPI:
    runner = runner or LiveRunner(settings)
    store: EventStore = runner.store
    keys = KeyStore(settings.keys_file)
    limiter = RateLimiter(settings.rate_per_minute, settings.rate_burst)
    similarity_slots = threading.BoundedSemaphore(SIMILARITY_CONCURRENCY)
    similarity_cache = _LRU(256)
    streams: dict[str, int] = {}
    streams_lock = threading.Lock()
    live = settings.live_enabled if start_live is None else start_live

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if live:
            runner.start()
        yield
        runner.stop()  # graceful shutdown: the current step finishes, then the thread exits

    app = FastAPI(
        title="PSYGRID Intelligence API",
        version=API_VERSION,
        lifespan=lifespan,
        docs_url="/v2/docs",
        openapi_url="/v2/openapi.json",
        redoc_url=None,
    )
    app.state.runner = runner

    @app.middleware("http")
    async def access_and_headers(request: Request, call_next):
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            access_log.exception("unhandled error on %s", request.url.path)
            response = JSONResponse({"detail": "internal error"}, status_code=500)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-PSYGRID-API-Version"] = API_VERSION
        access_log.info(
            json.dumps(
                {
                    "method": request.method,
                    "path": request.url.path,  # never the query string: it may carry a key
                    "status": response.status_code,
                    "ms": round((time.perf_counter() - started) * 1000, 1),
                    "key": getattr(request.state, "key_id", None),
                    "client": request.client.host if request.client else None,
                }
            )
        )
        return response

    def _key_from(headers, query_key: str | None = None) -> str | None:
        auth = headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        return headers.get("x-api-key") or query_key

    def authenticate(request: Request, response: Response) -> str:
        if not settings.require_keys:
            key_id, per_minute = f"ip:{request.client.host if request.client else '-'}", None
        else:
            record = keys.verify(_key_from(request.headers))
            if record is None:
                raise HTTPException(401, "a valid API key is required", headers={"WWW-Authenticate": "Bearer"})
            key_id, per_minute = record["id"], record.get("rate_per_minute")
        request.state.key_id = key_id
        allowed, remaining, wait = limiter.check(key_id, per_minute)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        if not allowed:
            raise HTTPException(429, "rate limit exceeded", headers={"Retry-After": str(max(1, round(wait)))})
        return key_id

    def current():
        snapshot = runner.snapshot
        if snapshot is None:
            raise HTTPException(503, "no intelligence computed yet; the engine is starting or no session is archived")
        return snapshot

    # --- public -------------------------------------------------------------------------------

    @app.get("/v2/health")
    def health():
        live_health = runner.health()
        try:
            stats = store.stats()
            store_ok = True
        except Exception as exc:  # a broken store is reported, not raised
            stats, store_ok = {"error": type(exc).__name__}, False
        degraded = (
            not store_ok
            or live_health["state"] in ("STALE",)
            or (live_health["last_error_at"] is not None and live_health["state"] == "WAITING_FOR_DATA")
        )
        return {
            "status": "DEGRADED" if degraded else "OK",
            "service": "psygrid-intelligence",
            "api_version": API_VERSION,
            "engine_version": ENGINE_VERSION,
            "live": live_health,
            "event_store": stats,
            "auth": {"required": settings.require_keys, "keys_configured": keys.configured()},
        }

    @app.get("/v2/ready")
    def ready():
        if runner.snapshot is None:
            return JSONResponse({"ready": False}, status_code=503)
        return {"ready": True}

    # --- authenticated -----------------------------------------------------------------------

    v2 = APIRouter(prefix="/v2", dependencies=[Depends(authenticate)])

    @v2.get("/meta")
    def meta():
        return {
            "api_version": API_VERSION,
            "engine_version": ENGINE_VERSION,
            "event_schema": SCHEMA_VERSION,
            "feature_version": FEATURE_VERSION,
            "features": {
                name: {"unit": s.unit, "min_bars": s.min_bars, "purpose": s.purpose} for name, s in CATALOGUE.items()
            },
            "market_features": MARKET_FEATURES,
            "anomaly_classes": list(CLASSES),
            "event_types": EVENT_TYPES,
            "event_categories": list(CATEGORIES),
            "severities": list(SEVERITY_RANK),
        }

    @v2.get("/market")
    def market():
        return current().market()

    @v2.get("/observations")
    def observations(
        keys: Annotated[str | None, Query(description="comma-separated symbols")] = None,
        sector: Annotated[str | None, Query(pattern=KEY_RE)] = None,
        fields: Annotated[str | None, Query(description="comma-separated feature names")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
    ):
        snapshot = current()
        names = _csv(fields, r"^[a-z0-9_]{1,40}$", 50)
        unknown = [f for f in names or () if f not in CATALOGUE]
        if unknown:
            raise HTTPException(422, f"unknown fields: {', '.join(unknown)}")
        rows = snapshot.observations(_csv(keys), names)
        if sector:
            rows = [r for r in rows if r["sector"] == sector]
        return {
            **snapshot.header(),
            "total": len(rows),
            "offset": offset,
            "observations": rows[offset : offset + limit],
        }

    @v2.get("/instruments/{key}")
    def instrument(key: Annotated[str, Path(pattern=KEY_RE)]):
        view = current().instrument(key)
        if view is None:
            raise HTTPException(404, f"{key} is not in the universe")
        return view

    @v2.get("/anomalies")
    def anomalies(
        minimum: Annotated[str, Query(pattern=f"^({UNUSUAL}|{EXTREME})$")] = UNUSUAL,
        measure: Annotated[str | None, Query(pattern="^(volume|return|range)$")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        snapshot = current()
        rows = [a for a in snapshot.anomalies(minimum) if measure is None or a["measure"] == measure]
        return {**snapshot.header(), "total": len(rows), "anomalies": rows[:limit], "market": snapshot.report.market}

    @v2.get("/relationships")
    def relationships(
        kind: Annotated[str | None, Query(pattern=r"^[a-z_]{1,40}$")] = None,
        subject: Annotated[str | None, Query(pattern=KEY_RE)] = None,
        flagged_only: bool = True,
        limit: Annotated[int, Query(ge=1, le=5000)] = 200,
    ):
        snapshot = current()
        rows = snapshot.relationship_views(kind, subject)
        if flagged_only:
            rows = [r for r in rows if r["classification"] in (UNUSUAL, EXTREME)]
        rows.sort(key=lambda r: -abs(r["z"] or 0))
        return {**snapshot.header(), "total": len(rows), "relationships": rows[:limit]}

    @v2.get("/events")
    def events(
        date: Annotated[str | None, Query(pattern=DATE_RE)] = None,
        date_from: Annotated[str | None, Query(pattern=DATE_RE)] = None,
        date_to: Annotated[str | None, Query(pattern=DATE_RE)] = None,
        subject: Annotated[str | None, Query(pattern=KEY_RE)] = None,
        instrument: Annotated[str | None, Query(pattern=KEY_RE)] = None,
        event_type: Annotated[str | None, Query(pattern=r"^[a-z_]{1,40}$")] = None,
        category: Annotated[str | None, Query(pattern=r"^[A-Z_]{1,20}$")] = None,
        scope: Annotated[str | None, Query(pattern="^(INSTRUMENT|SECTOR|INDEX|MARKET)$")] = None,
        min_severity: Annotated[str | None, Query(pattern="^(LOW|MEDIUM|HIGH)$")] = None,
        after_seq: Annotated[int | None, Query(ge=0)] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ):
        rows = store.search(
            session_date=date,
            date_from=date_from,
            date_to=date_to,
            subject=subject,
            instrument=instrument,
            event_type=event_type,
            category=category,
            scope=scope,
            min_severity=min_severity,
            after_seq=after_seq,
            limit=limit,
        )
        return {"count": len(rows), "latest_seq": store.latest_seq(), "events": rows}

    @v2.get("/events/{event_id}")
    def event(event_id: Annotated[str, Path(pattern=EVENT_ID_RE)]):
        found = store.get(event_id)
        if found is None:
            raise HTTPException(404, "no such event")
        return found

    def _similar(cache_key, compute):
        cached = similarity_cache.get(cache_key)
        if cached is not None:
            return cached
        if not similarity_slots.acquire(timeout=SIMILARITY_WAIT_SECONDS):
            raise HTTPException(503, "similarity search is busy; retry shortly", headers={"Retry-After": "5"})
        try:
            result = compute()
        finally:
            similarity_slots.release()
        similarity_cache.put(cache_key, result)
        return result

    @v2.get("/historical-matches/market")
    def market_history(k: Annotated[int, Query(ge=1, le=50)] = 10):
        snapshot = current()
        return _similar(
            ("market", snapshot.as_of, k),
            lambda: market_matches(
                snapshot.frame, settings.archive_dir, settings.store_dir, settings.similarity_lookback, k
            ),
        )

    @v2.get("/historical-matches/instruments/{key}")
    def instrument_history(key: Annotated[str, Path(pattern=KEY_RE)], k: Annotated[int, Query(ge=1, le=50)] = 10):
        snapshot = current()
        result = _similar(
            (key, snapshot.as_of, k),
            lambda: instrument_matches(
                snapshot.frame, key, settings.archive_dir, settings.store_dir, settings.similarity_lookback, k
            ),
        )
        if result.get("status") == "UNKNOWN_INSTRUMENT":
            raise HTTPException(404, f"{key} is not in the universe")
        return result

    app.include_router(v2)

    # --- streaming ----------------------------------------------------------------------------

    @app.websocket("/v2/stream")
    async def stream(websocket: WebSocket):
        params = websocket.query_params
        if settings.require_keys:
            record = keys.verify(_key_from(websocket.headers, params.get("api_key")))
            if record is None:
                await websocket.close(code=1008, reason="a valid API key is required")
                return
            key_id = record["id"]
        else:
            key_id = f"ip:{websocket.client.host if websocket.client else '-'}"
        allowed, _, _ = limiter.check(key_id)
        try:
            types = set(_csv(params.get("event_types"), r"^[a-z_]{1,40}$", 50) or ())
            instruments = set(_csv(params.get("instruments")) or ())
            minimum = params.get("min_severity")
            if minimum is not None and minimum not in SEVERITY_RANK:
                raise HTTPException(422, "min_severity must be LOW, MEDIUM or HIGH")
            after = int(params.get("after_seq")) if params.get("after_seq") else None
            if after is not None and after < 0:
                raise ValueError
        except (HTTPException, ValueError):
            await websocket.close(code=1008, reason="invalid parameters")
            return
        if not allowed:
            await websocket.close(code=1013, reason="rate limit exceeded")
            return
        with streams_lock:
            total = sum(streams.values())
            if total >= settings.max_streams or streams.get(key_id, 0) >= settings.max_streams_per_key:
                full = True
            else:
                full = False
                streams[key_id] = streams.get(key_id, 0) + 1
        if full:
            await websocket.close(code=1013, reason="too many streams")
            return
        snapshots = params.get("snapshots", "true").lower() != "false"
        receiver = None
        try:
            await websocket.accept()
            cursor = after if after is not None else await asyncio.to_thread(store.latest_seq)
            await websocket.send_json(
                {"type": "hello", "api_version": API_VERSION, "event_schema": SCHEMA_VERSION, "after_seq": cursor}
            )
            seen_version, last_sent = -1, time.monotonic()
            receiver = asyncio.ensure_future(websocket.receive_text())
            while True:
                rows = await asyncio.to_thread(store.search, after_seq=cursor, limit=500)
                for row in rows:
                    cursor = row["seq"]
                    if types and row["event_type"] not in types:
                        continue
                    if instruments and not instruments.intersection(
                        row["affected_instruments"] + [row["subject"]["key"]]
                    ):
                        continue
                    if minimum and SEVERITY_RANK[row["severity"]] < SEVERITY_RANK[minimum]:
                        continue
                    await websocket.send_json({"type": "event", "seq": row["seq"], "event": row})
                    last_sent = time.monotonic()
                snapshot = runner.snapshot
                if snapshots and snapshot is not None and runner.snapshot_version != seen_version:
                    seen_version = runner.snapshot_version
                    await websocket.send_json(
                        {
                            "type": "snapshot",
                            **snapshot.header(),
                            "anomaly_counts": snapshot.report.counts(),
                            "market_measures": snapshot.report.market,
                        }
                    )
                    last_sent = time.monotonic()
                if time.monotonic() - last_sent >= STREAM_HEARTBEAT_SECONDS:
                    await websocket.send_json({"type": "heartbeat", "latest_seq": cursor})
                    last_sent = time.monotonic()
                done, _ = await asyncio.wait({receiver}, timeout=STREAM_POLL_SECONDS)
                if done:
                    message = receiver.result()  # raises WebSocketDisconnect when the client leaves
                    if message.strip() == '{"type":"ping"}' or message.strip() == "ping":
                        await websocket.send_json({"type": "pong", "latest_seq": cursor})
                    receiver = asyncio.ensure_future(websocket.receive_text())
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            if receiver is not None:
                if receiver.done():
                    if not receiver.cancelled():
                        receiver.exception()  # the client's disconnect: retrieved so it is not logged as an error
                else:
                    receiver.cancel()
            with streams_lock:
                streams[key_id] -= 1
                if streams[key_id] <= 0:
                    streams.pop(key_id, None)

    return app
