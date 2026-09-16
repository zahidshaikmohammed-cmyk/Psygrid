from __future__ import annotations

import os
from contextlib import asynccontextmanager

import orjson
import uvicorn
from fastapi import FastAPI, Response
from starlette.middleware.gzip import GZipMiddleware

from config import load_instruments, load_settings
from dhan_api import DhanAPI
from feed_runtime import LiveFeed
from output import LIVE_TIMEFRAMES, stock_json
from output_runtime import market_live_json
from session import SessionManager
from state_runtime import RuntimeFreshnessState

settings = state = manager = None
config_error = ""


def startup() -> None:
    global settings, state, manager, config_error
    config_error = ""
    try:
        settings = load_settings()
        instruments = load_instruments()
        if len(instruments) != settings.max_instruments:
            raise RuntimeError(f"Universe integrity failure: expected {settings.max_instruments}, got {len(instruments)}")
        state = RuntimeFreshnessState(settings)
        dhan_api = DhanAPI(settings)
        manager = SessionManager(settings, state, dhan_api, LiveFeed(settings, state, instruments), instruments)
        manager.start()
    except Exception as exc:
        config_error = str(exc)


def shutdown() -> None:
    global manager
    if manager is not None:
        manager.stop()
        manager = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    startup()
    yield
    shutdown()


app = FastAPI(title="Psygrid", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)


def json_response(payload: dict, status_code: int = 200) -> Response:
    return Response(
        content=orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE),
        media_type="application/json",
        status_code=status_code,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0", "Vary": "Accept-Encoding"},
    )


def _error_response() -> Response | None:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    if state is None:
        return json_response({"service": "PSYGRID", "status": "STARTING"}, 503)
    return None


@app.get("/", response_class=Response)
def root() -> Response:
    return json_response({
        "service": "PSYGRID", "status": "ONLINE" if not config_error else "CONFIG_ERROR", "data_source": "DHAN",
        "output_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN", "synthetic_candles": False,
        "universe_size": 450, "live_endpoint": "/public/live.json",
        "canonical_shard_family": "live-a-through-live-j",
        "live_endpoints": ["/public/live.json"] + [f"/public/live-{x}.json" for x in "abcdefghij"],
        "legacy_overlapping_endpoints": [f"/public/live-{i:02d}.json" for i in range(1, 7)],
        "live_timeframes": ["1m"],
    })


@app.get("/health", response_class=Response)
def health() -> Response:
    return json_response({"service": "PSYGRID", "status": "OK"})


@app.get("/ready", response_class=Response)
def ready() -> Response:
    error = _error_response()
    if error:
        return error
    snap = state.snapshot()
    ready_now = bool(snap.get("session_status") == "LIVE" and snap.get("feed_status") == "CONNECTED" and snap.get("stock_count") == 450 and snap.get("subscribed_count") == 450 and snap.get("live_stock_count") == 450 and snap.get("stream_health") == "FULL_LIVE")
    return json_response({"service": "PSYGRID", "ready": ready_now, **snap}, 200 if ready_now else 503)


@app.get("/public/live.json", response_class=Response)
def public_live() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state))


def _public_live_range(start: int, end: int, preserve_instrument_order: bool = False, legacy: bool = False) -> Response:
    error = _error_response()
    if error:
        return error
    payload = market_live_json(state, (start, end), preserve_instrument_order)
    payload.setdefault("shard", {})["canonical"] = not legacy
    payload["shard"]["family"] = "live-a-through-live-j" if not legacy else "legacy-overlapping"
    payload["shard"]["range"] = [start, end]
    return json_response(payload)


for route, start, end, preserve in [
    ("a", 0, 45, False), ("b", 45, 90, False), ("c", 90, 135, False), ("d", 135, 180, False),
    ("e", 180, 225, False), ("f", 225, 270, False), ("g", 270, 315, True), ("h", 315, 360, True),
    ("i", 360, 405, True), ("j", 405, 450, True),
]:
    globals()[f"public_live_{route}"] = app.get(f"/public/live-{route}.json", response_class=Response)(lambda start=start, end=end, preserve=preserve: _public_live_range(start, end, preserve, False))

for route, start, end in [("01", 0, 15), ("02", 15, 30), ("03", 30, 45), ("04", 45, 60), ("05", 60, 75), ("06", 75, 90)]:
    globals()[f"public_live_{route}"] = app.get(f"/public/live-{route}.json", response_class=Response)(lambda start=start, end=end: _public_live_range(start, end, False, True))


@app.get("/public/stock/{symbol}.json", response_class=Response)
def public_stock(symbol: str) -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(stock_json(state, symbol))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")), access_log=False)
