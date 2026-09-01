from __future__ import annotations

import os

import orjson
import uvicorn
from fastapi import FastAPI, Response
from starlette.middleware.gzip import GZipMiddleware

from config import load_instruments, load_settings
from dhan_api import DhanAPI
from feed_runtime import LiveFeed
from output_runtime import market_live_json
from output import stock_json
from session import SessionManager
from state_runtime import RuntimeFreshnessState


app = FastAPI(title="Psygrid", docs_url=None, redoc_url=None)
# Compress large JSON responses on the wire. The underlying JSON/state is unchanged.
# A lower compression level keeps the free instance responsive while still
# shrinking repetitive market JSON substantially.
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)

settings = None
state = None
manager = None
config_error = ""


@app.on_event("startup")
def startup() -> None:
    global settings, state, manager, config_error
    try:
        settings = load_settings()
        instruments = load_instruments()
        if len(instruments) > settings.max_instruments:
            raise RuntimeError(
                f"Configured {len(instruments)} instruments; MAX_INSTRUMENTS={settings.max_instruments}"
            )
        state = RuntimeFreshnessState(settings)
        dhan_api = DhanAPI(settings)
        feed = LiveFeed(settings, state, instruments)
        manager = SessionManager(settings, state, dhan_api, feed, instruments)
        if instruments:
            manager.start()
        else:
            config_error = "NO_INSTRUMENTS_CONFIGURED"
    except Exception as exc:
        config_error = str(exc)


@app.on_event("shutdown")
def shutdown() -> None:
    if manager is not None:
        manager.stop()


def json_response(payload: dict, status_code: int = 200) -> Response:
    # orjson is used only for public serialization; all backend state and
    # calculations remain unchanged. This reduces CPU time for large payloads.
    body = orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE)
    return Response(
        content=body,
        media_type="application/json",
        status_code=status_code,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Vary": "Accept-Encoding",
        },
    )


def _error_response() -> Response | None:
    if config_error:
        return json_response(
            {"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error}
        )
    return None


@app.get("/", response_class=Response)
def root() -> Response:
    return json_response({
        "service": "PSYGRID",
        "status": "ONLINE" if not config_error else "CONFIG_ERROR",
        "data_source": "DHAN",
        "synthetic_candles": False,
        "storage": "RAM_ONLY",
        "live_endpoint": "/public/live.json",
        "live_endpoints": [
            "/public/live-a.json",
            "/public/live-b.json",
            "/public/live-c.json",
            "/public/live-d.json",
            "/public/live-01.json",
            "/public/live-02.json",
            "/public/live-03.json",
            "/public/live-04.json",
            "/public/live-05.json",
            "/public/live-06.json",
        ],
    })


@app.get("/health", response_class=Response)
def health() -> Response:
    error = _error_response()
    if error:
        return error
    snap = state.snapshot() if state else {}
    return json_response({"service": "PSYGRID", **snap})


@app.get("/public/live.json", response_class=Response)
def public_live() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state))


@app.get("/public/live-a.json", response_class=Response)
def public_live_a() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state, (0, 45)))


@app.get("/public/live-b.json", response_class=Response)
def public_live_b() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state, (45, 90)))


@app.get("/public/live-c.json", response_class=Response)
def public_live_c() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state, (90, 135)))


@app.get("/public/live-d.json", response_class=Response)
def public_live_d() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state, (135, 180)))


# Six smaller, identical-schema views. They do not create extra Dhan feeds;
# they only expose slices of the same 180-stock RAM snapshot for faster machine
# and browser consumption.
def _public_live_slice(start: int, end: int) -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state, (start, end)))


@app.get("/public/live-01.json", response_class=Response)
def public_live_01() -> Response:
    return _public_live_slice(0, 15)


@app.get("/public/live-02.json", response_class=Response)
def public_live_02() -> Response:
    return _public_live_slice(15, 30)


@app.get("/public/live-03.json", response_class=Response)
def public_live_03() -> Response:
    return _public_live_slice(30, 45)


@app.get("/public/live-04.json", response_class=Response)
def public_live_04() -> Response:
    return _public_live_slice(45, 60)


@app.get("/public/live-05.json", response_class=Response)
def public_live_05() -> Response:
    return _public_live_slice(60, 75)


@app.get("/public/live-06.json", response_class=Response)
def public_live_06() -> Response:
    return _public_live_slice(75, 90)


@app.get("/public/stock/{symbol}.json", response_class=Response)
def public_stock(symbol: str) -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(stock_json(state, symbol))


@app.get("/public/stock/{symbol}/{timeframe}.json", response_class=Response)
def public_stock_timeframe(symbol: str, timeframe: str) -> Response:
    timeframe = timeframe.lower()
    if timeframe not in {"1m", "5m", "15m", "1h", "1d", "1w"}:
        return json_response({"service": "PSYGRID", "status": "INVALID_TIMEFRAME"}, 400)
    error = _error_response()
    if error:
        return error
    return json_response(stock_json(state, symbol, timeframe))


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False)
