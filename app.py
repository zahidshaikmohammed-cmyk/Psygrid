from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, Response

from config import load_instruments, load_settings
from dhan_api import DhanAPI
from feed import LiveFeed
from output import dumps_json, market_live_json, stock_json
from session import SessionManager
from state import PsygridState


app = FastAPI(title="Psygrid", docs_url=None, redoc_url=None)
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
        state = PsygridState(settings)
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
    return Response(
        content=dumps_json(payload),
        media_type="application/json",
        status_code=status_code,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/", response_class=Response)
def root() -> Response:
    return json_response({
        "service": "PSYGRID",
        "status": "ONLINE" if not config_error else "CONFIG_ERROR",
        "data_source": "DHAN",
        "synthetic_candles": False,
        "storage": "RAM_ONLY",
        "live_endpoint": "/public/live.json",
        "live_endpoints": ["/public/live-a.json", "/public/live-b.json"],
    })


@app.get("/health", response_class=Response)
def health() -> Response:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    snap = state.snapshot() if state else {}
    return json_response({"service": "PSYGRID", **snap})


@app.get("/public/live.json", response_class=Response)
def public_live() -> Response:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    return json_response(market_live_json(state))


@app.get("/public/live-a.json", response_class=Response)
def public_live_a() -> Response:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    return json_response(market_live_json(state, (0, 45)))


@app.get("/public/live-b.json", response_class=Response)
def public_live_b() -> Response:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    return json_response(market_live_json(state, (45, 90)))


@app.get("/public/stock/{symbol}.json", response_class=Response)
def public_stock(symbol: str) -> Response:
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    return json_response(stock_json(state, symbol))


@app.get("/public/stock/{symbol}/{timeframe}.json", response_class=Response)
def public_stock_timeframe(symbol: str, timeframe: str) -> Response:
    timeframe = timeframe.lower()
    if timeframe not in {"1m", "5m", "15m", "1h", "1d", "1w"}:
        return json_response({"service": "PSYGRID", "status": "INVALID_TIMEFRAME"}, 400)
    if config_error:
        return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    return json_response(stock_json(state, symbol, timeframe))


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False)
