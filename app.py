from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, Response

from config import load_instruments, load_settings
from dhan_api import DhanAPI
from feed import LiveFeed
from output import market_live_text, stock_text
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


@app.get("/", response_class=Response)
def root() -> Response:
    return Response(
        content="PSYGRID\nSTATUS=ONLINE\nDATA=DHAN\nSYNTHETIC_CANDLES=FALSE\n",
        media_type="text/plain",
    )


@app.get("/health", response_class=Response)
def health() -> Response:
    if config_error:
        return Response(content=f"STATUS=CONFIG_ERROR\nERROR={config_error}\n", media_type="text/plain", status_code=200)
    snap = state.snapshot() if state else {}
    return Response(
        content=(
            "STATUS=OK\n"
            f"SESSION_STATUS={snap.get('session_status', 'CLOSED')}\n"
            f"FEED_STATUS={snap.get('feed_status', 'STOPPED')}\n"
        ),
        media_type="text/plain",
    )


@app.get("/public/live.txt", response_class=Response)
def public_live() -> Response:
    if config_error:
        return Response(content=f"PSYGRID=LIVE_MARKET_DATA\nSTATUS=CONFIG_ERROR\nERROR={config_error}\n", media_type="text/plain")
    return Response(content=market_live_text(state), media_type="text/plain")


@app.get("/public/stock/{symbol}.txt", response_class=Response)
def public_stock(symbol: str) -> Response:
    if config_error:
        return Response(content=f"PSYGRID=STOCK\nSTATUS=CONFIG_ERROR\nERROR={config_error}\n", media_type="text/plain")
    return Response(content=stock_text(state, symbol), media_type="text/plain")


@app.get("/public/stock/{symbol}/{timeframe}.txt", response_class=Response)
def public_stock_timeframe(symbol: str, timeframe: str) -> Response:
    timeframe = timeframe.lower()
    if timeframe not in {"1m", "5m", "15m", "1h", "1d", "1w"}:
        return Response(content="STATUS=INVALID_TIMEFRAME\n", media_type="text/plain", status_code=400)
    if config_error:
        return Response(content=f"PSYGRID=STOCK\nSTATUS=CONFIG_ERROR\nERROR={config_error}\n", media_type="text/plain")
    return Response(content=stock_text(state, symbol, timeframe), media_type="text/plain")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port, access_log=False)
