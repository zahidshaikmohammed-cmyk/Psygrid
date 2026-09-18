from __future__ import annotations

import os
from contextlib import asynccontextmanager

import orjson
import requests
import uvicorn
from fastapi import FastAPI, Response
from starlette.middleware.gzip import GZipMiddleware

from config import UNIVERSE_SIZE, load_instruments, load_settings
from dhan_api import DhanAPI
from feed_runtime import LiveFeed
from output import market_live_json, stock_json
from session import SessionManager
from state_runtime import RuntimeFreshnessState
from indicator_runtime import IndicatorRuntime

settings = state = manager = indicator_runtime = None
config_error = ""
indicator_error = ""


def indicator_source_payload(_source_state):
    url = os.getenv("PSYGRID_INDICATOR_SOURCE_URL", "http://127.0.0.1:10000/public/live.json")
    response = requests.get(url, timeout=10, headers={"Cache-Control": "no-cache"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Psygrid indicator source endpoint did not return an object")
    return payload


def startup() -> None:
    global settings, state, manager, indicator_runtime, config_error, indicator_error
    config_error = ""
    indicator_error = ""
    try:
        settings = load_settings()
        instruments = load_instruments()
        if len(instruments) != settings.max_instruments:
            raise RuntimeError(
                f"Universe integrity failure: expected {settings.max_instruments}, got {len(instruments)}"
            )

        # Preserve the one canonical universe order returned by config.
        # Shards are fixed contiguous slices of this exact order.
        symbols = [item.symbol for item in instruments]
        if len(symbols) != len(set(symbols)):
            seen = set()
            duplicates = []
            for symbol in symbols:
                if symbol in seen and symbol not in duplicates:
                    duplicates.append(symbol)
                seen.add(symbol)
            raise RuntimeError(
                f"Universe integrity failure: duplicate symbols in canonical universe: {duplicates}"
            )
        security_ids = [str(item.security_id) for item in instruments]
        if len(security_ids) != len(set(security_ids)):
            raise RuntimeError(
                "Universe integrity failure: duplicate security IDs in canonical universe"
            )
        if len(symbols) != settings.max_instruments:
            raise RuntimeError(
                f"Universe integrity failure: expected {settings.max_instruments} unique symbols, got {len(symbols)}"
            )

        state = RuntimeFreshnessState(settings)
        dhan_api = DhanAPI(settings)
        manager = SessionManager(
            settings,
            state,
            dhan_api,
            LiveFeed(settings, state, instruments),
            instruments,
        )
        manager.start()

        # Additive derived-data layer. It consumes the exact same canonical
        # PSYGRID live payload builder used by /public/live.json, so the core
        # OHLCV endpoints remain untouched and remain the source of truth.
        try:
            indicator_runtime = IndicatorRuntime(state, indicator_source_payload, interval_seconds=1.0)
            indicator_runtime.start()
        except Exception as exc:
            indicator_runtime = None
            indicator_error = str(exc)
    except Exception as exc:
        config_error = str(exc)



def shutdown() -> None:
    global manager, indicator_runtime
    if indicator_runtime is not None:
        indicator_runtime.stop()
        indicator_runtime = None
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
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Vary": "Accept-Encoding",
        },
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
        "service": "PSYGRID",
        "status": "ONLINE" if not config_error else "CONFIG_ERROR",
        "data_source": "DHAN",
        "output_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN",
        "synthetic_candles": False,
        "universe_size": UNIVERSE_SIZE,
        "live_endpoint": "/public/live.json",
        "canonical_shard_family": "live-a-through-live-v",
        "live_endpoints": ["/public/live.json"] + [f"/public/live-{x}.json" for x in "abcdefghijklmnopqrstuv"],
        "live_timeframes": ["1m"],
        "depth_enabled": False,
        "indicators_enabled": False,
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
    ready_now = bool(
        snap.get("session_status") == "LIVE"
        and snap.get("feed_status") == "CONNECTED"
        and snap.get("stock_count") == UNIVERSE_SIZE
        and snap.get("subscribed_count") == UNIVERSE_SIZE
        and snap.get("live_stock_count") == UNIVERSE_SIZE
        and snap.get("stream_health") == "FULL_LIVE"
    )
    return json_response({"service": "PSYGRID", "ready": ready_now, **snap}, 200 if ready_now else 503)


@app.get("/public/live.json", response_class=Response)
def public_live() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(market_live_json(state))



def _public_live_range(start: int, end: int) -> Response:
    error = _error_response()
    if error:
        return error
    # Never sort a shard independently. Every shard is a slice of the same
    # canonical 450-instrument order used by the feed and configuration.
    return json_response(market_live_json(state, (start, end), True))


# Canonical 450-stock universe: exactly 10 disjoint shards of 45.
SHARD_RANGES = tuple((name, index * 45, (index + 1) * 45) for index, name in enumerate("abcdefghijklmnopqrstuv"))

for route, start, end in SHARD_RANGES:
    globals()[f"public_live_{route}"] = app.get(
        f"/public/live-{route}.json", response_class=Response
    )(lambda start=start, end=end: _public_live_range(start, end))


@app.get("/public/stock/{symbol}.json", response_class=Response)
def public_stock(symbol: str) -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(stock_json(state, symbol))


# ---------------------------------------------------------------------------
# Additive Master Indicator endpoints. These do not modify or wrap the
# existing live OHLCV endpoints above.
# ---------------------------------------------------------------------------


def _indicator_error_response() -> Response | None:
    error = _error_response()
    if error:
        return error
    if indicator_error:
        return json_response({
            "service": "PSYGRID_MASTER_INDICATOR",
            "engine_version": "1.0.0",
            "status": "UNAVAILABLE",
            "error": indicator_error,
        }, 503)
    if indicator_runtime is None:
        return json_response({
            "service": "PSYGRID_MASTER_INDICATOR",
            "engine_version": "1.0.0",
            "status": "STARTING",
        }, 503)
    return None


@app.get("/public/indicators.json", response_class=Response)
def public_indicators() -> Response:
    error = _indicator_error_response()
    if error:
        return error
    return json_response(indicator_runtime.snapshot())


@app.get("/public/indicators/{symbol}.json", response_class=Response)
def public_indicator_stock(symbol: str) -> Response:
    error = _indicator_error_response()
    if error:
        return error
    return json_response(indicator_runtime.stock(symbol))



def _public_indicator_range(start: int, end: int) -> Response:
    error = _indicator_error_response()
    if error:
        return error
    return json_response(indicator_runtime.snapshot((start, end)))


for route, start, end in SHARD_RANGES:
    globals()[f"public_indicators_{route}"] = app.get(
        f"/public/indicators-{route}.json", response_class=Response
    )(lambda start=start, end=end: _public_indicator_range(start, end))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")), access_log=False)
