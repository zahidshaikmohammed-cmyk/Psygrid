from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

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
from index_layer import IndexLayerManager
from nifty_options import NiftyOptionsManager, nifty_options_json
from nifty_depth import NiftyDepthManager, nifty_depth_json
from banknifty_options import BankNiftyOptionsManager, banknifty_options_json
from banknifty_depth import BankNiftyDepthManager, banknifty_depth_json
from midcpnifty_options import MidcapNiftyOptionsManager, midcpnifty_options_json
from midcpnifty_depth import MidcapNiftyDepthManager, midcpnifty_depth_json
from midcpnifty_underlying import MidcapNiftyUnderlyingManager
from underlying_indicators import UnderlyingIndicatorRuntime
from futures_layer import FuturesManager, futures_json
from market_breadth import build_market_breadth, build_sector_breadth
from global_context import GlobalContextManager, global_context_json
from rbi_news import RbiNewsManager, rbi_news_json
from health_monitor import component_health, build_health

settings = state = manager = indicator_runtime = index_manager = None
nifty_options_manager = nifty_depth_manager = None
banknifty_options_manager = banknifty_depth_manager = None
midcpnifty_options_manager = midcpnifty_depth_manager = None
midcpnifty_underlying_manager = None
nifty_underlying_indicators = banknifty_underlying_indicators = midcpnifty_underlying_indicators = None
nifty_futures_manager = banknifty_futures_manager = None
global_context_manager = rbi_news_manager = None
config_error = ""
indicator_error = ""
index_error = ""


def indicator_source_payload(_source_state):
    url = os.getenv("PSYGRID_INDICATOR_SOURCE_URL", "http://127.0.0.1:10000/public/live.json")
    response = requests.get(url, timeout=10, headers={"Cache-Control": "no-cache"})
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Psygrid indicator source endpoint did not return an object")
    return payload


def _nifty_underlying_candles():
    if index_manager is None:
        return None
    snap = index_manager.snapshot("nifty")
    return snap.get("1m") if isinstance(snap, dict) else None


def _banknifty_underlying_candles():
    if index_manager is None:
        return None
    snap = index_manager.snapshot("banknifty")
    return snap.get("1m") if isinstance(snap, dict) else None


def _midcpnifty_underlying_candles():
    if midcpnifty_underlying_manager is None:
        return None
    return midcpnifty_underlying_manager.state.snapshot().get("candles_1m")


def startup() -> None:
    global settings, state, manager, indicator_runtime, index_manager, config_error, indicator_error, index_error
    global nifty_options_manager, nifty_depth_manager, banknifty_options_manager, banknifty_depth_manager, midcpnifty_options_manager, midcpnifty_depth_manager
    global midcpnifty_underlying_manager, nifty_underlying_indicators, banknifty_underlying_indicators, midcpnifty_underlying_indicators
    global nifty_futures_manager, banknifty_futures_manager, global_context_manager, rbi_news_manager
    config_error = ""
    indicator_error = ""
    index_error = ""
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

        # Completely separate index-data layer. It does not alter the 990-equity
        # universe, subscriptions, state, shards, or readiness contract.
        try:
            index_manager = IndexLayerManager(settings, dhan_api)
            index_manager.start()
        except Exception as exc:
            index_manager = None
            index_error = f"{type(exc).__name__}: {exc}"

        # Additive derived-data layer. It consumes the exact same canonical
        # PSYGRID live payload builder used by /public/live.json, so the core
        # OHLCV endpoints remain untouched and remain the source of truth.
        try:
            indicator_runtime = IndicatorRuntime(state, indicator_source_payload, interval_seconds=1.0)
            indicator_runtime.start()
        except Exception as exc:
            indicator_runtime = None
            indicator_error = str(exc)

        # Independent derivatives domain: NIFTY, BANKNIFTY and MIDCPNIFTY
        # option-chain (Dhan REST) and 20-level market depth (Dhan
        # WebSocket). Entirely separate from the 990-equity universe and
        # the 16-index layer above; a failure here never affects either.
        try:
            nifty_options_manager = NiftyOptionsManager(settings, dhan_api)
            nifty_options_manager.start()
        except Exception:
            nifty_options_manager = None
        if nifty_options_manager is not None:
            try:
                nifty_depth_manager = NiftyDepthManager(settings, dhan_api, nifty_options_manager)
                nifty_depth_manager.start()
            except Exception:
                nifty_depth_manager = None

        try:
            banknifty_options_manager = BankNiftyOptionsManager(settings, dhan_api)
            banknifty_options_manager.start()
        except Exception:
            banknifty_options_manager = None
        if banknifty_options_manager is not None:
            try:
                banknifty_depth_manager = BankNiftyDepthManager(settings, dhan_api, banknifty_options_manager)
                banknifty_depth_manager.start()
            except Exception:
                banknifty_depth_manager = None

        try:
            midcpnifty_options_manager = MidcapNiftyOptionsManager(settings, dhan_api)
            midcpnifty_options_manager.start()
        except Exception:
            midcpnifty_options_manager = None
        if midcpnifty_options_manager is not None:
            try:
                midcpnifty_depth_manager = MidcapNiftyDepthManager(settings, dhan_api, midcpnifty_options_manager)
                midcpnifty_depth_manager.start()
            except Exception:
                midcpnifty_depth_manager = None

        # MIDCPNIFTY has no WebSocket tick feed (it is not one of the sealed
        # 16 index-layer symbols); source its own real 1m candles from
        # Dhan's historical intraday API instead.
        try:
            midcpnifty_underlying_manager = MidcapNiftyUnderlyingManager(settings, dhan_api)
            midcpnifty_underlying_manager.start()
        except Exception:
            midcpnifty_underlying_manager = None

        # Real technical-indicator suite per underlying (EMA/SMA/RSI/MACD/
        # Bollinger/Supertrend/ADX/Stochastic/ATR/CCI/MFI/ROC/Momentum/
        # RVOL/CMF/Donchian), reusing the exact engine that powers the
        # 990-equity /public/indicators.json layer.
        try:
            nifty_underlying_indicators = UnderlyingIndicatorRuntime("NIFTY", _nifty_underlying_candles, settings)
            nifty_underlying_indicators.start()
        except Exception:
            nifty_underlying_indicators = None
        try:
            banknifty_underlying_indicators = UnderlyingIndicatorRuntime("BANKNIFTY", _banknifty_underlying_candles, settings)
            banknifty_underlying_indicators.start()
        except Exception:
            banknifty_underlying_indicators = None
        try:
            midcpnifty_underlying_indicators = UnderlyingIndicatorRuntime("MIDCPNIFTY", _midcpnifty_underlying_candles, settings)
            midcpnifty_underlying_indicators.start()
        except Exception:
            midcpnifty_underlying_indicators = None

        # Real NIFTY/BANKNIFTY front-month futures, resolved from Dhan's own
        # instrument master and polled via Dhan's market-quote API.
        try:
            nifty_futures_manager = FuturesManager("NIFTY", settings, dhan_api)
            nifty_futures_manager.start()
        except Exception:
            nifty_futures_manager = None
        try:
            banknifty_futures_manager = FuturesManager("BANKNIFTY", settings, dhan_api)
            banknifty_futures_manager.start()
        except Exception:
            banknifty_futures_manager = None

        # Tier 2: delayed official reference data (FRED) and RBI's own
        # official RSS feeds. Never presented as live; see market_data_status.
        try:
            global_context_manager = GlobalContextManager(settings)
            global_context_manager.start()
        except Exception:
            global_context_manager = None
        try:
            rbi_news_manager = RbiNewsManager(settings)
            rbi_news_manager.start()
        except Exception:
            rbi_news_manager = None
    except Exception as exc:
        config_error = str(exc)



def shutdown() -> None:
    global manager, indicator_runtime, index_manager
    global nifty_options_manager, nifty_depth_manager, banknifty_options_manager, banknifty_depth_manager, midcpnifty_options_manager, midcpnifty_depth_manager
    if indicator_runtime is not None:
        indicator_runtime.stop()
        indicator_runtime = None
    if index_manager is not None:
        index_manager.stop()
        index_manager = None
    for name in (
        "rbi_news_manager", "global_context_manager",
        "nifty_futures_manager", "banknifty_futures_manager",
        "midcpnifty_underlying_indicators", "banknifty_underlying_indicators", "nifty_underlying_indicators",
        "midcpnifty_underlying_manager",
        "midcpnifty_depth_manager", "midcpnifty_options_manager", "banknifty_depth_manager", "banknifty_options_manager", "nifty_depth_manager", "nifty_options_manager",
    ):
        obj = globals().get(name)
        if obj is not None:
            obj.stop()
            globals()[name] = None
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
        "derivatives_symbols": ["NIFTY", "BANKNIFTY", "MIDCPNIFTY"],
        "derivatives_endpoints": [
            "/public/nifty-options.json", "/public/nifty-depth.json",
            "/public/banknifty-options.json", "/public/banknifty-depth.json",
            "/public/midcpnifty-options.json", "/public/midcpnifty-depth.json",
        ],
        "underlying_indicator_endpoints": [
            "/public/nifty-indicators.json", "/public/banknifty-indicators.json", "/public/midcpnifty-indicators.json",
        ],
        "futures_endpoints": ["/public/nifty-futures.json", "/public/banknifty-futures.json"],
        "breadth_endpoints": ["/public/market-breadth.json", "/public/sectors.json"],
        "context_endpoints": {
            "global_context": {"endpoint": "/public/global-context.json", "market_data_status": "DELAYED", "source": "FRED"},
            "rbi_news": {"endpoint": "/public/rbi-news.json", "market_data_status": "NEAR_LIVE", "source": "RBI_OFFICIAL_RSS"},
        },
        "not_available": [
            "gift_nifty", "nasdaq", "dow_jones", "nikkei", "hang_seng", "shanghai", "kospi", "dxy", "gold",
            "general_global_market_news", "general_financial_news", "full_economic_calendar_with_forecast_actual_importance",
        ],
        "health_endpoint": "/public/health.json",
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
    # canonical 990-instrument order used by the feed and configuration.
    return json_response(market_live_json(state, (start, end), True))


# Canonical 990-stock universe: exactly 22 disjoint shards of 45.
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


INDEX_ROUTES = (
    "nifty", "banknifty", "sensex", "nifty500", "niftymidcap100",
    "niftysmallcap100", "finnifty", "indiavix", "niftyit", "niftyauto",
    "niftypharma", "niftymetal", "niftyfmcg", "niftyrealty", "niftyenergy", "niftyinfra",
)

def _index_endpoint(route: str) -> Response:
    error = _error_response()
    if error:
        return error
    if index_manager is None:
        return json_response({
            "service": "PSYGRID",
            "route": route,
            "status": "INDEX_LAYER_UNAVAILABLE",
            "error": index_error or "index layer failed during startup",
        }, 503)
    try:
        return json_response(index_manager.snapshot(route))
    except Exception as exc:
        return json_response({"service": "PSYGRID", "route": route, "status": "INDEX_ENDPOINT_ERROR", "error": str(exc)}, 503)

for _route in INDEX_ROUTES:
    globals()[f"public_{_route}"] = app.get(
        f"/public/{_route}.json", response_class=Response
    )(lambda route=_route: _index_endpoint(route))


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


# ---------------------------------------------------------------------------
# Independent derivatives domain: NIFTY, BANKNIFTY, MIDCPNIFTY option chain
# (Dhan REST) and 20-level market depth (Dhan WebSocket). Isolated from the
# 990-equity universe and the 16-index layer above.
# ---------------------------------------------------------------------------

_DERIVATIVES_ROUTES = (
    ("nifty-options", "nifty_options_manager", nifty_options_json, "NIFTY", "NIFTY_OPTIONS_UNAVAILABLE"),
    ("nifty-depth", "nifty_depth_manager", nifty_depth_json, "NIFTY", "NIFTY_DEPTH_UNAVAILABLE"),
    ("banknifty-options", "banknifty_options_manager", banknifty_options_json, "BANKNIFTY", "BANKNIFTY_OPTIONS_UNAVAILABLE"),
    ("banknifty-depth", "banknifty_depth_manager", banknifty_depth_json, "BANKNIFTY", "BANKNIFTY_DEPTH_UNAVAILABLE"),
    ("midcpnifty-options", "midcpnifty_options_manager", midcpnifty_options_json, "MIDCPNIFTY", "MIDCPNIFTY_OPTIONS_UNAVAILABLE"),
    ("midcpnifty-depth", "midcpnifty_depth_manager", midcpnifty_depth_json, "MIDCPNIFTY", "MIDCPNIFTY_DEPTH_UNAVAILABLE"),
    ("nifty-futures", "nifty_futures_manager", futures_json, "NIFTY", "NIFTY_FUTURES_UNAVAILABLE"),
    ("banknifty-futures", "banknifty_futures_manager", futures_json, "BANKNIFTY", "BANKNIFTY_FUTURES_UNAVAILABLE"),
)


def _derivatives_endpoint(manager_name: str, to_json, symbol: str, unavailable_status: str) -> Response:
    error = _error_response()
    if error:
        return error
    manager = globals().get(manager_name)
    if manager is None:
        return json_response({"service": "PSYGRID", "symbol": symbol, "status": unavailable_status}, 503)
    payload = to_json(manager.state)
    return json_response(payload, 200 if payload.get("status") == "LIVE" else 503)


for _path, _manager_name, _to_json, _symbol, _unavailable in _DERIVATIVES_ROUTES:
    globals()[f"public_{_path.replace('-', '_')}"] = app.get(
        f"/public/{_path}.json", response_class=Response
    )(lambda manager_name=_manager_name, to_json=_to_json, symbol=_symbol, unavailable=_unavailable: _derivatives_endpoint(manager_name, to_json, symbol, unavailable))


# ---------------------------------------------------------------------------
# Real technical-indicator suite per derivatives underlying (NIFTY,
# BANKNIFTY, MIDCPNIFTY), reusing the same engine that computes indicators
# for the 990-equity universe. Isolated: each runtime only reads an
# already-public 1m candle snapshot.
# ---------------------------------------------------------------------------

_UNDERLYING_INDICATOR_ROUTES = (
    ("nifty-indicators", "nifty_underlying_indicators", "NIFTY"),
    ("banknifty-indicators", "banknifty_underlying_indicators", "BANKNIFTY"),
    ("midcpnifty-indicators", "midcpnifty_underlying_indicators", "MIDCPNIFTY"),
)


def _underlying_indicator_endpoint(runtime_name: str, symbol: str) -> Response:
    error = _error_response()
    if error:
        return error
    runtime = globals().get(runtime_name)
    if runtime is None:
        return json_response({"service": "PSYGRID_MASTER_INDICATOR", "symbol": symbol, "status": f"{symbol}_INDICATORS_UNAVAILABLE"}, 503)
    payload = runtime.snapshot()
    return json_response(payload, 200 if payload.get("status") == "OK" else 503)


for _path, _runtime_name, _symbol in _UNDERLYING_INDICATOR_ROUTES:
    globals()[f"public_{_path.replace('-', '_')}"] = app.get(
        f"/public/{_path}.json", response_class=Response
    )(lambda runtime_name=_runtime_name, symbol=_symbol: _underlying_indicator_endpoint(runtime_name, symbol))


# ---------------------------------------------------------------------------
# Raw market breadth and sector aggregates, computed directly from the
# already-live 990-equity RAM state. No new data source, no labels.
# ---------------------------------------------------------------------------

@app.get("/public/market-breadth.json", response_class=Response)
def public_market_breadth() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(build_market_breadth(state))


@app.get("/public/sectors.json", response_class=Response)
def public_sectors() -> Response:
    error = _error_response()
    if error:
        return error
    return json_response(build_sector_breadth(state))


# ---------------------------------------------------------------------------
# Tier 2: delayed official reference data (FRED) and RBI's own official RSS
# feeds. Both are clearly marked with their true market_data_status.
# ---------------------------------------------------------------------------

@app.get("/public/global-context.json", response_class=Response)
def public_global_context() -> Response:
    error = _error_response()
    if error:
        return error
    if global_context_manager is None:
        return json_response({"service": "PSYGRID", "status": "GLOBAL_CONTEXT_UNAVAILABLE"}, 503)
    payload = global_context_json(global_context_manager.state)
    return json_response(payload, 200 if payload.get("status") == "LIVE" else 503)


@app.get("/public/rbi-news.json", response_class=Response)
def public_rbi_news() -> Response:
    error = _error_response()
    if error:
        return error
    if rbi_news_manager is None:
        return json_response({"service": "PSYGRID", "status": "RBI_NEWS_UNAVAILABLE"}, 503)
    payload = rbi_news_json(rbi_news_manager.state)
    return json_response(payload, 200 if payload.get("status") == "LIVE" else 503)


# ---------------------------------------------------------------------------
# Comprehensive, lightweight health aggregation across every live feed.
# Reads only what each manager already knows about itself; never refetches
# or recomputes upstream data.
# ---------------------------------------------------------------------------

def _build_health_payload() -> dict:
    now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
    components: list[dict] = []

    if state is not None:
        components.append(component_health(
            name="equity_990",
            status="LIVE" if state.session_status == "LIVE" and state.feed_status == "CONNECTED" else state.feed_status,
            updated_at=state.last_message_at,
            expected_refresh_seconds=2.0,
            now=now_ist,
            last_error=state.last_feed_error,
            record_count=state.subscribed_count,
            expected_record_count=UNIVERSE_SIZE,
            extra={"session_status": state.session_status, "feed_status": state.feed_status, "websocket_reconnects": state.websocket_reconnects},
        ))
    else:
        components.append(component_health(name="equity_990", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now_ist))

    if index_manager is not None:
        for route in INDEX_ROUTES:
            idx_state = index_manager.states.get(route)
            if idx_state is None:
                components.append(component_health(name=f"index_{route}", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now_ist, last_error=index_manager.resolution_errors.get(route, "unresolved")))
                continue
            components.append(component_health(
                name=f"index_{route}",
                status="LIVE" if idx_state.feed_status == "CONNECTED" else idx_state.feed_status,
                updated_at=idx_state.last_tick_received_epoch,
                expected_refresh_seconds=2.0,
                now=now_ist,
                last_error=idx_state.last_feed_error,
            ))
    else:
        components.append(component_health(name="index_layer", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now_ist, last_error=index_error))

    for name, mgr, refresh in (
        ("nifty_options", nifty_options_manager, 3.2), ("nifty_depth", nifty_depth_manager, 1.0),
        ("banknifty_options", banknifty_options_manager, 3.2), ("banknifty_depth", banknifty_depth_manager, 1.0),
        ("midcpnifty_options", midcpnifty_options_manager, 3.2), ("midcpnifty_depth", midcpnifty_depth_manager, 1.0),
        ("nifty_futures", nifty_futures_manager, 2.0), ("banknifty_futures", banknifty_futures_manager, 2.0),
        ("midcpnifty_underlying", midcpnifty_underlying_manager, 20.0),
        ("global_context", global_context_manager, 3600.0), ("rbi_news", rbi_news_manager, 300.0),
    ):
        if mgr is None:
            components.append(component_health(name=name, status=None, updated_at=None, expected_refresh_seconds=refresh, now=now_ist))
            continue
        st = mgr.state
        components.append(component_health(
            name=name, status=st.status, updated_at=st.updated_at, expected_refresh_seconds=refresh,
            now=now_ist, last_error=getattr(st, "last_error", ""),
        ))

    for name, runtime in (
        ("nifty_indicators", nifty_underlying_indicators), ("banknifty_indicators", banknifty_underlying_indicators),
        ("midcpnifty_indicators", midcpnifty_underlying_indicators),
    ):
        if runtime is None:
            components.append(component_health(name=name, status=None, updated_at=None, expected_refresh_seconds=5.0, now=now_ist))
            continue
        snap = runtime.snapshot()
        components.append(component_health(
            name=name, status=snap.get("status"), updated_at=runtime.last_updated_at(), expected_refresh_seconds=5.0, now=now_ist,
            last_error=(snap.get("error") or {}).get("message", "") if isinstance(snap.get("error"), dict) else "",
        ))

    components.append(component_health(
        name="equity_indicators", status="LIVE" if indicator_runtime is not None else None,
        updated_at=None, expected_refresh_seconds=1.0, now=now_ist, last_error=indicator_error,
    ))

    market_status = "OPEN" if (state is not None and state.session_status == "LIVE") else "CLOSED"
    return build_health(components, market_status)


@app.get("/public/health.json", response_class=Response)
def public_health() -> Response:
    return json_response(_build_health_payload())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")), access_log=False)
