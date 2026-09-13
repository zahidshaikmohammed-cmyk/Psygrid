from __future__ import annotations

import os
from contextlib import asynccontextmanager

import orjson
import uvicorn
from fastapi import FastAPI, Response
from starlette.middleware.gzip import GZipMiddleware

from banknifty import BankNiftyManager, banknifty_json
from config import load_instruments, load_settings
from dhan_api import DhanAPI
from feed_runtime import LiveFeed
from finnifty import FinNiftyManager, finnifty_json
from indiavix import IndiaVixManager, indiavix_json
from nifty import NiftyManager, nifty_json
from nifty500 import Nifty500Manager, nifty500_json
from niftymidcap100 import NiftyMidcap100Manager, niftymidcap100_json
from niftysmallcap100 import NiftySmallcap100Manager, niftysmallcap100_json
from niftyit import NiftyItManager, niftyit_json
from niftyauto import NiftyAutoManager, niftyauto_json
from niftypharma import NiftyPharmaManager, niftypharma_json
from niftymetal import NiftyMetalManager, niftymetal_json
from niftyfmcg import NiftyFmcgManager, niftyfmcg_json
from niftyrealty import NiftyRealtyManager, niftyrealty_json
from niftyenergy import NiftyEnergyManager, niftyenergy_json
from output import LIVE_TIMEFRAMES, market_live_json, stock_json
from sensex import SensexManager, sensex_json
from session import SessionManager
from state_runtime import RuntimeFreshnessState

settings = state = manager = nifty_manager = banknifty_manager = sensex_manager = None
nifty500_manager = niftymidcap100_manager = niftysmallcap100_manager = finnifty_manager = None
indiavix_manager = niftyit_manager = niftyauto_manager = niftypharma_manager = niftymetal_manager = niftyfmcg_manager = niftyrealty_manager = niftyenergy_manager = None
config_error = ""


def startup() -> None:
    global settings, state, manager, nifty_manager, banknifty_manager, sensex_manager, nifty500_manager, niftymidcap100_manager, niftysmallcap100_manager, finnifty_manager, indiavix_manager, niftyit_manager, niftyauto_manager, niftypharma_manager, niftymetal_manager, niftyfmcg_manager, niftyrealty_manager, niftyenergy_manager, config_error
    config_error = ""
    try:
        settings = load_settings(); instruments = load_instruments()
        if len(instruments) != settings.max_instruments:
            raise RuntimeError(f"Universe integrity failure: expected {settings.max_instruments}, got {len(instruments)}")
        state = RuntimeFreshnessState(settings); dhan_api = DhanAPI(settings)
        manager = SessionManager(settings, state, dhan_api, LiveFeed(settings, state, instruments), instruments); manager.start()
        managers = [("nifty_manager", NiftyManager), ("banknifty_manager", BankNiftyManager), ("sensex_manager", SensexManager), ("nifty500_manager", Nifty500Manager), ("niftymidcap100_manager", NiftyMidcap100Manager), ("niftysmallcap100_manager", NiftySmallcap100Manager), ("finnifty_manager", FinNiftyManager), ("indiavix_manager", IndiaVixManager), ("niftyit_manager", NiftyItManager), ("niftyauto_manager", NiftyAutoManager), ("niftypharma_manager", NiftyPharmaManager), ("niftymetal_manager", NiftyMetalManager), ("niftyfmcg_manager", NiftyFmcgManager), ("niftyrealty_manager", NiftyRealtyManager), ("niftyenergy_manager", NiftyEnergyManager)]
        for name, cls in managers:
            try:
                obj = cls(settings, dhan_api); obj.start(); globals()[name] = obj
            except Exception:
                globals()[name] = None
    except Exception as exc:
        config_error = str(exc)


def shutdown() -> None:
    global manager, nifty_manager, banknifty_manager, sensex_manager, nifty500_manager, niftymidcap100_manager, niftysmallcap100_manager, finnifty_manager, indiavix_manager, niftyit_manager, niftyauto_manager, niftypharma_manager, niftymetal_manager, niftyfmcg_manager, niftyrealty_manager, niftyenergy_manager
    for name in ("niftyenergy_manager", "niftyrealty_manager", "niftyfmcg_manager", "niftymetal_manager", "niftypharma_manager", "niftyauto_manager", "niftyit_manager", "indiavix_manager", "finnifty_manager", "niftysmallcap100_manager", "niftymidcap100_manager", "nifty500_manager", "sensex_manager", "banknifty_manager", "nifty_manager", "manager"):
        obj = globals().get(name)
        if obj is not None:
            obj.stop(); globals()[name] = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    startup(); yield; shutdown()

app = FastAPI(title="Psygrid", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)


def json_response(payload: dict, status_code: int = 200) -> Response:
    return Response(content=orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE), media_type="application/json", status_code=status_code, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0", "Pragma": "no-cache", "Expires": "0", "Vary": "Accept-Encoding"})


def _error_response() -> Response | None:
    if config_error: return json_response({"service": "PSYGRID", "status": "CONFIG_ERROR", "error": config_error})
    if state is None: return json_response({"service": "PSYGRID", "status": "STARTING"}, 503)
    return None

@app.get("/", response_class=Response)
def root() -> Response:
    return json_response({"service": "PSYGRID", "status": "ONLINE" if not config_error else "CONFIG_ERROR", "data_source": "DHAN", "synthetic_candles": False, "storage": "RAM_ONLY", "universe_size": 450, "live_endpoint": "/public/live.json", "live_endpoints": ["/public/live-a.json", "/public/live-b.json", "/public/live-c.json", "/public/live-d.json", "/public/live-e.json", "/public/live-f.json", "/public/live-g.json", "/public/live-h.json", "/public/live-i.json", "/public/live-j.json", "/public/live-01.json", "/public/live-02.json", "/public/live-03.json", "/public/live-04.json", "/public/live-05.json", "/public/live-06.json", "/public/nifty.json", "/public/banknifty.json", "/public/sensex.json", "/public/nifty500.json", "/public/niftymidcap100.json", "/public/niftysmallcap100.json", "/public/finnifty.json", "/public/indiavix.json", "/public/niftyit.json", "/public/niftyauto.json", "/public/niftypharma.json", "/public/niftymetal.json", "/public/niftyfmcg.json", "/public/niftyrealty.json", "/public/niftyenergy.json", "/public/stock/{symbol}.json", "/public/stock/{symbol}/{timeframe}.json"], "live_timeframes": list(LIVE_TIMEFRAMES)})

@app.get("/health", response_class=Response)
def health() -> Response: return json_response({"service": "PSYGRID", "status": "OK"})

@app.get("/ready", response_class=Response)
def ready() -> Response:
    error = _error_response()
    if error: return error
    snap = state.snapshot(); ready_now = bool(snap.get("session_status") == "LIVE" and snap.get("feed_status") == "CONNECTED" and snap.get("stock_count") == 450 and snap.get("subscribed_count") == 450 and snap.get("live_stock_count") == 450 and snap.get("stream_health") == "FULL_LIVE")
    return json_response({"service": "PSYGRID", "ready": ready_now, **snap}, 200 if ready_now else 503)

@app.get("/public/live.json", response_class=Response)
def public_live() -> Response:
    error = _error_response()
    if error: return error
    return json_response(market_live_json(state))

def _public_live_range(start: int, end: int, preserve_instrument_order: bool = False) -> Response:
    error = _error_response()
    if error: return error
    return json_response(market_live_json(state, (start, end), preserve_instrument_order))

for route, start, end, preserve in [("a",0,45,False),("b",45,90,False),("c",90,135,False),("d",135,180,False),("e",180,225,False),("f",225,270,False),("g",270,315,True),("h",315,360,True),("i",360,405,True),("j",405,450,True),("01",0,15,False),("02",15,30,False),("03",30,45,False),("04",45,60,False),("05",60,75,False),("06",75,90,False)]:
    globals()[f"public_live_{route}"] = app.get(f"/public/live-{route}.json", response_class=Response)(lambda start=start, end=end, preserve=preserve: _public_live_range(start,end,preserve))

def _index_response(obj, symbol, fn):
    error = _error_response()
    if error: return error
    if obj is None: return json_response({"service":"PSYGRID","symbol":symbol,"status":f"{symbol}_UNAVAILABLE"},503)
    return json_response(fn(obj.state, obj.instrument.security_id) if fn not in (nifty_json, banknifty_json, sensex_json) else fn(obj.state))

@app.get("/public/nifty.json", response_class=Response)
def public_nifty() -> Response: return _index_response(nifty_manager,"NIFTY",nifty_json)
@app.get("/public/banknifty.json", response_class=Response)
def public_banknifty() -> Response: return _index_response(banknifty_manager,"BANKNIFTY",banknifty_json)
@app.get("/public/sensex.json", response_class=Response)
def public_sensex() -> Response: return _index_response(sensex_manager,"SENSEX",sensex_json)
@app.get("/public/nifty500.json", response_class=Response)
def public_nifty500() -> Response: return _index_response(nifty500_manager,"NIFTY500",nifty500_json)
@app.get("/public/niftymidcap100.json", response_class=Response)
def public_niftymidcap100() -> Response: return _index_response(niftymidcap100_manager,"NIFTY_MIDCAP_100",niftymidcap100_json)
@app.get("/public/niftysmallcap100.json", response_class=Response)
def public_niftysmallcap100() -> Response: return _index_response(niftysmallcap100_manager,"NIFTY_SMALLCAP_100",niftysmallcap100_json)
@app.get("/public/finnifty.json", response_class=Response)
def public_finnifty() -> Response: return _index_response(finnifty_manager,"NIFTY_FIN_SERVICE",finnifty_json)
@app.get("/public/indiavix.json", response_class=Response)
def public_indiavix() -> Response: return _index_response(indiavix_manager,"INDIA VIX",indiavix_json)
@app.get("/public/niftyit.json", response_class=Response)
def public_niftyit() -> Response: return _index_response(niftyit_manager,"NIFTY_IT",niftyit_json)
@app.get("/public/niftyauto.json", response_class=Response)
def public_niftyauto() -> Response: return _index_response(niftyauto_manager,"NIFTY_AUTO",niftyauto_json)
@app.get("/public/niftypharma.json", response_class=Response)
def public_niftypharma() -> Response: return _index_response(niftypharma_manager,"NIFTY_PHARMA",niftypharma_json)
@app.get("/public/niftymetal.json", response_class=Response)
def public_niftymetal() -> Response: return _index_response(niftymetal_manager,"NIFTY_METAL",niftymetal_json)
@app.get("/public/niftyfmcg.json", response_class=Response)
def public_niftyfmcg() -> Response: return _index_response(niftyfmcg_manager,"NIFTY_FMCG",niftyfmcg_json)
@app.get("/public/niftyrealty.json", response_class=Response)
def public_niftyrealty() -> Response: return _index_response(niftyrealty_manager,"NIFTY_REALTY",niftyrealty_json)
@app.get("/public/niftyenergy.json", response_class=Response)
def public_niftyenergy() -> Response: return _index_response(niftyenergy_manager,"NIFTY_ENERGY",niftyenergy_json)

@app.get("/public/stock/{symbol}.json", response_class=Response)
def public_stock(symbol: str) -> Response:
    error = _error_response()
    if error: return error
    return json_response(stock_json(state, symbol))

@app.get("/public/stock/{symbol}/{timeframe}.json", response_class=Response)
def public_stock_timeframe(symbol: str, timeframe: str) -> Response:
    timeframe = timeframe.lower()
    if timeframe not in LIVE_TIMEFRAMES: return json_response({"service":"PSYGRID","status":"INVALID_TIMEFRAME","allowed_timeframes":list(LIVE_TIMEFRAMES)},400)
    error = _error_response()
    if error: return error
    return json_response(stock_json(state, symbol, timeframe))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT","10000")), access_log=False)
