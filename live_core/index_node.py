"""PSYGRID index node: NIFTY, BANKNIFTY and SENSEX only, RAM-only, from Dhan.

For each index it serves:
  * today's completed 1-minute OHLCV candles (Dhan's own intraday chart bars, polled every minute;
    indices have no traded volume, so volume is whatever Dhan reports, normally 0),
    the previous session's close and today's open;
  * the nearest-expiry option chain (``index_options``, Dhan's option-chain API);
  * 20-level market depth for the CE and PE of the 25 strikes nearest the index (``index_depth``).

The Dhan token comes from the stock node that generates it (``LIVE_CORE_TOKEN_SOURCE``); this node
never logs in itself. Nothing is written to disk.

    python -m live_core.index_node            # LIVE_CORE_PORT (default 10000), LIVE_CORE_TOKEN_SOURCE
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo

import orjson
from fastapi import FastAPI, HTTPException, Response

import config as psygrid_config
from dhan_api import DhanAPI
from index_depth import IndexDepthManager, index_depth_json
from index_options import BANKNIFTY, NIFTY, SENSEX, IndexOptionsManager, OptionsInstrument, index_options_json
from live_core.auth import TokenSource
from live_core.redact import redact
from output import _clean_candle

log = logging.getLogger("live_core.index_node")

INDICES = (NIFTY, BANKNIFTY, SENSEX)
TZ = ZoneInfo(psygrid_config.TIMEZONE)
SESSION_OPEN = dt_time(9, 15)
CANDLE_POLL_UNTIL = dt_time(15, 32)
CANDLE_POLL_SECOND = 3  # poll each minute this many seconds after it completes
TOKEN_POLL_SECONDS = 15.0
STALE_CANDLE_MINUTES = 3


def _trading_weekday(now: datetime) -> bool:
    return now.weekday() < 5


class IndexCandles:
    """Today's 1m candles, previous close and today's open for one index."""

    def __init__(self, spec):
        self.spec = spec
        self.lock = threading.Lock()
        self.session_date: str | None = None
        self.previous_close: float | None = None
        self.previous_close_date: str | None = None
        self.candles: list[dict] = []
        self.updated_at: str | None = None
        self.error = ""
        self.polls = 0

    def reset(self, session_date: str) -> None:
        with self.lock:
            self.session_date = session_date
            self.previous_close = None
            self.previous_close_date = None
            self.candles = []
            self.updated_at = None
            self.error = ""

    def payload(self) -> dict:
        with self.lock:
            candles = [_clean_candle(c) for c in self.candles]
            first = candles[0] if candles else None
            today_open = first["open"] if first and str(first["timestamp"])[11:16] == "09:15" else None
            return {
                "symbol": self.spec.symbol,
                "security_id": self.spec.security_id,
                "exchange_segment": "IDX_I",
                "session_date": self.session_date,
                "previous_close": self.previous_close,
                "previous_close_date": self.previous_close_date,
                "today_open": today_open,
                "ltp": candles[-1]["close"] if candles else None,
                "candle_count": len(candles),
                "candles_1m": candles,
                "updated_at": self.updated_at,
                **({"error": self.error} if self.error else {}),
            }


class IndexNode:
    def __init__(self, token_source: str, *, token_get=None, api_factory=DhanAPI, now=None, start_managers=True):
        self.token_source = TokenSource(token_source, get=token_get)
        self.api_factory = api_factory
        self.now = now or (lambda: datetime.now(TZ))
        self.start_managers = start_managers
        self.settings = None
        self.api = None
        self.options: dict[str, IndexOptionsManager] = {}
        self.depth: dict[str, IndexDepthManager] = {}
        self.candles = {spec.symbol: IndexCandles(spec) for spec in INDICES}
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.started_at = time.time()
        self.token_error = ""
        self.token_rotations = 0
        self._last_token_poll = 0.0
        self._last_candle_minute: str | None = None

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-index-node")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        for manager in [*self.depth.values(), *self.options.values()]:
            with contextlib.suppress(Exception):
                manager.stop()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=10)

    def _ensure_started(self) -> bool:
        if self.settings is not None:
            return True
        try:
            self.settings = self.token_source.load_settings()
        except Exception as exc:
            self.token_error = redact(f"{type(exc).__name__}: {exc}")[:300]
            return False
        self.token_error = ""
        self.api = self.api_factory(self.settings)
        for spec in INDICES:
            options = IndexOptionsManager(self.settings, self.api, spec)
            self.options[spec.symbol] = options
            self.depth[spec.symbol] = IndexDepthManager(self.settings, self.api, options, spec)
            if self.start_managers:
                options.start()
                self.depth[spec.symbol].start()
        log.info("index node started for %s", ", ".join(s.symbol for s in INDICES))
        return True

    def _poll_token(self) -> None:
        if time.monotonic() - self._last_token_poll < TOKEN_POLL_SECONDS:
            return
        self._last_token_poll = time.monotonic()
        try:
            changed = self.token_source.apply(self.settings)
        except Exception as exc:
            self.token_error = redact(f"{type(exc).__name__}: {exc}")[:300]
            return
        self.token_error = ""
        if changed:
            # Every manager shares this Settings object; the depth sockets reconnect with the new token.
            self.token_rotations += 1
            for manager in self.depth.values():
                with contextlib.suppress(Exception):
                    manager._close_socket()

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            if not self._ensure_started():
                self.stop_event.wait(10.0)
                continue
            self._poll_token()
            now = self.now()
            minute_key = now.strftime("%Y-%m-%d %H:%M")
            if (
                _trading_weekday(now)
                and SESSION_OPEN <= now.time() <= CANDLE_POLL_UNTIL
                and now.second >= CANDLE_POLL_SECOND
                and minute_key != self._last_candle_minute
            ):
                self._last_candle_minute = minute_key
                self.poll_candles(now)
            elif self._last_candle_minute is None and _trading_weekday(now) and now.time() > CANDLE_POLL_UNTIL:
                # Started after the close: load today's session once.
                self._last_candle_minute = minute_key
                self.poll_candles(now)
            self.stop_event.wait(1.0)

    # ------------------------------------------------------------------ candles

    def poll_candles(self, now: datetime) -> None:
        today = now.date().isoformat()
        for spec in INDICES:
            state = self.candles[spec.symbol]
            item = OptionsInstrument(spec.security_id)
            if state.session_date != today:
                state.reset(today)
            try:
                if state.previous_close is None:
                    rows = self.api.daily(item, now - timedelta(days=15), now)
                    prior = [r for r in rows if datetime.fromtimestamp(r["timestamp"], TZ).date() < now.date()]
                    if prior:
                        with state.lock:
                            state.previous_close = float(prior[-1]["close"])
                            state.previous_close_date = (
                                datetime.fromtimestamp(prior[-1]["timestamp"], TZ).date().isoformat()
                            )
                candles = self.api.load_today_completed_intraday(item, 1)
                with state.lock:
                    if candles:
                        state.candles = candles
                    state.updated_at = now.isoformat(timespec="seconds")
                    state.polls += 1
                    state.error = ""
            except Exception as exc:
                with state.lock:
                    state.error = redact(f"{type(exc).__name__}: {exc}")[:300]

    # ------------------------------------------------------------------ payloads

    def market_open(self) -> bool:
        now = self.now()
        return _trading_weekday(now) and SESSION_OPEN <= now.time() < dt_time(15, 30)

    def live_payload(self) -> dict:
        return {
            "service": "PSYGRID",
            "node": "INDEX",
            "generated_at": self.now().isoformat(timespec="seconds"),
            "market_open": self.market_open(),
            "data_policy": "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN",
            "indices": {symbol: state.payload() for symbol, state in self.candles.items()},
        }

    def options_payload(self, symbol: str) -> dict:
        manager = self.options.get(symbol)
        if manager is None:
            return {"symbol": symbol, "status": "STARTING"}
        return index_options_json(manager.state)

    def depth_payload(self, symbol: str) -> dict:
        manager = self.depth.get(symbol)
        if manager is None:
            return {"symbol": symbol, "status": "STARTING"}
        return index_depth_json(manager.state)

    def health(self) -> dict:
        now = self.now()
        market_open = self.market_open()
        reasons = []
        if self.settings is None:
            reasons.append(f"no Dhan token yet: {self.token_error}")
        elif self.token_error:
            reasons.append(f"token source: {self.token_error}")
        components = {}
        for spec in INDICES:
            sym = spec.symbol
            c = self.candles[sym].payload()
            last_ts = c["candles_1m"][-1]["timestamp"] if c["candles_1m"] else None
            age = None
            if last_ts:
                with contextlib.suppress(ValueError):
                    age = round((now - datetime.fromisoformat(str(last_ts))).total_seconds() / 60, 1)
            opt = self.options_payload(sym)
            dep = self.depth_payload(sym)
            components[sym] = {
                "candles": c["candle_count"],
                "last_candle": last_ts,
                "last_candle_age_minutes": age,
                "previous_close": c["previous_close"],
                "today_open": c["today_open"],
                "candle_error": c.get("error", ""),
                "options_status": opt.get("status"),
                "options_expiry": opt.get("expiry"),
                "options_strikes": len(opt.get("strikes") or []),
                "options_updated_at": opt.get("updated_at"),
                "options_error": opt.get("error", ""),
                "depth_status": dep.get("status"),
                "depth_contracts": len(dep.get("contracts") or []) if isinstance(dep.get("contracts"), list) else None,
                "depth_updated_at": dep.get("updated_at"),
                "depth_error": dep.get("error", ""),
            }
            if market_open and now.time() >= dt_time(9, 20):
                if age is None or age > STALE_CANDLE_MINUTES + 1:
                    reasons.append(f"{sym}: 1m candles stale ({age} min)")
                if opt.get("status") != "LIVE":
                    reasons.append(f"{sym}: option chain {opt.get('status')}")
                if dep.get("status") != "LIVE":
                    reasons.append(f"{sym}: depth {dep.get('status')}")
        return {
            "service": "PSYGRID",
            "runtime": "PSYGRID_INDEX_NODE",
            "status": "DEGRADED" if reasons else "OK",
            "reasons": reasons,
            "checked_at": now.isoformat(timespec="seconds"),
            "market_open": market_open,
            "uptime_seconds": round(time.time() - self.started_at, 1),
            "auth": {**self.token_source.describe(), "token_rotations_applied": self.token_rotations},
            "indices": components,
            "storage": {"market_data_on_disk": False},
        }


def _json(payload: dict, status_code: int = 200) -> Response:
    return Response(orjson.dumps(payload), status_code=status_code, media_type="application/json")


def create_app(node: IndexNode) -> FastAPI:
    app = FastAPI(title="PSYGRID index node", docs_url=None, redoc_url=None, openapi_url=None)
    symbols = {spec.symbol.lower(): spec.symbol for spec in INDICES}

    def _symbol(name: str) -> str:
        symbol = symbols.get(name.lower().removesuffix(".json"))
        if symbol is None:
            raise HTTPException(status_code=404, detail="unknown index; use nifty, banknifty or sensex")
        return symbol

    @app.get("/")
    def root() -> Response:
        return _json(
            {
                "service": "PSYGRID",
                "node": "INDEX",
                "indices": [spec.symbol for spec in INDICES],
                "endpoints": [
                    "/public/live.json",
                    "/public/index/{nifty|banknifty|sensex}.json",
                    "/public/options/{nifty|banknifty|sensex}.json",
                    "/public/depth/{nifty|banknifty|sensex}.json",
                    "/public/all.json",
                    "/health",
                ],
            }
        )

    @app.get("/health")
    @app.get("/health/node")
    def health() -> Response:
        return _json(node.health())

    @app.get("/public/live.json")
    def live() -> Response:
        return _json(node.live_payload())

    @app.get("/public/index/{name}")
    def index(name: str) -> Response:
        return _json(node.live_payload()["indices"][_symbol(name)])

    @app.get("/public/options/{name}")
    def options(name: str) -> Response:
        return _json(node.options_payload(_symbol(name)))

    @app.get("/public/depth/{name}")
    def depth(name: str) -> Response:
        return _json(node.depth_payload(_symbol(name)))

    @app.get("/public/all.json")
    def everything() -> Response:
        payload = node.live_payload()
        for spec in INDICES:
            payload["indices"][spec.symbol]["option_chain"] = node.options_payload(spec.symbol)
            payload["indices"][spec.symbol]["depth"] = node.depth_payload(spec.symbol)
        return _json(payload)

    return app


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    source = os.getenv("LIVE_CORE_TOKEN_SOURCE", "").strip()
    if not source.startswith(("http://", "https://")):
        print("LIVE_CORE_TOKEN_SOURCE must be the token-generating node's URL", file=sys.stderr)
        return 2
    port = int(os.getenv("LIVE_CORE_PORT", "10000"))
    node = IndexNode(source)
    node.start()
    import uvicorn

    try:
        uvicorn.run(create_app(node), host="0.0.0.0", port=port, log_level="warning", access_log=False)
    finally:
        node.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
