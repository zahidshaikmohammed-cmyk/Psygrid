"""Option chains for NIFTY 50 stocks, polled round-robin from Dhan's option-chain API."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from auth_retry import AuthRetryGuard
from config import Instrument
from index_options import _is_market_open, _normalize_chain
from instrument_master import fetch_nse_equity_security_ids
from option_analytics import ChainAnalyticsTracker

OPTION_CHAIN_REFRESH_SECONDS = 3.2
EXPIRY_REFRESH_SECONDS = 1800.0

# NIFTY 50 index constituents as of this build. This list is periodically
# reconstituted by NSE (typically semi-annually) and, like stocks.json's
# equity universe, needs manual maintenance when that happens - Dhan's
# instrument master has no "index membership" field to resolve this from.
# Updated 2026-09-28: TATAMOTORS, INDUSINDBK, BRITANNIA, DIVISLAB,
# HEROMOTOCO, BPCL, UPL, LTIM removed; BEL, ETERNAL, HINDALCO, INDIGO,
# JIOFIN, MAXHEALTH, TMPV, TRENT added, per user-confirmed current
# constituents.
NIFTY50_SYMBOLS = (
    "ADANIENT",
    "ADANIPORTS",
    "APOLLOHOSP",
    "ASIANPAINT",
    "AXISBANK",
    "BAJAJ-AUTO",
    "BAJAJFINSV",
    "BAJFINANCE",
    "BEL",
    "BHARTIARTL",
    "CIPLA",
    "COALINDIA",
    "DRREDDY",
    "EICHERMOT",
    "ETERNAL",
    "GRASIM",
    "HCLTECH",
    "HDFCBANK",
    "HDFCLIFE",
    "HINDALCO",
    "HINDUNILVR",
    "ICICIBANK",
    "INDIGO",
    "INFY",
    "ITC",
    "JIOFIN",
    "JSWSTEEL",
    "KOTAKBANK",
    "LT",
    "M&M",
    "MARUTI",
    "MAXHEALTH",
    "NESTLEIND",
    "NTPC",
    "ONGC",
    "POWERGRID",
    "RELIANCE",
    "SBILIFE",
    "SBIN",
    "SHRIRAMFIN",
    "SUNPHARMA",
    "TATACONSUM",
    "TATASTEEL",
    "TCS",
    "TECHM",
    "TITAN",
    "TMPV",
    "TRENT",
    "ULTRACEMCO",
    "WIPRO",
)


class StockOptionState:
    """RAM-only per-symbol state for one NIFTY 50 stock's option chain."""

    def __init__(self, symbol: str, settings, security_id: str | None = None, exchange_segment: str = "NSE_EQ"):
        self.symbol = symbol
        self.security_id = security_id
        self.exchange_segment = exchange_segment
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "PENDING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.underlying_ltp: float | None = None
        self.expiry_list: list[str] = []
        self.expiry: str | None = None
        self.rows: list[dict] = []
        self.fetch_count = 0
        self.analytics: dict = {}

    def set_snapshot(self, payload: dict, expiry_list: list[str], expiry: str, analytics: dict | None = None) -> None:
        with self.lock:
            self.underlying_ltp = payload.get("last_price")
            self.expiry_list = list(expiry_list)
            self.expiry = expiry
            chain = payload.get("oc", [])
            self.rows = list(chain) if isinstance(chain, list) else _normalize_chain(payload)
            if analytics is not None:
                self.analytics = analytics
            self.updated_at = datetime.now(self.tz).isoformat()
            self.fetch_count += 1
            self.status = "LIVE"
            self.last_error = ""

    def set_error(self, error: str) -> None:
        with self.lock:
            self.status = "ERROR"
            self.last_error = error

    def snapshot(self) -> dict:
        with self.lock:
            now = datetime.now(self.tz)
            market_open = _is_market_open(now)
            return {
                "service": "PSYGRID",
                "symbol": self.symbol,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_OPTION_CHAIN_API",
                "security_id": self.security_id,
                "exchange_segment": self.exchange_segment,
                "instrument": "EQUITY",
                "underlying_ltp": self.underlying_ltp,
                "expiry": self.expiry,
                "expiry_list": list(self.expiry_list),
                "strikes": [dict(row) for row in self.rows],
                "updated_at": self.updated_at,
                "fetch_count": self.fetch_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "refresh_seconds": OPTION_CHAIN_REFRESH_SECONDS,
                "analytics": dict(self.analytics),
                **({"error": self.last_error} if self.last_error else {}),
            }


class StockOptionsManager:
    """Round-robin poll Dhan's option-chain API across the NIFTY 50 stocks
    that resolve against the equity universe. One process-wide Dhan REST
    queue is shared with every other option-chain poller (NIFTY/BANKNIFTY/
    MIDCPNIFTY/SENSEX index options); polling 50 stocks at the same
    ~3.2s-per-request cadence means a full rotation across all of them takes
    roughly len(resolved) * 3.2s - by design, not a bug. Resolves each
    symbol's NSE equity security ID independently, directly against Dhan's
    instrument master - deliberately not reusing the equity-universe
    universe's already-resolved instrument list, since that list's coverage
    is unrelated to NIFTY 50 membership (two current constituents, e.g.
    SBILIFE and SHRIRAMFIN, are not part of it) and this domain should not
    silently lose a real, resolvable stock just because stocks.json happens
    not to include it. Never fabricates data for a symbol that fails to
    resolve or fails to fetch; that symbol's own state simply reports its
    own error/status."""

    def __init__(self, settings, dhan_api) -> None:
        self.settings = settings
        self.dhan_api = dhan_api
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._auth = AuthRetryGuard(settings, dhan_api)

        self.instruments: dict[str, object] = {}
        self.states: dict[str, StockOptionState] = {}
        self.resolution_errors: dict[str, str] = {}
        try:
            security_ids = fetch_nse_equity_security_ids(NIFTY50_SYMBOLS)
            fetch_error = ""
        except Exception as exc:
            security_ids = {}
            fetch_error = f"{type(exc).__name__}: {exc}"

        for symbol in NIFTY50_SYMBOLS:
            security_id = security_ids.get(symbol)
            if security_id is None:
                self.states[symbol] = StockOptionState(symbol, settings)
                self.states[symbol].status = "UNRESOLVED"
                self.resolution_errors[symbol] = fetch_error or "not found in Dhan's NSE equity instrument master"
                continue
            item = Instrument(symbol=symbol, security_id=security_id)
            self.instruments[symbol] = item
            self.states[symbol] = StockOptionState(symbol, settings, item.security_id, item.exchange_segment)

        self._expiry_loaded_at: dict[str, float] = {}
        self._expiries: dict[str, list[str]] = {}
        self._expiry: dict[str, str | None] = {}
        self._trackers: dict[str, ChainAnalyticsTracker] = {
            symbol: ChainAnalyticsTracker() for symbol in self.instruments
        }

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-stock-options")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _poll_one(self, symbol: str) -> None:
        item = self.instruments[symbol]
        state = self.states[symbol]
        try:
            loaded_at = self._expiry_loaded_at.get(symbol, 0.0)
            expiries = self._expiries.get(symbol) or []
            if not expiries or time.monotonic() - loaded_at >= EXPIRY_REFRESH_SECONDS:
                expiries = self._auth.call(lambda: self.dhan_api.option_expiry_list(item))
                if not expiries:
                    raise RuntimeError(f"DHAN_{symbol}_OPTIONS_NO_ACTIVE_EXPIRIES")
                self._expiries[symbol] = expiries
                self._expiry_loaded_at[symbol] = time.monotonic()
                self._expiry[symbol] = expiries[0]
            elif self._expiry.get(symbol) not in expiries:
                self._expiry[symbol] = expiries[0]
            expiry = self._expiry[symbol]

            raw = self._auth.call(lambda: self.dhan_api.option_chain(item, expiry))
            payload = raw.get("data") if isinstance(raw, dict) else None
            if not isinstance(payload, dict):
                raise RuntimeError(f"DHAN_{symbol}_OPTIONS_INVALID_RESPONSE")
            rows = _normalize_chain(payload)
            if not rows:
                raise RuntimeError(f"DHAN_{symbol}_OPTIONS_EMPTY_CHAIN")
            payload = dict(payload)
            payload["oc"] = rows
            analytics = self._trackers[symbol].update(rows, payload.get("last_price"))
            state.set_snapshot(payload, self._expiries.get(symbol, []), expiry, analytics)
        except Exception as exc:
            state.set_error(f"{type(exc).__name__}: {exc}")

    def _loop(self) -> None:
        symbols = list(self.instruments.keys())
        if not symbols:
            return
        index = 0
        while not self.stop_event.is_set():
            self._poll_one(symbols[index % len(symbols)])
            index += 1
            self.stop_event.wait(OPTION_CHAIN_REFRESH_SECONDS)

    def snapshot(self, symbol: str) -> dict:
        key = symbol.strip().upper()
        if key not in self.states:
            return {
                "service": "PSYGRID",
                "symbol": key,
                "status": "UNKNOWN_SYMBOL",
                "error": "not part of the NIFTY 50 stock-options universe",
            }
        return self.states[key].snapshot()

    def listing(self) -> dict:
        entries = []
        for symbol in NIFTY50_SYMBOLS:
            state = self.states[symbol]
            with state.lock:
                entry = {
                    "symbol": symbol,
                    "status": state.status,
                    "underlying_ltp": state.underlying_ltp,
                    "expiry": state.expiry,
                    "fetch_count": state.fetch_count,
                    "updated_at": state.updated_at,
                }
                if state.last_error:
                    entry["error"] = state.last_error
                entries.append(entry)
        return {
            "service": "PSYGRID",
            "universe": "NIFTY_50_STOCK_OPTIONS",
            "symbol_count": len(NIFTY50_SYMBOLS),
            "resolved_count": len(self.instruments),
            "resolution_errors": dict(self.resolution_errors),
            "rotation_cycle_seconds": round(len(self.instruments) * OPTION_CHAIN_REFRESH_SECONDS, 1),
            "data_source": "DHAN_OPTION_CHAIN_API",
            "synthetic_data": False,
            "storage": "RAM_ONLY",
            "per_symbol_endpoint": "/public/stock-options/{SYMBOL}.json",
            "stocks": entries,
        }


def stock_options_json(manager: StockOptionsManager, symbol: str) -> dict:
    return manager.snapshot(symbol)


def stock_options_listing_json(manager: StockOptionsManager) -> dict:
    return manager.listing()
