from __future__ import annotations

"""Real NIFTY/BANKNIFTY index-futures data (front-month contract), sourced
from Dhan's own instrument master (contract identity) and market-quote API
(LTP/OHLC/volume/OI/bid/ask), using the exact same credentials and rate
limiting already in use elsewhere in Psygrid.

RAW DATA ONLY: no rollover recommendation, no basis/premium interpretation,
no trading signal. Just the contract's current published quote.
"""

import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time
from zoneinfo import ZoneInfo

from derivatives_instruments import FuturesContract, fetch_front_month_index_futures

FUTURES_CONTRACT_REFRESH_SECONDS = 1800.0
FUTURES_QUOTE_REFRESH_SECONDS = 2.0
FUTURES_MARKET_OPEN = datetime_time(9, 15)
FUTURES_MARKET_CLOSE = datetime_time(15, 30)


@dataclass(frozen=True)
class _QuoteInstrument:
    security_id: str
    exchange_segment: str


def _is_market_open(now: datetime) -> bool:
    return now.weekday() < 5 and FUTURES_MARKET_OPEN <= now.time() < FUTURES_MARKET_CLOSE


def _num(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    return float(value)


class FuturesState:
    """RAM-only current front-month futures quote for a single underlying."""

    def __init__(self, symbol: str, settings):
        self.symbol = symbol
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.contract: FuturesContract | None = None
        self.quote: dict = {}
        self._previous_oi: float | None = None
        self.oi_change: float | None = None
        self.fetch_count = 0

    def set_contract(self, contract: FuturesContract | None) -> None:
        with self.lock:
            if contract is None:
                return
            if self.contract is None or self.contract.security_id != contract.security_id:
                self._previous_oi = None
                self.oi_change = None
            self.contract = contract

    def set_quote(self, raw_quote: dict) -> None:
        with self.lock:
            oi = _num(raw_quote.get("oi"))
            if oi is not None and self._previous_oi is not None:
                self.oi_change = oi - self._previous_oi
            if oi is not None:
                self._previous_oi = oi
            self.quote = dict(raw_quote)
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
            contract = self.contract
            ohlc = self.quote.get("ohlc") if isinstance(self.quote.get("ohlc"), dict) else {}
            depth = self.quote.get("depth") if isinstance(self.quote.get("depth"), dict) else {}
            payload = {
                "service": "PSYGRID",
                "symbol": self.symbol,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_MARKET_QUOTE_API",
                "security_id": contract.security_id if contract else None,
                "exchange_segment": contract.exchange_segment if contract else None,
                "instrument": contract.instrument if contract else None,
                "trading_symbol": contract.trading_symbol if contract else None,
                "expiry": contract.expiry_date if contract else None,
                "lot_size": contract.lot_size if contract else None,
                "tick_size": contract.tick_size if contract else None,
                "last_price": _num(self.quote.get("last_price", self.quote.get("LTP"))),
                "ohlc": {
                    "open": _num(ohlc.get("open")),
                    "high": _num(ohlc.get("high")),
                    "low": _num(ohlc.get("low")),
                    "close": _num(ohlc.get("close")),
                } if ohlc else None,
                "volume": _num(self.quote.get("volume")),
                "oi": _num(self.quote.get("oi")),
                "oi_change": self.oi_change,
                "average_price": _num(self.quote.get("average_price")),
                "buy_quantity": _num(self.quote.get("buy_quantity")),
                "sell_quantity": _num(self.quote.get("sell_quantity")),
                "top_bid_price": _num(depth.get("buy_price") if depth else self.quote.get("top_bid_price")),
                "top_ask_price": _num(depth.get("sell_price") if depth else self.quote.get("top_ask_price")),
                "raw_quote": dict(self.quote),
                "updated_at": self.updated_at,
                "fetch_count": self.fetch_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "refresh_seconds": FUTURES_QUOTE_REFRESH_SECONDS,
            }
            if self.last_error:
                payload["error"] = self.last_error
            return payload


class FuturesManager:
    """Resolves the front-month contract and polls its live quote."""

    def __init__(self, symbol: str, settings, dhan_api, exchange: str = "NSE"):
        self.symbol = symbol
        self.settings = settings
        self.dhan_api = dhan_api
        self.exchange = exchange
        self.state = FuturesState(symbol, settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._contract_resolved_at = 0.0

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name=f"psygrid-{self.symbol.lower()}-futures")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _resolve_contract(self) -> None:
        contracts = fetch_front_month_index_futures((self.symbol,), exchange=self.exchange)
        contract = contracts.get(self.symbol)
        if contract is None:
            raise RuntimeError(f"DHAN_{self.symbol}_FUTURES_NOT_RESOLVED")
        self.state.set_contract(contract)
        self._contract_resolved_at = time.monotonic()

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                if self.state.contract is None or time.monotonic() - self._contract_resolved_at >= FUTURES_CONTRACT_REFRESH_SECONDS:
                    self._resolve_contract()
                contract = self.state.contract
                instrument = _QuoteInstrument(security_id=contract.security_id, exchange_segment=contract.exchange_segment)
                quotes = self.dhan_api.quote_snapshot([instrument])
                row = quotes.get(str(contract.security_id))
                if not isinstance(row, dict):
                    raise RuntimeError(f"DHAN_{self.symbol}_FUTURES_QUOTE_UNAVAILABLE")
                self.state.set_quote(row)
                self.stop_event.wait(FUTURES_QUOTE_REFRESH_SECONDS)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
                self.stop_event.wait(FUTURES_QUOTE_REFRESH_SECONDS)


def futures_json(state: FuturesState) -> dict:
    return state.snapshot()
