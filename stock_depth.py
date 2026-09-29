from __future__ import annotations

import json
import struct
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time
from typing import Optional
from zoneinfo import ZoneInfo

import websocket

STOCK_DEPTH_EXCHANGE_SEGMENT = "NSE_FNO"
STOCK_DEPTH_INSTRUMENT = "OPTSTK"
STOCK_DEPTH_LEVELS = 20
# Dhan's 20-level depth WebSocket allows at most 50 subscribed instruments
# per connection - every existing per-underlying depth module in this
# codebase (NIFTY/BANKNIFTY/MIDCPNIFTY/SENSEX) independently lands on the
# exact same cap. Real depth for all 50 NIFTY 50 stocks at 5 strikes x
# CE/PE each would need 10 separate connections; instead this shares ONE
# connection and rotates through the stocks in small batches, the same
# spirit as StockOptionsManager's own option-chain rotation - deliberately
# NOT opening 10 more WebSocket connections on top of the 6 already
# running (equity feed, the shared 16-index feed, and 4 existing
# per-underlying depth feeds).
STOCK_DEPTH_MAX_INSTRUMENTS = 50
STOCK_DEPTH_STRIKES_PER_SYMBOL = 5
STOCK_DEPTH_CONTRACTS_PER_SYMBOL = STOCK_DEPTH_STRIKES_PER_SYMBOL * 2
STOCK_DEPTH_SYMBOLS_PER_BATCH = STOCK_DEPTH_MAX_INSTRUMENTS // STOCK_DEPTH_CONTRACTS_PER_SYMBOL
STOCK_DEPTH_BATCH_SECONDS = 30.0
STOCK_DEPTH_RECONNECT_SECONDS = 3.0
STOCK_DEPTH_QUOTE_REFRESH_SECONDS = 1.0
STOCK_DEPTH_MARKET_OPEN = datetime_time(9, 15)
STOCK_DEPTH_MARKET_CLOSE = datetime_time(15, 30)


@dataclass(frozen=True)
class StockDepthContract:
    security_id: str
    symbol: str
    strike: float
    option_type: str
    expiry: str


def _is_market_open(now: datetime) -> bool:
    return now.weekday() < 5 and STOCK_DEPTH_MARKET_OPEN <= now.time() < STOCK_DEPTH_MARKET_CLOSE


class StockDepthState:
    """RAM-only current 20-level option premium-depth snapshot for one
    stock. Persists its last known depth between this stock's rotation
    windows - rotation_status tells the caller whether that data is from
    the currently-subscribed batch or an earlier one."""

    def __init__(self, symbol: str, settings):
        self.symbol = symbol
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "PENDING"
        self.last_error = ""
        self.updated_at: Optional[str] = None
        self.expiry: Optional[str] = None
        self.underlying_ltp: Optional[float] = None
        self.contracts: dict[str, dict] = {}
        self.rotation_status = "IDLE"
        self.packet_count = 0

    def set_contracts(self, contracts: list[StockDepthContract], expiry: str) -> None:
        with self.lock:
            self.expiry = expiry
            allowed = {c.security_id: c for c in contracts}
            self.contracts = {
                security_id: self.contracts.get(security_id, {
                    "security_id": security_id, "strike": c.strike, "option_type": c.option_type,
                    "expiry": c.expiry, "bid": [], "ask": [],
                })
                for security_id, c in allowed.items()
            }

    def update_depth(self, security_id: str, side: str, levels: list[dict]) -> None:
        with self.lock:
            row = self.contracts.get(str(security_id))
            if row is None:
                return
            row[side] = levels
            row["updated_at"] = datetime.now(self.tz).isoformat()
            self.updated_at = row["updated_at"]
            self.packet_count += 1
            self.status = "LIVE"
            self.last_error = ""

    def update_quotes(self, quotes: dict[str, dict]) -> None:
        with self.lock:
            for security_id, quote in quotes.items():
                row = self.contracts.get(str(security_id))
                if row is None or not isinstance(quote, dict):
                    continue
                for key in ("last_price", "average_price", "buy_quantity", "sell_quantity", "volume", "oi"):
                    if key in quote:
                        row[key] = quote[key]
                ohlc = quote.get("ohlc")
                if isinstance(ohlc, dict):
                    row["ohlc"] = dict(ohlc)
                row["quote_updated_at"] = datetime.now(self.tz).isoformat()

    def set_underlying_ltp(self, value) -> None:
        with self.lock:
            self.underlying_ltp = value

    def set_rotation_status(self, value: str) -> None:
        with self.lock:
            self.rotation_status = value

    def set_error(self, error: str) -> None:
        with self.lock:
            self.status = "ERROR"
            self.last_error = error

    def snapshot(self) -> dict:
        with self.lock:
            now = datetime.now(self.tz)
            market_open = _is_market_open(now)
            rows = []
            for row in self.contracts.values():
                cleaned = {
                    key: (list(value) if key in ("bid", "ask") else dict(value) if key == "ohlc" and isinstance(value, dict) else value)
                    for key, value in row.items()
                }
                bid_levels = cleaned.get("bid") or []
                ask_levels = cleaned.get("ask") or []
                cleaned["crossed_book"] = bool(
                    bid_levels and ask_levels and bid_levels[0].get("price") is not None
                    and ask_levels[0].get("price") is not None and bid_levels[0]["price"] > ask_levels[0]["price"]
                )
                rows.append(cleaned)
            rows.sort(key=lambda row: (row.get("strike", 0.0), row.get("option_type", "")))
            return {
                "service": "PSYGRID",
                "symbol": self.symbol,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_FULL_MARKET_DEPTH_WEBSOCKET",
                "exchange_segment": STOCK_DEPTH_EXCHANGE_SEGMENT,
                "instrument": STOCK_DEPTH_INSTRUMENT,
                "depth_levels": STOCK_DEPTH_LEVELS,
                "underlying_ltp": self.underlying_ltp,
                "expiry": self.expiry,
                # "ACTIVE" while this stock is inside the depth WebSocket's
                # current subscribed batch; "IDLE" between its rotation
                # turns - contracts/bid/ask below are its last known depth
                # from whenever it was last ACTIVE, not fabricated for the gap.
                "rotation_status": self.rotation_status,
                "contract_count": len(rows),
                "contracts": rows,
                "updated_at": self.updated_at,
                "packet_count": self.packet_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                **({"error": self.last_error} if self.last_error else {}),
            }


def _select_contracts_for_symbol(symbol: str, option_state) -> tuple[list[StockDepthContract], Optional[str], Optional[float]]:
    snapshot = option_state.snapshot()
    expiry = snapshot.get("expiry")
    underlying_ltp = snapshot.get("underlying_ltp")
    strikes = snapshot.get("strikes", [])
    if not expiry or not isinstance(underlying_ltp, (int, float)):
        return [], expiry, underlying_ltp
    normalized = []
    for row in strikes:
        if not isinstance(row, dict):
            continue
        strike = row.get("strike")
        if not isinstance(strike, (int, float)):
            continue
        for option_type, key in (("CE", "ce"), ("PE", "pe")):
            contract = row.get(key)
            security_id = contract.get("security_id") if isinstance(contract, dict) else None
            if security_id:
                normalized.append((abs(float(strike) - float(underlying_ltp)), float(strike), option_type, str(security_id)))
    if not normalized:
        return [], expiry, float(underlying_ltp)
    strikes_sorted = sorted({item[1] for item in normalized}, key=lambda strike: (abs(strike - float(underlying_ltp)), strike))
    selected_strikes = set(strikes_sorted[:STOCK_DEPTH_STRIKES_PER_SYMBOL])
    contracts = [
        StockDepthContract(security_id=item[3], symbol=symbol, strike=item[1], option_type=item[2], expiry=str(expiry))
        for item in normalized if item[1] in selected_strikes
    ]
    contracts.sort(key=lambda item: (abs(item.strike - float(underlying_ltp)), item.strike, item.option_type))
    return contracts[:STOCK_DEPTH_CONTRACTS_PER_SYMBOL], str(expiry), float(underlying_ltp)


def _parse_depth_message(data: bytes) -> list[tuple[str, str, list[dict]]]:
    messages = []
    offset = 0
    while offset + 12 <= len(data):
        message_length = struct.unpack_from("<H", data, offset)[0]
        if message_length < 12 or offset + message_length > len(data):
            break
        response_code = data[offset + 2]
        security_id = str(struct.unpack_from("<i", data, offset + 4)[0])
        if response_code in (41, 51) and message_length >= 332:
            side = "bid" if response_code == 41 else "ask"
            levels = []
            base = offset + 12
            for index in range(STOCK_DEPTH_LEVELS):
                price, quantity, orders = struct.unpack_from("<dII", data, base + index * 16)
                levels.append({"level": index + 1, "price": float(price), "quantity": int(quantity), "orders": int(orders)})
            messages.append((security_id, side, levels))
        offset += message_length
    return messages


class StockDepthManager:
    """One shared Dhan 20-level depth WebSocket connection, rotating its
    50-instrument subscription through the NIFTY 50 stock universe in
    batches of STOCK_DEPTH_SYMBOLS_PER_BATCH stocks (5 strikes x CE/PE
    each). A full rotation across every resolved stock takes roughly
    (resolved_count / symbols_per_batch) * STOCK_DEPTH_BATCH_SECONDS."""

    def __init__(self, settings, dhan_api, stock_options_manager) -> None:
        self.settings = settings
        self.dhan_api = dhan_api
        self.stock_options_manager = stock_options_manager
        self.states: dict[str, StockDepthState] = {
            symbol: StockDepthState(symbol, settings) for symbol in stock_options_manager.states
        }
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.ws = None
        self._contracts: list[StockDepthContract] = []
        self._batch_symbols: list[str] = []
        self._batch_index = 0

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-stock-depth")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None
        self.ws = None

    def _batches(self) -> list[list[str]]:
        symbols = list(self.stock_options_manager.instruments.keys())
        if not symbols:
            return []
        return [symbols[i:i + STOCK_DEPTH_SYMBOLS_PER_BATCH] for i in range(0, len(symbols), STOCK_DEPTH_SYMBOLS_PER_BATCH)]

    def _refresh_contracts_for_batch(self, batch_symbols: list[str]) -> list[StockDepthContract]:
        contracts: list[StockDepthContract] = []
        for symbol in batch_symbols:
            option_state = self.stock_options_manager.states.get(symbol)
            if option_state is None:
                continue
            symbol_contracts, expiry, underlying_ltp = _select_contracts_for_symbol(symbol, option_state)
            state = self.states[symbol]
            state.set_underlying_ltp(underlying_ltp)
            if symbol_contracts and expiry:
                state.set_contracts(symbol_contracts, expiry)
                contracts.extend(symbol_contracts)
        return contracts[:STOCK_DEPTH_MAX_INSTRUMENTS]

    def _ws_url(self) -> str:
        return f"wss://depth-api-feed.dhan.co/twentydepth?token={self.settings.access_token}&clientId={self.settings.client_id}&authType=2"

    def _subscribe_payload(self) -> str:
        instruments = [{"ExchangeSegment": STOCK_DEPTH_EXCHANGE_SEGMENT, "SecurityId": c.security_id} for c in self._contracts]
        return json.dumps({"RequestCode": 23, "InstrumentCount": len(instruments), "InstrumentList": instruments})

    def _symbol_for_security_id(self, security_id: str) -> Optional[str]:
        for contract in self._contracts:
            if contract.security_id == security_id:
                return contract.symbol
        return None

    def _run_socket(self, deadline: float) -> None:
        self.ws = websocket.create_connection(self._ws_url(), timeout=15, enable_multithread=True)
        self.ws.settimeout(5)
        self.ws.send(self._subscribe_payload())
        while not self.stop_event.is_set() and time.monotonic() < deadline:
            try:
                raw = self.ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if raw is None:
                raise RuntimeError("DHAN_DEPTH_SOCKET_CLOSED")
            if isinstance(raw, str):
                continue
            for security_id, side, levels in _parse_depth_message(raw):
                symbol = self._symbol_for_security_id(security_id)
                if symbol:
                    self.states[symbol].update_depth(security_id, side, levels)
        try:
            self.ws.close()
        except Exception:
            pass

    def _quote_loop(self) -> None:
        while not self.stop_event.is_set():
            contracts = list(self._contracts)
            if contracts:
                try:
                    instruments = [
                        type("DepthInstrument", (), {"security_id": c.security_id, "exchange_segment": STOCK_DEPTH_EXCHANGE_SEGMENT})()
                        for c in contracts
                    ]
                    quotes = self.dhan_api.quote_snapshot(instruments)
                    by_symbol: dict[str, dict] = {}
                    for c in contracts:
                        quote = quotes.get(c.security_id)
                        if quote:
                            by_symbol.setdefault(c.symbol, {})[c.security_id] = quote
                    for symbol, symbol_quotes in by_symbol.items():
                        self.states[symbol].update_quotes(symbol_quotes)
                except Exception:
                    pass
            self.stop_event.wait(STOCK_DEPTH_QUOTE_REFRESH_SECONDS)

    def _loop(self) -> None:
        quote_thread = threading.Thread(target=self._quote_loop, daemon=True, name="psygrid-stock-depth-quotes")
        quote_thread.start()
        while not self.stop_event.is_set():
            batches = self._batches()
            if not batches:
                self.stop_event.wait(5.0)
                continue
            batch_symbols = batches[self._batch_index % len(batches)]
            self._batch_index += 1

            for symbol in self._batch_symbols:
                self.states[symbol].set_rotation_status("IDLE")
            self._batch_symbols = batch_symbols
            for symbol in batch_symbols:
                self.states[symbol].set_rotation_status("ACTIVE")

            try:
                contracts = self._refresh_contracts_for_batch(batch_symbols)
                if not contracts:
                    self.stop_event.wait(STOCK_DEPTH_RECONNECT_SECONDS)
                    continue
                self._contracts = contracts
                self._run_socket(deadline=time.monotonic() + STOCK_DEPTH_BATCH_SECONDS)
            except Exception as exc:
                for symbol in batch_symbols:
                    self.states[symbol].set_error(f"{type(exc).__name__}: {exc}")
                self.stop_event.wait(STOCK_DEPTH_RECONNECT_SECONDS)
        quote_thread.join(timeout=2)

    def snapshot(self, symbol: str) -> dict:
        key = symbol.strip().upper()
        if key not in self.states:
            return {
                "service": "PSYGRID", "symbol": key, "status": "UNKNOWN_SYMBOL",
                "error": "not part of the NIFTY 50 stock-options universe",
            }
        return self.states[key].snapshot()

    def listing(self) -> dict:
        entries = []
        for symbol, state in self.states.items():
            with state.lock:
                entry = {
                    "symbol": symbol, "status": state.status, "rotation_status": state.rotation_status,
                    "contract_count": len(state.contracts), "updated_at": state.updated_at,
                }
                if state.last_error:
                    entry["error"] = state.last_error
                entries.append(entry)
        resolved_count = len(self.stock_options_manager.instruments)
        batch_count = max(1, -(-resolved_count // STOCK_DEPTH_SYMBOLS_PER_BATCH)) if resolved_count else 1
        return {
            "service": "PSYGRID",
            "universe": "NIFTY_50_STOCK_DEPTH",
            "symbol_count": len(self.states),
            "resolved_count": resolved_count,
            "max_instruments_per_connection": STOCK_DEPTH_MAX_INSTRUMENTS,
            "strikes_per_symbol": STOCK_DEPTH_STRIKES_PER_SYMBOL,
            "symbols_per_batch": STOCK_DEPTH_SYMBOLS_PER_BATCH,
            "batch_seconds": STOCK_DEPTH_BATCH_SECONDS,
            "full_rotation_seconds": round(batch_count * STOCK_DEPTH_BATCH_SECONDS, 1),
            "data_source": "DHAN_FULL_MARKET_DEPTH_WEBSOCKET",
            "synthetic_data": False,
            "storage": "RAM_ONLY",
            "per_symbol_endpoint": "/public/stock-depth/{SYMBOL}.json",
            "stocks": entries,
        }


def stock_depth_json(manager: StockDepthManager, symbol: str) -> dict:
    return manager.snapshot(symbol)


def stock_depth_listing_json(manager: StockDepthManager) -> dict:
    return manager.listing()
