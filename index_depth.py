"""Real-time 20-level option depth for the index option chains in ``index_options``.

Each index gets one Dhan full-market-depth WebSocket subscribed to the CE and PE
contracts of the 25 strikes nearest the underlying (50 instruments, Dhan's per-
connection cap), plus a REST quote poll for LTP/volume/OI on the same contracts.
"""

from __future__ import annotations

import contextlib
import json
import struct
import threading
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import websocket

from index_options import (
    MARKET_CLOSED_RECHECK_SECONDS,
    MARKET_CLOSED_STATUS,
    IndexDerivativesSpec,
    _is_market_open,
)

DEPTH_INSTRUMENT = "OPTIDX"
DEPTH_LEVELS = 20
DEPTH_MAX_INSTRUMENTS = 50
DEPTH_NEAREST_STRIKES = 25
DEPTH_RECONNECT_SECONDS = 3.0
DEPTH_QUOTE_REFRESH_SECONDS = 1.0
DEPTH_WS_URL = "wss://depth-api-feed.dhan.co/twentydepth"

_BID_RESPONSE_CODE = 41
_ASK_RESPONSE_CODE = 51
_HEADER_BYTES = 12
_LEVEL_STRUCT = "<dII"
_LEVEL_BYTES = struct.calcsize(_LEVEL_STRUCT)
_SIDE_PACKET_BYTES = _HEADER_BYTES + DEPTH_LEVELS * _LEVEL_BYTES
_QUOTE_FIELDS = ("last_price", "average_price", "buy_quantity", "sell_quantity", "volume", "oi")


@dataclass(frozen=True)
class DepthContract:
    security_id: str
    strike: float
    option_type: str
    expiry: str


class IndexDepthState:
    """RAM-only current option premium-depth snapshot for one index."""

    def __init__(self, settings, spec: IndexDerivativesSpec):
        self.settings = settings
        self.spec = spec
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.expiry: str | None = None
        self.underlying_ltp: float | None = None
        self.contracts: dict[str, dict] = {}
        # Optional research recorder (option_depth_recorder.OptionDepthRecorder.observe), called once per
        # side packet while this state's lock is held.
        self.observer = None
        self.connection_count = 0
        self.packet_count = 0

    def set_contracts(self, contracts: list[DepthContract], expiry: str) -> None:
        """Replace the tracked contract set, keeping existing depth for contracts that remain."""
        with self.lock:
            self.expiry = expiry
            self.contracts = {
                contract.security_id: self.contracts.get(
                    contract.security_id,
                    {
                        "security_id": contract.security_id,
                        "strike": contract.strike,
                        "option_type": contract.option_type,
                        "expiry": contract.expiry,
                        "bid": [],
                        "ask": [],
                    },
                )
                for contract in contracts
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
            observer = self.observer
            if observer is not None:  # the option-depth recorder: constant time, never raises
                observer(self.spec.symbol, row, side, levels, self.underlying_ltp)

    def update_quotes(self, quotes: dict[str, dict]) -> None:
        with self.lock:
            for security_id, quote in quotes.items():
                row = self.contracts.get(str(security_id))
                if row is None or not isinstance(quote, dict):
                    continue
                for key in _QUOTE_FIELDS:
                    if key in quote:
                        row[key] = quote[key]
                ohlc = quote.get("ohlc")
                if isinstance(ohlc, dict):
                    row["ohlc"] = dict(ohlc)
                row["quote_updated_at"] = datetime.now(self.tz).isoformat()

    def set_underlying_ltp(self, value: float | None) -> None:
        with self.lock:
            self.underlying_ltp = value

    def set_error(self, error: str) -> None:
        with self.lock:
            self.status = "ERROR"
            self.last_error = error

    def set_market_closed(self) -> None:
        """Pause outside market hours; the last depth stays available."""
        with self.lock:
            self.status = MARKET_CLOSED_STATUS
            self.last_error = ""

    def snapshot(self) -> dict:
        with self.lock:
            market_open = _is_market_open(datetime.now(self.tz))
            rows = [_snapshot_row(row) for row in self.contracts.values()]
            rows.sort(key=lambda row: (row.get("strike", 0.0), row.get("option_type", "")))
            return {
                "service": "PSYGRID",
                "symbol": self.spec.symbol,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_FULL_MARKET_DEPTH_WEBSOCKET",
                "underlying_security_id": self.spec.security_id,
                "exchange_segment": self.spec.fno_segment,
                "instrument": DEPTH_INSTRUMENT,
                "depth_levels": DEPTH_LEVELS,
                "underlying_ltp": self.underlying_ltp,
                "expiry": self.expiry,
                "contract_count": len(rows),
                "contracts": rows,
                "updated_at": self.updated_at,
                "connection_count": self.connection_count,
                "packet_count": self.packet_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "quote_refresh_seconds": DEPTH_QUOTE_REFRESH_SECONDS,
                **({"error": self.last_error} if self.last_error else {}),
            }


def _snapshot_row(row: dict) -> dict:
    """Copy a contract row and flag a crossed book (top bid above top ask)."""
    cleaned = {}
    for key, value in row.items():
        if key in ("bid", "ask"):
            cleaned[key] = list(value)
        elif key == "ohlc" and isinstance(value, dict):
            cleaned[key] = dict(value)
        else:
            cleaned[key] = value
    bids = cleaned.get("bid") or []
    asks = cleaned.get("ask") or []
    top_bid = bids[0].get("price") if bids else None
    top_ask = asks[0].get("price") if asks else None
    cleaned["crossed_book"] = top_bid is not None and top_ask is not None and top_bid > top_ask
    return cleaned


def _select_contracts(option_state) -> tuple[list[DepthContract], str | None, float | None]:
    """Pick CE and PE contracts for the strikes nearest the underlying, capped at Dhan's 50 instruments."""
    snapshot = option_state.snapshot()
    expiry = snapshot.get("expiry")
    underlying_ltp = snapshot.get("underlying_ltp")
    if not expiry or not isinstance(underlying_ltp, (int, float)):
        return [], expiry, underlying_ltp
    ltp = float(underlying_ltp)
    candidates: list[tuple[float, str, str]] = []
    for row in snapshot.get("strikes", []):
        if not isinstance(row, dict) or not isinstance(row.get("strike"), (int, float)):
            continue
        strike = float(row["strike"])
        for option_type, key in (("CE", "ce"), ("PE", "pe")):
            contract = row.get(key)
            security_id = contract.get("security_id") if isinstance(contract, dict) else None
            if security_id:
                candidates.append((strike, option_type, str(security_id)))
    if not candidates:
        return [], expiry, ltp
    nearest = sorted({strike for strike, _, _ in candidates}, key=lambda strike: (abs(strike - ltp), strike))
    selected = set(nearest[:DEPTH_NEAREST_STRIKES])
    contracts = [
        DepthContract(security_id=security_id, strike=strike, option_type=option_type, expiry=str(expiry))
        for strike, option_type, security_id in candidates
        if strike in selected
    ]
    contracts.sort(key=lambda item: (abs(item.strike - ltp), item.strike, item.option_type))
    return contracts[:DEPTH_MAX_INSTRUMENTS], str(expiry), ltp


def _parse_depth_message(data: bytes) -> list[tuple[str, str, list[dict]]]:
    """Decode Dhan 20-depth binary frames into ``(security_id, side, levels)`` tuples."""
    messages = []
    offset = 0
    while offset + _HEADER_BYTES <= len(data):
        message_length = struct.unpack_from("<H", data, offset)[0]
        if message_length < _HEADER_BYTES or offset + message_length > len(data):
            break
        response_code = data[offset + 2]
        security_id = str(struct.unpack_from("<i", data, offset + 4)[0])
        if response_code in (_BID_RESPONSE_CODE, _ASK_RESPONSE_CODE) and message_length >= _SIDE_PACKET_BYTES:
            side = "bid" if response_code == _BID_RESPONSE_CODE else "ask"
            base = offset + _HEADER_BYTES
            levels = []
            for index in range(DEPTH_LEVELS):
                price, quantity, orders = struct.unpack_from(_LEVEL_STRUCT, data, base + index * _LEVEL_BYTES)
                levels.append(
                    {"level": index + 1, "price": float(price), "quantity": int(quantity), "orders": int(orders)}
                )
            messages.append((security_id, side, levels))
        offset += message_length
    return messages


class IndexDepthManager:
    """Maintain 20-level premium depth for one index's nearest strikes."""

    def __init__(self, settings, dhan_api, option_manager, spec: IndexDerivativesSpec):
        self.settings = settings
        self.dhan_api = dhan_api
        self.option_manager = option_manager
        self.spec = spec
        self.state = IndexDepthState(settings, spec)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.ws = None
        self._contracts: list[DepthContract] = []
        self._expiry: str | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name=f"psygrid-{self.spec.key}-depth")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self._close_socket()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None
        self.ws = None

    def _close_socket(self) -> None:
        ws = self.ws
        if ws is None:
            return
        with contextlib.suppress(Exception):
            ws.close()

    def _market_open(self) -> bool:
        return _is_market_open(datetime.now(self.state.tz))

    def _refresh_contracts(self) -> bool:
        """Re-select contracts from the option chain; return True if the subscription set changed."""
        contracts, expiry, underlying_ltp = _select_contracts(self.option_manager.state)
        if not contracts or not expiry:
            return False
        self.state.set_underlying_ltp(underlying_ltp)
        ids = {contract.security_id for contract in contracts}
        current_ids = {contract.security_id for contract in self._contracts}
        if expiry == self._expiry and ids == current_ids:
            return False
        self._contracts = contracts
        self._expiry = expiry
        self.state.set_contracts(contracts, expiry)
        return True

    def _ws_url(self) -> str:
        return f"{DEPTH_WS_URL}?token={self.settings.access_token}&clientId={self.settings.client_id}&authType=2"

    def _subscribe_payload(self) -> str:
        instruments = [
            {"ExchangeSegment": self.spec.fno_segment, "SecurityId": contract.security_id}
            for contract in self._contracts
        ]
        return json.dumps({"RequestCode": 23, "InstrumentCount": len(instruments), "InstrumentList": instruments})

    def _run_socket(self) -> None:
        self.ws = websocket.create_connection(self._ws_url(), timeout=15, enable_multithread=True)
        self.ws.settimeout(5)
        self.ws.send(self._subscribe_payload())
        self.state.connection_count += 1
        while not self.stop_event.is_set() and self._market_open():
            try:
                raw = self.ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if raw is None:
                raise RuntimeError("DHAN_DEPTH_SOCKET_CLOSED")
            if isinstance(raw, str):
                continue
            for security_id, side, levels in _parse_depth_message(raw):
                self.state.update_depth(security_id, side, levels)
        self._close_socket()

    def _quote_loop(self) -> None:
        while not self.stop_event.is_set():
            if not self._market_open():
                self.stop_event.wait(MARKET_CLOSED_RECHECK_SECONDS)
                continue
            try:
                contracts, expiry, underlying_ltp = _select_contracts(self.option_manager.state)
                if contracts:
                    instruments = [
                        SimpleNamespace(security_id=c.security_id, exchange_segment=self.spec.fno_segment)
                        for c in contracts
                    ]
                    self.state.update_quotes(self.dhan_api.quote_snapshot(instruments))
                    self.state.set_underlying_ltp(underlying_ltp)
                    if expiry and expiry != self._expiry:
                        self._contracts = contracts
                        self._expiry = expiry
                        self.state.set_contracts(contracts, expiry)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(DEPTH_QUOTE_REFRESH_SECONDS)

    def _loop(self) -> None:
        quote_thread = threading.Thread(
            target=self._quote_loop, daemon=True, name=f"psygrid-{self.spec.key}-depth-quotes"
        )
        quote_thread.start()
        while not self.stop_event.is_set():
            if not self._market_open():
                self._close_socket()
                self.state.set_market_closed()
                self.stop_event.wait(MARKET_CLOSED_RECHECK_SECONDS)
                continue
            try:
                changed = self._refresh_contracts()
                if not self._contracts:
                    self.state.status = "STARTING"
                    self.stop_event.wait(1.0)
                    continue
                if changed:
                    self._close_socket()
                self._run_socket()
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
                self.stop_event.wait(DEPTH_RECONNECT_SECONDS)
        quote_thread.join(timeout=2)


def index_depth_json(state: IndexDepthState) -> dict:
    return state.snapshot()
