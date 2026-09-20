from __future__ import annotations

import json
import struct
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time
from zoneinfo import ZoneInfo

import websocket


# SENSEX options trade on BSE's derivatives segment, unlike the underlying
# index identity itself (IDX_I) used by the option-chain endpoint.
SENSEX_DEPTH_EXCHANGE_SEGMENT = "BSE_FNO"
SENSEX_DEPTH_INSTRUMENT = "OPTIDX"
SENSEX_DEPTH_SYMBOL = "SENSEX"
SENSEX_DEPTH_SECURITY_ID = "51"
SENSEX_DEPTH_LEVELS = 20
SENSEX_DEPTH_MAX_INSTRUMENTS = 50
SENSEX_DEPTH_RECONNECT_SECONDS = 3.0
SENSEX_DEPTH_QUOTE_REFRESH_SECONDS = 1.0
SENSEX_MARKET_OPEN = datetime_time(9, 15)
SENSEX_MARKET_CLOSE = datetime_time(15, 30)


@dataclass(frozen=True)
class SensexDepthContract:
    security_id: str
    strike: float
    option_type: str
    expiry: str


class SensexDepthState:
    """RAM-only current SENSEX option premium-depth snapshot."""

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.expiry: str | None = None
        self.underlying_ltp: float | None = None
        self.contracts: dict[str, dict] = {}
        self.connection_count = 0
        self.packet_count = 0

    def set_contracts(self, contracts: list[SensexDepthContract], expiry: str) -> None:
        with self.lock:
            self.expiry = expiry
            allowed = {contract.security_id: contract for contract in contracts}
            self.contracts = {
                security_id: self.contracts.get(security_id, {
                    "security_id": security_id,
                    "strike": contract.strike,
                    "option_type": contract.option_type,
                    "expiry": contract.expiry,
                    "bid": [],
                    "ask": [],
                })
                for security_id, contract in allowed.items()
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

    def set_underlying_ltp(self, value: float | None) -> None:
        with self.lock:
            self.underlying_ltp = value

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
                cleaned = {key: (list(value) if key in ("bid", "ask") else dict(value) if key == "ohlc" and isinstance(value, dict) else value) for key, value in row.items()}
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
                "symbol": SENSEX_DEPTH_SYMBOL,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_FULL_MARKET_DEPTH_WEBSOCKET",
                "underlying_security_id": SENSEX_DEPTH_SECURITY_ID,
                "exchange_segment": SENSEX_DEPTH_EXCHANGE_SEGMENT,
                "instrument": SENSEX_DEPTH_INSTRUMENT,
                "depth_levels": SENSEX_DEPTH_LEVELS,
                "underlying_ltp": self.underlying_ltp,
                "expiry": self.expiry,
                "contract_count": len(rows),
                "contracts": rows,
                "updated_at": self.updated_at,
                "connection_count": self.connection_count,
                "packet_count": self.packet_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "quote_refresh_seconds": SENSEX_DEPTH_QUOTE_REFRESH_SECONDS,
                **({"error": self.last_error} if self.last_error else {}),
            }


def _is_market_open(now: datetime) -> bool:
    return now.weekday() < 5 and SENSEX_MARKET_OPEN <= now.time() < SENSEX_MARKET_CLOSE


def _select_contracts(option_state) -> tuple[list[SensexDepthContract], str | None, float | None]:
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
    selected_strikes = set(strikes_sorted[:25])
    contracts = [SensexDepthContract(security_id=item[3], strike=item[1], option_type=item[2], expiry=str(expiry)) for item in normalized if item[1] in selected_strikes]
    contracts.sort(key=lambda item: (abs(item.strike - float(underlying_ltp)), item.strike, item.option_type))
    return contracts[:SENSEX_DEPTH_MAX_INSTRUMENTS], str(expiry), float(underlying_ltp)


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
            for index in range(SENSEX_DEPTH_LEVELS):
                price, quantity, orders = struct.unpack_from("<dII", data, base + index * 16)
                levels.append({"level": index + 1, "price": float(price), "quantity": int(quantity), "orders": int(orders)})
            messages.append((security_id, side, levels))
        offset += message_length
    return messages


class SensexDepthManager:
    """Maintains real-time 20-level premium depth for the nearest 25 SENSEX strikes."""

    def __init__(self, settings, dhan_api, option_manager):
        self.settings = settings
        self.dhan_api = dhan_api
        self.option_manager = option_manager
        self.state = SensexDepthState(settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.ws = None
        self._contracts: list[SensexDepthContract] = []
        self._expiry: str | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-sensex-depth")
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

    def _refresh_contracts(self) -> bool:
        contracts, expiry, underlying_ltp = _select_contracts(self.option_manager.state)
        if not contracts or not expiry:
            return False
        ids = {contract.security_id for contract in contracts}
        current_ids = {contract.security_id for contract in self._contracts}
        if expiry != self._expiry or ids != current_ids:
            self._contracts = contracts
            self._expiry = expiry
            self.state.set_contracts(contracts, expiry)
            self.state.set_underlying_ltp(underlying_ltp)
            return True
        self.state.set_underlying_ltp(underlying_ltp)
        return False

    def _ws_url(self) -> str:
        return f"wss://depth-api-feed.dhan.co/twentydepth?token={self.settings.access_token}&clientId={self.settings.client_id}&authType=2"

    def _subscribe_payload(self) -> str:
        instruments = [{"ExchangeSegment": SENSEX_DEPTH_EXCHANGE_SEGMENT, "SecurityId": contract.security_id} for contract in self._contracts]
        return json.dumps({"RequestCode": 23, "InstrumentCount": len(instruments), "InstrumentList": instruments})

    def _run_socket(self) -> None:
        self.ws = websocket.create_connection(self._ws_url(), timeout=15, enable_multithread=True)
        self.ws.settimeout(5)
        self.ws.send(self._subscribe_payload())
        self.state.connection_count += 1
        while not self.stop_event.is_set():
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
        try:
            self.ws.close()
        except Exception:
            pass

    def _quote_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                contracts, expiry, underlying_ltp = _select_contracts(self.option_manager.state)
                if contracts:
                    instruments = [type("DepthInstrument", (), {"security_id": c.security_id, "exchange_segment": SENSEX_DEPTH_EXCHANGE_SEGMENT})() for c in contracts]
                    quotes = self.dhan_api.quote_snapshot(instruments)
                    self.state.update_quotes(quotes)
                    self.state.set_underlying_ltp(underlying_ltp)
                    if expiry and expiry != self._expiry:
                        self._contracts = contracts
                        self._expiry = expiry
                        self.state.set_contracts(contracts, expiry)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(SENSEX_DEPTH_QUOTE_REFRESH_SECONDS)

    def _loop(self) -> None:
        quote_thread = threading.Thread(target=self._quote_loop, daemon=True, name="psygrid-sensex-depth-quotes")
        quote_thread.start()
        while not self.stop_event.is_set():
            try:
                changed = self._refresh_contracts()
                if not self._contracts:
                    self.state.status = "STARTING"
                    self.stop_event.wait(1.0)
                    continue
                if changed and self.ws is not None:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                self._run_socket()
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
                self.stop_event.wait(SENSEX_DEPTH_RECONNECT_SECONDS)
        quote_thread.join(timeout=2)


def sensex_depth_json(state: SensexDepthState) -> dict:
    return state.snapshot()
