from __future__ import annotations

import threading
import time
from datetime import datetime
from dataclasses import dataclass
from zoneinfo import ZoneInfo


NIFTY_OPTIONS_SYMBOL = "NIFTY"
NIFTY_OPTIONS_SECURITY_ID = "13"
NIFTY_OPTIONS_EXCHANGE_SEGMENT = "IDX_I"
NIFTY_OPTIONS_INSTRUMENT = "INDEX"
OPTION_CHAIN_REFRESH_SECONDS = 3.2
EXPIRY_REFRESH_SECONDS = 1800.0


@dataclass(frozen=True)
class NiftyOptionsInstrument:
    security_id: str = NIFTY_OPTIONS_SECURITY_ID
    exchange_segment: str = NIFTY_OPTIONS_EXCHANGE_SEGMENT
    instrument: str = NIFTY_OPTIONS_INSTRUMENT


class NiftyOptionsState:
    """RAM-only state for the isolated NIFTY option-chain domain."""

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.underlying_ltp: float | None = None
        self.expiry_list: list[str] = []
        self.expiry: str | None = None
        self.rows: list[dict] = []
        self.fetch_count = 0

    def set_snapshot(self, payload: dict, expiry_list: list[str], expiry: str) -> None:
        with self.lock:
            self.underlying_ltp = payload.get("last_price")
            self.expiry_list = list(expiry_list)
            self.expiry = expiry
            chain = payload.get("oc", [])
            self.rows = list(chain) if isinstance(chain, list) else _normalize_chain(payload)
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
            return {
                "service": "PSYGRID",
                "symbol": NIFTY_OPTIONS_SYMBOL,
                "status": self.status,
                "data_source": "DHAN_OPTION_CHAIN_API",
                "security_id": NIFTY_OPTIONS_SECURITY_ID,
                "exchange_segment": NIFTY_OPTIONS_EXCHANGE_SEGMENT,
                "instrument": NIFTY_OPTIONS_INSTRUMENT,
                "underlying_ltp": self.underlying_ltp,
                "expiry": self.expiry,
                "expiry_list": list(self.expiry_list),
                "strikes": [dict(row) for row in self.rows],
                "updated_at": self.updated_at,
                "fetch_count": self.fetch_count,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "refresh_seconds": OPTION_CHAIN_REFRESH_SECONDS,
                **({"error": self.last_error} if self.last_error else {}),
            }


def _normalize_chain(raw: dict) -> list[dict]:
    chain = raw.get("oc", {}) if isinstance(raw, dict) else {}
    if not isinstance(chain, dict):
        return []
    rows: list[dict] = []
    for strike_key, pair in chain.items():
        if not isinstance(pair, dict):
            continue
        try:
            strike = float(strike_key)
        except (TypeError, ValueError):
            continue
        ce = pair.get("ce") if isinstance(pair.get("ce"), dict) else None
        pe = pair.get("pe") if isinstance(pair.get("pe"), dict) else None
        rows.append({"strike": strike, "ce": dict(ce) if ce else None, "pe": dict(pe) if pe else None})
    rows.sort(key=lambda row: row["strike"])
    return rows


class NiftyOptionsManager:
    """Poll Dhan's native option-chain API; never reconstructs synthetic options data."""

    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self.instrument = NiftyOptionsInstrument()
        self.state = NiftyOptionsState(settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._expiry_loaded_at = 0.0

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-nifty-options")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _load_expiries(self) -> list[str]:
        expiries = self.dhan_api.option_expiry_list(self.instrument)
        if not expiries:
            raise RuntimeError("DHAN_NIFTY_OPTIONS_NO_ACTIVE_EXPIRIES")
        self._expiry_loaded_at = time.monotonic()
        return expiries

    def _loop(self) -> None:
        expiries: list[str] = []
        expiry: str | None = None
        while not self.stop_event.is_set():
            try:
                if not expiries or time.monotonic() - self._expiry_loaded_at >= EXPIRY_REFRESH_SECONDS:
                    expiries = self._load_expiries()
                    expiry = expiries[0]
                elif expiry not in expiries:
                    expiry = expiries[0]

                raw = self.dhan_api.option_chain(self.instrument, expiry)
                payload = raw.get("data") if isinstance(raw, dict) else None
                if not isinstance(payload, dict):
                    raise RuntimeError("DHAN_NIFTY_OPTIONS_INVALID_RESPONSE")
                rows = _normalize_chain(payload)
                if not rows:
                    raise RuntimeError("DHAN_NIFTY_OPTIONS_EMPTY_CHAIN")
                payload = dict(payload)
                payload["oc"] = rows
                self.state.set_snapshot(payload, expiries, expiry)
                self.stop_event.wait(OPTION_CHAIN_REFRESH_SECONDS)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
                self.stop_event.wait(OPTION_CHAIN_REFRESH_SECONDS)


def nifty_options_json(state: NiftyOptionsState) -> dict:
    return state.snapshot()
