"""Index option chains (NIFTY, BANKNIFTY, MIDCPNIFTY, SENSEX) from Dhan's native option-chain API.

One polling manager per index, each fully isolated from the equity universe and the
index layer. Nothing here interpolates or reconstructs option data: every row is
exactly what Dhan's option-chain endpoint returned.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import time as datetime_time
from zoneinfo import ZoneInfo

from auth_retry import AuthRetryGuard
from option_analytics import ChainAnalyticsTracker

OPTION_CHAIN_REFRESH_SECONDS = 3.2
EXPIRY_REFRESH_SECONDS = 1800.0
MARKET_OPEN = datetime_time(9, 15)
MARKET_CLOSE = datetime_time(15, 30)

UNDERLYING_EXCHANGE_SEGMENT = "IDX_I"
UNDERLYING_INSTRUMENT = "INDEX"


@dataclass(frozen=True)
class IndexDerivativesSpec:
    """Identity of one index whose options Psygrid tracks.

    ``security_id`` is the Dhan underlying index id (segment ``IDX_I``) used by the
    option-chain API; ``fno_segment`` is where that index's option contracts trade,
    which the depth feed subscribes on.
    """

    symbol: str
    security_id: str
    fno_segment: str

    @property
    def key(self) -> str:
        return self.symbol.lower()


NIFTY = IndexDerivativesSpec("NIFTY", "13", "NSE_FNO")
BANKNIFTY = IndexDerivativesSpec("BANKNIFTY", "25", "NSE_FNO")
MIDCPNIFTY = IndexDerivativesSpec("MIDCPNIFTY", "442", "NSE_FNO")
SENSEX = IndexDerivativesSpec("SENSEX", "51", "BSE_FNO")

INDEX_DERIVATIVES: tuple[IndexDerivativesSpec, ...] = (NIFTY, BANKNIFTY, MIDCPNIFTY, SENSEX)


@dataclass(frozen=True)
class OptionsInstrument:
    security_id: str
    exchange_segment: str = UNDERLYING_EXCHANGE_SEGMENT
    instrument: str = UNDERLYING_INSTRUMENT


def _is_market_open(now: datetime) -> bool:
    """Return regular index-derivatives session status in the configured timezone."""
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE


def _normalize_chain(raw: dict) -> list[dict]:
    """Convert Dhan's ``{"oc": {"<strike>": {"ce": {...}, "pe": {...}}}}`` into strike-sorted rows."""
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


class IndexOptionsState:
    """RAM-only option-chain state for one index."""

    def __init__(self, settings, spec: IndexDerivativesSpec):
        self.settings = settings
        self.spec = spec
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
        self.analytics: dict = {}

    def set_snapshot(self, payload: dict, expiry_list: list[str], expiry: str, analytics: dict | None = None) -> None:
        """Store a chain payload whose ``oc`` is either Dhan's raw strike map or already-normalized rows."""
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
            market_open = _is_market_open(datetime.now(self.tz))
            return {
                "service": "PSYGRID",
                "symbol": self.spec.symbol,
                "status": self.status,
                "market_status": "OPEN" if market_open else "CLOSED",
                "market_open": market_open,
                "data_source": "DHAN_OPTION_CHAIN_API",
                "security_id": self.spec.security_id,
                "exchange_segment": UNDERLYING_EXCHANGE_SEGMENT,
                "instrument": UNDERLYING_INSTRUMENT,
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


class IndexOptionsManager:
    """Poll Dhan's option-chain API for one index's nearest expiry."""

    def __init__(self, settings, dhan_api, spec: IndexDerivativesSpec):
        self.settings = settings
        self.dhan_api = dhan_api
        self.spec = spec
        self.instrument = OptionsInstrument(spec.security_id)
        self.state = IndexOptionsState(settings, spec)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._expiry_loaded_at = 0.0
        self._auth = AuthRetryGuard(settings, dhan_api)
        self._analytics_tracker = ChainAnalyticsTracker()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name=f"psygrid-{self.spec.key}-options")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _error_code(self, reason: str) -> str:
        return f"DHAN_{self.spec.symbol}_OPTIONS_{reason}"

    def _load_expiries(self) -> list[str]:
        expiries = self._auth.call(lambda: self.dhan_api.option_expiry_list(self.instrument))
        if not expiries:
            raise RuntimeError(self._error_code("NO_ACTIVE_EXPIRIES"))
        self._expiry_loaded_at = time.monotonic()
        return expiries

    def _load_chain(self, expiry: str) -> dict:
        return self._auth.call(lambda: self.dhan_api.option_chain(self.instrument, expiry))

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

                raw = self._load_chain(expiry)
                payload = raw.get("data") if isinstance(raw, dict) else None
                if not isinstance(payload, dict):
                    raise RuntimeError(self._error_code("INVALID_RESPONSE"))
                rows = _normalize_chain(payload)
                if not rows:
                    raise RuntimeError(self._error_code("EMPTY_CHAIN"))
                payload = {**payload, "oc": rows}
                analytics = self._analytics_tracker.update(rows, payload.get("last_price"))
                self.state.set_snapshot(payload, expiries, expiry, analytics)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(OPTION_CHAIN_REFRESH_SECONDS)


def index_options_json(state: IndexOptionsState) -> dict:
    return state.snapshot()
