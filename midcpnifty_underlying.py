from __future__ import annotations

"""Real (non-synthetic) 1-minute OHLCV history for the MIDCPNIFTY index
itself, independent of the option-chain/depth domain.

MIDCPNIFTY is not one of Psygrid's sealed 16 index-layer symbols, so it has
no live WebSocket tick subscription anywhere in this codebase. Rather than
add a new WebSocket feed for it (widening the depth/streaming surface for a
single underlying that only needs 1-minute bars), this polls Dhan's own
completed-candle historical API on the same instrument identity already
used by the MIDCPNIFTY option chain (security_id 442, IDX_I, INDEX). Candles
are never fabricated: only what Dhan reports as completed is kept.
"""

import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from midcpnifty_options import MidcapNiftyOptionsInstrument
from output import _clean_candle, _dedupe_candles

MIDCPNIFTY_UNDERLYING_POLL_SECONDS = 20.0


class MidcapNiftyUnderlyingState:
    """RAM-only current MIDCPNIFTY index 1-minute candle history."""

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: str | None = None
        self.candles_1m: list[dict] = []

    def merge_candles(self, rows: list[dict]) -> None:
        with self.lock:
            merged = _dedupe_candles(self.candles_1m + list(rows))
            self.candles_1m = merged
            self.updated_at = datetime.now(self.tz).isoformat()
            self.status = "LIVE" if merged else "STARTING"
            self.last_error = ""

    def set_error(self, error: str) -> None:
        with self.lock:
            self.status = "ERROR"
            self.last_error = error

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "service": "PSYGRID",
                "symbol": "MIDCPNIFTY",
                "status": self.status,
                "data_source": "DHAN_HISTORICAL_API",
                "synthetic_candles": False,
                "storage": "RAM_ONLY",
                "candles_1m": [_clean_candle(c) for c in self.candles_1m],
                "updated_at": self.updated_at,
                **({"error": self.last_error} if self.last_error else {}),
            }


class MidcapNiftyUnderlyingManager:
    """Independently polls real MIDCPNIFTY 1-minute candles from Dhan."""

    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self.instrument = MidcapNiftyOptionsInstrument()
        self.state = MidcapNiftyUnderlyingState(settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-midcpnifty-underlying")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                rows = self.dhan_api.load_today_completed_intraday(self.instrument, 1)
                self.state.merge_candles(rows)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(MIDCPNIFTY_UNDERLYING_POLL_SECONDS)
