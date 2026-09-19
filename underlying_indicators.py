from __future__ import annotations

"""Real technical-indicator suite (EMA/SMA/RSI/MACD/Bollinger/Supertrend/
ADX/Stochastic/ATR/CCI/MFI/ROC/Momentum/RVOL/CMF/Donchian) for a single
underlying's own 1-minute OHLCV history.

Reuses the exact same PsygridMasterIndicatorEngine that computes indicators
for the 990-equity universe: no separate/looser indicator math for
derivatives underlyings. Each instance only reads an already-public 1m
candle snapshot (from the sealed index layer, or MIDCPNIFTY's own candle
poller) through a callable; it never mutates or depends on the internals of
whatever produced those candles.
"""

import threading
from datetime import datetime
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from psygrid_master_indicator import IndicatorConfig, PsygridMasterIndicatorEngine

UNDERLYING_INDICATOR_INTERVAL_SECONDS = 5.0


class UnderlyingIndicatorRuntime:
    def __init__(self, symbol: str, candles_source: Callable[[], Optional[list[dict]]], settings, interval_seconds: float = UNDERLYING_INDICATOR_INTERVAL_SECONDS):
        self.symbol = symbol
        self.candles_source = candles_source
        self.tz = ZoneInfo(settings.timezone)
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.engine = PsygridMasterIndicatorEngine(IndicatorConfig(include_series=False))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._result: Optional[dict[str, Any]] = None
        self._error: Optional[dict[str, str]] = None
        self._fingerprint: Optional[tuple] = None
        self._sync_count = 0

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"psygrid-{self.symbol.lower()}-indicators", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5.0)
        self._thread = None

    @staticmethod
    def _fingerprint_of(candles: list[dict]) -> tuple:
        if not candles:
            return (None,)
        c = candles[-1]
        return (c.get("timestamp"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume"), len(candles))

    def _sync_once(self) -> None:
        candles = self.candles_source()
        if not candles:
            return
        fingerprint = self._fingerprint_of(candles)
        with self._lock:
            if fingerprint == self._fingerprint and self._result is not None:
                return
        endpoint_now = datetime.now(self.tz).strftime("%Y-%m-%d %H:%M:%S IST")
        try:
            result = self.engine.compute_stock({"symbol": self.symbol, "candles_1m": candles}, endpoint_now)
            with self._lock:
                self._result = result
                self._error = None
                self._fingerprint = fingerprint
                self._sync_count += 1
        except Exception as exc:
            with self._lock:
                self._error = {"error_type": type(exc).__name__, "message": str(exc)}
                self._fingerprint = fingerprint

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._sync_once()
            except Exception as exc:
                with self._lock:
                    self._error = {"error_type": type(exc).__name__, "message": str(exc)}
            self._stop.wait(self.interval_seconds)

    def last_updated_at(self) -> Optional[str]:
        with self._lock:
            return self._result.get("as_of") if self._result else None

    def snapshot(self) -> dict:
        with self._lock:
            result = self._result
            error = self._error
            sync_count = self._sync_count
        if result is not None:
            return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "symbol": self.symbol, "status": "OK", "timeframe": "1m", "sync_count": sync_count, "result": result}
        if error is not None:
            return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "symbol": self.symbol, "status": "ERROR", "timeframe": "1m", "error": error}
        return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "symbol": self.symbol, "status": "STARTING", "timeframe": "1m"}
