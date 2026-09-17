from __future__ import annotations

import threading
from typing import Any, Optional

from psygrid_master_indicator import (
    IndicatorConfig,
    PsygridMasterIndicatorEngine,
    _freshness,
    _parse_ist_timestamp,
)


class IndicatorRuntime:
    """Synchronized in-process derived-data layer for Psygrid's live 1m feed."""

    def __init__(self, state, source_builder, interval_seconds: float = 1.0):
        self.state = state
        self.source_builder = source_builder
        self.interval_seconds = max(0.25, float(interval_seconds))
        self.engine = PsygridMasterIndicatorEngine(IndicatorConfig(include_series=False))
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._results: dict[str, dict[str, Any]] = {}
        self._fingerprints: dict[str, tuple[Any, ...]] = {}
        self._errors: dict[str, dict[str, str]] = {}
        self._source_meta: dict[str, Any] = {}
        self._sync_count = 0
        self._last_error: Optional[str] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="psygrid-indicators", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5.0)
        self._thread = None

    @staticmethod
    def _fingerprint(stock: dict[str, Any]) -> tuple[Any, ...]:
        candles = stock.get("candles_1m") or []
        if not candles:
            return (None, None, None, None, None, None)
        c = candles[-1]
        return (c.get("timestamp"), c.get("open"), c.get("high"), c.get("low"), c.get("close"), c.get("volume"))

    def _sync_once(self) -> None:
        payload = self.source_builder(self.state)
        if payload.get("service") != "PSYGRID":
            raise ValueError("indicator source builder returned non-PSYGRID payload")
        stocks = payload.get("stocks")
        if not isinstance(stocks, dict):
            raise ValueError("indicator source payload stocks must be an object")
        endpoint_now = payload.get("session", {}).get("current_time_ist")
        with self._lock:
            results = dict(self._results)
            fingerprints = dict(self._fingerprints)
            errors = dict(self._errors)
        for symbol, stock in stocks.items():
            fingerprint = self._fingerprint(stock)
            if fingerprint == fingerprints.get(symbol) and symbol in results:
                existing = dict(results[symbol])
                as_of = existing.get("as_of")
                if as_of:
                    freshness = _freshness(endpoint_now, _parse_ist_timestamp(as_of), self.engine.config)
                    existing["freshness"] = freshness
                    if freshness["status"] != "FRESH":
                        existing["indicators"] = {k: None for k in existing.get("indicators", {})}
                    results[symbol] = existing
                fingerprints[symbol] = fingerprint
                continue
            try:
                results[symbol] = self.engine.compute_stock(stock, endpoint_now)
                fingerprints[symbol] = fingerprint
                errors.pop(symbol, None)
            except Exception as exc:
                errors[symbol] = {"error_type": type(exc).__name__, "message": str(exc)}
                fingerprints[symbol] = fingerprint
        with self._lock:
            self._results = results
            self._fingerprints = fingerprints
            self._errors = errors
            self._source_meta = {
                "service": payload.get("service"),
                "schema_version": payload.get("schema_version"),
                "data_policy": payload.get("data_policy"),
                "synthetic_candles": payload.get("synthetic_candles"),
                "universe_size": payload.get("universe_size"),
                "endpoint_current_time_ist": endpoint_now,
                "source_endpoint": "/public/live.json",
            }
            self._sync_count += 1
            self._last_error = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                snap = self.state.snapshot() if hasattr(self.state, "snapshot") else {}
                if (
                    snap.get("session_status", getattr(self.state, "session_status", None)) == "LIVE"
                    and snap.get("feed_status") == "CONNECTED"
                    and snap.get("stock_count") == 450
                    and snap.get("subscribed_count") == 450
                    and snap.get("live_stock_count") == 450
                    and snap.get("stream_health") == "FULL_LIVE"
                ):
                    self._sync_once()
            except Exception as exc:
                with self._lock:
                    self._last_error = f"{type(exc).__name__}: {exc}"
            self._stop.wait(self.interval_seconds)

    def snapshot(self, stock_range: Optional[tuple[int, int]] = None) -> dict[str, Any]:
        with self._lock:
            results = dict(self._results)
            errors = dict(self._errors)
            meta = dict(self._source_meta)
            sync_count = self._sync_count
            last_error = self._last_error
        source = self.source_builder(self.state)
        ordered_symbols = list(source.get("stocks", {}).keys())
        expected_count = 450 if stock_range is None else (stock_range[1] - stock_range[0])
        if stock_range is not None:
            ordered_symbols = ordered_symbols[stock_range[0]:stock_range[1]]
        selected = {symbol: results[symbol] for symbol in ordered_symbols if symbol in results}
        selected_errors = {symbol: errors[symbol] for symbol in ordered_symbols if symbol in errors}
        fresh_count = sum(v.get("freshness", {}).get("status") == "FRESH" for v in selected.values())
        stale_count = sum(v.get("freshness", {}).get("status") == "STALE" for v in selected.values())
        time_error_count = sum(v.get("freshness", {}).get("status") == "TIME_ERROR" for v in selected.values())
        return {
            "service": "PSYGRID_MASTER_INDICATOR",
            "engine_version": "1.0.0",
            "status": "OK" if len(selected) == expected_count else ("STARTING" if not selected else "PARTIAL"),
            "source": meta,
            "timeframe": "1m",
            "universe_size": 450,
            "stock_count": len(selected),
            "processed_count": len(selected),
            "error_count": len(selected_errors),
            "fresh_count": fresh_count,
            "stale_count": stale_count,
            "time_error_count": time_error_count,
            "sync_count": sync_count,
            "errors": selected_errors,
            "results": selected,
            "runtime_error": last_error,
        }

    def stock(self, symbol: str) -> dict[str, Any]:
        symbol = symbol.upper()
        with self._lock:
            result = self._results.get(symbol)
            error = self._errors.get(symbol)
        if result is not None:
            return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "status": "OK", "source_endpoint": "/public/live.json", "timeframe": "1m", "result": result}
        if error is not None:
            return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "status": "ERROR", "source_endpoint": "/public/live.json", "timeframe": "1m", "symbol": symbol, "error": error}
        return {"service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0", "status": "NOT_FOUND", "source_endpoint": "/public/live.json", "timeframe": "1m", "symbol": symbol}
