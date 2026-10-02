"""Per-stock and market research views for the API: the engines, evaluated once per minute and cached.

``ResearchViews`` turns the live runner's latest snapshot into:

- ``stock(key)``: everything PSYGRID knows about one stock at ``as_of``:
  current features and anomalies, the expected-response state (expected and
  actual move, gap, gap in sigma, market/sector/statistical contributions,
  pending response, delay profile), Full-packet microstructure, the index
  derivatives expectation, relationships, today's events, data quality and
  freshness;
- ``market()``: the market state and the derivatives expectation per index;
- ``ranked(...)``: stocks ordered by a response measure, for search.

Everything is computed from data available at ``as_of`` and cached by
``(session, as_of)``. The response model needs earlier sessions; it is built
once per session in a background thread (its first build reads 20 archived
sessions), and until it is ready the response section says ``WARMING``.
Nothing here is a recommendation; every section states what it measures.
"""

from __future__ import annotations

import logging
import threading

import numpy as np

from intelligence.archive import load_day
from intelligence.expectation import expectation
from intelligence.market_state import measure, with_history
from intelligence.microstructure_engine import index_rows, micro_state
from intelligence.response import align, evaluate_state, model_for, session_returns
from intelligence.stream import micro_rows, stream_day

log = logging.getLogger("psygrid.intelligence.research")

UNDERLYINGS = ("nifty", "banknifty", "midcpnifty")
RANKABLE = ("response_gap_sigma", "pending_response_sigma", "response_gap", "pending_response")


def _market_summary(state: dict) -> dict:
    if "regime" not in state:
        return state  # an error report
    keys = ("regime", "mean_correlation", "effective_dimension", "dispersion_bps", "percentiles")
    return {k: state.get(k) for k in keys}


class ResearchViews:
    def __init__(self, settings, runner):
        self.settings = settings
        self.runner = runner
        self._lock = threading.Lock()
        self._cache: dict = {}
        self._models: dict[str, object] = {}
        self._building: set[str] = set()
        self.model_errors: dict[str, str] = {}

    # --- inputs ------------------------------------------------------------------------------

    def _day(self, snapshot):
        day = getattr(self.runner, "day", None)
        if day is not None and day.session_date == snapshot.session_date:
            return day
        merged, _, _ = stream_day(self.settings.archive_dir, snapshot.session_date)
        return merged if merged is not None else load_day(self.settings.archive_dir, snapshot.session_date)

    def _model(self, session_date: str):
        with self._lock:
            if session_date in self._models:
                return self._models[session_date], "READY"
            if session_date in self.model_errors:
                return None, "UNAVAILABLE"
            if session_date not in self._building:
                self._building.add(session_date)
                threading.Thread(target=self._build, args=(session_date,), daemon=True,
                                 name="psygrid-intelligence-response-model").start()  # fmt: skip
        return None, "WARMING"

    def _build(self, session_date: str) -> None:
        try:
            model = model_for(self.settings.archive_dir, session_date, cache_root=self.settings.store_dir)
            with self._lock:
                if model is None:
                    self.model_errors[session_date] = "fewer than 5 earlier qualified sessions"
                else:
                    self._models = {session_date: model}  # keep only the current session's model
        except Exception as exc:
            log.warning("response model for %s failed: %s", session_date, exc, exc_info=True)
            with self._lock:
                self.model_errors[session_date] = f"{type(exc).__name__}: {exc}"[:300]
        finally:
            with self._lock:
                self._building.discard(session_date)

    def wait_for_model(self, session_date: str, timeout: float = 120.0) -> bool:
        """Block until the model for ``session_date`` is built or failed (tests and the CLI)."""
        import time

        end = time.monotonic() + timeout
        while time.monotonic() < end:
            with self._lock:
                if session_date in self._models or session_date in self.model_errors:
                    return session_date in self._models
                building = session_date in self._building
            if not building:
                return False
            time.sleep(0.05)
        return False

    # --- per-minute computation (cached) -----------------------------------------------------

    def _minute(self, snapshot) -> dict:
        key = (snapshot.session_date, snapshot.as_of)
        with self._lock:
            cached = self._cache.get(key)
        if cached is not None:
            return cached
        day = self._day(snapshot)
        as_of = snapshot.frame.as_of
        out: dict = {"session_date": snapshot.session_date, "as_of": snapshot.as_of}
        sr = session_returns(day, as_of)
        model, status = self._model(snapshot.session_date)
        out["response_status"] = status
        out["response_error"] = self.model_errors.get(snapshot.session_date)
        out["market_source"] = sr.market_source
        if model is not None and sr.r.shape[1]:
            out["model"] = model
            # The model covers every stock of its training sessions; today's key set grows as stocks trade.
            out["response"] = evaluate_state(model, align(sr, model.keys))
        try:
            state = measure(day, as_of)
            out["market_state"] = with_history(state, self.settings.archive_dir, self.settings.store_dir).view()
        except Exception as exc:
            out["market_state"] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
        out["expectation"] = {}
        for underlying in UNDERLYINGS:
            try:
                out["expectation"][underlying] = expectation(
                    self.settings.archive_dir, self.settings.store_dir, snapshot.session_date, underlying, as_of
                ).view()
            except Exception as exc:
                out["expectation"][underlying] = {"error": f"{type(exc).__name__}: {exc}"[:300]}
        try:
            out["micro"] = index_rows(micro_rows(self.settings.archive_dir, snapshot.session_date))
        except Exception as exc:
            out["micro"], out["micro_error"] = {}, f"{type(exc).__name__}: {exc}"[:300]
        with self._lock:
            self._cache = {key: out}  # only the latest minute is kept
        return out

    # --- views -------------------------------------------------------------------------------

    def market(self, snapshot) -> dict:
        m = self._minute(snapshot)
        return {
            **snapshot.header(),
            "market_state": m["market_state"],
            "derivatives_expectation": m["expectation"],
            "response_model": m["response_status"],
            "market_factor": m["market_source"],
        }

    def stock(self, snapshot, key: str, store) -> dict | None:
        base = snapshot.instrument(key)
        if base is None:
            return None
        m = self._minute(snapshot)
        response = m.get("response")
        if response is not None and key in response.keys:
            resp = {"status": "READY", **response.of(key, m["model"])}
        else:
            resp = {"status": m["response_status"], "reason": m.get("response_error")}
        series = m["micro"].get(key, [])
        micro = micro_state(series, key, int(snapshot.frame.as_of.timestamp())).view()
        events = store.search(instrument=key, session_date=snapshot.session_date, limit=20)
        freshness = {
            "as_of": snapshot.as_of,
            "engine_lag_seconds": self.runner.health().get("lag_seconds") if self.runner else None,
            "last_bar": (base.get("data_quality") or {}).get("last_bar"),
            "stale_bar": resp.get("stale"),
            "last_microstructure_minute": series[-1][1].get("timestamp") if series else None,
        }
        return {
            **snapshot.header(),
            "key": key,
            "sector": base["sector"],
            "state": {"features": base["features"], "anomalies": base["anomalies"]},
            "expected_response": resp,
            "liquidity_microstructure": micro,
            "derivatives_context": {
                "note": "index option chains are recorded each minute; stock option chains are not",
                "nifty": m["expectation"].get("nifty"),
            },
            "market_state": _market_summary(m["market_state"]),
            "relationships": base["relationships"],
            "events": events,
            "data_quality": base["data_quality"],
            "freshness": freshness,
            "disclaimer": "measurements of co-movement and order flow; not forecasts or recommendations",
        }

    def ranked(self, snapshot, by: str, limit: int, sector: str | None = None) -> dict:
        m = self._minute(snapshot)
        response = m.get("response")
        if response is None:
            return {**snapshot.header(), "status": m["response_status"], "stocks": []}
        values = getattr(response, {"response_gap_sigma": "gap_sigma", "pending_response_sigma": "pending_sigma",
                                    "response_gap": "gap", "pending_response": "pending"}[by])  # fmt: skip
        sectors = dict(zip(snapshot.features.keys, snapshot.features.sectors, strict=True))
        rows = []
        for i, key in enumerate(response.keys):
            v = float(values[i])
            if not np.isfinite(v) or response.stale[i] or (sector and sectors.get(key) != sector):
                continue
            rows.append({"key": key, "sector": sectors.get(key), by: round(v, 6),
                         "minutes_observed": int(response.observed[i])})  # fmt: skip
        rows.sort(key=lambda r: -abs(r[by]))
        return {**snapshot.header(), "status": "READY", "by": by, "total": len(rows), "stocks": rows[:limit]}
