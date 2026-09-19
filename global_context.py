from __future__ import annotations

"""Delayed, official reference data for global-market context, sourced
only from FRED (Federal Reserve Bank of St. Louis) — a free, official,
ToS-clean API with no production-use restriction.

Every value here is explicitly marked market_data_status: "DELAYED": FRED
publishes end-of-day or next-business-day official releases, never live
ticks. Deliberately NOT included, because no free, ToS-clean, reliable
official source was found for them: GIFT NIFTY, NASDAQ, Dow Jones, Nikkei,
Hang Seng, Shanghai, KOSPI, DXY, gold. Nothing here is approximated or
substituted for those.
"""

import os
import threading
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_SERIES = {
    "sp500": "SP500",
    "vix": "VIXCLS",
    "us_10y_yield": "DGS10",
    "wti_crude_oil": "DCOILWTICO",
    "usd_inr": "DEXINUS",
}
GLOBAL_CONTEXT_REFRESH_SECONDS = 3600.0  # FRED series update at most once/day


class GlobalContextState:
    def __init__(self, settings):
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: Optional[str] = None
        self.series: dict = {}

    def set_series(self, series: dict) -> None:
        with self.lock:
            self.series = series
            self.updated_at = datetime.now(self.tz).isoformat()
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
                "status": self.status,
                "market_data_status": "DELAYED",
                "note": "Official end-of-day/next-business-day FRED releases. Never live ticks.",
                "not_available": ["gift_nifty", "nasdaq", "dow_jones", "nikkei", "hang_seng", "shanghai", "kospi", "dxy", "gold"],
                "series": dict(self.series),
                "updated_at": self.updated_at,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "refresh_seconds": GLOBAL_CONTEXT_REFRESH_SECONDS,
                **({"error": self.last_error} if self.last_error else {}),
            }


class GlobalContextManager:
    """Polls FRED for delayed official reference series. Requires the
    FRED_API_KEY environment variable; if it is absent, this reports a
    clear error rather than silently omitting data or fabricating values.
    """

    def __init__(self, settings):
        self.settings = settings
        self.state = GlobalContextState(settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.session = requests.Session()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-global-context")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _fetch_series(self, api_key: str, series_id: str) -> dict:
        response = self.session.get(
            FRED_BASE_URL,
            params={"series_id": series_id, "api_key": api_key, "file_type": "json", "sort_order": "desc", "limit": 1},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        observations = data.get("observations") or []
        if not observations:
            raise RuntimeError(f"FRED_{series_id}_NO_OBSERVATIONS")
        latest = observations[0]
        try:
            value = float(latest.get("value"))
        except (TypeError, ValueError):
            value = None
        return {
            "series_id": series_id,
            "value": value,
            "source_date": latest.get("date"),
            "source": "FRED_FEDERAL_RESERVE_BANK_OF_ST_LOUIS",
        }

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            api_key = os.getenv("FRED_API_KEY", "").strip()
            if not api_key:
                self.state.set_error("FRED_API_KEY environment variable is not set; global-context is unavailable")
                self.stop_event.wait(GLOBAL_CONTEXT_REFRESH_SECONDS)
                continue
            try:
                series = {name: self._fetch_series(api_key, series_id) for name, series_id in FRED_SERIES.items()}
                self.state.set_series(series)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(GLOBAL_CONTEXT_REFRESH_SECONDS)


def global_context_json(state: GlobalContextState) -> dict:
    return state.snapshot()
