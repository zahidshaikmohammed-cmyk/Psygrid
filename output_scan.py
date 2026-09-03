from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from output_runtime import _higher_timeframes_ready, market_live_json
from output import normalize_candle

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
UNIVERSE_SIZE = 270


def _latest_completed_1m(state, security_id: str) -> Optional[dict]:
    with state.lock:
        rows = [dict(c) for c in state.live_candles.get(security_id, []) if c.get("complete", True)]
    if not rows:
        return None
    rows.sort(key=lambda row: int(row.get("epoch", row.get("timestamp", 0))))
    return normalize_candle(rows[-1])


def build_scan(state, stock_range: tuple[int, int] = (0, UNIVERSE_SIZE), name: str = "WHOLE_UNIVERSE") -> dict:
    """Return a compact, complete machine-readable scan for the requested universe slice.

    Full candle histories stay on the normal live endpoints. This scan exposes
    exactly the records needed for qualification without truncation: freshness,
    current quote, latest completed 1m, and latest native 5m/15m/1h candles.
    """
    start, end = stock_range
    requested = max(0, end - start)
    payload = market_live_json(state, (start, end))
    stocks = payload.get("stocks", {})

    compact = {}
    for symbol, stock in stocks.items():
        security_id = str(stock["security_id"])
        timeframes = stock.get("timeframes", {})
        compact[symbol] = {
            "security_id": security_id,
            "freshness": stock.get("freshness", {}),
            "current": stock.get("current", {}),
            "latest_completed_1m": _latest_completed_1m(state, security_id),
            "latest_5m": (timeframes.get("5m") or [])[-1] if timeframes.get("5m") else None,
            "latest_15m": (timeframes.get("15m") or [])[-1] if timeframes.get("15m") else None,
            "latest_1h": (timeframes.get("1h") or [])[-1] if timeframes.get("1h") else None,
            "higher_timeframes_ready": _higher_timeframes_ready(stock),
        }

    symbols = list(compact.keys())
    fresh = sum(1 for row in compact.values() if row["freshness"].get("live_data_valid"))
    ready = sum(
        1
        for row in compact.values()
        if row["freshness"].get("live_data_valid") and row["higher_timeframes_ready"]
    )
    unique_count = len(set(symbols))

    return {
        "service": "PSYGRID",
        "schema_version": "scan-1.1",
        "scan": {
            "name": name,
            "requested_stock_count": requested,
            "returned_stock_count": len(symbols),
            "unique_stock_count": unique_count,
            "all_requested_stocks_accounted_for": len(symbols) == requested and unique_count == requested,
            "ordering": "SAME_AS_UNIVERSE_CONFIG",
            "source": "SAME_RAM_STATE_AS_LIVE_ENDPOINTS",
            "generated_at_ist": datetime.now(PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST"),
        },
        "session": payload.get("session", {}),
        "signal_input": payload.get("signal_input", {}),
        "rules": {
            "max_live_age_seconds": state.settings.max_live_age_seconds,
            "synthetic_candles": False,
            "native_higher_timeframes": True,
            "higher_timeframe_policy": "NATIVE_DHAN_ONLY_NO_1M_AGGREGATION",
            "incomplete_1m_not_used_as_latest_completed_1m": True,
        },
        "coverage": {
            "requested_stock_count": requested,
            "fresh_stock_count": fresh,
            "stale_or_missing_stock_count": requested - fresh,
            "analysis_ready_stock_count": ready,
            "unprocessed_stock_count": requested - len(symbols),
        },
        "stocks": compact,
    }


def build_scan_270(state) -> dict:
    return build_scan(state, (0, UNIVERSE_SIZE), "WHOLE_UNIVERSE_270")


def build_scan_90(state) -> dict:
    """Backward-compatible compact A45+B45 scan."""
    return build_scan(state, (0, 90), "A45+B45")
