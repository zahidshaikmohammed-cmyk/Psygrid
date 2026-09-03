from __future__ import annotations

from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from output_runtime import _higher_timeframes_ready, _stock_fixups
from output import market_live_json as _base_market_live_json

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
SCAN_START = 0
SCAN_END = 90


def _latest_completed_1m(state, security_id: str) -> Optional[dict]:
    with state.lock:
        rows = [dict(c) for c in state.live_candles.get(security_id, []) if c.get("complete", True)]
    if not rows:
        return None
    rows.sort(key=lambda row: int(row.get("epoch", row.get("timestamp", 0))))
    from output import normalize_candle
    return normalize_candle(rows[-1])


def build_scan_90(state) -> dict:
    """Return a compact, complete machine-readable scan for the first 90 sorted stocks.

    This endpoint intentionally omits the full candle histories that make the
    normal live endpoints very large. It preserves the latest current quote,
    freshness, latest completed 1m candle, and latest native 5m/15m/1h candles
    needed for universe qualification.
    """
    payload = _base_market_live_json(state, (SCAN_START, SCAN_END))
    _stock_fixups(state, payload)
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
    ready = sum(1 for row in compact.values() if row["freshness"].get("live_data_valid") and row["higher_timeframes_ready"])
    return {
        "service": "PSYGRID",
        "schema_version": "scan-1.0",
        "scan": {
            "name": "A45+B45",
            "requested_stock_count": 90,
            "returned_stock_count": len(symbols),
            "unique_stock_count": len(set(symbols)),
            "all_requested_stocks_accounted_for": len(symbols) == 90 and len(set(symbols)) == 90,
            "ordering": "SAME_AS_LIVE_A_THEN_LIVE_B",
            "source_endpoints": ["/public/live-a.json", "/public/live-b.json"],
            "generated_at_ist": datetime.now(PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST"),
        },
        "session": payload.get("session", {}),
        "rules": {
            "max_live_age_seconds": state.settings.max_live_age_seconds,
            "synthetic_candles": False,
            "native_higher_timeframes": True,
            "higher_timeframe_policy": "NATIVE_DHAN_ONLY_NO_1M_AGGREGATION",
            "incomplete_1m_not_used_as_latest_completed_1m": True,
        },
        "coverage": {
            "fresh_stock_count": fresh,
            "stale_or_missing_stock_count": 90 - fresh,
            "analysis_ready_stock_count": ready,
            "unprocessed_stock_count": 90 - len(symbols),
        },
        "stocks": compact,
    }
