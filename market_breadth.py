from __future__ import annotations

"""Raw market-breadth and sector aggregates computed directly from the
already-live 990-equity RAM state. No new data source: this only aggregates
numbers Psygrid already holds (LTP, previous close, today's session
high/low) into counts and medians.

RAW DATA ONLY. No BULLISH/BEARISH/CONFIRMED/STRONG labels — those are for
a future interpretation layer, not this data-supply layer.
"""

from datetime import datetime
from statistics import median
from typing import Optional
from zoneinfo import ZoneInfo

from sector_taxonomy import sector_for_symbol


def _float(value) -> Optional[float]:
    try:
        value = float(value)
        return value if value == value else None  # reject NaN
    except (TypeError, ValueError):
        return None


def _session_high_low(state, security_id: str) -> tuple[Optional[float], Optional[float]]:
    with state.lock:
        candles = list(state.live_candles.get(security_id, []))
        current = state.current_1m.get(security_id)
    if current is not None:
        candles = candles + [current]
    highs = [_float(c.get("high")) for c in candles if isinstance(c, dict)]
    lows = [_float(c.get("low")) for c in candles if isinstance(c, dict)]
    highs = [h for h in highs if h is not None]
    lows = [l for l in lows if l is not None]
    return (max(highs) if highs else None, min(lows) if lows else None)


def _constituent_rows(state) -> list[dict]:
    with state.lock:
        instruments = dict(state.instruments)
        ltp_by_id = dict(state.last_ltp_by_security)
        reference = {sid: dict(row) for sid, row in state.market_reference.items()}

    rows: list[dict] = []
    for security_id, meta in instruments.items():
        symbol = meta.get("symbol", security_id)
        ltp = _float(ltp_by_id.get(security_id))
        ref = reference.get(security_id, {})
        previous_close = _float(ref.get("previous_close"))
        today_open = _float(ref.get("today_open"))
        day_high, day_low = _session_high_low(state, security_id)

        change_pct = None
        if ltp is not None and previous_close is not None and previous_close > 0:
            change_pct = round(((ltp / previous_close) - 1.0) * 100.0, 4)

        rows.append({
            "symbol": symbol,
            "security_id": security_id,
            "sector": sector_for_symbol(symbol),
            "ltp": ltp,
            "previous_close": previous_close,
            "today_open": today_open,
            "day_high": day_high,
            "day_low": day_low,
            "change_pct": change_pct,
            "is_new_session_high": bool(ltp is not None and day_high is not None and ltp >= day_high),
            "is_new_session_low": bool(ltp is not None and day_low is not None and ltp <= day_low),
        })
    return rows


def build_market_breadth(state) -> dict:
    """Raw advance/decline breadth across the full live equity universe."""
    rows = _constituent_rows(state)
    now = datetime.now(ZoneInfo(state.settings.timezone))

    advancing = sum(1 for r in rows if r["change_pct"] is not None and r["change_pct"] > 0)
    declining = sum(1 for r in rows if r["change_pct"] is not None and r["change_pct"] < 0)
    unchanged = sum(1 for r in rows if r["change_pct"] is not None and r["change_pct"] == 0)
    unknown = sum(1 for r in rows if r["change_pct"] is None)
    new_highs = sum(1 for r in rows if r["is_new_session_high"])
    new_lows = sum(1 for r in rows if r["is_new_session_low"])
    coverage = len(rows) - unknown

    return {
        "service": "PSYGRID",
        "status": "OK" if state.session_status == "LIVE" else state.session_status,
        "data_source": "PSYGRID_990_EQUITY_LIVE_RAM",
        "synthetic_data": False,
        "storage": "RAM_ONLY",
        "as_of": now.strftime("%Y-%m-%d %H:%M:%S IST"),
        "universe_size": len(rows),
        "coverage_count": coverage,
        "advancing": advancing,
        "declining": declining,
        "unchanged": unchanged,
        "unknown": unknown,
        "advance_decline_ratio": round(advancing / declining, 4) if declining > 0 else None,
        "new_session_highs": new_highs,
        "new_session_lows": new_lows,
        "note": "new_session_highs/lows are intraday (today's session) highs/lows, not 52-week",
        "constituents": rows,
    }


def build_sector_breadth(state) -> dict:
    """Raw per-sector aggregates (median return, volume, breadth counts)."""
    rows = _constituent_rows(state)
    now = datetime.now(ZoneInfo(state.settings.timezone))

    buckets: dict[str, list[dict]] = {}
    for row in rows:
        buckets.setdefault(row["sector"], []).append(row)

    sectors = []
    for sector, members in buckets.items():
        returns = [r["change_pct"] for r in members if r["change_pct"] is not None]
        advancing = sum(1 for r in members if r["change_pct"] is not None and r["change_pct"] > 0)
        declining = sum(1 for r in members if r["change_pct"] is not None and r["change_pct"] < 0)
        sectors.append({
            "sector": sector,
            "constituent_count": len(members),
            "coverage_count": len(returns),
            "advancing": advancing,
            "declining": declining,
            "median_change_pct": round(median(returns), 4) if returns else None,
            "constituents": [
                {"symbol": r["symbol"], "security_id": r["security_id"], "ltp": r["ltp"], "change_pct": r["change_pct"]}
                for r in members
            ],
        })
    sectors.sort(key=lambda s: s["sector"])

    return {
        "service": "PSYGRID",
        "status": "OK" if state.session_status == "LIVE" else state.session_status,
        "data_source": "PSYGRID_990_EQUITY_LIVE_RAM",
        "synthetic_data": False,
        "storage": "RAM_ONLY",
        "as_of": now.strftime("%Y-%m-%d %H:%M:%S IST"),
        "universe_size": len(rows),
        "sector_count": len(sectors),
        "sectors": sectors,
    }
