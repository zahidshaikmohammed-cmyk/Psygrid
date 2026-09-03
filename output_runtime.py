from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from indicators import enrich
from output import market_live_json as _market_live_json, normalize_candle

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
NATIVE_HIGHER_TIMEFRAMES = ("5m", "15m", "1h")
MAX_SIGNAL_LIVE_AGE_SECONDS = 30
TIMEFRAME_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}


def _enrich_native(seed: list[dict], rows: list[dict], settings) -> list[dict]:
    """Enrich genuine Dhan candles without constructing new candles."""
    combined = [dict(c) for c in seed] + [dict(c) for c in rows]
    combined.sort(key=lambda c: int(c["timestamp"]))
    seed_count = len(seed)
    enriched = enrich(
        combined,
        settings.ma_period,
        settings.ema_period,
        settings.rsi_period,
        day_key=lambda candle: datetime.fromtimestamp(
            int(candle["timestamp"]), PUBLIC_TZ
        ).date(),
    )
    return enriched[seed_count:]


def _native_timeframe_payload(state, security_id: str, key: str) -> list[dict]:
    today = datetime.now(PUBLIC_TZ).date()
    with state.lock:
        history = state.historical.get(security_id, {})
        seed = [dict(c) for c in history.get(f"{key}_seed", []) if c.get("complete", True)]
        rows = [dict(c) for c in history.get(key, []) if c.get("complete", True)]
    rows = [
        row for row in rows
        if datetime.fromtimestamp(int(row["timestamp"]), PUBLIC_TZ).date() == today
    ]
    enriched = _enrich_native(seed, rows, state.settings)
    return [normalize_candle(row) for row in enriched]


def _parse_public_epoch(value) -> int | None:
    try:
        return int(datetime.strptime(value, "%Y-%m-%d %H:%M:%S IST").replace(tzinfo=PUBLIC_TZ).timestamp())
    except (TypeError, ValueError, OverflowError):
        return None


def _mark_continuity(rows: list[dict], minutes: int) -> tuple[list[dict], bool]:
    """Mark missing native bars; never fill or synthesize the missing interval."""
    expected = minutes * 60
    previous_epoch = None
    valid = True
    out = []
    for row in rows:
        item = dict(row)
        epoch = _parse_public_epoch(item.get("timestamp"))
        gap = False if previous_epoch is None or epoch is None else epoch - previous_epoch != expected
        item["is_gap"] = gap
        if gap:
            valid = False
        if epoch is not None:
            previous_epoch = epoch
        out.append(item)
    return out, valid


def _execution_context(state, security_id: str) -> dict:
    with state.lock:
        context = dict(getattr(state, "market_context", {}).get(security_id, {}))
    def price(key):
        value = context.get(key)
        try:
            return round(float(value), 4) if value is not None else None
        except (TypeError, ValueError):
            return None
    best_bid = price("best_bid")
    best_ask = price("best_ask")
    spread = round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None and best_ask >= best_bid else None
    spread_bps = round((spread / best_bid) * 10000, 3) if spread is not None and best_bid and best_bid > 0 else None
    depth = context.get("depth") if isinstance(context.get("depth"), list) else []
    depth_valid = bool(best_bid and best_ask and best_ask >= best_bid and depth)
    return {
        "ltp": price("ltp"),
        "bid": best_bid,
        "ask": best_ask,
        "bid_qty": context.get("bid_qty"),
        "ask_qty": context.get("ask_qty"),
        "bid_orders": context.get("bid_orders"),
        "ask_orders": context.get("ask_orders"),
        "spread": spread,
        "spread_bps": spread_bps,
        "day_open": price("day_open"),
        "day_high": price("day_high"),
        "day_low": price("day_low"),
        "prev_close": price("prev_close"),
        "dhan_day_vwap": price("dhan_day_vwap"),
        "depth_valid": depth_valid,
        "depth_levels": depth,
        "market_context_source": context.get("source"),
    }


def _completed_rows(rows: list[dict], minutes: int) -> list[dict]:
    """Return only bars whose full native interval has elapsed."""
    now_epoch = int(time.time())
    duration = minutes * 60
    return [
        row for row in rows
        if (epoch := _parse_public_epoch(row.get("timestamp"))) is not None
        and epoch + duration <= now_epoch
    ]


def _stock_fixups(state, payload: dict) -> None:
    """Attach native HTFs, execution context and explicit data-quality state."""
    for stock in payload.get("stocks", {}).values():
        security_id = str(stock.get("security_id", ""))
        received = state.last_tick_received_by_security.get(security_id)
        stock["last_tick_timestamp"] = (
            datetime.fromtimestamp(received, PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
            if received is not None else None
        )
        for key in NATIVE_HIGHER_TIMEFRAMES:
            stock.setdefault("timeframes", {})[key] = _native_timeframe_payload(
                state, security_id, key
            )

        for key, minutes in TIMEFRAME_MINUTES.items():
            rows = stock.get("timeframes", {}).get(key)
            if isinstance(rows, list):
                stock["timeframes"][key], _ = _mark_continuity(rows, minutes)

        current = stock.setdefault("current", {})
        context = _execution_context(state, security_id)
        if context.get("ltp") is not None:
            current["ltp"] = context["ltp"]
        for key in (
            "bid", "ask", "bid_qty", "ask_qty", "bid_orders", "ask_orders",
            "spread", "spread_bps", "day_open", "day_high", "day_low", "prev_close",
        ):
            current[key] = context.get(key)
        current["depth_valid"] = context["depth_valid"]
        current["depth_levels"] = context["depth_levels"]
        current["market_context_source"] = context["market_context_source"]

        for rows in stock.get("timeframes", {}).values():
            if isinstance(rows, list):
                for candle in rows:
                    if isinstance(candle, dict):
                        candle.pop("dhan_day_vwap", None)

        completed_1m = _completed_rows(stock.get("timeframes", {}).get("1m", []), 1)
        latest_1m = completed_1m[-1] if completed_1m else None
        one_min_ready = bool(latest_1m) and all(latest_1m.get(k) is not None for k in ("ma9", "ema20", "rsi14"))
        continuity_valid = all(
            not any(bool(c.get("is_gap")) for c in _completed_rows(stock.get("timeframes", {}).get(key, []), minutes))
            for key, minutes in TIMEFRAME_MINUTES.items()
        )
        fresh = bool(stock.get("freshness", {}).get("live_data_valid", False))
        native_ready = _higher_timeframes_ready(stock)
        depth_ready = context["depth_valid"]
        execution_ready = bool(fresh and one_min_ready and native_ready and continuity_valid and depth_ready)
        stock["data_quality"] = {
            "live_quote_valid": fresh,
            "market_depth_valid": depth_ready,
            "1m_ready": one_min_ready,
            "5m_ready": bool(stock.get("timeframes", {}).get("5m")) and all(stock["timeframes"]["5m"][-1].get(k) is not None for k in ("ma9", "ema20", "rsi14")),
            "15m_ready": bool(stock.get("timeframes", {}).get("15m")) and all(stock["timeframes"]["15m"][-1].get(k) is not None for k in ("ma9", "ema20", "rsi14")),
            "1h_ready": bool(stock.get("timeframes", {}).get("1h")) and all(stock["timeframes"]["1h"][-1].get(k) is not None for k in ("ma9", "ema20", "rsi14")),
            "continuity_valid": continuity_valid,
            "no_forming_candle_used_for_readiness": True,
            "execution_ready": execution_ready,
        }
        stock["signal_engine"] = {
            "status": "READY" if execution_ready else "BLOCKED",
            "last_signal": None,
            "confidence_score": None,
            "block_reason": None if execution_ready else (
                "STALE_LIVE_DATA" if not fresh else
                "MARKET_DEPTH_UNAVAILABLE" if not depth_ready else
                "1M_INDICATORS_NOT_READY" if not one_min_ready else
                "NATIVE_HIGHER_TIMEFRAME_NOT_READY" if not native_ready else
                "CANDLE_CONTINUITY_GAP"
            ),
            "last_updated_ist": stock.get("last_tick_timestamp"),
        }


def _higher_timeframes_ready(stock: dict) -> bool:
    """Require real native 5m/15m/1h candles and calculated indicators."""
    timeframes = stock.get("timeframes", {})
    for key in NATIVE_HIGHER_TIMEFRAMES:
        rows = timeframes.get(key) or []
        if not rows:
            return False
        latest = rows[-1]
        if any(latest.get(field) is None for field in ("ma9", "ema20", "rsi14")):
            return False
    return True


def market_live_json(state, stock_range=None) -> dict:
    payload = _market_live_json(state, stock_range)
    session = payload.get("session", {})
    session["max_live_age_seconds"] = MAX_SIGNAL_LIVE_AGE_SECONDS
    payload["session"] = session

    rules = payload.setdefault("rules", {})
    rules["live_freshness_policy"] = "MAX_30_SECONDS"
    rules["synthetic_candles"] = False
    rules["native_higher_timeframes"] = True
    rules["higher_timeframe_source"] = "DHAN_HISTORICAL_API"
    rules["higher_timeframe_policy"] = "NATIVE_DHAN_ONLY_NO_1M_AGGREGATION"
    rules["live_market_feed_mode"] = "DHAN_FULL"
    rules["market_depth_levels"] = 5
    rules["repeated_candle_dhan_day_vwap"] = "REMOVED; SESSION_LEVEL_CURRENT_FIELD_ONLY"
    rules["candle_continuity_policy"] = "EXPLICIT_IS_GAP_NO_SYNTHETIC_FILL"
    rules["execution_readiness_requires"] = "FRESH_LIVE+1M_INDICATORS+NATIVE_5M_15M_1H+CONTINUITY+MARKET_DEPTH"

    if session.get("status") == "LIVE":
        _stock_fixups(state, payload)

    stocks = payload.get("stocks", {})
    fresh_count = sum(
        1 for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
    )
    analysis_ready_count = sum(
        1 for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
        and _higher_timeframes_ready(stock)
    )
    execution_ready_count = sum(
        1 for stock in stocks.values()
        if stock.get("signal_engine", {}).get("status") == "READY"
    )
    stock_count = int(payload.get("stock_count", len(stocks)) or 0)

    signal_valid = bool(
        session.get("status") == "LIVE"
        and session.get("feed_status") == "CONNECTED"
        and stock_count > 0
        and analysis_ready_count == stock_count
        and execution_ready_count == stock_count
    )
    if signal_valid:
        reason = None
    elif session.get("status") != "LIVE":
        reason = "MARKET_SESSION_NOT_LIVE"
    elif session.get("feed_status") != "CONNECTED":
        reason = f"FEED_STATUS_{session.get('feed_status', 'UNKNOWN')}"
    elif fresh_count < stock_count:
        reason = "INCOMPLETE_FRESH_LIVE_COVERAGE"
    elif analysis_ready_count < stock_count:
        reason = "NATIVE_HIGHER_TIMEFRAME_HISTORY_NOT_READY"
    else:
        reason = "EXECUTION_DATA_QUALITY_NOT_READY"

    payload["signal_input"] = {
        "valid": signal_valid,
        "status": "LIVE" if signal_valid else "BLOCKED",
        "stock_count": stock_count,
        "fresh_stock_count": fresh_count,
        "analysis_ready_stock_count": analysis_ready_count,
        "execution_ready_stock_count": execution_ready_count,
        "max_live_age_seconds": MAX_SIGNAL_LIVE_AGE_SECONDS,
        "block_reason": reason,
        "per_stock_freshness_required": True,
        "native_higher_timeframes_required": True,
        "synthetic_candles_allowed": False,
        "all_stocks_must_be_analysis_ready": True,
        "execution_depth_required_per_stock": True,
    }
    return payload
