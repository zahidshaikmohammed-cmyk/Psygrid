from __future__ import annotations

import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from output import market_live_json as base_market_live_json, stock_json

PUBLIC_TZ = ZoneInfo("Asia/Kolkata")
MAX_LIVE_AGE_SECONDS = 30


def _freshness(state, security_id: str) -> dict:
    """Use RuntimeFreshnessState so REST quote recovery counts as fresh."""
    freshness_fn = getattr(state, "freshness", None)
    if callable(freshness_fn):
        return dict(freshness_fn(str(security_id), time.time()))
    return {"status": "UNKNOWN", "data_age_seconds": None, "live_data_valid": False, "source": None}


def _context(state, security_id: str) -> dict:
    with state.lock:
        return dict(getattr(state, "market_context", {}).get(str(security_id), {}))


def _stock_runtime_fixup(state, stock: dict) -> None:
    security_id = str(stock.get("security_id", ""))
    freshness = _freshness(state, security_id)
    context = _context(state, security_id)

    stock["freshness"] = freshness
    stock["ltp_source"] = freshness.get("source")
    stock["ltp_age_seconds"] = freshness.get("data_age_seconds")
    if context.get("ltp") is not None:
        stock["ltp"] = round(float(context["ltp"]), 4)

    received = None
    if freshness.get("source") == "DHAN_REST_QUOTE_RECOVERY":
        received = context.get("received_epoch")
    else:
        received = getattr(state, "last_tick_received_by_security", {}).get(security_id)
    stock["ltp_received_at"] = (
        datetime.fromtimestamp(float(received), timezone.utc).astimezone(PUBLIC_TZ).strftime("%Y-%m-%d %H:%M:%S IST")
        if received is not None else None
    )

    current = stock.setdefault("current", {})
    if context.get("ltp") is not None:
        current["ltp"] = round(float(context["ltp"]), 4)
    current["market_context_source"] = context.get("source")
    current["quote_age_seconds"] = freshness.get("data_age_seconds")
    current["quote_valid"] = bool(freshness.get("live_data_valid"))
    current["quote_source"] = freshness.get("source")
    for key in (
        "bid", "ask", "bid_qty", "ask_qty", "bid_orders", "ask_orders",
        "day_open", "day_high", "day_low", "prev_close", "depth",
    ):
        source_key = {"bid": "best_bid", "ask": "best_ask"}.get(key, key)
        if source_key in context:
            current[key] = context[source_key]


def market_live_json(state, stock_range=None, preserve_instrument_order=False) -> dict:
    payload = base_market_live_json(state, stock_range, preserve_instrument_order)
    payload.setdefault("rules", {})["live_freshness_policy"] = "MAX_30_SECONDS"
    payload["rules"]["live_quote_freshness_source"] = "WEBSOCKET_RECEIPT_OR_DHAN_REST_QUOTE_RECEIPT"
    payload["rules"]["rest_quote_recovery_is_not_a_candle"] = True
    payload["rules"]["canonical_shard_family"] = "live-a-through-live-j"

    for stock in payload.get("stocks", {}).values():
        _stock_runtime_fixup(state, stock)

    fresh_count = sum(1 for stock in payload.get("stocks", {}).values() if stock.get("freshness", {}).get("live_data_valid"))
    payload["coverage"] = {
        "stock_count": len(payload.get("stocks", {})),
        "fresh_stock_count": fresh_count,
        "stale_or_missing_stock_count": len(payload.get("stocks", {})) - fresh_count,
        "max_live_age_seconds": MAX_LIVE_AGE_SECONDS,
    }
    return payload
