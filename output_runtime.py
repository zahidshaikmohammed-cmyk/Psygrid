from __future__ import annotations

from output import market_live_json as _market_live_json


def market_live_json(state, stock_range=None) -> dict:
    """Expose the existing payload plus a surgical signal-input safety gate.

    No candle, indicator, price, or timeframe data is changed. The additional
    fields tell an external consumer such as Obsidian/ChatGPT whether the
    stocks in this particular endpoint slice are currently backed by fresh
    accepted WebSocket quotes.
    """
    payload = _market_live_json(state, stock_range)

    stocks = payload.get("stocks", {})
    session = payload.get("session", {})
    stock_count = int(payload.get("stock_count", len(stocks)) or 0)
    fresh_count = sum(
        1
        for stock in stocks.values()
        if bool(stock.get("freshness", {}).get("live_data_valid", False))
    )

    feed_ok = (
        session.get("status") == "LIVE"
        and session.get("feed_status") == "CONNECTED"
    )
    slice_ok = stock_count > 0 and fresh_count == stock_count
    signal_valid = bool(feed_ok and slice_ok)

    if signal_valid:
        block_reason = None
    elif session.get("status") != "LIVE":
        block_reason = "MARKET_SESSION_NOT_LIVE"
    elif session.get("feed_status") != "CONNECTED":
        block_reason = f"FEED_STATUS_{session.get('feed_status', 'UNKNOWN')}"
    elif stock_count == 0:
        block_reason = "NO_STOCKS_IN_ENDPOINT"
    else:
        block_reason = "ONE_OR_MORE_STOCKS_NOT_FRESH"

    payload["signal_input"] = {
        "valid": signal_valid,
        "status": "LIVE" if signal_valid else "BLOCKED",
        "stock_count": stock_count,
        "fresh_stock_count": fresh_count,
        "max_live_age_seconds": session.get("max_live_age_seconds"),
        "block_reason": block_reason,
    }
    return payload
