from __future__ import annotations

from output import market_live_json as _base_market_live_json, stock_json as _base_stock_json


def _clean_candle(candle: dict) -> dict:
    return {"timestamp": candle.get("timestamp"), "open": candle.get("open"), "high": candle.get("high"), "low": candle.get("low"), "close": candle.get("close"), "volume": candle.get("volume", 0), "complete": bool(candle.get("complete", True)), "source": candle.get("source")}


def _clean_stock(stock: dict) -> dict:
    current = stock.get("current") if isinstance(stock.get("current"), dict) else {}
    timeframes = stock.get("timeframes") if isinstance(stock.get("timeframes"), dict) else {}
    candles = timeframes.get("1m") if isinstance(timeframes.get("1m"), list) else []
    return {"symbol": stock.get("symbol"), "security_id": stock.get("security_id"), "previous_close": stock.get("previous_close", current.get("prev_close")), "today_open": stock.get("today_open", current.get("day_open")), "timeframes": {"1m": [_clean_candle(c) for c in candles]}}


def market_live_json(state, stock_range=None, preserve_instrument_order=False) -> dict:
    base = _base_market_live_json(state, stock_range, preserve_instrument_order)
    stocks = base.get("stocks", {})
    return {"service": "PSYGRID", "status": base.get("status", "OK"), "session": {"status": base.get("session", {}).get("status"), "timezone": "Asia/Kolkata"}, "universe_size": 450, "data_policy": "1M_OHLCV_ONLY", "synthetic_candles": False, "stocks": {symbol: _clean_stock(stock) for symbol, stock in stocks.items()}}


def stock_json(state, symbol: str) -> dict:
    return _clean_stock(_base_stock_json(state, symbol))
