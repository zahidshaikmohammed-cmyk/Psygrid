from __future__ import annotations

from typing import Dict, List, Optional


def sma(values: List[float], period: int) -> Optional[float]:
    """Simple moving average of the last `period` completed observations."""
    if period <= 0 or len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> Optional[float]:
    """Standard EMA using an SMA(period) seed and alpha=2/(period+1)."""
    if period <= 0 or len(values) < period:
        return None
    seed = sum(values[:period]) / period
    alpha = 2.0 / (period + 1.0)
    result = seed
    for value in values[period:]:
        result = result + alpha * (value - result)
    return result


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    """Wilder RSI using the standard initial average gain/loss and smoothing."""
    if period <= 0 or len(values) < period + 1:
        return None

    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0.0:
        return 100.0 if avg_gain > 0.0 else 50.0

    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap(candles: List[Dict]) -> Optional[float]:
    """Candle-based VWAP for the supplied session window.

    Historical OHLCV does not contain every tick's trade-price distribution, so
    this is explicitly a candle-derived VWAP using typical price. Live Dhan
    Quote packets expose Dhan's day average-price field separately.
    """
    total_volume = 0.0
    total_value = 0.0
    for candle in candles:
        volume = float(candle.get("volume", 0) or 0)
        if volume <= 0:
            continue
        typical = (
            float(candle["high"])
            + float(candle["low"])
            + float(candle["close"])
        ) / 3.0
        total_value += typical * volume
        total_volume += volume
    if total_volume <= 0.0:
        return None
    return total_value / total_volume


def enrich(candles: List[Dict], ma_period: int = 9, ema_period: int = 20, rsi_period: int = 14) -> List[Dict]:
    """Return copies with indicators calculated independently per candle series."""
    ordered = sorted((dict(c) for c in candles), key=lambda c: int(c["timestamp"]))
    closes = [float(c["close"]) for c in ordered]
    out: List[Dict] = []
    for index, candle in enumerate(ordered):
        prefix = ordered[: index + 1]
        prefix_closes = closes[: index + 1]
        item = dict(candle)
        item["vwap"] = vwap(prefix)
        item["ma9"] = sma(prefix_closes, ma_period)
        item["ema20"] = ema(prefix_closes, ema_period)
        item["rsi14"] = rsi(prefix_closes, rsi_period)
        item["complete"] = bool(item.get("complete", True))
        out.append(item)
    return out
