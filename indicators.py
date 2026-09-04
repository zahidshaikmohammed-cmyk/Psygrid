from __future__ import annotations

from typing import Callable, Dict, List, Optional


def sma(values: List[float], period: int) -> Optional[float]:
    """Simple moving average of the last `period` observations."""
    if period <= 0 or len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> Optional[float]:
    """Standard EMA using an SMA seed and alpha=2/(period+1)."""
    if period <= 0 or len(values) < period:
        return None
    seed = sum(values[:period]) / period
    alpha = 2.0 / (period + 1.0)
    result = seed
    for value in values[period:]:
        result += alpha * (value - result)
    return result


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    """Wilder RSI using the standard initial average gain/loss and smoothing."""
    if period <= 0 or len(values) < period + 1:
        return None
    gains = []
    losses = []
    for index in range(1, len(values)):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
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
    """Candle-based VWAP using typical price weighted by candle volume."""
    total_volume = 0.0
    total_value = 0.0
    for candle in candles:
        volume = float(candle.get("volume", 0) or 0)
        if volume <= 0:
            continue
        typical = (
            float(candle["high"]) + float(candle["low"]) + float(candle["close"])
        ) / 3.0
        total_value += typical * volume
        total_volume += volume
    if total_volume <= 0.0:
        return None
    return total_value / total_volume


def enrich(
    candles: List[Dict],
    ma_period: int = 9,
    ema_period: int = 20,
    rsi_period: int = 14,
    day_key: Optional[Callable[[Dict], object]] = None,
) -> List[Dict]:
    """Enrich genuine candles with EMA(9), EMA(20), RSI(14) and VWAP.

    The legacy MA(9) output is intentionally removed. ``ma_period`` remains
    the internal period setting for backward-compatible configuration, but it
    now controls EMA(9). No candles are created or filled here.
    """
    ordered = sorted((dict(c) for c in candles), key=lambda c: int(c["timestamp"]))
    out: List[Dict] = []
    closes: List[float] = []

    ema9_value: Optional[float] = None
    ema9_alpha = 2.0 / (ma_period + 1.0) if ma_period > 0 else 0.0
    ema20_value: Optional[float] = None
    ema20_alpha = 2.0 / (ema_period + 1.0) if ema_period > 0 else 0.0

    rsi_avg_gain: Optional[float] = None
    rsi_avg_loss: Optional[float] = None
    prev_close: Optional[float] = None
    gains_window: List[float] = []
    losses_window: List[float] = []

    vwap_day = object()
    vwap_volume = 0.0
    vwap_value = 0.0

    for index, candle in enumerate(ordered):
        close = float(candle["close"])
        closes.append(close)

        if day_key is not None:
            current_day = day_key(candle)
            if index == 0 or current_day != vwap_day:
                vwap_day = current_day
                vwap_volume = 0.0
                vwap_value = 0.0

        volume = float(candle.get("volume", 0) or 0)
        if volume > 0:
            typical = (
                float(candle["high"]) + float(candle["low"]) + close
            ) / 3.0
            vwap_value += typical * volume
            vwap_volume += volume
        vwap_value_out = vwap_value / vwap_volume if vwap_volume > 0 else None

        if ma_period > 0:
            if len(closes) == ma_period:
                ema9_value = sum(closes[:ma_period]) / ma_period
            elif len(closes) > ma_period and ema9_value is not None:
                ema9_value += ema9_alpha * (close - ema9_value)
        else:
            ema9_value = None

        if ema_period > 0:
            if len(closes) == ema_period:
                ema20_value = sum(closes[:ema_period]) / ema_period
            elif len(closes) > ema_period and ema20_value is not None:
                ema20_value += ema20_alpha * (close - ema20_value)
        else:
            ema20_value = None

        rsi_value: Optional[float] = None
        if prev_close is not None:
            change = close - prev_close
            gain = max(change, 0.0)
            loss = max(-change, 0.0)
            if len(gains_window) < rsi_period:
                gains_window.append(gain)
                losses_window.append(loss)
                if len(gains_window) == rsi_period:
                    rsi_avg_gain = sum(gains_window) / rsi_period
                    rsi_avg_loss = sum(losses_window) / rsi_period
            elif rsi_avg_gain is not None and rsi_avg_loss is not None:
                rsi_avg_gain = ((rsi_avg_gain * (rsi_period - 1)) + gain) / rsi_period
                rsi_avg_loss = ((rsi_avg_loss * (rsi_period - 1)) + loss) / rsi_period

        if rsi_avg_loss is not None and rsi_avg_gain is not None:
            if rsi_avg_loss == 0.0:
                rsi_value = 100.0 if rsi_avg_gain > 0.0 else 50.0
            else:
                rs = rsi_avg_gain / rsi_avg_loss
                rsi_value = 100.0 - (100.0 / (1.0 + rs))

        item = dict(candle)
        item.pop("ma9", None)
        item["vwap"] = vwap_value_out
        item["ema9"] = ema9_value
        item["ema20"] = ema20_value
        item["rsi14"] = rsi_value
        item["complete"] = bool(item.get("complete", True))
        out.append(item)
        prev_close = close

    return out
