from __future__ import annotations

from typing import Dict, List, Optional


def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def ema(values: List[float], period: int) -> Optional[float]:
    if not values:
        return None
    if len(values) < period:
        return None
    seed = sum(values[:period]) / period
    multiplier = 2.0 / (period + 1)
    result = seed
    for value in values[period:]:
        result = (value - result) * multiplier + result
    return result


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains = []
    losses = []
    for prev, curr in zip(values[-(period + 1):-1], values[-period:]):
        change = curr - prev
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def vwap(candles: List[Dict]) -> Optional[float]:
    total_volume = 0.0
    total_value = 0.0
    for candle in candles:
        volume = float(candle.get("volume", 0) or 0)
        if volume <= 0:
            continue
        typical = (float(candle["high"]) + float(candle["low"]) + float(candle["close"])) / 3.0
        total_value += typical * volume
        total_volume += volume
    if total_volume == 0:
        return None
    return total_value / total_volume


def enrich(candles: List[Dict], ma_period: int = 9, ema_period: int = 20, rsi_period: int = 14) -> List[Dict]:
    closes = [float(c["close"]) for c in candles]
    out: List[Dict] = []
    for index, candle in enumerate(candles):
        window = candles[: index + 1]
        window_closes = closes[: index + 1]
        item = dict(candle)
        item["vwap"] = vwap(window)
        item["ma9"] = sma(window_closes, ma_period)
        item["ema20"] = ema(window_closes, ema_period)
        item["rsi14"] = rsi(window_closes, rsi_period)
        out.append(item)
    return out
