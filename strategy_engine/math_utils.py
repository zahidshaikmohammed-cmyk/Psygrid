from __future__ import annotations

from math import isfinite, sqrt
from statistics import median


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0 or not isfinite(b):
        return default
    value = a / b
    return value if isfinite(value) else default


def true_ranges(high, low, prev_close):
    out = []
    for h, l, pc in zip(high, low, [None] + list(prev_close[:-1])):
        out.append(max(h - l, abs(h - pc), abs(l - pc)) if pc is not None else h - l)
    return out


def median_abs_deviation(values) -> float:
    values = [float(x) for x in values if isfinite(float(x))]
    if not values:
        return 0.0
    m = median(values)
    return median([abs(x - m) for x in values])


def robust_z(value: float, history) -> float:
    values = [float(x) for x in history if isfinite(float(x))]
    if len(values) < 5:
        return 0.0
    m = median(values)
    mad = median_abs_deviation(values)
    if mad <= 1e-12:
        return 0.0
    return 0.6744897501960817 * (value - m) / mad


def session_vwap(candles):
    pv = 0.0
    vol = 0.0
    result = []
    for c in candles:
        typical = (c.high + c.low + c.close) / 3.0
        pv += typical * c.volume
        vol += c.volume
        result.append(safe_div(pv, vol, c.close))
    return result


def atr(candles, period: int = 20) -> float:
    if not candles:
        return 0.0
    trs = []
    prev = None
    for c in candles:
        tr = c.high - c.low if prev is None else max(c.high-c.low, abs(c.high-prev), abs(c.low-prev))
        trs.append(tr)
        prev = c.close
    window = trs[-period:]
    return sum(window) / len(window) if window else 0.0


def directional_efficiency(candles) -> float:
    if len(candles) < 2:
        return 0.0
    displacement = abs(candles[-1].close - candles[0].open)
    path = sum(abs(candles[i].close - candles[i-1].close) for i in range(1, len(candles)))
    return max(0.0, min(1.0, safe_div(displacement, path)))


def normalized_return(start: float, end: float) -> float:
    return safe_div(end - start, start)


def aggregate(candles, minutes: int):
    """Build deterministic session-relative candles from 1m candles."""
    if minutes <= 0:
        raise ValueError("minutes must be positive")
    if not candles:
        return []
    groups = {}
    for c in candles:
        # 09:15 is bucket zero; this prevents pre-open clock alignment errors.
        session_minute = (c.timestamp.hour * 60 + c.timestamp.minute) - (9 * 60 + 15)
        if session_minute < 0:
            continue
        bucket = session_minute // minutes
        groups.setdefault(bucket, []).append(c)
    out = []
    for bucket in sorted(groups):
        rows = groups[bucket]
        out.append(type(rows[0])(
            timestamp=rows[0].timestamp,
            open=rows[0].open,
            high=max(x.high for x in rows),
            low=min(x.low for x in rows),
            close=rows[-1].close,
            volume=sum(x.volume for x in rows),
            complete=all(x.complete for x in rows),
        ))
    return out
