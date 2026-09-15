from __future__ import annotations

from .math_utils import atr, directional_efficiency, robust_z, safe_div, session_vwap
from .models import Features


def build_features(symbol, candles, previous_close, market_return, sector_return,
                   historical_gaps=(), side="LONG", config=None):
    cfg = config or {}
    if len(candles) < 3:
        raise ValueError(f"{symbol}: insufficient completed 1m candles")
    if previous_close <= 0:
        raise ValueError(f"{symbol}: previous close must be positive")

    opening = candles[0]
    last = candles[-1]
    gap_pct = safe_div(opening.open - previous_close, previous_close)
    gap_z = robust_z(gap_pct, historical_gaps)

    prices = [c.close for c in candles]
    ranges = [c.high - c.low for c in candles]
    baseline_atr = atr(candles, int(cfg.get("atr_period", 20)))
    if baseline_atr <= 0:
        baseline_atr = max(sum(ranges) / len(ranges), 1e-9)

    bullish = side == "LONG"
    impulse_extreme = max(c.high for c in candles) if bullish else min(c.low for c in candles)
    impulse_start = opening.open
    impulse_size = (impulse_extreme - impulse_start) if bullish else (impulse_start - impulse_extreme)
    impulse_pct = safe_div(impulse_size, impulse_start)
    impulse_atr = safe_div(impulse_size, baseline_atr)
    de = directional_efficiency(candles)

    if bullish:
        retracement_depth = safe_div(impulse_extreme - last.close, impulse_size)
    else:
        retracement_depth = safe_div(last.close - impulse_extreme, impulse_size)
    retracement_depth = max(0.0, min(1.5, retracement_depth))

    impulse_volume = sum(c.volume for c in candles[:max(1, len(candles)//2)])
    retrace_volume = sum(c.volume for c in candles[max(1, len(candles)//2):])
    retrace_n = max(1, len(candles) - len(candles)//2)
    impulse_n = max(1, len(candles)//2)
    retrace_avg = retrace_volume / retrace_n
    impulse_avg = impulse_volume / impulse_n
    retrace_volume_ratio = safe_div(retrace_avg, impulse_avg, 1.0)

    vwap_series = session_vwap(candles)
    vwap = vwap_series[-1]
    vwap_prev = vwap_series[-2] if len(vwap_series) > 1 else vwap
    vwap_slope = safe_div(vwap - vwap_prev, baseline_atr)
    vwap_distance_atr = safe_div(last.close - vwap, baseline_atr)
    if not bullish:
        vwap_distance_atr *= -1

    stock_return = normalized = safe_div(last.close - opening.open, opening.open)
    rs_market = normalized - market_return
    rs_sector = normalized - sector_return
    if not bullish:
        rs_market *= -1
        rs_sector *= -1

    avg_volume = sum(c.volume for c in candles[:-1]) / max(1, len(candles)-1)
    volume_ratio = safe_div(last.volume, avg_volume, 0.0)
    range_atr = safe_div(last.high - last.low, baseline_atr)
    extension_atr = abs(vwap_distance_atr)

    exhaustion = (
        0.35 * min(extension_atr / float(cfg.get("extension_atr_limit", 2.0)), 1.0)
        + 0.35 * min(abs(impulse_atr) / float(cfg.get("impulse_exhaustion_atr", 3.0)), 1.0)
        + 0.30 * min(volume_ratio / float(cfg.get("volume_climax", 3.0)), 1.0)
    )
    structure_score = max(0.0, min(1.0, de * (1.0 - min(abs(retracement_depth - 0.5), 0.5))))

    trap_penalty = 0.0
    if abs(gap_pct) > float(cfg.get("absolute_gap_limit", 0.03)):
        trap_penalty += 100.0
    if gap_z > float(cfg.get("gap_z_limit", 3.0)):
        trap_penalty += 100.0
    if exhaustion >= float(cfg.get("hard_exhaustion_limit", 0.90)):
        trap_penalty += 100.0
    if retracement_depth < float(cfg.get("min_retracement", 0.30)):
        trap_penalty += 100.0

    # Scores are deliberately transparent and bounded.
    impulse_score = min(22.0, max(0.0, impulse_atr / 2.0 * 22.0))
    retrace_score = 24.0 * max(0.0, 1.0 - abs(retracement_depth - 0.52) / 0.52)
    rs_score = min(18.0, max(0.0, (rs_market + rs_sector) * 1000.0))
    volume_score = min(10.0, max(0.0, (1.0 - min(retrace_volume_ratio, 1.0)) * 10.0))
    vwap_score = min(8.0, max(0.0, vwap_distance_atr / 1.5 * 8.0))
    structure = structure_score * 7.0
    volatility_score = min(6.0, max(0.0, range_atr / 2.0 * 6.0))
    persistence = de * 5.0
    score = impulse_score + retrace_score + rs_score + volume_score + vwap_score + structure + volatility_score + persistence - trap_penalty

    eligible = trap_penalty < 100.0 and impulse_size > 0 and baseline_atr > 0
    reasons = []
    if abs(gap_pct) > float(cfg.get("absolute_gap_limit", 0.03)): reasons.append("EXTREME_GAP")
    if gap_z > float(cfg.get("gap_z_limit", 3.0)): reasons.append("ABNORMAL_GAP")
    if exhaustion >= float(cfg.get("hard_exhaustion_limit", 0.90)): reasons.append("MOMENTUM_EXHAUSTION")
    if retracement_depth < float(cfg.get("min_retracement", 0.30)): reasons.append("SHALLOW_RETRACEMENT")
    if not reasons: reasons.append("ELIGIBLE")

    return Features(symbol, side, gap_pct, gap_z, impulse_pct, impulse_atr, de,
                    retracement_depth, retrace_volume_ratio, rs_market, rs_sector,
                    vwap, vwap_slope, vwap_distance_atr, range_atr, volume_ratio,
                    extension_atr, exhaustion, structure_score, trap_penalty, score,
                    eligible, reasons)
