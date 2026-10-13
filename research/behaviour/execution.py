"""Execution model for the pre-registered hypotheses (FROZEN, see prereg.lock).

Decisions are taken at the close of a 5-minute bar t. Orders are filled on the 1-MINUTE path:
  * entry: market order at the OPEN of minute 5*(t+1) + LATENCY_MIN (one full minute after the
    signal bar closes), plus adverse slippage; no entry after LAST_ENTRY_MIN (15:00).
  * stop: checked on every minute from the entry minute; a stop fills at the stop price or at the
    minute's open if the open is already through it, plus adverse slippage.
  * target: a resting limit order; it fills only if the price trades THROUGH it (strictly beyond).
    If a minute touches both the stop and the target, the STOP is assumed (worst case).
  * time exit: market order at the close of the last minute of the holding period, at the latest
    the 15:14 minute (15:15), plus adverse slippage.
  * a trade whose entry fill is already beyond its stop is not taken (counted as skipped).
Costs are itemised: gross move, spread (round trip, multiple of the stock's estimated spread),
fees (brokerage + STT + exchange + GST + stamp), slippage (per market fill). net = gross - costs.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

LATENCY_MIN = 1
LAST_ENTRY_MIN = 345  # 15:00
EXIT_MIN = 359  # 15:14 minute; its close is 15:15
SPREAD_FLOOR_TICK = 0.05  # INR


@dataclass(frozen=True)
class Signal:
    d: int  # session index
    t: int  # 5-minute decision bar
    side: int  # +1 long, -1 short
    stop: float | None  # absolute price, or None for no stop
    target_r: float | None  # target as a multiple of the entry-to-stop distance, or None
    hold_min: int | None  # maximum holding minutes, or None = until 15:15


@dataclass(frozen=True)
class Costs:
    fees_pct: float  # round trip
    slip_pct: float  # per market fill
    spread_mult: float  # x estimated round-trip spread


def simulate(m1_day: np.ndarray, sig: Signal, spread_pct: float, costs: Costs) -> dict | None:
    """One trade on one session's 1-minute bars [375, 5] (o, h, l, c, v). None if not taken."""
    start = 5 * (sig.t + 1) + LATENCY_MIN
    if start > LAST_ENTRY_MIN:
        return None
    o, h, lo, c = (m1_day[:, k] for k in range(4))
    side = sig.side
    ref_in = float(o[start])
    if not np.isfinite(ref_in) or ref_in <= 0:
        return None
    stop = sig.stop
    if stop is not None and ((side > 0 and ref_in <= stop) or (side < 0 and ref_in >= stop)):
        return {"skipped": True}
    target = None
    if sig.target_r is not None and stop is not None:
        target = ref_in + side * sig.target_r * abs(ref_in - stop)
    end = EXIT_MIN if sig.hold_min is None else min(start + sig.hold_min - 1, EXIT_MIN)
    exit_ref, reason, market_exit, m_exit = float(c[end]), "time", True, end
    for m in range(start, end + 1):
        hit_stop = stop is not None and (lo[m] <= stop if side > 0 else h[m] >= stop)
        if hit_stop:
            exit_ref = min(o[m], stop) if side > 0 else max(o[m], stop)
            reason, market_exit, m_exit = "stop", True, m
            break
        if target is not None and (h[m] > target if side > 0 else lo[m] < target):
            exit_ref, reason, market_exit, m_exit = target, "target", False, m
            break
    gross = side * (exit_ref / ref_in - 1) * 100
    slip = costs.slip_pct * (2 if market_exit else 1)
    spread = costs.spread_mult * spread_pct
    net = gross - costs.fees_pct - slip - spread
    return {
        "skipped": False,
        "entry_min": start,
        "exit_min": int(m_exit),
        "reason": reason,
        "gross": gross,
        "fees": costs.fees_pct,
        "slippage": slip,
        "spread": spread,
        "net": net,
        "market_fills": 2 if market_exit else 1,
    }


def spread_estimate_pct(m1: np.ndarray, cap: float = 0.20) -> tuple[float, bool]:
    """Round-trip spread in % from 1-minute Corwin-Schultz (within-session pairs), floored at one
    tick. Returns (estimate, capped)."""
    H, L = m1[:, :, 1], m1[:, :, 2]
    with np.errstate(divide="ignore", invalid="ignore"):
        lh = np.log(H / L) ** 2
        beta = lh[:, 1:] + lh[:, :-1]
        gamma = np.log(np.maximum(H[:, 1:], H[:, :-1]) / np.minimum(L[:, 1:], L[:, :-1])) ** 2
        k = 3 - 2 * np.sqrt(2)
        alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / k - np.sqrt(gamma / k)
        s = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    s = np.where(np.isfinite(s) & (s > 0), s, 0.0)
    cs = float(np.median(s.mean(axis=1))) * 100
    px = float(np.nanmedian(m1[:, :, 3]))
    tick = SPREAD_FLOOR_TICK / px * 100 if px > 0 else 0.02
    est = max(cs, tick)
    return (min(est, cap), est > cap)
