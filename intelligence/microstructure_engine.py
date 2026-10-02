"""Microstructure engine: what PSYGRID's Full packets say about how a stock is moving.

Reads the per-minute features the PSYGRID recorder writes
(``microstructure.py``: the live stream during the session, the compacted
archive afterwards) and summarises one stock over the trailing ``window``
minutes that closed by ``as_of``.

Cadence comes first. Dhan disseminates book snapshots, not exchange events,
so each metric is only as good as the packet rate behind it. Every result
carries the measured cadence (median packets per minute, median largest gap
between packets) and a support flag per metric family:

- ``quote`` metrics (OFI, depletion and replenishment, imbalance) need at
  least ``QUOTE_MIN_PACKETS`` packets a minute and no gap above
  ``QUOTE_MAX_GAP_MS`` in a typical minute;
- ``trade`` metrics (signed flow, the price-impact slope) need at least
  ``TRADE_MIN_PACKETS`` packets with a trade each minute.

An unsupported metric is reported as ``null`` with the reason, never
estimated. Supported metrics are packet-level approximations: quantities
between two packets are unobserved and trade signs come from the quote rule,
then the tick rule.

Outputs: normalised order-flow imbalance (OFI over mean top-of-book depth),
signed-flow share, a Kyle-style impact slope (mid change in bps per unit of
signed volume, in units of the window's mean minute volume), the replenish
ratio at the top of book, spread and depth, and a descriptive classification
of the window's move: AGGRESSIVE_CONSUMPTION (one-sided flow moving price its
way), LIQUIDITY_WITHDRAWAL (price moved through a thinning book without
matching flow), ABSORPTION (strong one-sided flow without a move), QUIET or
MIXED. The labels describe mechanics; they are not forecasts.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from intelligence.archive import parse_timestamp

WINDOW = 15
QUOTE_MIN_PACKETS = 20
QUOTE_MAX_GAP_MS = 10_000
TRADE_MIN_PACKETS = 5
FLOW_STRONG = 0.3
DEPTH_THINNING = 0.7  # the side the price moved through ended with < 70% of its starting depth


def _f(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


@dataclass
class MicroState:
    symbol: str
    as_of: str
    minutes: int
    cadence: dict = field(default_factory=dict)
    support: dict = field(default_factory=dict)
    ofi_norm: float | None = None
    signed_flow: float | None = None
    impact_bps: float | None = None
    replenish_ratio: float | None = None
    imbalance1: float | None = None
    spread_bps: float | None = None
    depth5: float | None = None
    move_bps: float | None = None
    classification: str = "UNSUPPORTED"
    notes: list = field(default_factory=list)

    def view(self) -> dict:
        return {k: (round(v, 6) if isinstance(v, float) else v) for k, v in self.__dict__.items()}


def index_rows(rows: list[dict]) -> dict[str, list[tuple[int, dict]]]:
    """Group recorder rows by symbol, ordered by minute (do this once per read, not per stock)."""
    out: dict[str, list[tuple[int, dict]]] = {}
    for row in rows:
        try:
            minute = parse_timestamp(row["timestamp"])
        except (KeyError, TypeError, ValueError):
            continue
        out.setdefault(row.get("symbol", ""), []).append((minute, row))
    for series in out.values():
        series.sort(key=lambda item: item[0])
    return out


def cadence(rows: list[dict]) -> dict:
    packets = [_f(r.get("packets")) or 0 for r in rows]
    trades = [_f(r.get("trade_packets")) or 0 for r in rows]
    gaps = [_f(r.get("max_gap_ms")) for r in rows]
    gaps = [g for g in gaps if g is not None]
    return {
        "packets_per_minute": float(np.median(packets)) if packets else 0.0,
        "trade_packets_per_minute": float(np.median(trades)) if trades else 0.0,
        "max_gap_ms": float(np.median(gaps)) if gaps else None,
        "invalid_book_minutes": sum(1 for r in rows if (_f(r.get("invalid_book")) or 0) > 0),
    }


def support(c: dict) -> dict:
    quote = c["packets_per_minute"] >= QUOTE_MIN_PACKETS and (c["max_gap_ms"] or 0) <= QUOTE_MAX_GAP_MS
    trade = c["trade_packets_per_minute"] >= TRADE_MIN_PACKETS
    return {
        "quote": quote,
        "trade": trade,
        "quote_reason": None
        if quote
        else f"needs >= {QUOTE_MIN_PACKETS} packets/min and gaps <= {QUOTE_MAX_GAP_MS} ms",
        "trade_reason": None if trade else f"needs >= {TRADE_MIN_PACKETS} trade packets/min",
    }


def _sum(rows, name) -> float:
    return sum(_f(r.get(name)) or 0.0 for r in rows)


def _mean(rows, name) -> float | None:
    values = [_f(r.get(name)) for r in rows]
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else None


def classify(move_bps, spread_bps, flow, depth_through_ratio, supported: bool) -> str:
    if not supported or move_bps is None:
        return "UNSUPPORTED"
    threshold = max(2 * (spread_bps or 0.0), 5.0)
    moved = abs(move_bps) >= threshold
    if not moved:
        return "ABSORPTION" if flow is not None and abs(flow) >= FLOW_STRONG else "QUIET"
    direction = math.copysign(1, move_bps)
    if flow is not None and flow * direction >= FLOW_STRONG:
        return "AGGRESSIVE_CONSUMPTION"
    if (
        depth_through_ratio is not None
        and depth_through_ratio < DEPTH_THINNING
        and (flow is None or abs(flow) < FLOW_STRONG)
    ):
        return "LIQUIDITY_WITHDRAWAL"
    return "MIXED"


def micro_state(series: list[tuple[int, dict]], symbol: str, as_of_epoch: int, window: int = WINDOW) -> MicroState:
    """One stock's microstructure over the ``window`` minutes that closed by ``as_of``."""
    from datetime import datetime

    from intelligence.archive import IST

    rows = [row for minute, row in series if as_of_epoch - window * 60 <= minute and minute + 60 <= as_of_epoch]
    out = MicroState(symbol, datetime.fromtimestamp(as_of_epoch, IST).strftime("%Y-%m-%d %H:%M:%S IST"), len(rows))
    if not rows:
        out.notes.append("no Full-packet minutes recorded in the window")
        return out
    out.cadence = cadence(rows)
    out.support = support(out.cadence)
    quote, trade = out.support["quote"], out.support["trade"]
    out.spread_bps = _mean(rows, "spread_bps_mean")
    depth = [((_f(r.get("bid5_qty_mean")) or 0) + (_f(r.get("ask5_qty_mean")) or 0)) for r in rows]
    out.depth5 = float(np.mean(depth)) if depth else None
    first, last = _f(rows[0].get("mid_first")), _f(rows[-1].get("mid_last"))
    out.move_bps = math.log(last / first) * 1e4 if first and last and first > 0 and last > 0 else None
    through = None
    if quote:
        top = _mean(rows, "bid1_qty_mean"), _mean(rows, "ask1_qty_mean")
        scale = (top[0] + top[1]) / 2 if None not in top and (top[0] + top[1]) > 0 else None
        out.ofi_norm = _sum(rows, "ofi") / scale if scale else None
        dep = _sum(rows, "bid1_depletion") + _sum(rows, "ask1_depletion")
        out.replenish_ratio = (_sum(rows, "bid1_replenish") + _sum(rows, "ask1_replenish")) / dep if dep else None
        out.imbalance1 = _mean(rows, "imbalance1_mean")
        if out.move_bps is not None:  # the side the price moved through: asks for a rise, bids for a fall
            side = "ask5_qty_last" if out.move_bps > 0 else "bid5_qty_last"
            start, end = _f(rows[0].get(side)), _f(rows[-1].get(side))
            through = end / start if start and end is not None else None
    else:
        out.notes.append(f"quote metrics unsupported: {out.support['quote_reason']}")
    if trade:
        buy, sell, other = _sum(rows, "buy_volume"), _sum(rows, "sell_volume"), _sum(rows, "unclassified_volume")
        total = buy + sell + other
        out.signed_flow = (buy - sell) / total if total else None
        signed = np.array([(_f(r.get("buy_volume")) or 0) - (_f(r.get("sell_volume")) or 0) for r in rows])
        moves = []
        for r in rows:
            a, b = _f(r.get("mid_first")), _f(r.get("mid_last"))
            moves.append(math.log(b / a) * 1e4 if a and b and a > 0 and b > 0 else np.nan)
        moves = np.array(moves)
        unit = np.mean([_f(r.get("volume")) or 0 for r in rows])
        ok = np.isfinite(moves)
        if ok.sum() >= 5 and unit > 0 and np.var(signed[ok]) > 0:
            x = signed[ok] / unit
            out.impact_bps = float(np.cov(x, moves[ok])[0, 1] / np.var(x, ddof=1))
    else:
        out.notes.append(f"trade metrics unsupported: {out.support['trade_reason']}")
    out.classification = classify(out.move_bps, out.spread_bps, out.signed_flow, through, quote)
    out.notes.append("packet-level approximation from Dhan snapshots, not exchange events")
    return out
