from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Side = Literal["LONG", "SHORT"]

@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    complete: bool = True

@dataclass
class Features:
    symbol: str
    side: Side
    gap_pct: float
    gap_z: float
    impulse_pct: float
    impulse_atr: float
    directional_efficiency: float
    retracement_depth: float
    retracement_volume_ratio: float
    relative_strength_market: float
    relative_strength_sector: float
    vwap: float
    vwap_slope: float
    vwap_distance_atr: float
    range_atr: float
    volume_ratio: float
    extension_atr: float
    exhaustion_score: float
    structure_score: float
    trap_penalty: float
    score: float = 0.0
    eligible: bool = True
    reasons: list[str] = field(default_factory=list)

@dataclass(frozen=True)
class Signal:
    timestamp: datetime
    symbol: str
    side: Side
    entry: float
    stop_loss: float
    target: float
    score: float
    rank: int
    reasons: tuple[str, ...]
