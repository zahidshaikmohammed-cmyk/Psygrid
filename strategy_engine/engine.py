from __future__ import annotations

from datetime import datetime
from .features import build_features
from .models import Signal

DEFAULT_CONFIG = {
    "atr_period": 20,
    "absolute_gap_limit": 0.03,
    "gap_z_limit": 3.0,
    "hard_exhaustion_limit": 0.90,
    "extension_atr_limit": 2.0,
    "impulse_exhaustion_atr": 3.0,
    "volume_climax": 3.0,
    "min_retracement": 0.30,
    "preferred_rr": 2.0,
    "atr_stop_buffer": 0.35,
    "min_stop_pct": 0.0025,
    "max_stop_pct": 0.025,
}

class StrategyEngine:
    """Pure strategy layer. It consumes normalized 1m OHLCV and never uses Psygrid indicators."""

    def __init__(self, config=None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._frozen = False
        self._frozen_features = None

    def freeze(self, features_by_symbol):
        self._frozen_features = tuple(features_by_symbol)
        self._frozen = True
        return self.rank_frozen()

    def rank_frozen(self):
        if not self._frozen:
            raise RuntimeError("Features must be frozen before ranking")
        return sorted(
            [f for f in self._frozen_features if f.eligible],
            key=lambda f: f.score,
            reverse=True,
        )

    def generate_signal(self, features_by_symbol, ltp_by_symbol, timestamp: datetime):
        ranked = self.freeze(features_by_symbol)
        if not ranked:
            # Forced-entry mode: if all hypotheses fail, choose the least-bad
            # hypothesis rather than inventing a symbol or returning NO TRADE.
            fallback = sorted(features_by_symbol, key=lambda f: f.score, reverse=True)[0]
            ranked = [fallback]
        winner = ranked[0]
        entry = float(ltp_by_symbol[winner.symbol])
        atr_value = max(abs(entry * winner.impulse_pct) / max(abs(winner.impulse_atr), 1e-9), entry * 1e-5)
        stop_distance = max(entry * self.config["min_stop_pct"], atr_value * self.config["atr_stop_buffer"])
        stop_distance = min(stop_distance, entry * self.config["max_stop_pct"])
        stop = entry - stop_distance if winner.side == "LONG" else entry + stop_distance
        target = entry + stop_distance * self.config["preferred_rr"] if winner.side == "LONG" else entry - stop_distance * self.config["preferred_rr"]
        return Signal(timestamp, winner.symbol, winner.side, entry, stop, target,
                      winner.score, 1, tuple(winner.reasons))
