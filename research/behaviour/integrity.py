"""Data-integrity checks used by the pre-registered confirmation (FROZEN, see prereg.lock).

Corporate actions: Dhan intraday candles are not guaranteed to be adjusted for splits, bonuses or
demergers. Any session whose overnight move, net of NIFTY's overnight move, is at least
CA_GAP_PCT is flagged; signals are suppressed for CA_EXCLUDE_SESSIONS sessions either side and the
flagged day's daily return is removed from every rolling window (beta, drift). Flags are reported.
Genuine news gaps of that size are excluded too (conservative: they become not-applicable, not
failures).
"""

from __future__ import annotations

import numpy as np

CA_GAP_PCT = 8.0
CA_EXCLUDE_SESSIONS = 5
SPLIT_RATIOS = (1 / 2, 1 / 3, 1 / 4, 1 / 5, 1 / 10, 2 / 3, 3 / 4, 1 / 1.5)


def corporate_action_flags(open0: np.ndarray, prev_close: np.ndarray, index_gap_pct: np.ndarray | None = None):
    """(flag[D] bool, likely_split[D] bool) from session opens and previous closes."""
    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = open0 / prev_close
        gap = (ratio - 1) * 100
    adj = gap - (np.nan_to_num(index_gap_pct) if index_gap_pct is not None else 0.0)
    flag = np.abs(np.nan_to_num(adj)) >= CA_GAP_PCT
    near = np.zeros(len(ratio), bool)
    for s in SPLIT_RATIOS:
        near |= np.abs(np.nan_to_num(ratio, nan=1.0) / s - 1) < 0.03
    return flag, flag & near


def exclusion_mask(flag: np.ndarray, k: int = CA_EXCLUDE_SESSIONS) -> np.ndarray:
    """True for sessions within k sessions of a flagged session (inclusive)."""
    out = np.zeros(len(flag), bool)
    for i in np.nonzero(flag)[0]:
        out[max(0, i - k) : i + k + 1] = True
    return out
