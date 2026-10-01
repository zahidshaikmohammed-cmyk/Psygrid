"""Historical baselines: what is normal for each instrument at each minute of the day.

Each archived day is summarised once into per-minute arrays (log volume, absolute
1m return, log bar range) and market series (median absolute return, dispersion,
breadth, active share), cached as ``summaries/<date>.npz`` in the intelligence
store. A baseline for date D uses only sessions strictly before D, so it never
sees the day it judges.

Baselines are robust: the median and the MAD (scaled to a standard deviation
under normality) over the last ``window_sessions`` sessions, pooling +/- 2
minutes around each minute of the day to steady the estimate. A cell with fewer
than ``min_sessions`` contributing sessions is NaN: no baseline, not a guess.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from config import MARKET_END
from intelligence.archive import available_days, load_day
from intelligence.features import log_returns
from intelligence.frame import as_of_time, frame_at

SUMMARY_VERSION = 3
MAD_TO_SD = 1.4826
POOL_MINUTES = 2
INSTRUMENT_FIELDS = ("log_volume_1m", "abs_ret_1m", "log_range_1m")
MARKET_FIELDS = ("median_abs_ret_1m", "dispersion_15m", "breadth_session", "active_share_1m")
DEFAULT_WINDOW_SESSIONS = 20
DEFAULT_MIN_SESSIONS = 5


def store_dir() -> Path:
    configured = os.getenv("PSYGRID_INTELLIGENCE_DIR", "").strip()
    return Path(configured).expanduser() if configured else Path.home() / "psygrid-intelligence"


@dataclass(frozen=True)
class DaySummary:
    session_date: str
    keys: tuple[str, ...]
    instrument: dict[str, np.ndarray]  # field -> (n, minutes)
    market: dict[str, np.ndarray]  # field -> (minutes,)


def summarise_day(day) -> DaySummary:
    """Per-minute base quantities for a whole archived day."""
    frame = frame_at(day, as_of_time(day.session_date, MARKET_END))
    bars = frame.equity
    with warnings.catch_warnings(), np.errstate(divide="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        r = log_returns(bars.close, bars.open)
        instrument = {
            "log_volume_1m": np.log1p(bars.volume).astype(np.float32),  # volume is skewed; log stabilises it
            "abs_ret_1m": np.abs(r).astype(np.float32),
            "log_range_1m": np.log((bars.high - bars.low) / bars.close).astype(np.float32),
        }
        close = bars.close
        ret15 = np.full(close.shape, np.nan)
        ret15[:, 15:] = np.log(close[:, 15:] / close[:, :-15])
        present = ~np.isnan(bars.open)
        first_col = np.argmax(present, axis=1)
        first_open = np.where(present.any(axis=1), bars.open[np.arange(len(first_col)), first_col], np.nan)
        session = np.log(close / first_open[:, None])
        up = np.sum(session > 0, axis=0)
        down = np.sum(session < 0, axis=0)
        market = {
            "median_abs_ret_1m": np.nanmedian(np.abs(r), axis=0),
            "dispersion_15m": np.nanstd(ret15, axis=0, ddof=1),
            "breadth_session": np.where(up + down > 0, (up - down) / np.maximum(up + down, 1), np.nan),
            "active_share_1m": np.mean(present, axis=0),
        }
    return DaySummary(day.session_date, bars.keys, instrument, {k: v.astype(np.float32) for k, v in market.items()})


def _summary_path(root: Path, session_date: str) -> Path:
    return root / "summaries" / f"{session_date}.v{SUMMARY_VERSION}.npz"


def load_summary(archive_root: Path, session_date: str, cache_root: Path | None = None) -> DaySummary:
    """A day's summary, from the cache when present, else computed from the archive and cached."""
    cache_root = cache_root or store_dir()
    path = _summary_path(cache_root, session_date)
    if path.exists():
        data = np.load(path, allow_pickle=False)
        return DaySummary(
            session_date,
            tuple(str(k) for k in data["keys"]),
            {f: data[f"i_{f}"] for f in INSTRUMENT_FIELDS},
            {f: data[f"m_{f}"] for f in MARKET_FIELDS},
        )
    summary = summarise_day(load_day(archive_root, session_date))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp.npz")
    np.savez_compressed(
        tmp,
        keys=np.array(summary.keys),
        **{f"i_{f}": summary.instrument[f] for f in INSTRUMENT_FIELDS},
        **{f"m_{f}": summary.market[f] for f in MARKET_FIELDS},
    )
    os.replace(tmp, path)
    return summary


@dataclass(frozen=True)
class Baselines:
    """Robust per-minute baselines for one session date, from earlier sessions only."""

    session_date: str
    sessions: tuple[str, ...]  # the earlier sessions used
    keys: tuple[str, ...]
    instrument_median: dict[str, np.ndarray]  # field -> (n, minutes)
    instrument_scale: dict[str, np.ndarray]  # field -> (n, minutes), MAD scaled to an SD
    instrument_count: dict[str, np.ndarray]  # field -> (n, minutes), sessions contributing
    market_median: dict[str, np.ndarray]  # field -> (minutes,)
    market_scale: dict[str, np.ndarray]
    min_sessions: int

    @property
    def available(self) -> bool:
        return len(self.sessions) >= self.min_sessions

    def lookup(self, field: str, minute: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """(median, scale, sessions) per instrument at a minute of the day; NaN where unavailable."""
        n = len(self.keys)
        median = self.instrument_median.get(field)
        if median is None or minute < 0 or minute >= median.shape[1]:
            return np.full(n, np.nan), np.full(n, np.nan), np.zeros(n)
        return median[:, minute], self.instrument_scale[field][:, minute], self.instrument_count[field][:, minute]

    def market_lookup(self, field: str, minute: int) -> tuple[float, float]:
        median = self.market_median.get(field)
        if median is None or minute < 0 or minute >= len(median):
            return float("nan"), float("nan")
        return float(median[minute]), float(self.market_scale[field][minute])


def _pooled(stack: np.ndarray) -> np.ndarray:
    """Pool +/- POOL_MINUTES along the last axis: (..., minutes) -> (..., minutes, 2*POOL+1)."""
    pad = [(0, 0)] * (stack.ndim - 1) + [(POOL_MINUTES, POOL_MINUTES)]
    padded = np.pad(stack.astype(np.float32), pad, constant_values=np.nan)
    width = 2 * POOL_MINUTES + 1
    return np.stack([padded[..., i : i + stack.shape[-1]] for i in range(width)], axis=-1)


ROW_CHUNK = 32  # instruments per chunk: bounds the working set of the median computation


def _robust_core(stack: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Median, scaled MAD and contributing-session count over sessions (axis 0) and the pooled minute window."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        sessions_present = np.sum(~np.isnan(stack), axis=0)
        pooled = np.moveaxis(_pooled(stack), 0, -2)  # (..., minutes, sessions, pool)
        flat = pooled.reshape(*pooled.shape[:-2], -1)
        median = np.nanmedian(flat, axis=-1)
        mad = np.nanmedian(np.abs(flat - median[..., None]), axis=-1) * MAD_TO_SD
    return median.astype(np.float32), mad.astype(np.float32), sessions_present


def _robust(stack: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """``_robust_core`` over (sessions, minutes) or (sessions, instruments, minutes), chunked by instrument."""
    if stack.ndim == 2:
        return _robust_core(stack)
    parts = [_robust_core(stack[:, i : i + ROW_CHUNK]) for i in range(0, stack.shape[1], ROW_CHUNK)]
    return tuple(np.concatenate([part[k] for part in parts], axis=0) for k in range(3))


def build_baselines(
    archive_root: Path,
    session_date: str,
    keys: tuple[str, ...],
    window_sessions: int = DEFAULT_WINDOW_SESSIONS,
    min_sessions: int = DEFAULT_MIN_SESSIONS,
    cache_root: Path | None = None,
) -> Baselines:
    """Baselines for ``session_date`` from up to ``window_sessions`` archived sessions strictly before it."""
    earlier = [d for d in available_days(archive_root) if d < session_date][-window_sessions:]
    summaries = [load_summary(archive_root, d, cache_root) for d in earlier]
    n = len(keys)
    minutes = max((next(iter(s.market.values())).shape[0] for s in summaries), default=0)
    inst_median, inst_scale, inst_count, mkt_median, mkt_scale = {}, {}, {}, {}, {}
    if summaries and minutes:
        for field in INSTRUMENT_FIELDS:
            stack = np.full((len(summaries), n, minutes), np.nan, dtype=np.float32)
            for s_idx, summary in enumerate(summaries):
                index = {k: i for i, k in enumerate(summary.keys)}
                rows = [index.get(k) for k in keys]
                present = [i for i, r in enumerate(rows) if r is not None]
                source = summary.instrument[field]
                stack[s_idx, present, : source.shape[1]] = source[[rows[i] for i in present]]
            median, scale, count = _robust(stack)
            inst_median[field], inst_scale[field], inst_count[field] = median, scale, count
        for field in MARKET_FIELDS:
            stack = np.full((len(summaries), minutes), np.nan, dtype=np.float32)
            for s_idx, summary in enumerate(summaries):
                series = summary.market[field]
                stack[s_idx, : series.shape[0]] = series
            median, scale, _ = _robust(stack)
            mkt_median[field], mkt_scale[field] = median, scale
    for field in INSTRUMENT_FIELDS:
        if field in inst_count:
            insufficient = inst_count[field] < min_sessions
            inst_median[field][insufficient] = np.nan
            inst_scale[field][insufficient] = np.nan
    if len(summaries) < min_sessions:
        mkt_median = {f: np.full_like(v, np.nan) for f, v in mkt_median.items()}
        mkt_scale = {f: np.full_like(v, np.nan) for f, v in mkt_scale.items()}
    return Baselines(
        session_date=session_date,
        sessions=tuple(earlier),
        keys=keys,
        instrument_median=inst_median,
        instrument_scale=inst_scale,
        instrument_count=inst_count,
        market_median=mkt_median,
        market_scale=mkt_scale,
        min_sessions=min_sessions,
    )
