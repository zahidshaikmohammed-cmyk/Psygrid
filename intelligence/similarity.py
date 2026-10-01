"""Historical similarity: what followed past states that looked like now.

At a minute of today the market (or one instrument) is described by a small
state vector. Earlier sessions are searched for the closest states at a
similar time of day (within ``WINDOW_MINUTES``); each session contributes at
most its single closest minute, because neighbouring minutes of one session are
not independent evidence. The ``k`` closest sessions are the matches.

The result is the *distribution* of what followed those matches (quantiles,
share positive, count), always beside the base rate: the same outcome over
every earlier session at this minute. A distribution that looks like the base
rate means the state carries no information; ``separation`` measures that
(0.5 = indistinguishable). Nothing here is a prediction: fewer than
``MIN_MATCHES`` usable sessions gives ``INSUFFICIENT_HISTORY`` and no
distribution at all.

Only sessions strictly before today are searched, and their outcomes are
complete, so no match can carry information from after ``as_of``. State
vectors are computed by the same causal function for today's frame and for
past days, so a past minute's state is exactly what a frame at that minute
would have shown.
"""

from __future__ import annotations

import json
import os
import shutil
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from intelligence.archive import available_days, load_day
from intelligence.features import _session_open, log_returns
from intelligence.frame import MarketFrame, as_of_time, frame_at
from intelligence.history import store_dir
from intelligence.universe import BROAD_INDEX, MARKET_INDEX

STATE_VERSION = 1
WINDOW_MINUTES = 30
INSTRUMENT_STEP = 5  # instrument states are kept every 5 minutes to bound storage
DEFAULT_K = 10
MIN_MATCHES = 5
FEW_SESSIONS = 20
QUANTILES = (0.1, 0.25, 0.5, 0.75, 0.9)
HORIZONS = (15, 30, 60)

MARKET_STATE = ("index_ret_session", "index_ret_15m", "index_rvol_15m", "breadth_session", "dispersion_15m")
MARKET_OUTCOMES = ("index_fwd_ret_15m", "index_fwd_ret_30m", "index_fwd_ret_60m", "index_fwd_rvol_30m")
INSTRUMENT_STATE = ("ret_15m", "ret_session", "rvol_15m", "rel_index_session", "volume_ratio_15m")
INSTRUMENT_OUTCOMES = ("fwd_ret_15m", "fwd_ret_30m", "fwd_ret_60m", "fwd_rel_index_ret_30m")

CAVEAT = (
    "Distribution of what followed similar past states, beside the base rate for this time of day. "
    "It is not a forecast; a separation near 0.5 means the state carried no information."
)


# --- causal state series ----------------------------------------------------------------


def _rolling(a: np.ndarray, window: int, min_periods: int, how: str) -> np.ndarray:
    frame = pd.DataFrame(a.T).rolling(window, min_periods=min_periods)
    return getattr(frame, how)().to_numpy().T


def _lag_return(close: np.ndarray, lag: int) -> np.ndarray:
    out = np.full(close.shape, np.nan)
    if close.shape[-1] > lag:
        with np.errstate(divide="ignore", invalid="ignore"):
            out[..., lag:] = np.log(close[..., lag:] / close[..., :-lag])
    return out


def _forward_return(close: np.ndarray, lead: int) -> np.ndarray:
    out = np.full(close.shape, np.nan)
    if close.shape[-1] > lead:
        with np.errstate(divide="ignore", invalid="ignore"):
            out[..., :-lead] = np.log(close[..., lead:] / close[..., :-lead])
    return out


def _index_close(frame: MarketFrame, key: str) -> np.ndarray | None:
    if frame.indices is None or key not in frame.indices.keys:
        return None
    i = frame.indices.keys.index(key)
    return frame.indices.close[i], frame.indices.open[i]


def state_series(frame: MarketFrame) -> dict[str, np.ndarray]:
    """Per-minute state for every column of the frame, each value using only bars up to its column."""
    bars = frame.equity
    n, m = bars.shape
    with warnings.catch_warnings(), np.errstate(divide="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        r = log_returns(bars.close, bars.open)
        ret15 = _lag_return(bars.close, 15)
        session = np.log(bars.close / _session_open(frame)[:, None])
        rvol = _rolling(r, 15, 10, "std") if m else r
        vol_recent = _rolling(bars.volume, 15, 10, "mean") if m else r
        vol_so_far = pd.DataFrame(bars.volume.T).expanding(min_periods=10).mean().to_numpy().T if m else r
        volume_ratio = np.log(vol_recent / vol_so_far)
        up, down = np.sum(session > 0, axis=0), np.sum(session < 0, axis=0)
        out = {
            "breadth_session": np.where(up + down > 0, (up - down) / np.maximum(up + down, 1), np.nan),
            "dispersion_15m": np.nanstd(ret15, axis=0, ddof=1) if n > 1 else np.full(m, np.nan),
            "ret_15m": ret15,
            "ret_session": session,
            "rvol_15m": rvol,
            "volume_ratio_15m": volume_ratio,
        }
        index = _index_close(frame, MARKET_INDEX) or _index_close(frame, BROAD_INDEX)
        if index is not None:
            close, open_ = index
            index_r = log_returns(close[None], open_[None])[0]
            out["index_close"] = close
            out["index_ret_session"] = np.log(close / open_[0]) if m else close
            out["index_ret_15m"] = _lag_return(close, 15)
            out["index_rvol_15m"] = _rolling(index_r[None], 15, 10, "std")[0] if m else close
            out["index_r"] = index_r
        else:
            for name in ("index_close", "index_ret_session", "index_ret_15m", "index_rvol_15m", "index_r"):
                out[name] = np.full(m, np.nan)
        broad = _index_close(frame, BROAD_INDEX)
        broad_session = np.log(broad[0] / broad[1][0]) if broad is not None and m else np.full(m, np.nan)
        out["broad_close"] = broad[0] if broad is not None else np.full(m, np.nan)
        out["rel_index_session"] = session - broad_session[None, :]
    return out


def _stack(series: dict, names) -> np.ndarray:
    return np.stack([series[name] for name in names], axis=-1).astype(np.float64)


def market_outcomes(series: dict) -> np.ndarray:
    """(minutes, outcomes): what followed each minute of a completed session."""
    close = series["index_close"]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        fwd_rvol = np.full(len(close), np.nan)
        r = series["index_r"]
        for t in range(len(close) - 30):
            window = r[t + 1 : t + 31]
            if np.sum(np.isfinite(window)) >= 20:
                fwd_rvol[t] = np.nanstd(window, ddof=1)
    return np.stack([*(_forward_return(close, h) for h in HORIZONS), fwd_rvol], axis=-1)


def instrument_outcomes(series: dict, close: np.ndarray) -> np.ndarray:
    """(n, minutes, outcomes) for a completed session."""
    fwd = [_forward_return(close, h) for h in HORIZONS]
    broad_fwd = _forward_return(series["broad_close"], 30)
    return np.stack([*fwd, fwd[1] - broad_fwd[None, :]], axis=-1)


# --- per-session cache --------------------------------------------------------------------


@dataclass(frozen=True)
class SessionStates:
    session_date: str
    keys: tuple[str, ...]
    market_state: np.ndarray  # (minutes, len(MARKET_STATE))
    market_outcome: np.ndarray  # (minutes, len(MARKET_OUTCOMES))
    instrument_minutes: np.ndarray  # minute indices kept (every INSTRUMENT_STEP)
    instrument_state: np.ndarray  # (n, len(instrument_minutes), len(INSTRUMENT_STATE))
    instrument_outcome: np.ndarray  # (n, len(instrument_minutes), len(INSTRUMENT_OUTCOMES))

    def row(self, key: str) -> int | None:
        return self.keys.index(key) if key in self.keys else None


def summarise_states(day) -> SessionStates:
    frame = frame_at(day, as_of_time(day.session_date, "15:30"))
    series = state_series(frame)
    minutes = np.arange(0, frame.equity.shape[1], INSTRUMENT_STEP)
    return SessionStates(
        day.session_date,
        frame.equity.keys,
        _stack(series, MARKET_STATE).astype(np.float32),
        market_outcomes(series).astype(np.float32),
        minutes,
        _stack(series, INSTRUMENT_STATE)[:, minutes].astype(np.float32),
        instrument_outcomes(series, frame.equity.close)[:, minutes].astype(np.float32),
    )


def _states_dir(cache_root: Path, session_date: str) -> Path:
    return cache_root / "states" / f"{session_date}.v{STATE_VERSION}"


def load_states(archive_root: Path, session_date: str, cache_root: Path | None = None) -> SessionStates:
    """A past session's states, from the cache when present (instrument arrays memory-mapped)."""
    folder = _states_dir(cache_root or store_dir(), session_date)
    if (folder / "complete").exists():
        meta = json.loads((folder / "meta.json").read_text())
        market = np.load(folder / "market.npz")
        return SessionStates(
            session_date,
            tuple(meta["keys"]),
            market["state"],
            market["outcome"],
            market["instrument_minutes"],
            np.load(folder / "instrument_state.npy", mmap_mode="r"),
            np.load(folder / "instrument_outcome.npy", mmap_mode="r"),
        )
    states = summarise_states(load_day(archive_root, session_date))
    tmp = folder.with_name(folder.name + f".tmp{os.getpid()}")
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / "meta.json").write_text(json.dumps({"keys": list(states.keys), "version": STATE_VERSION}))
    np.savez(tmp / "market.npz", state=states.market_state, outcome=states.market_outcome,
             instrument_minutes=states.instrument_minutes)  # fmt: skip
    np.save(tmp / "instrument_state.npy", states.instrument_state)
    np.save(tmp / "instrument_outcome.npy", states.instrument_outcome)
    (tmp / "complete").write_text("")
    try:
        os.replace(tmp, folder)
    except OSError:  # another process cached it first; keep theirs
        shutil.rmtree(tmp, ignore_errors=True)
    return states


# --- matching -----------------------------------------------------------------------------


def _distribution(values: np.ndarray) -> dict | None:
    values = values[np.isfinite(values)]
    if not len(values):
        return None
    q = np.quantile(values, QUANTILES)
    return {
        "count": len(values),
        "quantiles": {f"p{int(p * 100)}": float(v) for p, v in zip(QUANTILES, q, strict=True)},
        "mean": float(np.mean(values)),
        "positive_share": float(np.mean(values > 0)),
    }


def _separation(matched: np.ndarray, base: np.ndarray) -> float | None:
    """P(a matched outcome exceeds a base-rate outcome), ties counting half; 0.5 = no difference."""
    matched, base = matched[np.isfinite(matched)], base[np.isfinite(base)]
    if not len(matched) or not len(base):
        return None
    greater = (matched[:, None] > base[None, :]).mean()
    ties = (matched[:, None] == base[None, :]).mean()
    return float(greater + 0.5 * ties)


def _match(query: np.ndarray, candidates: list[tuple[str, np.ndarray, np.ndarray, np.ndarray]],
           k: int, names, outcome_names, minute: int, base: list[np.ndarray]) -> dict:  # fmt: skip
    """``candidates``: (session, minutes, states (c, d), outcomes (c, o)) per earlier session."""
    used = [i for i, v in enumerate(query) if np.isfinite(v)]
    if len(used) < 2:
        return {"status": "INSUFFICIENT_DATA", "reason": "fewer than two state features are known at this minute"}
    pool = (
        np.concatenate([states[:, used] for _, _, states, _ in candidates]) if candidates else np.empty((0, len(used)))
    )
    pool = pool[np.all(np.isfinite(pool), axis=1)]
    if len(pool) < MIN_MATCHES:
        return _insufficient(len(candidates))
    centre = np.median(pool, axis=0)
    scale = np.median(np.abs(pool - centre), axis=0) * 1.4826
    fallback = np.std(pool, axis=0)
    scale = np.where(scale > 0, scale, np.where(fallback > 0, fallback, 1.0))
    q = (query[used] - centre) / scale
    best = []
    for session, minutes, states, outcomes in candidates:
        z = (states[:, used] - centre) / scale
        d = np.sqrt(np.sum((z - q) ** 2, axis=1))
        d[~np.isfinite(d)] = np.inf
        if not np.isfinite(d).any():
            continue
        j = int(np.argmin(d))
        best.append((float(d[j]), session, int(minutes[j]), states[j], outcomes[j]))
    if len(best) < MIN_MATCHES:
        return _insufficient(len(best))
    best.sort(key=lambda b: (b[0], b[1]))
    chosen = best[:k]
    typical = float(np.median([b[0] for b in best]))
    flags = []
    if len(best) < FEW_SESSIONS:
        flags.append("FEW_SESSIONS")
    outcomes = {}
    for o, name in enumerate(outcome_names):
        matched = np.array([b[4][o] for b in chosen], dtype=float)
        base_values = np.array([b[o] for b in base], dtype=float)
        dist = _distribution(matched)
        outcomes[name] = {
            "matched": dist,
            "base_rate": _distribution(base_values),
            "separation": _separation(matched, base_values),
        }
    match_distance = float(np.median([b[0] for b in chosen]))
    return {
        "status": "OK",
        "features_used": [names[i] for i in used],
        "query_state": {names[i]: float(query[i]) for i in used},
        "sessions_searched": len(best),
        "matches": [
            {
                "session_date": s,
                "minute_index": mi,
                "distance": round(d, 4),
                "state": {names[i]: _f(st[i]) for i in used},
            }
            for d, s, mi, st, _ in chosen
        ],
        "outcomes": outcomes,
        "uncertainty": {
            "matches": len(chosen),
            "median_match_distance": round(match_distance, 4),
            "median_distance_all_sessions": round(typical, 4),
            "match_quality": round(match_distance / typical, 4) if typical > 0 else None,
            "flags": flags,
        },
        "minute_index": minute,
        "caveat": CAVEAT,
    }


def _f(value) -> float | None:
    value = float(value)
    return value if np.isfinite(value) else None


def _insufficient(sessions: int) -> dict:
    return {
        "status": "INSUFFICIENT_HISTORY",
        "reason": f"{sessions} earlier sessions with usable states; at least {MIN_MATCHES} are needed",
        "sessions_searched": sessions,
        "caveat": CAVEAT,
    }


def _earlier_sessions(archive_root: Path, session_date: str, lookback: int) -> list[str]:
    return [d for d in available_days(archive_root) if d < session_date][-lookback:]


def market_matches(frame: MarketFrame, archive_root: Path, cache_root: Path | None = None,
                   lookback: int = 60, k: int = DEFAULT_K) -> dict:  # fmt: skip
    """Earlier sessions whose market state at a similar time looked most like now, and what followed."""
    m = frame.equity.shape[1] - 1
    if m < 0:
        return {"status": "INSUFFICIENT_DATA", "reason": "no completed bar yet", "caveat": CAVEAT}
    query = _stack(state_series(frame), MARKET_STATE)[-1]
    candidates, base = [], []
    for session in _earlier_sessions(archive_root, frame.session_date, lookback):
        states = load_states(archive_root, session, cache_root)
        total = len(states.market_state)
        lo, hi = max(0, m - WINDOW_MINUTES), min(total, m + WINDOW_MINUTES + 1)
        if lo >= hi:
            continue
        candidates.append((session, np.arange(lo, hi), states.market_state[lo:hi], states.market_outcome[lo:hi]))
        if m < total:
            base.append(states.market_outcome[m])
    return {"scope": "MARKET", "as_of": _as_of(frame),
            **_match(query, candidates, k, MARKET_STATE, MARKET_OUTCOMES, m, base)}  # fmt: skip


def instrument_matches(frame: MarketFrame, key: str, archive_root: Path, cache_root: Path | None = None,
                       lookback: int = 60, k: int = DEFAULT_K) -> dict:  # fmt: skip
    """The instrument's own earlier sessions whose state at a similar time looked most like now."""
    if key not in frame.equity.keys:
        return {"status": "UNKNOWN_INSTRUMENT", "key": key, "caveat": CAVEAT}
    m = frame.equity.shape[1] - 1
    if m < 0:
        return {"status": "INSUFFICIENT_DATA", "reason": "no completed bar yet", "caveat": CAVEAT}
    i = frame.equity.keys.index(key)
    query = _stack(state_series(frame), INSTRUMENT_STATE)[i, -1]
    candidates, base = [], []
    for session in _earlier_sessions(archive_root, frame.session_date, lookback):
        states = load_states(archive_root, session, cache_root)
        row = states.row(key)
        if row is None:
            continue
        minutes = states.instrument_minutes
        near = np.flatnonzero(np.abs(minutes - m) <= WINDOW_MINUTES)
        if not len(near):
            continue
        state_rows = np.asarray(states.instrument_state[row, near], dtype=np.float64)
        outcome_rows = np.asarray(states.instrument_outcome[row, near], dtype=np.float64)
        candidates.append((session, minutes[near], state_rows, outcome_rows))
        nearest = int(np.argmin(np.abs(minutes[near] - m)))
        base.append(outcome_rows[nearest])
    return {"scope": "INSTRUMENT", "key": key, "as_of": _as_of(frame),
            **_match(query, candidates, k, INSTRUMENT_STATE, INSTRUMENT_OUTCOMES, m, base)}  # fmt: skip


def _as_of(frame: MarketFrame) -> str:
    return frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST")
