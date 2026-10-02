"""Expected-response engine: what each stock should be doing, given everything else, and where it is not.

For every stock the model explains its 1m log return by common drivers:

    r_i,t = sum_l b_i,l * m_t-l  +  sum_l g_i,l * s_i,t-l  +  sum_k d_i,k * p_k,t  +  e_i,t     (l = 0..LAGS)

- ``m`` is the market factor: NIFTY 500's 1m return (NIFTY, then the
  cross-sectional median, as fallbacks);
- ``s_i`` is the stock's sector factor: the mean market-residual return of the
  *other* members of its sector (leave-one-out, so a stock never explains
  itself; stocks without a sector have none);
- ``p`` are statistical factors: the leading principal components of the
  market- and sector-residual returns over the training sessions, with
  loadings fixed by training and realisations estimated each minute by a
  cross-sectional regression of current residuals on those loadings.

Coefficients come from a ridge regression per stock, on sessions strictly
before the day judged. Lags never cross a session boundary. A missing bar
(no trade) is a missing return, never zero.

Outputs, using only bars completed by ``as_of``:

- expected and actual cumulative response over the last ``window`` minutes,
  the **response gap** (expected minus actual) and its size in units of the
  stock's residual volatility (``gap_sigma``);
- the decomposition of the expected response into market, sector and
  statistical contributions;
- the **pending response**: the part of the stock's expected move that the
  lag structure still owes for factor moves already observed. It is the only
  forward-looking quantity, and the evaluation harness tests whether it
  carries information;
- the **delay profile** (share of the total market response at each lag),
  the delay index (1 - immediate share), the half-life (minutes until half
  of the response has arrived) and the mean lag (sum of lag x share).

These are measurements of conditional association under a linear model, not
causes, and they say nothing about what to trade.
"""

from __future__ import annotations

import json
import os
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from intelligence.archive import ArchiveDay, load_day, session_days
from intelligence.features import log_returns
from intelligence.frame import as_of_time, frame_at
from intelligence.history import store_dir
from intelligence.universe import BROAD_INDEX, MARKET_INDEX, sector_of

MODEL_VERSION = "1"
LAGS = 5
STAT_FACTORS = 5
RIDGE_ALPHA = 0.02  # ridge penalty as a fraction of the mean diagonal of X'X
MIN_TRAIN_ROWS = 300
MIN_SECTOR_MEMBERS = 3
DEFAULT_WINDOW = 15
CLIP = 0.05  # 1m log returns beyond +-5% are clipped when building factors (not when judging stocks)


@dataclass
class SessionReturns:
    session_date: str
    keys: tuple[str, ...]
    grid: np.ndarray
    r: np.ndarray  # (n, m) 1m log returns; NaN where no bar
    volume: np.ndarray  # (n, m)
    close: np.ndarray  # (n, m)
    market: np.ndarray  # (m,)
    market_source: str


def _index_return(frame, key: str):
    if frame.indices is None or key not in frame.indices.keys:
        return None
    i = frame.indices.keys.index(key)
    r = log_returns(frame.indices.close[i : i + 1], frame.indices.open[i : i + 1])[0]
    return r if np.isfinite(r).sum() > len(r) * 0.5 else None


def session_returns(day: ArchiveDay, as_of=None) -> SessionReturns:
    frame = frame_at(day, as_of or as_of_time(day.session_date, "15:30"))
    bars = frame.equity
    with np.errstate(divide="ignore", invalid="ignore"):
        r = log_returns(bars.close, bars.open)
    market, source = None, ""
    for key in (BROAD_INDEX, MARKET_INDEX):
        market = _index_return(frame, key)
        if market is not None:
            source = key
            break
    if market is None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            market = np.nanmedian(r, axis=0) if r.shape[1] else np.zeros(0)
        source = "cross_sectional_median"
    return SessionReturns(day.session_date, bars.keys, frame.grid, r, bars.volume, bars.close,
                          np.nan_to_num(market, nan=0.0), source)  # fmt: skip


def _sector_groups(keys) -> dict[str, list[int]]:
    groups: dict[str, list[int]] = {}
    for i, key in enumerate(keys):
        sector = sector_of(key)
        if sector:
            groups.setdefault(sector, []).append(i)
    return {s: idx for s, idx in groups.items() if len(idx) >= MIN_SECTOR_MEMBERS}


def sector_factor(r: np.ndarray, market: np.ndarray, groups: dict[str, list[int]]) -> np.ndarray:
    """(n, m) leave-one-out mean market-residual return of each stock's sector peers; NaN without a sector."""
    out = np.full(r.shape, np.nan)
    resid = np.clip(r - market[None, :], -CLIP, CLIP)
    for idx in groups.values():
        block = resid[idx]
        valid = np.isfinite(block)
        total = np.where(valid, block, 0.0).sum(axis=0)
        count = valid.sum(axis=0)
        for pos, i in enumerate(idx):
            own_valid = valid[pos]
            peers = count - own_valid
            with np.errstate(invalid="ignore", divide="ignore"):
                out[i] = np.where(peers > 0, (total - np.where(own_valid, block[pos], 0.0)) / peers, np.nan)
    return out


def _lagged(x: np.ndarray, lag: int) -> np.ndarray:
    """Shift along the last axis by ``lag`` minutes within the session (NaN at the start)."""
    if lag == 0:
        return x
    out = np.full(x.shape, np.nan)
    out[..., lag:] = x[..., :-lag]
    return out


def _factor_realisations(e: np.ndarray, loadings: np.ndarray, min_names: int = 50) -> np.ndarray:
    """(K, m) cross-sectional least-squares projection of residuals e (n, m) on loadings (n, K)."""
    m = e.shape[1]
    k = loadings.shape[1]
    out = np.zeros((k, m))
    usable = np.isfinite(loadings).all(axis=1)
    for t in range(m):
        valid = usable & np.isfinite(e[:, t])
        if valid.sum() < max(min_names, k + 1):
            continue
        a = loadings[valid]
        out[:, t] = np.linalg.lstsq(a, e[valid, t], rcond=None)[0]
    return out


@dataclass
class ResponseModel:
    trained_on: tuple[str, ...]
    keys: tuple[str, ...]
    sectors: tuple[str | None, ...]
    beta_market: np.ndarray  # (n, LAGS+1)
    beta_sector: np.ndarray  # (n, LAGS+1); zero without a sector
    beta_stat: np.ndarray  # (n, K)
    loadings: np.ndarray  # (n, K)
    resid_sd: np.ndarray  # (n,)
    train_rows: np.ndarray  # (n,)
    median_turnover: np.ndarray  # (n,) median 1m traded value in training, for liquidity buckets
    version: str = MODEL_VERSION

    @property
    def total_market_beta(self) -> np.ndarray:
        return self.beta_market.sum(axis=1)

    def delay_profile(self) -> dict[str, np.ndarray]:
        total = self.beta_market.sum(axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            share = np.where(np.abs(total)[:, None] > 1e-9, self.beta_market / total[:, None], np.nan)
            cumulative = np.cumsum(share, axis=1)
        half_life = np.full(len(total), np.nan)
        for i in range(len(total)):
            c = cumulative[i]
            if not np.isfinite(c).all() or total[i] <= 0:
                continue
            above = np.flatnonzero(c >= 0.5)
            if not len(above):
                half_life[i] = float(LAGS)
                continue
            j = above[0]
            half_life[i] = 0.0 if j == 0 else (j - 1) + (0.5 - c[j - 1]) / max(c[j] - c[j - 1], 1e-9)
        mean_lag = (share * np.arange(LAGS + 1)[None, :]).sum(axis=1)
        return {"share": share, "delay_index": 1 - share[:, 0], "half_life": half_life, "mean_lag": mean_lag}


def _design(sr: SessionReturns, groups) -> tuple[np.ndarray, np.ndarray]:
    """Per-session lagged factors: market lags (L+1, m) and sector lags (n, L+1, m)."""
    s = sector_factor(sr.r, sr.market, groups)
    m_lags = np.stack([_lagged(sr.market, lag) for lag in range(LAGS + 1)])
    s_lags = np.stack([_lagged(s, lag) for lag in range(LAGS + 1)], axis=1)
    return m_lags, s_lags


def lag_parts(bm: np.ndarray, bs: np.ndarray, m_lags: np.ndarray, s_lags: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """(n, m) market and sector parts of the expected return from the distributed-lag coefficients."""
    return (np.einsum("nl,lm->nm", bm, np.nan_to_num(m_lags)),
            np.einsum("nl,nlm->nm", bs, np.nan_to_num(s_lags)))  # fmt: skip


def stat_realisations(lag_residual: np.ndarray, loadings: np.ndarray) -> np.ndarray:
    """(K, m) statistical-factor realisations from residuals left *after* the lag model.

    Taking residuals after the distributed-lag market and sector terms matters:
    a delayed response to the market is common across stocks, and factors
    extracted before removing it would absorb it and hide it.
    """
    if loadings is None or not loadings.shape[1]:
        return np.zeros((0, lag_residual.shape[1]))
    return _factor_realisations(np.clip(lag_residual, -CLIP, CLIP), loadings)


def _ridge(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    xtx = x.T @ x
    penalty = max(RIDGE_ALPHA * np.trace(xtx) / max(xtx.shape[0], 1), 1e-12)
    return np.linalg.solve(xtx + penalty * np.eye(xtx.shape[0]), x.T @ y)


def align(sr: SessionReturns, keys: tuple[str, ...]) -> SessionReturns:
    """``sr`` with rows in ``keys`` order; a key ``sr`` lacks gets NaN rows (no bars), never zeros."""
    if tuple(sr.keys) == tuple(keys):
        return sr
    pos = {k: i for i, k in enumerate(sr.keys)}
    idx = np.array([pos.get(k, -1) for k in keys], dtype=int)

    def take(a):
        if a is None:
            return None
        return (
            np.where(idx[:, None] >= 0, a[np.maximum(idx, 0)], np.nan)
            if len(sr.keys)
            else np.full((len(keys), a.shape[1]), np.nan)
        )

    return SessionReturns(sr.session_date, tuple(keys), sr.grid, take(sr.r), take(sr.volume), take(sr.close),
                          sr.market, sr.market_source)  # fmt: skip


def _value(sr: SessionReturns, i: int) -> np.ndarray:
    return sr.volume[i] if sr.close is None else sr.close[i] * sr.volume[i]


def compact(sr: SessionReturns) -> SessionReturns:
    """Training form: ``volume`` holds traded value (close x volume) and ``close`` is dropped (a third the memory)."""
    with np.errstate(invalid="ignore"):
        value = sr.volume * sr.close if sr.close is not None else sr.volume
    return SessionReturns(sr.session_date, sr.keys, sr.grid, sr.r, value, None, sr.market, sr.market_source)


def fit(sessions: list[SessionReturns], keys: tuple[str, ...]) -> ResponseModel:
    """Fit on earlier sessions only. ``keys`` is the universe to model (today's keys)."""
    n = len(keys)
    aligned = [align(sr, keys) for sr in sessions]
    groups = _sector_groups(keys)
    sectors = tuple(next((s for s, idx in groups.items() if i in idx), None) for i in range(n))

    # Market lags (L+1, m) and the sector factor (n, m) per session; a stock's sector lags are built when it is
    # fitted, so memory stays at one (n, m) array per session instead of (n, L+1, m).
    designs = [(np.stack([_lagged(sr.market, lag) for lag in range(LAGS + 1)]), sector_factor(sr.r, sr.market, groups))
               for sr in aligned]  # fmt: skip
    bm = np.zeros((n, LAGS + 1))
    bs = np.zeros((n, LAGS + 1))
    rows = np.zeros(n, dtype=int)
    turnover = np.full(n, np.nan)
    fitted = np.zeros(n, dtype=bool)
    # Stage 1: distributed-lag market and sector model per stock.
    for i in range(n):
        xs, ys = [], []
        for sr, (m_lags, s_now) in zip(aligned, designs, strict=True):
            parts = [m_lags.T]
            if sectors[i] is not None:
                parts.append(np.stack([_lagged(s_now[i], lag) for lag in range(LAGS + 1)], axis=1))
            x = np.concatenate(parts, axis=1)
            y = sr.r[i]
            ok = np.isfinite(y) & np.isfinite(x).all(axis=1)
            ok[:LAGS] = False  # lags do not cross the session boundary
            xs.append(x[ok])
            ys.append(y[ok])
        x = np.concatenate(xs) if xs else np.zeros((0, 1))
        y = np.concatenate(ys) if ys else np.zeros(0)
        rows[i] = len(y)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            values = np.concatenate([_value(sr, i) for sr in aligned]) if aligned else np.zeros(0)
            turnover[i] = np.nanmedian(values) if np.isfinite(values).any() else np.nan
        if len(y) < MIN_TRAIN_ROWS:
            continue
        coef = _ridge(x, y)
        bm[i] = coef[: LAGS + 1]
        if sectors[i] is not None:
            bs[i] = coef[LAGS + 1 :]
        fitted[i] = True
    # Residuals after the lag model, per session; the first LAGS minutes are not modelled.
    lag_resid = []
    for sr, (m_lags, s_now) in zip(aligned, designs, strict=True):
        s_lags = np.stack([_lagged(s_now, lag) for lag in range(LAGS + 1)], axis=1)  # transient, one session
        market_part, sector_part = lag_parts(bm, bs, m_lags, s_lags)
        del s_lags
        e = sr.r - market_part - sector_part
        e[:, :LAGS] = np.nan
        e[~fitted] = np.nan
        lag_resid.append(e)
    # Stage 2: statistical factors from what the lag model leaves, then each stock's exposure to them.
    resid = np.concatenate(lag_resid, axis=1) if lag_resid else np.zeros((n, 0))
    loadings = np.zeros((n, STAT_FACTORS))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        clipped = np.clip(resid, -CLIP, CLIP, out=resid)  # in place: resid is not used again
        sd = np.nanstd(clipped, axis=1)
        good = (np.isfinite(clipped).sum(axis=1) >= MIN_TRAIN_ROWS) & (sd > 0)
    if good.sum() > STAT_FACTORS * 4:
        z = np.nan_to_num(clipped[good], nan=0.0, copy=False)
        z /= sd[good, None]
        del clipped, resid
        cov = z @ z.T / max(z.shape[1], 1)
        _, vectors = np.linalg.eigh(cov)
        loadings[good] = vectors[:, ::-1][:, :STAT_FACTORS] * sd[good, None]  # loadings in return units
    realisations = [stat_realisations(e, loadings) for e in lag_resid]
    bp = np.zeros((n, STAT_FACTORS))
    resid_sd = np.full(n, np.nan)
    for i in np.flatnonzero(fitted):
        x = np.concatenate([p.T for p in realisations])
        y = np.concatenate([e[i] for e in lag_resid])
        ok = np.isfinite(y) & np.isfinite(x).all(axis=1)
        if ok.sum() < MIN_TRAIN_ROWS:
            continue
        bp[i] = _ridge(x[ok], y[ok]) if good[i] else 0.0
        resid_sd[i] = float(np.std(y[ok] - x[ok] @ bp[i], ddof=2 * (LAGS + 1) + STAT_FACTORS))
    return ResponseModel(tuple(sr.session_date for sr in sessions), tuple(keys), sectors, bm, bs, bp,
                         loadings, resid_sd, rows, turnover)  # fmt: skip


@dataclass
class ResponseState:
    """The model's view of every stock at one minute (arrays aligned with ``keys``)."""

    keys: tuple[str, ...]
    minute_index: int
    window: int
    expected: np.ndarray
    actual: np.ndarray
    gap: np.ndarray
    gap_sigma: np.ndarray
    market_part: np.ndarray
    sector_part: np.ndarray
    stat_part: np.ndarray
    pending: np.ndarray  # expected move over the next LAGS minutes from factor moves already observed
    pending_sigma: np.ndarray
    observed: np.ndarray  # minutes with a bar in the window
    stale: np.ndarray  # latest bar missing

    def of(self, key: str, model: ResponseModel) -> dict:
        i = self.keys.index(key)
        delay = model.delay_profile()

        def num(x):
            x = float(x)
            return round(x, 8) if np.isfinite(x) else None

        return {
            "window_minutes": self.window,
            "expected_response": num(self.expected[i]),
            "actual_response": num(self.actual[i]),
            "response_gap": num(self.gap[i]),
            "response_gap_sigma": num(self.gap_sigma[i]),
            "contributions": {
                "market": num(self.market_part[i]),
                "sector": num(self.sector_part[i]),
                "statistical": num(self.stat_part[i]),
                "residual": num(self.actual[i] - self.expected[i]),
            },
            "pending_response": num(self.pending[i]),
            "pending_response_sigma": num(self.pending_sigma[i]),
            "minutes_observed": int(self.observed[i]),
            "stale": bool(self.stale[i]),
            "model": {
                "market_beta_total": num(model.total_market_beta[i]),
                "market_beta_by_lag": [num(b) for b in model.beta_market[i]],
                "sector": model.sectors[i],
                "sector_beta_total": num(model.beta_sector[i].sum()),
                "delay_index": num(delay["delay_index"][i]),
                "half_life_minutes": num(delay["half_life"][i]),
                "mean_lag_minutes": num(delay["mean_lag"][i]),
                "residual_sd": num(model.resid_sd[i]),
                "training_minutes": int(model.train_rows[i]),
                "trained_on": [model.trained_on[0], model.trained_on[-1]] if model.trained_on else None,
            },
        }


def evaluate_state(model: ResponseModel, sr: SessionReturns, window: int = DEFAULT_WINDOW, upto: int | None = None):
    """The response state at minute ``upto`` (default: the last bar of ``sr``), using bars up to it only."""
    m_total = sr.r.shape[1] if upto is None else upto + 1
    r = sr.r[:, :m_total]
    market = sr.market[:m_total]
    trimmed = SessionReturns(sr.session_date, sr.keys, sr.grid[:m_total], r, sr.volume[:, :m_total],
                             sr.close[:, :m_total], market, sr.market_source)  # fmt: skip
    groups = _sector_groups(sr.keys)
    m_lags, s_lags = _design(trimmed, groups)
    n = len(sr.keys)
    market_part, sector_part = lag_parts(model.beta_market, model.beta_sector, m_lags, s_lags)
    p = stat_realisations(r - market_part - sector_part, model.loadings)
    stat_part = model.beta_stat @ p if p.shape[0] else np.zeros_like(market_part)
    expected_minute = market_part + sector_part + stat_part
    lo = max(0, m_total - window)
    span = slice(lo, m_total)
    valid = np.isfinite(r[:, span])
    actual = np.where(valid, r[:, span], 0.0).sum(axis=1)
    expected = np.where(valid, expected_minute[:, span], 0.0).sum(axis=1)
    observed = valid.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        gap = np.where(observed > 0, expected - actual, np.nan)
        gap_sigma = gap / (model.resid_sd * np.sqrt(np.maximum(observed, 1)))
    # Pending: the lagged terms that will apply to the next LAGS minutes, from factor values already seen.
    pending = np.zeros(n)
    s_now = sector_factor(r, market, groups) if m_total else np.zeros((n, 0))
    for h in range(1, LAGS + 1):
        for lag in range(h, LAGS + 1):
            src = m_total - 1 - (lag - h)
            if 0 <= src < m_total:
                pending += model.beta_market[:, lag] * market[src]
                pending += model.beta_sector[:, lag] * np.nan_to_num(s_now[:, src])
    with np.errstate(invalid="ignore", divide="ignore"):
        pending_sigma = pending / (model.resid_sd * np.sqrt(LAGS))
    stale = ~np.isfinite(r[:, -1]) if m_total else np.ones(n, dtype=bool)
    return ResponseState(sr.keys, m_total - 1, window, expected, actual, gap, gap_sigma,
                         np.where(valid, market_part[:, span], 0).sum(axis=1),
                         np.where(valid, sector_part[:, span], 0).sum(axis=1),
                         np.where(valid, stat_part[:, span], 0).sum(axis=1), pending, pending_sigma, observed, stale)  # fmt: skip


# --- training cache ---------------------------------------------------------------------------------


def _cache_path(cache_root: Path, session_date: str, train: int) -> Path:
    return cache_root / "response" / f"{session_date}.t{train}.v{MODEL_VERSION}.npz"


def model_for(archive_root: Path, session_date: str, keys: tuple[str, ...] | None = None, train_sessions: int = 20,
              cache_root: Path | None = None, min_sessions: int = 5) -> ResponseModel | None:  # fmt: skip
    """The model for a session, trained on the ``train_sessions`` sessions before it (cached).

    ``keys`` defaults to every stock seen in the training sessions: the live day's own key set grows
    through the morning as stocks trade, so the live service aligns each minute to the model's keys.
    """
    cache_root = Path(cache_root or store_dir())
    path = _cache_path(cache_root, session_date, train_sessions)
    if path.exists():
        data = np.load(path, allow_pickle=False)
        meta = json.loads(str(data["meta"]))
        if keys is None or tuple(meta["keys"]) == tuple(keys):
            return ResponseModel(tuple(meta["trained_on"]), tuple(meta["keys"]), tuple(meta["sectors"]),
                                 data["bm"], data["bs"], data["bp"], data["loadings"], data["resid_sd"],
                                 data["rows"], data["turnover"])  # fmt: skip
    earlier = [d for d in session_days(archive_root) if d < session_date][-train_sessions:]
    if len(earlier) < min_sessions:
        return None
    sessions = [compact(session_returns(load_day(archive_root, d))) for d in earlier]
    if keys is None:
        keys = tuple(sorted({k for sr in sessions for k in sr.keys}))
    model = fit(sessions, tuple(keys))
    del sessions
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".{os.getpid()}.tmp.npz")
    meta = {"trained_on": list(model.trained_on), "keys": list(model.keys), "sectors": list(model.sectors)}
    np.savez(tmp, meta=json.dumps(meta), bm=model.beta_market, bs=model.beta_sector, bp=model.beta_stat,
             loadings=model.loadings, resid_sd=model.resid_sd, rows=model.train_rows, turnover=model.median_turnover)  # fmt: skip
    os.replace(tmp, path)
    return model
