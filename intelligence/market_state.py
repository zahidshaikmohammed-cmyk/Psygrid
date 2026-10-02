"""Market-state engine: how the whole universe is moving together, measured, not predicted.

At a moment ``t`` each metric uses only the 1m returns of the trailing
``window`` minutes that closed by ``t``:

- **mean correlation**: average pairwise correlation of the stocks with a
  bar in at least 80% of the window's minutes;
- **eigen concentration** (absorption ratio): the share of total variance in
  the leading ``TOP_EIGEN`` eigenvectors of that correlation matrix, and the
  leading eigenvalue's share alone;
- **effective dimension**: the participation ratio (sum l)^2 / sum l^2 of
  the eigenvalues: about 1 when everything moves as one, large when moves
  are independent;
- **dispersion**: cross-sectional standard deviation of the window's
  cumulative returns (bps); **breadth**: share of stocks up over the window
  and since the open;
- **move concentration**: how much of the turnover-weighted move comes from
  the 10 largest contributors. PSYGRID has no NSE index weights, so the
  weights are each stock's traded value in the window, a proxy;
- **change score**: the largest standardised change of mean correlation and
  effective dimension between the window and the window before it, against
  the same changes in earlier sessions at the same time of day.

Each metric is placed as a percentile among earlier qualified sessions at the
same time of day (+-30 minutes), and a descriptive regime label is derived
from the percentiles. Labels describe co-movement; they are not forecasts.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from itertools import pairwise
from pathlib import Path

import numpy as np

from intelligence.archive import load_day, session_days
from intelligence.frame import as_of_time
from intelligence.response import session_returns

MARKET_STATE_VERSION = "2"
WINDOW = 30
MIN_COVERAGE = 0.8
MIN_STOCKS = 20
TOP_EIGEN = 5
TOP_CONTRIBUTORS = 10
HISTORY_TIMES = tuple(f"{h:02d}:{m:02d}" for h in range(9, 16) for m in (0, 15, 30, 45) if "09:45" <= f"{h:02d}:{m:02d}" <= "15:15")  # fmt: skip
NEIGHBOURHOOD_MINUTES = 30
METRICS = ("mean_correlation", "absorption_ratio", "top_eigen_share", "effective_dimension", "dispersion_bps",
           "breadth_window", "breadth_session", "move_concentration", "market_vol_bps", "sector_sync",
           "change_score")  # fmt: skip
STATES = ("NORMAL", "TRANSITION", "STRESSED", "DISLOCATED")


@dataclass
class MarketState:
    session_date: str
    as_of: str
    window: int
    stocks: int
    mean_correlation: float | None
    absorption_ratio: float | None
    top_eigen_share: float | None
    effective_dimension: float | None
    dispersion_bps: float | None
    breadth_window: float | None
    breadth_session: float | None
    move_concentration: float | None
    corr_change: float | None
    dim_change: float | None
    market_vol_bps: float | None = None
    sector_sync: float | None = None
    change_score: float | None = None
    percentiles: dict | None = None
    regime: str | None = None
    state: str | None = None  # NORMAL / TRANSITION / STRESSED / DISLOCATED, or UNCALIBRATED
    persistence: float | None = None  # P(same state 15 minutes later) in earlier sessions
    notes: tuple = ("move_concentration uses traded value as the weight: NSE index weights are not available",)

    def view(self) -> dict:
        return {k: (round(v, 4) if isinstance(v, float) else v) for k, v in asdict(self).items()}


def _window_stats(r: np.ndarray, close: np.ndarray, volume: np.ndarray, end: int, window: int,
                  market: np.ndarray | None = None, groups: dict | None = None) -> dict:  # fmt: skip
    """Metrics for columns ``[end - window, end)``."""
    start = max(0, end - window)
    block = r[:, start:end]
    out = dict.fromkeys(("mean_correlation", "absorption_ratio", "top_eigen_share", "effective_dimension",
                         "dispersion_bps", "breadth_window", "breadth_session", "move_concentration",
                         "market_vol_bps", "sector_sync"))  # fmt: skip
    if market is not None and end - start >= 5:
        out["market_vol_bps"] = float(np.std(market[start:end], ddof=1) * 1e4)
    out["stocks"] = 0
    if block.shape[1] < max(5, window // 2):
        return out
    coverage = np.isfinite(block).mean(axis=1)
    keep = coverage >= MIN_COVERAGE
    if keep.sum() >= MIN_STOCKS:
        x = np.nan_to_num(block[keep], nan=0.0)
        x = x - x.mean(axis=1, keepdims=True)
        sd = x.std(axis=1)
        rows = np.flatnonzero(keep)[sd > 0]  # original row of each standardised series
        x = x[sd > 0] / sd[sd > 0, None]
        n = x.shape[0]
        out["stocks"] = int(n)
        if n >= MIN_STOCKS:
            # Eigenvalues of the n x n correlation matrix from the (window x window) Gram matrix: cheap for n=989.
            gram = x.T @ x / x.shape[1]
            eig = np.clip(np.linalg.eigvalsh(gram), 0, None)[::-1]
            total = float(eig.sum())  # = n
            if total > 0:
                out["absorption_ratio"] = float(eig[:TOP_EIGEN].sum() / total)
                out["top_eigen_share"] = float(eig[0] / total)
                out["effective_dimension"] = float(total**2 / (eig**2).sum())
                # mean off-diagonal correlation: (sum of all entries - n) / (n (n - 1))
                col = x.sum(axis=0)
                out["mean_correlation"] = float(((col @ col) / x.shape[1] - n) / (n * (n - 1)))
                if groups:  # within-sector mean correlation minus the all-pairs mean: sector synchronisation
                    position = {row: j for j, row in enumerate(rows)}
                    pairs, total = 0, 0.0
                    for members in groups.values():
                        idx = [position[i] for i in members if i in position]
                        k = len(idx)
                        if k >= 3:
                            g = x[idx].sum(axis=0)
                            total += (g @ g) / x.shape[1] - k
                            pairs += k * (k - 1)
                    if pairs:
                        out["sector_sync"] = float(total / pairs - out["mean_correlation"])
    cum = np.nansum(block, axis=1)
    has = np.isfinite(block).any(axis=1)
    if has.sum() >= MIN_STOCKS:
        out["dispersion_bps"] = float(np.std(cum[has]) * 1e4)
        out["breadth_window"] = float((cum[has] > 0).mean())
        session = np.nansum(r[:, :end], axis=1)
        live = np.isfinite(r[:, :end]).any(axis=1)
        out["breadth_session"] = float((session[live] > 0).mean())
        value = np.nansum(np.where(np.isfinite(block), np.nan_to_num(close[:, start:end]) * np.nan_to_num(volume[:, start:end]), 0), axis=1)  # fmt: skip
        contribution = np.abs(value[has] * cum[has])
        if contribution.sum() > 0:
            out["move_concentration"] = float(np.sort(contribution)[::-1][:TOP_CONTRIBUTORS].sum() / contribution.sum())
    return out


def measure(day, as_of, window: int = WINDOW) -> MarketState:
    """The market state of ``day`` at ``as_of`` from bars completed by then."""
    from intelligence.response import _sector_groups

    sr = session_returns(day, as_of)
    end = sr.r.shape[1]
    groups = _sector_groups(sr.keys)
    now = _window_stats(sr.r, sr.close, sr.volume, end, window, sr.market, groups)
    before = (
        _window_stats(sr.r, sr.close, sr.volume, end - window, window, sr.market, groups) if end >= 2 * window else {}
    )

    def diff(name):
        a, b = now.get(name), before.get(name)
        return None if a is None or b is None else a - b

    return MarketState(
        session_date=sr.session_date, as_of=as_of.strftime("%Y-%m-%d %H:%M:%S IST"), window=window,
        stocks=now["stocks"], **{k: now[k] for k in now if k != "stocks"},
        corr_change=diff("mean_correlation"), dim_change=diff("effective_dimension"),
    )  # fmt: skip


def classify_state(p: dict) -> str:
    """The market state from percentiles against the same time of day in earlier sessions.

    Pre-registered rule (percentiles, 0-1):
    - STRESSED: stocks moving together (mean correlation >= 0.8) and far (dispersion or market volatility >= 0.8);
    - DISLOCATED: stocks moving far (dispersion >= 0.9) but apart (mean correlation <= 0.5): the usual
      co-movement has broken down;
    - TRANSITION: the correlation structure is changing fast (change score >= 0.9);
    - NORMAL: otherwise.
    UNCALIBRATED without enough history.
    """
    corr, disp, vol = p.get("mean_correlation"), p.get("dispersion_bps"), p.get("market_vol_bps")
    if corr is None or disp is None:
        return "UNCALIBRATED"
    if corr >= 0.8 and (disp >= 0.8 or (vol or 0) >= 0.8):
        return "STRESSED"
    if disp >= 0.9 and corr <= 0.5:
        return "DISLOCATED"
    if (p.get("change_score") or 0) >= 0.9:
        return "TRANSITION"
    return "NORMAL"


# --- history ---------------------------------------------------------------------------------------


def _history_path(store_root: Path, session_date: str) -> Path:
    return Path(store_root) / "market_state" / f"{session_date}.v{MARKET_STATE_VERSION}.json"


def session_profile(archive_root: Path, session_date: str, store_root: Path | None = None, day=None) -> list[dict]:
    """Market states of one finished session at ``HISTORY_TIMES`` (cached when ``store_root`` is given)."""
    path = _history_path(store_root, session_date) if store_root else None
    if path is not None and path.exists():
        try:
            return json.loads(path.read_text())
        except ValueError:
            pass
    day = day if day is not None else load_day(archive_root, session_date)
    rows = []
    for hhmm in HISTORY_TIMES:
        state = measure(day, as_of_time(session_date, hhmm))
        rows.append({"time": hhmm, **{k: getattr(state, k) for k in (*METRICS[:-1], "corr_change", "dim_change")}})
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows))
        tmp.replace(path)
    return rows


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _percentile(value, sample) -> float | None:
    sample = [s for s in sample if s is not None]
    if value is None or len(sample) < 5:
        return None
    sample = np.asarray(sample, dtype=float)
    return float(((sample < value).sum() + 0.5 * (sample == value).sum()) / len(sample))


def regime(p: dict) -> str:
    """A descriptive label from percentiles (None when history is too short)."""
    corr, disp, dim = p.get("mean_correlation"), p.get("dispersion_bps"), p.get("effective_dimension")
    if corr is None or disp is None:
        return "UNCALIBRATED"
    prefix = "SHIFTING_" if (p.get("change_score") or 0) >= 0.95 else ""
    if corr >= 0.8 and disp >= 0.8:
        label = "COUPLED_STRESS"  # everything moving together, and far
    elif corr >= 0.8 or (dim is not None and dim <= 0.2):
        label = "COUPLED"
    elif corr <= 0.2 and disp >= 0.8:
        label = "DISPERSED"  # stocks moving far, separately
    elif disp <= 0.2:
        label = "QUIET"
    else:
        label = "TYPICAL"
    return prefix + label


def with_history(state: MarketState, archive_root: Path, store_root: Path | None = None,
                 lookback: int = 60) -> MarketState:  # fmt: skip
    """Percentiles against earlier qualified sessions at the same time of day, change score and regime."""
    earlier = [d for d in session_days(archive_root) if d < state.session_date][-lookback:]
    at = _minutes(state.as_of[11:16])
    sample: dict[str, list] = {k: [] for k in (*METRICS, "corr_change", "dim_change")}
    profiles = []
    for session in earlier:
        try:
            rows = session_profile(archive_root, session, store_root)
        except FileNotFoundError:
            continue
        profiles.append(rows)
        for row in rows:
            if abs(_minutes(row["time"]) - at) <= NEIGHBOURHOOD_MINUTES:
                for k in sample:
                    if k in row:
                        sample[k].append(row[k])
    # change score: the larger standardised change of correlation and dimension, as a percentile of |change|
    scores = []
    for name in ("corr_change", "dim_change"):
        value = getattr(state, name)
        history = [abs(v) for v in sample[name] if v is not None]
        if value is not None and len(history) >= 5:
            scores.append(_percentile(abs(value), history))
    state.change_score = max(scores) if scores else None
    percentiles = {k: _percentile(getattr(state, k), sample[k]) for k in METRICS if k != "change_score"}
    percentiles["change_score"] = state.change_score
    percentiles["history_sessions"] = len(earlier)
    state.percentiles = percentiles
    state.regime = regime(percentiles)
    state.state = classify_state(percentiles)
    state.persistence = _persistence(profiles, sample, state.state)
    return state


def _persistence(profiles: list[list[dict]], sample: dict, current: str) -> float | None:
    """Share of earlier occurrences of ``current`` (at 15-minute steps) still in that state 15 minutes later.

    Earlier rows are classified against the pooled history sample (an in-sample convenience used only to
    describe how sticky a state has been; it never feeds a decision about the past)."""
    if current in (None, "UNCALIBRATED"):
        return None
    stay = total = 0
    for rows in profiles:
        labels = []
        for row in rows:
            p = {
                k: _percentile(row.get(k), sample.get(k, []))
                for k in ("mean_correlation", "dispersion_bps", "market_vol_bps")
            }
            labels.append(classify_state(p))
        for a, b in pairwise(labels):
            if a == current:
                total += 1
                stay += b == current
    return round(stay / total, 4) if total >= 5 else None
