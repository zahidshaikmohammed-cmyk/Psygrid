"""The 989-stock state matrix: one standardised, machine-readable state vector per stock at ``as_of``.

``build_matrix(frame, ...)`` turns a ``MarketFrame`` (bars completed by
``as_of`` only) plus optional engine outputs into a ``StateMatrix``: one float
column per state variable, one row per stock, in a fixed order (``COLUMNS``).
A value that cannot be computed honestly is NaN; missingness is explicit and
counted per column and per stock. Nothing here reads a bar that had not
closed by ``as_of``; history comes only from sessions strictly before the
day (``MatrixHistory``).

Groups: price, momentum, volume, structure, relative, volatility, liquidity,
derivatives (market-level context: stock option chains are not recorded),
market context, historical context, data quality. ``COLUMNS`` documents each
column's group, unit and definition; ``/v2/945`` and the selector use these
names, so a column is never renamed without a ``MATRIX_VERSION`` bump.
"""

from __future__ import annotations

import hashlib
import json
import warnings
from dataclasses import dataclass, field

import numpy as np

from intelligence.features import log_returns
from intelligence.frame import MarketFrame
from intelligence.universe import BROAD_INDEX, MARKET_INDEX, sector_of

MATRIX_VERSION = "2"
OPENING_RANGE_MINUTES = 15
STALE_AFTER_MINUTES = 3

# name: (group, unit, definition)
COLUMNS: dict[str, tuple[str, str, str]] = {
    # price
    "ltp": ("price", "INR", "close of the latest completed bar"),
    "prev_close": ("price", "INR", "previous session close (archive reference)"),
    "open": ("price", "INR", "today's open: reference, else the first bar's open"),
    "gap_pct": ("price", "%", "open / previous close - 1"),
    "ret_open_pct": ("price", "%", "ltp / open - 1"),
    "ret_prev_close_pct": ("price", "%", "ltp / previous close - 1"),
    "day_high": ("price", "INR", "highest high so far"),
    "day_low": ("price", "INR", "lowest low so far"),
    "range_pct": ("price", "%", "(day high - day low) / open"),
    # momentum
    "ret_1m": ("momentum", "log", "latest bar's close over the previous close"),
    "ret_5m": ("momentum", "log", "close now over close 5 bars ago"),
    "ret_10m": ("momentum", "log", "close now over close 10 bars ago"),
    "ret_15m": ("momentum", "log", "close now over close 15 bars ago"),
    "ret_30m": ("momentum", "log", "close now over close 30 bars ago (NaN before 30 bars)"),
    "accel_5m": ("momentum", "log", "ret_5m minus the 5m return that ended 5 bars ago"),
    "persistence_10m": ("momentum", "fraction", "share of the last 10 1m returns with the sign of ret_10m"),
    # volume
    "volume_session": ("volume", "shares", "cumulative volume today"),
    "turnover_session_cr": ("volume", "INR crore", "cumulative traded value today (close x volume)"),
    "rel_volume": ("volume", "ratio", "volume so far / median volume by the same minute in earlier sessions"),
    "volume_accel": ("volume", "ratio", "volume of the last 5 bars / the 5 bars before"),
    "pv_corr_15m": ("volume", "correlation", "correlation of |1m return| and log volume over the last 15 bars"),
    # structure
    "or_high": ("structure", "INR", "opening-range high (first 15 bars; NaN until they close)"),
    "or_low": ("structure", "INR", "opening-range low"),
    "or_position": ("structure", "fraction", "(ltp - OR low) / (OR high - OR low); <0 below, >1 above"),
    "breakout_state": ("structure", "-1/0/+1", "+1 close above OR high, -1 below OR low, 0 inside"),
    "range_position": ("structure", "fraction", "(ltp - day low) / (day high - day low): 0 at the low, 1 at the high"),
    "range_vs_vol": ("structure", "ratio", "day range / (1m vol x sqrt(bars)): range expansion vs a random walk"),
    "trend_state": ("structure", "-1/0/+1", "+1 if ret_5m, ret_15m and ret_open all > 0; -1 if all < 0"),
    # relative
    "rel_market_open": ("relative", "log", "log return since open minus the market's (NIFTY 500, else median)"),
    "rel_sector_open": ("relative", "log", "log return since open minus the leave-one-out sector mean"),
    "rel_market_15m": ("relative", "log", "ret_15m minus the market's 15m return"),
    "response_gap": ("relative", "log", "expected minus actual response over 15 min (expected-response engine)"),
    "response_gap_sigma": ("relative", "sigma", "response gap in residual standard deviations"),
    "pending_response_sigma": ("relative", "sigma", "response still owed through lags, in sigma"),
    "pct_ret_open": ("relative", "percentile", "cross-sectional percentile of the return since open"),
    "pct_rel_volume": ("relative", "percentile", "cross-sectional percentile of relative volume"),
    # volatility
    "rvol_session": ("volatility", "log/min", "standard deviation of 1m returns today"),
    "rvol_15m": ("volatility", "log/min", "standard deviation of the last 15 1m returns"),
    "vol_ratio": ("volatility", "ratio", "mean |1m return| today / the same in earlier sessions by this minute"),
    "vol_expansion": ("volatility", "ratio", "rvol_15m / rvol_session"),
    # liquidity
    "activity": ("liquidity", "fraction", "share of today's minutes with a bar"),
    "illiquidity": ("liquidity", "|log| per INR cr", "mean |1m return| per crore traded today (Amihud)"),
    "spread_bps": ("liquidity", "bps", "mean bid-ask spread, last 15 min (Full packets; NaN if not recorded)"),
    "depth5": ("liquidity", "shares", "mean 5-level bid + ask quantity (Full packets)"),
    "imbalance1": ("liquidity", "fraction", "mean top-of-book imbalance (Full packets)"),
    "ofi_norm": ("liquidity", "ratio", "order-flow imbalance over mean top depth (only if cadence supports it)"),
    "signed_flow": ("liquidity", "fraction", "(buy - sell) / volume (only if cadence supports it)"),
    # derivatives (market level)
    "nifty_atm_iv": ("derivatives", "fraction", "NIFTY ATM implied volatility (latest chain snapshot)"),
    "nifty_budget_ratio": ("derivatives", "ratio", "NIFTY realised variance budget used vs the time-of-day norm"),
    # market context
    "market_ret_open": ("market", "log", "the market's return since open"),
    "breadth": ("market", "fraction", "share of stocks up since open"),
    "dispersion": ("market", "log", "cross-sectional standard deviation of returns since open"),
    "sector_ret_open": ("market", "log", "the stock's sector mean return since open (NaN without a sector)"),
    "sector_breadth": ("market", "fraction", "share of the stock's sector up since open"),
    "market_state_code": ("market", "code", "0 NORMAL, 1 TRANSITION, 2 STRESSED, 3 DISLOCATED, NaN uncalibrated"),
    # historical context
    "history_sessions": ("history", "count", "earlier sessions behind the volume and volatility norms"),
    # data quality
    "bars_present": ("quality", "count", "bars recorded today"),
    "completeness": ("quality", "fraction", "bars recorded / bars elapsed"),
    "last_bar_age_min": ("quality", "minutes", "minutes since the stock's latest bar closed"),
    "stale": ("quality", "0/1", "1 if no bar closed in the last 3 minutes"),
    "frozen": ("quality", "0/1", "1 if every bar of the last 10 has high == low (no price discovery)"),
}
MARKET_STATE_CODES = {"NORMAL": 0, "TRANSITION": 1, "STRESSED": 2, "DISLOCATED": 3}


@dataclass(frozen=True)
class MatrixHistory:
    """Per-stock norms from earlier sessions only: cumulative volume and mean |1m return| by minute of day."""

    keys: tuple[str, ...]
    sessions: tuple[str, ...]
    cum_volume: np.ndarray  # (n, minutes) median cumulative volume by the end of each minute
    mean_abs_ret: np.ndarray  # (n, minutes) median of the running mean |1m return|

    def at(self, keys: tuple[str, ...], bars: int) -> tuple[np.ndarray, np.ndarray]:
        index = {k: i for i, k in enumerate(self.keys)}
        rows = np.array([index.get(k, -1) for k in keys])
        out_v, out_r = np.full(len(keys), np.nan), np.full(len(keys), np.nan)
        if bars <= 0 or not self.cum_volume.size or bars > self.cum_volume.shape[1]:
            return out_v, out_r
        ok = rows >= 0
        out_v[ok] = self.cum_volume[rows[ok], bars - 1]
        out_r[ok] = self.mean_abs_ret[rows[ok], bars - 1]
        return out_v, out_r


def history_from_summaries(summaries) -> MatrixHistory:
    """Norms from cached day summaries (``intelligence.history.load_summary``) of earlier sessions."""
    if not summaries:
        return MatrixHistory((), (), np.zeros((0, 0)), np.zeros((0, 0)))
    keys = tuple(sorted({k for s in summaries for k in s.keys}))
    minutes = max(s.instrument["log_volume_1m"].shape[1] for s in summaries)
    index = {k: i for i, k in enumerate(keys)}
    vol = np.full((len(summaries), len(keys), minutes), np.nan, dtype=np.float32)
    ret = np.full_like(vol, np.nan)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for s_i, s in enumerate(summaries):
            rows = [index[k] for k in s.keys]
            v = np.expm1(s.instrument["log_volume_1m"].astype(np.float64))
            m = v.shape[1]
            vol[s_i, rows, :m] = np.cumsum(np.nan_to_num(v), axis=1)
            a = s.instrument["abs_ret_1m"].astype(np.float64)
            counts = np.cumsum(np.isfinite(a), axis=1)
            ret[s_i, rows, :m] = np.cumsum(np.nan_to_num(a), axis=1) / np.where(counts > 0, counts, np.nan)
        return MatrixHistory(keys, tuple(s.session_date for s in summaries),
                             np.nanmedian(vol, axis=0), np.nanmedian(ret, axis=0))  # fmt: skip


@dataclass
class StateMatrix:
    session_date: str
    as_of: str
    keys: tuple[str, ...]
    sectors: tuple[str | None, ...]
    values: dict[str, np.ndarray]  # column -> (n,)
    context: dict = field(default_factory=dict)  # market-level values and their sources
    version: str = MATRIX_VERSION
    input_hash: str = ""

    def column(self, name: str) -> np.ndarray:
        return self.values[name]

    def row(self, key: str) -> dict:
        i = self.keys.index(key)
        return {name: _num(self.values[name][i]) for name in COLUMNS}

    def missingness(self) -> dict[str, float]:
        return {name: round(float(np.mean(~np.isfinite(v))), 4) for name, v in self.values.items()}

    def array(self, names: list[str] | tuple[str, ...]) -> np.ndarray:
        return np.column_stack([self.values[n] for n in names]) if names else np.zeros((len(self.keys), 0))


def _num(x):
    x = float(x)
    return round(x, 8) if np.isfinite(x) else None


def average_ranks(x: np.ndarray) -> np.ndarray:
    """1-based ranks of a finite array; tied values share the mean of their ranks."""
    order = np.argsort(x, kind="mergesort")
    sorted_x = x[order]
    ranks = np.empty(len(x))
    ranks[order] = np.arange(1, len(x) + 1, dtype=float)
    if len(x):
        starts = np.flatnonzero(np.r_[True, sorted_x[1:] != sorted_x[:-1]])
        ends = np.r_[starts[1:], len(x)]
        for a, b in zip(starts, ends, strict=True):
            if b - a > 1:
                ranks[order[a:b]] = (a + 1 + b) / 2
    return ranks


def _pct_rank(x: np.ndarray) -> np.ndarray:
    """Cross-sectional percentile in (0, 1); NaN stays NaN; ties share their mean rank."""
    out = np.full(x.shape, np.nan)
    ok = np.isfinite(x)
    if ok.sum():
        out[ok] = (average_ranks(x[ok]) - 0.5) / ok.sum()
    return out


def _ret(close: np.ndarray, k: int) -> np.ndarray:
    """log(close[-1] / close[-1-k]) using the last *recorded* closes at those columns (NaN if either is missing)."""
    if close.shape[1] <= k:
        return np.full(close.shape[0], np.nan)
    return np.log(close[:, -1] / close[:, -1 - k])


def frame_hash(frame: MarketFrame) -> str:
    """SHA-256 of everything a decision at ``as_of`` may depend on: bars on the grid and the reference."""
    digest = hashlib.sha256()
    digest.update(frame.session_date.encode())
    digest.update(str(frame.as_of_epoch).encode())
    for bars in (frame.equity, frame.indices):
        if bars is None:
            continue
        digest.update("|".join(bars.keys).encode())
        digest.update(np.ascontiguousarray(bars.minutes).tobytes())
        for name in ("open", "high", "low", "close", "volume"):
            digest.update(np.ascontiguousarray(np.nan_to_num(bars.field(name), nan=-1.0)).tobytes())
    digest.update(json.dumps(frame.reference, sort_keys=True, default=str).encode())
    return digest.hexdigest()


def build_matrix(frame: MarketFrame, history: MatrixHistory | None = None, response=None, market_state: dict | None = None,
                 micro: dict | None = None, expectation: dict | None = None) -> StateMatrix:  # fmt: skip
    """The state matrix at ``frame.as_of``. Optional inputs add their columns; absent ones stay NaN.

    ``response`` is a ``ResponseState`` aligned to any key order (matched by key), ``market_state`` a
    market-state view, ``micro`` a ``{symbol: MicroState.view()}`` mapping, ``expectation`` the NIFTY
    expectation view.
    """
    bars = frame.equity
    keys, (n, m) = bars.keys, bars.shape
    sectors = tuple(sector_of(k) for k in keys)
    v: dict[str, np.ndarray] = {name: np.full(n, np.nan) for name in COLUMNS}
    context: dict = {"bars_elapsed": m}
    with warnings.catch_warnings(), np.errstate(divide="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        close, high, low, volume, open_ = bars.close, bars.high, bars.low, bars.volume, bars.open
        present = np.isfinite(close)
        v["bars_present"] = present.sum(axis=1).astype(float)
        v["completeness"] = v["bars_present"] / m if m else np.zeros(n)
        if m:
            last_col = np.where(present.any(axis=1), m - 1 - np.argmax(present[:, ::-1], axis=1), -1)
            v["last_bar_age_min"] = np.where(last_col >= 0, m - 1 - last_col, np.nan).astype(float)
            v["stale"] = np.where(np.isfinite(v["last_bar_age_min"]), v["last_bar_age_min"] >= STALE_AFTER_MINUTES, 1.0).astype(float)  # fmt: skip
        else:
            v["stale"] = np.ones(n)
        # Carry the last recorded close forward for "price now" (a missing bar means no trade, not no price).
        filled = close.copy()
        if m:
            idx = np.where(present, np.arange(m)[None, :], 0)
            np.maximum.accumulate(idx, axis=1, out=idx)
            filled = np.take_along_axis(close, idx, axis=1)
            filled[~np.maximum.accumulate(present, axis=1)] = np.nan
        ref_prev = np.array([(frame.reference.get(k) or {}).get("previous_close") or np.nan for k in keys], dtype=float)
        ref_open = np.array([(frame.reference.get(k) or {}).get("today_open") or np.nan for k in keys], dtype=float)
        first_col = np.argmax(present, axis=1)
        first_open = np.where(present.any(axis=1), open_[np.arange(n), first_col], np.nan) if m else np.full(n, np.nan)
        day_open = np.where(np.isfinite(ref_open), ref_open, first_open)
        ltp = filled[:, -1] if m else np.full(n, np.nan)
        v.update(ltp=ltp, prev_close=ref_prev, open=day_open)
        v["gap_pct"] = (day_open / ref_prev - 1) * 100
        v["ret_open_pct"] = (ltp / day_open - 1) * 100
        v["ret_prev_close_pct"] = (ltp / ref_prev - 1) * 100
        if m:
            v["day_high"], v["day_low"] = np.nanmax(high, axis=1), np.nanmin(low, axis=1)
            v["range_pct"] = (v["day_high"] - v["day_low"]) / day_open * 100
        r = log_returns(close, open_) if m else np.zeros((n, 0))
        for k, name in ((1, "ret_1m"), (5, "ret_5m"), (10, "ret_10m"), (15, "ret_15m"), (30, "ret_30m")):
            v[name] = _ret(filled, k) if k > 1 else (r[:, -1] if m else v[name])
        if m > 10:
            v["accel_5m"] = v["ret_5m"] - np.log(filled[:, -6] / filled[:, -11])
            last10 = r[:, -10:]
            sign = np.sign(v["ret_10m"])[:, None]
            valid = np.isfinite(last10) & (last10 != 0)
            v["persistence_10m"] = np.where(valid.sum(axis=1) >= 5, ((np.sign(last10) == sign) & valid).sum(axis=1) / np.maximum(valid.sum(axis=1), 1), np.nan)  # fmt: skip
        vol_sum = np.where(present.any(axis=1), np.nansum(volume, axis=1), np.nan)
        v["volume_session"] = vol_sum
        v["turnover_session_cr"] = np.nansum(volume * close, axis=1) / 1e7
        if m >= 10:
            recent, before = np.nansum(volume[:, -5:], axis=1), np.nansum(volume[:, -10:-5], axis=1)
            v["volume_accel"] = np.where(before > 0, recent / before, np.nan)
        if m >= 15:
            a, b = np.abs(r[:, -15:]), np.log1p(volume[:, -15:])
            ok = np.isfinite(a) & np.isfinite(b)
            enough = ok.sum(axis=1) >= 10
            am = np.where(ok, a, np.nan) - np.nanmean(np.where(ok, a, np.nan), axis=1, keepdims=True)
            bm = np.where(ok, b, np.nan) - np.nanmean(np.where(ok, b, np.nan), axis=1, keepdims=True)
            cov = np.nansum(am * bm, axis=1)
            den = np.sqrt(np.nansum(am**2, axis=1) * np.nansum(bm**2, axis=1))
            v["pv_corr_15m"] = np.where(enough & (den > 0), cov / den, np.nan)
        if m >= OPENING_RANGE_MINUTES:
            v["or_high"] = np.nanmax(high[:, :OPENING_RANGE_MINUTES], axis=1)
            v["or_low"] = np.nanmin(low[:, :OPENING_RANGE_MINUTES], axis=1)
            width = v["or_high"] - v["or_low"]
            v["or_position"] = np.where(width > 0, (ltp - v["or_low"]) / width, np.nan)
            v["breakout_state"] = np.where(np.isfinite(ltp) & np.isfinite(width),
                                           np.where(ltp > v["or_high"], 1.0, np.where(ltp < v["or_low"], -1.0, 0.0)), np.nan)  # fmt: skip
        span = v["day_high"] - v["day_low"]
        v["range_position"] = np.where(span > 0, (ltp - v["day_low"]) / span, np.nan)
        counts = np.isfinite(r).sum(axis=1)
        v["rvol_session"] = np.where(counts >= 10, np.nanstd(r, axis=1, ddof=1), np.nan)
        if m:
            c15 = np.isfinite(r[:, -15:]).sum(axis=1)
            v["rvol_15m"] = np.where(c15 >= 10, np.nanstd(r[:, -15:], axis=1, ddof=1), np.nan)
        v["vol_expansion"] = v["rvol_15m"] / v["rvol_session"]
        v["range_vs_vol"] = (span / day_open) / (v["rvol_session"] * np.sqrt(max(m, 1)))
        ret_open_log = np.log(ltp / day_open)
        v["trend_state"] = np.where(np.isfinite(v["ret_5m"]) & np.isfinite(v["ret_15m"]) & np.isfinite(ret_open_log),
                                    np.where((v["ret_5m"] > 0) & (v["ret_15m"] > 0) & (ret_open_log > 0), 1.0,
                                             np.where((v["ret_5m"] < 0) & (v["ret_15m"] < 0) & (ret_open_log < 0), -1.0, 0.0)), np.nan)  # fmt: skip
        v["activity"] = v["completeness"]
        tcr = volume * close / 1e7
        v["illiquidity"] = np.where(
            counts >= 10, np.nanmean(np.abs(r) / np.where(tcr > 0, tcr, np.nan), axis=1), np.nan
        )
        if m >= 10:
            v["frozen"] = np.where((high[:, -10:] == low[:, -10:]).all(axis=1) & present[:, -10:].all(axis=1), 1.0, 0.0)
        else:
            v["frozen"] = np.zeros(n)

        # market and sector context
        market_open, market_15m, source = _market_returns(frame, ret_open_log, v["ret_15m"])
        context.update(market_source=source, market_ret_open=market_open, market_ret_15m=market_15m)
        v["market_ret_open"] = np.full(n, market_open)
        v["rel_market_open"] = ret_open_log - market_open
        v["rel_market_15m"] = v["ret_15m"] - market_15m
        up = np.isfinite(ret_open_log)
        v["breadth"] = np.full(n, float(np.mean(ret_open_log[up] > 0)) if up.any() else np.nan)
        v["dispersion"] = np.full(n, float(np.nanstd(ret_open_log, ddof=1)) if up.sum() > 2 else np.nan)
        sector_arr = np.array([s or "" for s in sectors])
        for sector in sorted({s for s in sectors if s}):
            members = (sector_arr == sector) & up
            if members.sum() < 3:
                continue
            total, count = ret_open_log[members].sum(), members.sum()
            idx = np.flatnonzero(sector_arr == sector)
            v["sector_ret_open"][idx] = total / count
            v["sector_breadth"][idx] = float(np.mean(ret_open_log[members] > 0))
            own = np.where(up[idx], ret_open_log[idx], 0.0)
            peers = count - up[idx]
            v["rel_sector_open"][idx] = np.where(up[idx] & (peers > 0), ret_open_log[idx] - (total - own) / np.maximum(peers, 1), np.nan)  # fmt: skip
        v["pct_ret_open"] = _pct_rank(ret_open_log)

        # history norms (earlier sessions only)
        if history is not None and m:
            norm_vol, norm_ret = history.at(keys, m)
            v["rel_volume"] = np.where(norm_vol > 0, vol_sum / norm_vol, np.nan)
            today_abs = np.nanmean(np.abs(r), axis=1)
            v["vol_ratio"] = np.where(norm_ret > 0, today_abs / norm_ret, np.nan)
            v["history_sessions"] = np.full(n, float(len(history.sessions)))
            context["history_sessions"] = len(history.sessions)
        v["pct_rel_volume"] = _pct_rank(v["rel_volume"])

    # engine outputs, matched by key
    if response is not None:
        index = {k: i for i, k in enumerate(response.keys)}
        rows = np.array([index.get(k, -1) for k in keys])
        ok = rows >= 0
        for col, attr in (("response_gap", "gap"), ("response_gap_sigma", "gap_sigma"),
                          ("pending_response_sigma", "pending_sigma")):  # fmt: skip
            values = getattr(response, attr)
            v[col][ok] = values[rows[ok]]
        context["response_model"] = True
    if micro:
        for i, key in enumerate(keys):
            state = micro.get(key)
            if not state:
                continue
            for col, name in (("spread_bps", "spread_bps"), ("depth5", "depth5"), ("imbalance1", "imbalance1"),
                              ("ofi_norm", "ofi_norm"), ("signed_flow", "signed_flow")):  # fmt: skip
                value = state.get(name)
                v[col][i] = np.nan if value is None else float(value)
    if expectation:
        for col, name in (("nifty_atm_iv", "atm_iv"), ("nifty_budget_ratio", "budget_ratio")):
            value = expectation.get(name)
            v[col] = np.full(n, np.nan if value is None else float(value))
    if market_state:
        label = market_state.get("state")
        v["market_state_code"] = np.full(n, float(MARKET_STATE_CODES[label]) if label in MARKET_STATE_CODES else np.nan)
        context["market_state"] = label
    for array in v.values():
        array[~np.isfinite(array)] = np.nan
    return StateMatrix(frame.session_date, frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST"), keys, sectors, v, context,
                       input_hash=frame_hash(frame))  # fmt: skip


def _market_returns(frame: MarketFrame, ret_open: np.ndarray, ret_15m: np.ndarray) -> tuple[float, float, str]:
    """The market's return since open and over 15 minutes: NIFTY 500, then NIFTY, then the universe median."""
    indices = frame.indices
    if indices is not None and indices.shape[1]:
        for key in (BROAD_INDEX, MARKET_INDEX):
            if key in indices.keys:
                i = indices.keys.index(key)
                closes = indices.close[i]
                ok = np.flatnonzero(np.isfinite(closes))
                if len(ok) and np.isfinite(indices.open[i, ok[0]]):
                    with np.errstate(divide="ignore", invalid="ignore"):
                        session = float(np.log(closes[ok[-1]] / indices.open[i, ok[0]]))
                        r15 = float(np.log(closes[-1] / closes[-16])) if len(closes) > 15 else float("nan")
                    if np.isfinite(session):
                        return session, r15, key
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return float(np.nanmedian(ret_open)), float(np.nanmedian(ret_15m)), "cross_sectional_median"
