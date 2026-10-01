"""Universe-wide features at one minute, computed from a ``MarketFrame``.

Every feature is in ``CATALOGUE`` with its unit, the minimum history it needs and
the question it answers; a feature without a purpose does not belong here. All
computation is vectorised over the whole universe. A value that cannot be
computed honestly (missing bars, too little history) is NaN, never guessed.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field

import numpy as np

from intelligence.frame import MarketFrame
from intelligence.universe import BROAD_INDEX, sector_index_of, sector_of

FEATURE_VERSION = "1"


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    unit: str
    min_bars: int
    purpose: str


CATALOGUE: dict[str, FeatureSpec] = {
    spec.name: spec
    for spec in (
        FeatureSpec("ret_1m", "log return", 1, "Size of the latest minute's move; base input for return shocks."),
        FeatureSpec("ret_5m", "log return", 6, "Short-horizon move that a single noisy minute cannot fake."),
        FeatureSpec("ret_15m", "log return", 16, "Move over a quarter hour; the window used for relative performance."),
        FeatureSpec("ret_session", "log return", 1, "Move since today's open: the day's direction and size."),
        FeatureSpec("ret_prev_close", "log return", 1, "Move since yesterday's close, including the opening gap."),
        FeatureSpec("gap", "log return", 0, "Overnight gap; separates opening repricing from intraday moves."),
        FeatureSpec(
            "range_1m",
            "fraction of price",
            1,
            "Latest bar's high-low range; intrabar volatility even when close-to-close is flat.",
        ),
        FeatureSpec(
            "rvol_15m",
            "log return std per minute",
            10,
            "Recent realised volatility; the scale against which a move is judged.",
        ),
        FeatureSpec(
            "rvol_session",
            "log return std per minute",
            10,
            "Volatility so far today; detects regime change within the day.",
        ),
        FeatureSpec("volume_1m", "shares", 1, "Latest minute's traded volume; base input for volume surges."),
        FeatureSpec("volume_session", "shares", 1, "Cumulative volume today."),
        FeatureSpec("turnover_1m", "INR", 1, "Value traded in the latest minute; comparable across share prices."),
        FeatureSpec(
            "activity_30m",
            "fraction of minutes",
            1,
            "Share of the last 30 minutes with any trade; a direct liquidity measure.",
        ),
        FeatureSpec(
            "illiquidity_15m",
            "abs log return per INR crore",
            10,
            "Amihud-style price impact: how far price moves per value traded.",
        ),
        FeatureSpec(
            "vwap_distance",
            "log ratio",
            1,
            "Price relative to today's volume-weighted average; where trading has concentrated.",
        ),
        FeatureSpec(
            "rel_market_15m",
            "log return",
            16,
            "15-minute move minus the universe median: idiosyncratic vs market-wide.",
        ),
        FeatureSpec("rel_market_session", "log return", 1, "Session move minus the universe median."),
        FeatureSpec(
            "rel_sector_session", "log return", 1, "Session move minus the sector median; NaN without a known sector."
        ),
        FeatureSpec("rel_index_session", "log return", 1, "Session move minus the broad index (NIFTY 500)."),
        FeatureSpec(
            "rel_sector_index_session", "log return", 1, "Session move minus the sector's own index, where one exists."
        ),
    )
}

MARKET_FEATURES = {
    "median_ret_1m": "Typical one-minute move across the universe.",
    "median_ret_session": "Typical move since the open.",
    "dispersion_15m": "Cross-sectional standard deviation of 15-minute returns: how differently stocks are moving.",
    "dispersion_session": "Cross-sectional standard deviation of session returns.",
    "breadth_session": "(advancers - decliners) / (advancers + decliners) on session returns.",
    "active_share_1m": "Share of instruments with a bar in the latest minute.",
    "median_rvol_15m": "Typical recent realised volatility.",
    "turnover_1m": "Total value traded in the latest minute.",
}


@dataclass(frozen=True)
class FeatureSet:
    """Features at the frame's latest completed minute."""

    as_of: str
    minute_index: int  # 0-based column of the latest bar; -1 before the first bar
    keys: tuple[str, ...]
    sectors: tuple[str | None, ...]
    values: dict[str, np.ndarray]  # feature name -> one value per instrument
    market: dict[str, float]
    sectors_median: dict[str, dict[str, float]]  # sector -> {ret_session, ret_15m, count}
    indices: dict[str, dict[str, float]]  # index key -> {ret_1m, ret_15m, ret_session}
    version: str = FEATURE_VERSION
    returns_1m: np.ndarray = field(default=None, repr=False)  # (n, m) matrix, kept for relationship work

    def get(self, name: str, key: str) -> float:
        return float(self.values[name][self.keys.index(key)])


def log_returns(close: np.ndarray, open_: np.ndarray) -> np.ndarray:
    """1m log returns: close over the previous minute's close; the first column uses its own open."""
    with np.errstate(divide="ignore", invalid="ignore"):
        prev = np.concatenate([open_[:, :1], close[:, :-1]], axis=1)
        return np.log(close / prev)


def _window_return(close: np.ndarray, minutes: int) -> np.ndarray:
    if close.shape[1] <= minutes:
        return np.full(close.shape[0], np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(close[:, -1] / close[:, -1 - minutes])


def _nanstd(window: np.ndarray, min_count: int) -> np.ndarray:
    counts = np.sum(~np.isnan(window), axis=1)
    out = np.nanstd(window, axis=1, ddof=1) if window.shape[1] > 1 else np.full(window.shape[0], np.nan)
    out[counts < min_count] = np.nan
    return out


def _session_open(frame: MarketFrame) -> np.ndarray:
    """Today's open per instrument: the archived reference, else the first bar's open."""
    bars = frame.equity
    first_open = np.full(len(bars.keys), np.nan)
    present = ~np.isnan(bars.open)
    has_any = present.any(axis=1)
    first_col = np.argmax(present, axis=1)
    first_open[has_any] = bars.open[has_any, first_col[has_any]]
    reference = np.array([(frame.reference.get(k) or {}).get("today_open") or np.nan for k in bars.keys], dtype=float)
    return np.where(np.isnan(reference), first_open, reference)


def _index_features(frame: MarketFrame) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    if frame.indices is None or frame.indices.shape[1] == 0:
        return out
    bars = frame.indices
    r = log_returns(bars.close, bars.open)
    first_open = bars.open[:, 0]
    with np.errstate(divide="ignore", invalid="ignore"):
        session = np.log(bars.close[:, -1] / first_open)
    r15 = _window_return(bars.close, 15)
    for i, key in enumerate(bars.keys):
        out[key] = {"ret_1m": float(r[i, -1]), "ret_15m": float(r15[i]), "ret_session": float(session[i])}
    return out


def compute_features(frame: MarketFrame) -> FeatureSet:
    bars = frame.equity
    n, m = bars.shape
    keys = bars.keys
    sectors = tuple(sector_of(k) for k in keys)
    as_of = frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST")
    nan = np.full(n, np.nan)
    if m == 0:
        return FeatureSet(
            as_of, -1, keys, sectors, {name: nan.copy() for name in CATALOGUE}, {}, {}, {}, returns_1m=np.empty((n, 0))
        )

    with warnings.catch_warnings(), np.errstate(divide="ignore", invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)  # all-NaN slices are expected and yield NaN
        close, high, low, volume = bars.close, bars.high, bars.low, bars.volume
        r = log_returns(close, bars.open)
        session_open = _session_open(frame)
        prev_close = np.array(
            [(frame.reference.get(k) or {}).get("previous_close") or np.nan for k in keys], dtype=float
        )
        last_close = close[:, -1]
        values: dict[str, np.ndarray] = {
            "ret_1m": r[:, -1],
            "ret_5m": _window_return(close, 5),
            "ret_15m": _window_return(close, 15),
            "ret_session": np.log(last_close / session_open),
            "ret_prev_close": np.log(last_close / prev_close),
            "gap": np.log(session_open / prev_close),
            "range_1m": (high[:, -1] - low[:, -1]) / last_close,
            "rvol_15m": _nanstd(r[:, -15:], 10),
            "rvol_session": _nanstd(r, 10),
            "volume_1m": volume[:, -1],
            "volume_session": np.where(np.isnan(volume).all(axis=1), np.nan, np.nansum(volume, axis=1)),
            "turnover_1m": volume[:, -1] * last_close,
            "activity_30m": np.mean(~np.isnan(close[:, -30:]), axis=1),
        }
        turnover_cr = volume[:, -15:] * close[:, -15:] / 1e7
        impact = np.abs(r[:, -15:]) / np.where(turnover_cr > 0, turnover_cr, np.nan)
        counts = np.sum(~np.isnan(impact), axis=1)
        values["illiquidity_15m"] = np.where(counts >= 10, np.nanmean(impact, axis=1), np.nan)
        typical = (high + low + close) / 3
        vwap = np.nansum(typical * volume, axis=1) / np.nansum(np.where(np.isnan(typical), np.nan, volume), axis=1)
        values["vwap_distance"] = np.log(last_close / vwap)

        market_ret_15m = np.nanmedian(values["ret_15m"])
        market_ret_session = np.nanmedian(values["ret_session"])
        values["rel_market_15m"] = values["ret_15m"] - market_ret_15m
        values["rel_market_session"] = values["ret_session"] - market_ret_session

        sectors_median: dict[str, dict[str, float]] = {}
        rel_sector = nan.copy()
        sector_arr = np.array([s or "" for s in sectors])
        for sector in sorted({s for s in sectors if s}):
            members = sector_arr == sector
            median_session = float(np.nanmedian(values["ret_session"][members]))
            sectors_median[sector] = {
                "ret_session": median_session,
                "ret_15m": float(np.nanmedian(values["ret_15m"][members])),
                "count": int(members.sum()),
            }
            rel_sector[members] = values["ret_session"][members] - median_session
        values["rel_sector_session"] = rel_sector

        indices = _index_features(frame)
        broad = indices.get(BROAD_INDEX, {}).get("ret_session", np.nan)
        values["rel_index_session"] = values["ret_session"] - broad
        sector_index_ret = np.array(
            [indices.get(sector_index_of(s) or "", {}).get("ret_session", np.nan) for s in sectors], dtype=float
        )
        values["rel_sector_index_session"] = values["ret_session"] - sector_index_ret

        session = values["ret_session"]
        up, down = int(np.sum(session > 0)), int(np.sum(session < 0))
        market = {
            "median_ret_1m": float(np.nanmedian(values["ret_1m"])),
            "median_ret_session": float(market_ret_session),
            "dispersion_15m": float(np.nanstd(values["ret_15m"], ddof=1)),
            "dispersion_session": float(np.nanstd(session, ddof=1)),
            "breadth_session": (up - down) / (up + down) if up + down else float("nan"),
            "active_share_1m": float(np.mean(~np.isnan(close[:, -1]))),
            "median_rvol_15m": float(np.nanmedian(values["rvol_15m"])),
            "turnover_1m": float(np.nansum(values["turnover_1m"])),
        }
    for array in values.values():
        array[~np.isfinite(array)] = np.nan  # inf from a zero price or volume is not a measurement
    return FeatureSet(as_of, m - 1, keys, sectors, values, market, sectors_median, indices, returns_1m=r)
