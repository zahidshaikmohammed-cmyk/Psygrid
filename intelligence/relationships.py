"""Relationships between instruments, and when an established one breaks.

A relationship is only judged once it has been *established* today: over an
estimation window the instrument's 1m returns must track its benchmark with a
correlation of at least ``MIN_CORRELATION``. The estimation window ends before
the recent window being judged, so a break cannot hide inside its own
baseline. A break is the cumulative residual over the recent window,
``sum(r - beta * b)``, measured in units of its expected spread
``residual_sd * sqrt(minutes)``.

Relationships covered:

- stock vs its sector (leave-one-out median of the other members) and vs its
  sector index and the broad index;
- sector vs sector (the 15-minute spread of median returns vs what earlier 1m spreads today imply);
- price vs volume (an unusual move without volume, or volume without a move);
- spot vs futures (basis) and option-chain measures (PCR, IV skew), from the
  recorded derivatives snapshots, each vs its own earlier values today.

Every result states the relationship measured, its strength and the size of
the change. None of them says what to trade.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass

import numpy as np

from intelligence.anomaly import EXTREME, INSUFFICIENT_DATA, NORMAL, UNUSUAL, classify_z
from intelligence.derivatives import DerivativesDay
from intelligence.features import FeatureSet, log_returns
from intelligence.frame import MarketFrame
from intelligence.universe import BROAD_INDEX, sector_index_of

ESTIMATION_MINUTES = 60
RECENT_MINUTES = 15
MIN_ESTIMATION_PAIRS = 40
MIN_RECENT_PAIRS = 10
MIN_CORRELATION = 0.3
MIN_SECTOR_MEMBERS = 3
MIN_SPREAD_HISTORY = 60  # earlier 1m spreads today needed before a sector pair is judged
NOT_ESTABLISHED = "NOT_ESTABLISHED"


@dataclass(frozen=True)
class Relationship:
    kind: str  # stock_sector, stock_index, stock_sector_index, sector_sector, price_volume, spot_futures, option_*
    subject: str
    counterpart: str
    classification: str  # NORMAL, UNUSUAL, EXTREME, NOT_ESTABLISHED, INSUFFICIENT_DATA
    z: float | None
    evidence: dict


def _num(value) -> float | None:
    value = float(value)
    return value if np.isfinite(value) else None


def _pairwise_fit(y: np.ndarray, x: np.ndarray) -> tuple[np.ndarray, ...]:
    """Row-wise beta, correlation, residual SD, pair count and the benchmark's sum of squares, ignoring NaN pairs."""
    valid = ~np.isnan(y) & ~np.isnan(x)
    count = valid.sum(axis=1)
    ym = np.where(valid, y, np.nan)
    xm = np.where(valid, x, np.nan)
    with warnings.catch_warnings(), np.errstate(invalid="ignore", divide="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        mx, my = np.nanmean(xm, axis=1), np.nanmean(ym, axis=1)
        dx, dy = xm - mx[:, None], ym - my[:, None]
        sxx, syy, sxy = np.nansum(dx * dx, axis=1), np.nansum(dy * dy, axis=1), np.nansum(dx * dy, axis=1)
        beta = sxy / sxx
        corr = sxy / np.sqrt(sxx * syy)
        resid = ym - beta[:, None] * xm
        resid_sd = np.nanstd(resid, axis=1, ddof=2)
    return beta, corr, resid_sd, count, sxx


def judge_against(r: np.ndarray, benchmark: np.ndarray) -> dict[str, np.ndarray]:
    """Fit on the estimation window, then measure the recent window's cumulative residual."""
    m = r.shape[1]
    est = slice(max(0, m - RECENT_MINUTES - ESTIMATION_MINUTES), max(0, m - RECENT_MINUTES))
    recent = slice(max(0, m - RECENT_MINUTES), m)
    beta, corr, resid_sd, pairs, sxx = _pairwise_fit(r[:, est], benchmark[:, est])
    with warnings.catch_warnings(), np.errstate(invalid="ignore"):
        warnings.simplefilter("ignore", RuntimeWarning)
        resid = r[:, recent] - beta[:, None] * benchmark[:, recent]
        recent_pairs = np.sum(~np.isnan(resid), axis=1)
        deviation = np.nansum(resid, axis=1)
        stock_move = np.nansum(r[:, recent], axis=1)
        bench_move = np.nansum(benchmark[:, recent], axis=1)
        # Prediction variance of the cumulative residual: the noise over n minutes plus the error in the
        # fitted beta carried by the benchmark's move (var(beta_hat) = resid_sd^2 / sxx).
        z = deviation / (resid_sd * np.sqrt(recent_pairs + bench_move**2 / sxx))
    enough = (pairs >= MIN_ESTIMATION_PAIRS) & (recent_pairs >= MIN_RECENT_PAIRS) & np.isfinite(z)
    established = enough & (corr >= MIN_CORRELATION)
    classification = classify_z(z).astype(object)
    classification[~established] = NOT_ESTABLISHED
    classification[~enough] = INSUFFICIENT_DATA
    return {
        "beta": beta, "correlation": corr, "residual_sd": resid_sd, "estimation_pairs": pairs,
        "recent_pairs": recent_pairs, "deviation": deviation, "z": np.where(established, z, np.nan),
        "subject_move": stock_move, "benchmark_move": bench_move, "classification": classification,
    }  # fmt: skip


def _leave_one_out_sector(r: np.ndarray, sectors: tuple) -> tuple[np.ndarray, list[str]]:
    """Each stock's benchmark: the per-minute median return of the other members of its sector."""
    bench = np.full_like(r, np.nan)
    names = [""] * len(sectors)
    members: dict[str, list[int]] = {}
    for i, s in enumerate(sectors):
        if s:
            members.setdefault(s, []).append(i)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for sector, idx in members.items():
            if len(idx) < MIN_SECTOR_MEMBERS:
                continue
            block = r[idx]
            for pos, i in enumerate(idx):
                bench[i] = np.nanmedian(np.delete(block, pos, axis=0), axis=0)
                names[i] = sector
    return bench, names


def _index_returns(frame: MarketFrame) -> dict[str, np.ndarray]:
    if frame.indices is None:
        return {}
    r = log_returns(frame.indices.close, frame.indices.open)
    return {key: r[i] for i, key in enumerate(frame.indices.keys)}


def _rows(kind: str, keys, counterparts, result, only_flagged: bool) -> list[Relationship]:
    out = []
    for i, key in enumerate(keys):
        if not counterparts[i]:
            continue
        cls = str(result["classification"][i])
        if only_flagged and cls not in (UNUSUAL, EXTREME):
            continue
        out.append(
            Relationship(
                kind=kind,
                subject=key,
                counterpart=counterparts[i],
                classification=cls,
                z=_num(result["z"][i]),
                evidence={
                    "beta": _num(result["beta"][i]),
                    "correlation": _num(result["correlation"][i]),
                    "residual_sd_per_minute": _num(result["residual_sd"][i]),
                    "estimation_minutes": int(result["estimation_pairs"][i]),
                    "recent_minutes": int(result["recent_pairs"][i]),
                    "cumulative_residual": _num(result["deviation"][i]),
                    "subject_return": _num(result["subject_move"][i]),
                    "counterpart_return": _num(result["benchmark_move"][i]),
                },
            )
        )
    return out


def stock_relationships(frame: MarketFrame, features: FeatureSet, only_flagged: bool = True) -> list[Relationship]:
    r = features.returns_1m
    if r is None or r.shape[1] == 0:
        return []
    out = []
    sector_bench, sector_names = _leave_one_out_sector(r, features.sectors)
    out += _rows("stock_sector", features.keys, sector_names, judge_against(r, sector_bench), only_flagged)
    indices = _index_returns(frame)
    if BROAD_INDEX in indices:
        bench = np.tile(indices[BROAD_INDEX], (len(features.keys), 1))
        out += _rows(
            "stock_index", features.keys, [BROAD_INDEX] * len(features.keys), judge_against(r, bench), only_flagged
        )
    sector_index = [sector_index_of(s) for s in features.sectors]
    if any(k in indices for k in sector_index if k):
        bench = np.array([indices.get(k or "", np.full(r.shape[1], np.nan)) for k in sector_index])
        names = [k if k in indices else "" for k in sector_index]
        out += _rows("stock_sector_index", features.keys, names, judge_against(r, bench), only_flagged)
    return out


def sector_relationships(features: FeatureSet, only_flagged: bool = True) -> list[Relationship]:
    """Each pair of sectors' 15-minute spread of median returns, against the 1m spreads earlier today."""
    r = features.returns_1m
    if r is None or r.shape[1] < 2 * RECENT_MINUTES:
        return []
    sectors = sorted({s for s in features.sectors if s})
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        series = {
            s: np.nanmedian(r[[i for i, x in enumerate(features.sectors) if x == s]], axis=0)
            for s in sectors
            if sum(x == s for x in features.sectors) >= MIN_SECTOR_MEMBERS
        }
    names = sorted(series)
    out = []
    for a_pos, a in enumerate(names):
        for b in names[a_pos + 1 :]:
            spread = series[a] - series[b]  # 1m spread of the two sectors' median returns
            history, recent = spread[:-RECENT_MINUTES], spread[-RECENT_MINUTES:]
            history = history[np.isfinite(history)]
            recent = recent[np.isfinite(recent)]
            if len(history) < MIN_SPREAD_HISTORY or len(recent) < MIN_RECENT_PAIRS:
                continue
            # The 15-minute spread against what independent 1m spreads like today's earlier ones would give:
            # mean 15 * mu, SD sigma * sqrt(15). Mean and SD rather than median and MAD: the 1m spread is close to
            # normal, and the SD's lower estimation noise keeps the null flag rate near nominal (an outlier only
            # widens sigma, making the test more conservative).
            mu = float(np.mean(history))
            sd = float(np.std(history, ddof=1))
            latest = float(np.sum(recent))
            expected = mu * len(recent)
            z = (latest - expected) / (sd * np.sqrt(len(recent))) if sd > 0 else float("nan")
            corr = (
                float(np.corrcoef(series[a], series[b])[0, 1])
                if np.nanstd(series[a]) and np.nanstd(series[b])
                else float("nan")
            )
            cls = str(classify_z(np.array([z]))[0]) if np.isfinite(z) else INSUFFICIENT_DATA
            if only_flagged and cls not in (UNUSUAL, EXTREME):
                continue
            evidence = {
                "spread_15m": _num(latest), "expected_spread_15m": _num(expected),
                "spread_1m_mean": _num(mu), "spread_1m_sd": _num(sd),
                "history_minutes": len(history), "session_correlation": _num(corr),
            }  # fmt: skip
            out.append(Relationship("sector_sector", a, b, cls, _num(z), evidence))
    return out


def price_volume_relationships(report) -> list[Relationship]:
    """Moves and volume that disagree: an unusual move on ordinary volume, or unusual volume without a move."""
    volume, ret = report.measures["volume"], report.measures["return"]
    out = []
    for i, key in enumerate(report.keys):
        vz, rz = volume.z[i], ret.z[i]
        if not (np.isfinite(vz) and np.isfinite(rz)):
            continue
        if abs(vz) < 1 and abs(rz) >= 3:
            pattern, z = "move_without_volume", rz
        elif vz >= 3 and abs(rz) < 1:
            pattern, z = "volume_without_move", vz
        else:
            continue
        evidence = {
            "volume_z": _num(vz), "return_z": _num(rz),
            "volume_baseline": str(volume.baseline[i]), "return_baseline": str(ret.baseline[i]),
        }  # fmt: skip
        out.append(Relationship("price_volume", key, pattern, str(classify_z(np.array([z]))[0]), _num(z), evidence))
    return out


def _series_z(points: list[tuple[int, float]], min_history: int = 20) -> tuple[float, dict]:
    """Latest value vs the robust centre and spread of the earlier values today."""
    if len(points) < min_history + 1:
        return float("nan"), {"history_points": max(0, len(points) - 1)}
    values = np.array([v for _, v in points])
    history, latest = values[:-1], values[-1]
    median = float(np.median(history))
    scale = float(np.median(np.abs(history - median)) * 1.4826)
    z = (latest - median) / scale if scale > 0 else float("nan")
    return z, {"latest": _num(latest), "median": _num(median), "scale": _num(scale), "history_points": len(history)}


def derivatives_relationships(
    derivatives: DerivativesDay | None, as_of_epoch: int, only_flagged: bool = True
) -> list[Relationship]:
    """Spot-futures basis, futures spread and option-chain measures, each vs its own earlier values today."""
    if derivatives is None:
        return []
    known = derivatives.known_at(as_of_epoch)
    if not known:
        return []
    out = []
    keys = sorted(
        {k for s in known for k in (s.get("futures") or {})} | {k for s in known for k in (s.get("options") or {})}
    )
    for key in keys:
        basis, spread = [], []
        for snap in known:
            fut = (snap.get("futures") or {}).get(key) or {}
            spot = (snap.get("spot") or {}).get(key)
            ltp, bid, ask = fut.get("last_price"), fut.get("top_bid_price"), fut.get("top_ask_price")
            if ltp and spot:
                basis.append((snap["minute"], (ltp - spot) / spot))
            if bid and ask and ask >= bid:
                spread.append((snap["minute"], (ask - bid) / ((ask + bid) / 2)))
        measures = {
            "spot_futures_basis": basis,
            "futures_spread": spread,
            "option_pcr_oi": derivatives.series(as_of_epoch, "options", key, "pcr_oi"),
            "option_iv_skew": derivatives.series(as_of_epoch, "options", key, "iv_skew"),
        }
        for kind, points in measures.items():
            z, evidence = _series_z(points)
            cls = str(classify_z(np.array([z]))[0]) if np.isfinite(z) else INSUFFICIENT_DATA
            if only_flagged and cls not in (UNUSUAL, EXTREME):
                continue
            out.append(Relationship(kind, key, "own_history_today", cls, _num(z), evidence))
    return out


def all_relationships(frame, features, report, derivatives=None, only_flagged: bool = True) -> list[Relationship]:
    rels = stock_relationships(frame, features, only_flagged)
    rels += sector_relationships(features, only_flagged)
    rels += price_volume_relationships(report)
    rels += derivatives_relationships(derivatives, frame.as_of_epoch, only_flagged)
    return rels


__all__ = ["NORMAL", "Relationship", "all_relationships", "judge_against"]
