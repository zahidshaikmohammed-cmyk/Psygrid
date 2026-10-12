"""Synthetic market for tests and dry runs: random walks with an optional planted behaviour."""

from __future__ import annotations

import datetime as dt

import numpy as np

from .data import IST_OFFSET, OPEN_MIN, build


def sessions(start=dt.date(2022, 11, 1), end=dt.date(2026, 10, 9)) -> list[dt.date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def make_rows(
    seed: int, days: list[dt.date], market: np.ndarray | None = None, plant: bool = False, sigma=0.0015, volume=True
):
    """5-minute rows (epoch, o, h, l, c, v). market: [D,75] shared returns added with beta 1.

    plant=True: after a 5-minute candle falling more than 2 sigma between 09:45 and 11:00, the next
    six candles drift up by 0.07% each (a stock-specific, recurring, tradable behaviour).
    """
    rng = np.random.default_rng(seed)
    p = 1000.0
    rows = []
    for di, d in enumerate(days):
        p *= 1 + rng.normal(0, 0.006)
        boost = 0
        base_ts = (d - dt.date(1970, 1, 1)).days * 86400 - IST_OFFSET + OPEN_MIN * 60
        for t in range(75):
            o = p
            vol = sigma * (1.6 if t < 6 or t > 66 else 1.0)
            r = rng.normal(0, vol) + (market[di, t] if market is not None else 0.0)
            if boost > 0:
                r += 0.0007
                boost -= 1
            c = o * (1 + r)
            h = max(o, c) * (1 + abs(rng.normal(0, vol / 3)))
            lo = min(o, c) * (1 - abs(rng.normal(0, vol / 3)))
            v = float(rng.lognormal(10, 0.5)) if volume else 0.0
            rows.append((base_ts + 300 * t, o, h, lo, c, v))
            if plant and 6 <= t < 21 and r < -2 * sigma:
                boost = 6
            p = c
    return np.array(rows, float)


def market(seed: int = 99, n_days: int | None = None) -> tuple[list[dt.date], np.ndarray, list]:
    days = sessions()
    if n_days:
        days = days[:n_days]
    rng = np.random.default_rng(seed)
    mk = rng.normal(0, 0.0008, size=(len(days), 75))
    insts = [build("NIFTY", "INDEX", "INDEX", make_rows(1, days, mk, volume=False, sigma=0.0004), 5)]
    return days, mk, insts


def universe(n_noise: int = 2, plant: bool = True, n_days: int | None = None):
    days, mk, insts = market(n_days=n_days)
    if plant:
        insts.append(build("PLANT", "EQUITY", "BANKING", make_rows(11, days, mk, plant=True), 5))
    for i in range(n_noise):
        insts.append(build(f"NOISE{i}", "EQUITY", "BANKING", make_rows(20 + i, days, mk), 5))
    return insts
