"""Seeded synthetic market days, for tests and benchmarks only.

Nothing here is ever served or mixed with real data. Days are written through
the real ``DailyArchive`` writer, so the intelligence layer reads them exactly
as it reads a production archive. Prices follow a market factor, a sector
factor and stock-specific noise; volume follows a U-shaped intraday profile.
Anomalies can be injected at chosen minutes to give detectors known answers.
"""

from __future__ import annotations

import zlib
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from daily_archive import DailyArchive

IST = ZoneInfo("Asia/Kolkata")
SESSION_MINUTES = 360  # 09:15 to 15:15

# Real symbols so sector mapping works: (symbol, taxonomy sector).
DEFAULT_SYMBOLS = (
    "TCS", "INFY", "WIPRO", "HCLTECH", "TECHM",
    "HDFCBANK", "ICICIBANK", "KOTAKBANK", "AXISBANK", "SBIN",
    "RELIANCE", "ONGC", "NTPC", "POWERGRID", "COALINDIA",
    "SUNPHARMA", "CIPLA", "DRREDDY", "DIVISLAB", "LUPIN",
    "MARUTI", "TATAMOTORS", "M&M", "EICHERMOT", "BAJAJ-AUTO",
    "HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "DABUR",
)  # fmt: skip
INDEX_KEYS = ("nifty", "nifty500", "banknifty", "niftyit", "niftyenergy", "niftypharma", "niftyauto", "niftyfmcg")


@dataclass(frozen=True)
class Injection:
    """A known anomaly: from ``minute`` for ``length`` minutes, scale a symbol's volume and/or add a return."""

    session_date: str
    symbol: str
    minute: int
    length: int = 1
    volume_multiplier: float = 1.0
    extra_return: float = 0.0  # added log return per affected minute


@dataclass
class SyntheticMarket:
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    seed: int = 7
    injections: list[Injection] = field(default_factory=list)

    def _sector_codes(self) -> np.ndarray:
        from intelligence.universe import sector_of

        names = sorted({sector_of(s) or s for s in self.symbols})
        return np.array([names.index(sector_of(s) or s) for s in self.symbols])

    def generate(self, session_date: str, start_prices: np.ndarray | None = None) -> dict:
        """One day's bars: arrays (n, SESSION_MINUTES) of open, high, low, close, volume, plus index closes."""
        n = len(self.symbols)
        rng = np.random.default_rng(zlib.crc32(f"{self.seed}:{session_date}".encode()))  # stable across processes
        sectors = self._sector_codes()
        market = rng.normal(0, 0.0005, SESSION_MINUTES)
        sector_moves = rng.normal(0, 0.0004, (sectors.max() + 1, SESSION_MINUTES))
        idio = rng.normal(0, 0.0008, (n, SESSION_MINUTES))
        r = market[None, :] + sector_moves[sectors] + idio
        minutes = np.arange(SESSION_MINUTES)
        profile = 1.0 + 1.5 * np.exp(-minutes / 30) + 1.0 * np.exp(-(SESSION_MINUTES - minutes) / 30)
        base_volume = rng.uniform(2e3, 5e4, n)
        volume = base_volume[:, None] * profile[None, :] * rng.lognormal(0, 0.35, (n, SESSION_MINUTES))
        for inj in self.injections:
            if inj.session_date != session_date or inj.symbol not in self.symbols:
                continue
            i = self.symbols.index(inj.symbol)
            span = slice(inj.minute, inj.minute + inj.length)
            volume[i, span] *= inj.volume_multiplier
            r[i, span] += inj.extra_return
        prev_close = start_prices if start_prices is not None else rng.uniform(100, 3000, n)
        gap = rng.normal(0, 0.004, n)
        open0 = prev_close * np.exp(gap)
        close = open0[:, None] * np.exp(np.cumsum(r, axis=1))
        open_ = np.concatenate([open0[:, None], close[:, :-1]], axis=1)
        wiggle = np.abs(rng.normal(0, 0.0006, (n, SESSION_MINUTES)))
        high = np.maximum(open_, close) * (1 + wiggle)
        low = np.minimum(open_, close) * (1 - wiggle)
        index_close = {
            key: 10000 * np.exp(np.cumsum(market + rng.normal(0, 0.0001, SESSION_MINUTES))) for key in INDEX_KEYS
        }
        return {
            "open": open_, "high": high, "low": low, "close": close, "volume": np.round(volume),
            "prev_close": prev_close, "today_open": open0, "index_close": index_close,
        }  # fmt: skip

    def write_day(self, root: Path, session_date: str, start_prices: np.ndarray | None = None) -> dict:
        data = self.generate(session_date, start_prices)
        start = datetime.combine(date.fromisoformat(session_date), datetime.min.time(), IST) + timedelta(
            hours=9, minutes=15
        )
        stamps = [(start + timedelta(minutes=m)).strftime("%Y-%m-%d %H:%M:%S IST") for m in range(SESSION_MINUTES)]
        stocks = {}
        for i, symbol in enumerate(self.symbols):
            candles = [
                {"timestamp": stamps[m], "open": round(float(data["open"][i, m]), 2), "high": round(float(data["high"][i, m]), 2),
                 "low": round(float(data["low"][i, m]), 2), "close": round(float(data["close"][i, m]), 2), "volume": int(data["volume"][i, m])}
                for m in range(SESSION_MINUTES)
            ]  # fmt: skip
            stocks[symbol] = {
                "security_id": str(1000 + i),
                "previous_close": round(float(data["prev_close"][i]), 2),
                "today_open": round(float(data["today_open"][i]), 2),
                "candles_1m": candles,
            }
        archive = DailyArchive(root)
        archive.write_equity({"session": {"date": session_date}, "stocks": stocks})
        indices = {}
        for key, closes in data["index_close"].items():
            opens = np.concatenate([[closes[0]], closes[:-1]])
            indices[key] = {
                "symbol": key.upper(),
                "session": {"date": session_date},
                "1m": [{"timestamp": stamps[m], "open": float(opens[m]), "high": float(max(opens[m], closes[m])),
                        "low": float(min(opens[m], closes[m])), "close": float(closes[m]), "volume": 0}
                       for m in range(SESSION_MINUTES)],
            }  # fmt: skip
        archive.write_indices(indices)
        return data

    def write_days(self, root: Path, dates: list[str]) -> None:
        prices = None
        for session_date in dates:
            prices = self.write_day(root, session_date, prices)["close"][:, -1]


def trading_dates(start: str, count: int) -> list[str]:
    """``count`` weekdays from ``start`` (holidays are not modelled)."""
    out, day = [], date.fromisoformat(start)
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day += timedelta(days=1)
    return out
