"""The market as known at one minute: the single input every engine takes.

A ``MarketFrame`` at ``as_of`` holds exactly the 1m bars that had *completed* by
then (bar open + 60 s <= as_of) inside the session window, aligned on a regular
minute grid. Later bars are never present, so nothing computed from a frame can
look ahead. Arrays are copies, not views of the full day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time

import numpy as np

from config import MARKET_END, MARKET_START
from intelligence.archive import BAR_FIELDS, IST, ArchiveDay, Bars

BAR_SECONDS = 60


@dataclass(frozen=True)
class MarketFrame:
    session_date: str
    as_of: datetime
    grid: np.ndarray  # bar-open epochs known complete at as_of, one per column
    equity: Bars
    indices: Bars | None
    reference: dict[str, dict[str, float | None]]

    @property
    def as_of_epoch(self) -> int:
        return int(self.as_of.timestamp())


def _clock(session_date: str, hhmm: str) -> int:
    hour, minute = map(int, hhmm.split(":"))
    day = datetime.strptime(session_date, "%Y-%m-%d").date()
    return int(datetime.combine(day, time(hour, minute), IST).timestamp())


def session_grid(session_date: str, as_of: datetime) -> np.ndarray:
    """Bar-open epochs from the session open whose bars had completed by ``as_of``."""
    first = _clock(session_date, MARKET_START)
    end = min(int(as_of.timestamp()), _clock(session_date, MARKET_END))
    last = end - BAR_SECONDS
    if last < first:
        return np.empty(0, dtype=np.int64)
    return np.arange(first, last + 1, BAR_SECONDS, dtype=np.int64)


def _align(bars: Bars, grid: np.ndarray, as_of_epoch: int) -> Bars:
    """Copy ``bars`` onto ``grid``; columns outside the grid are dropped, missing ones are NaN."""
    columns = np.searchsorted(grid, bars.minutes)
    inside = columns < len(grid)
    inside[inside] = grid[columns[inside]] == bars.minutes[inside]
    aligned = {}
    for name in BAR_FIELDS:
        out = np.full((len(bars.keys), len(grid)), np.nan)
        out[:, columns[inside]] = bars.field(name)[:, inside]
        aligned[name] = out
    # Only rejections of bars that had completed by as_of are known at as_of.
    rejected = {
        key: [(epoch, reason) for epoch, reason in rows if epoch is not None and epoch + BAR_SECONDS <= as_of_epoch]
        for key, rows in bars.rejected.items()
    }
    return Bars(
        keys=bars.keys,
        names=bars.names,
        minutes=grid.copy(),
        rejected={key: rows for key, rows in rejected.items() if rows},
        **aligned,
    )


def frame_at(day: ArchiveDay, as_of: datetime) -> MarketFrame:
    """The market as known at ``as_of`` (timezone-aware) on an archived day."""
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    grid = session_grid(day.session_date, as_of)
    as_of_epoch = int(as_of.timestamp())
    return MarketFrame(
        session_date=day.session_date,
        as_of=as_of.astimezone(IST),
        grid=grid,
        equity=_align(day.equity, grid, as_of_epoch),
        indices=_align(day.indices, grid, as_of_epoch) if day.indices is not None else None,
        reference={symbol: dict(values) for symbol, values in day.reference.items()},
    )


def as_of_time(session_date: str, hhmm: str) -> datetime:
    """``HH:MM`` on ``session_date`` in IST, as a timezone-aware datetime."""
    return datetime.fromtimestamp(_clock(session_date, hhmm), IST)
