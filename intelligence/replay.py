"""Replay an archived session minute by minute, as if it were live.

Each step yields the ``MarketFrame`` that was knowable at that minute, so an
engine driven by replay sees exactly what it would have seen live, and the same
day replayed twice yields the same frames.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

from config import MARKET_END, MARKET_START
from intelligence.archive import ArchiveDay
from intelligence.frame import BAR_SECONDS, MarketFrame, as_of_time, frame_at


def replay(day: ArchiveDay, start: str = MARKET_START, end: str = MARKET_END, every: int = 1) -> Iterator[MarketFrame]:
    """Frames at each minute boundary from ``start`` to ``end`` (HH:MM, inclusive), every ``every`` minutes."""
    if every < 1:
        raise ValueError("every must be at least 1 minute")
    first, last = as_of_time(day.session_date, start), as_of_time(day.session_date, end)
    step = every * BAR_SECONDS
    epoch = int(first.timestamp())
    while epoch <= int(last.timestamp()):
        yield frame_at(day, datetime.fromtimestamp(epoch, first.tzinfo))
        epoch += step
