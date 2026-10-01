"""Data quality of a ``MarketFrame``: what is present, missing, stale or rejected.

Quality is measured, never repaired. Every engine output carries the quality of
the frame it came from, so a gap in the data cannot silently become a finding.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime

import numpy as np

from intelligence.archive import IST, Bars
from intelligence.frame import BAR_SECONDS, MarketFrame

COMPLETE = "COMPLETE"
GAPS = "GAPS"
NO_DATA = "NO_DATA"


@dataclass(frozen=True)
class InstrumentQuality:
    key: str
    status: str  # COMPLETE, GAPS or NO_DATA
    expected_bars: int
    present_bars: int
    missing_bars: int
    rejected_bars: int
    last_bar: str | None  # IST bar-open time of the latest present bar
    minutes_since_last_bar: float | None  # from that bar's close to as_of


@dataclass(frozen=True)
class BlockQuality:
    instruments: int
    expected_bars_each: int
    complete: int
    with_gaps: int
    no_data: int
    coverage: float | None  # present bars / expected bars across the block
    rejected_bars: int
    per_instrument: tuple[InstrumentQuality, ...]

    def worst(self, count: int = 10) -> list[InstrumentQuality]:
        """Instruments with the most missing bars, then the stalest."""
        return sorted(
            (q for q in self.per_instrument if q.status != COMPLETE),
            key=lambda q: (-q.missing_bars, -(q.minutes_since_last_bar or 0), q.key),
        )[:count]

    def summary(self) -> dict:
        data = asdict(self)
        data.pop("per_instrument")
        return data


@dataclass(frozen=True)
class FrameQuality:
    session_date: str
    as_of: str
    equity: BlockQuality
    indices: BlockQuality | None


def _ist(epoch: int) -> str:
    return datetime.fromtimestamp(int(epoch), IST).strftime("%Y-%m-%d %H:%M:%S IST")


def block_quality(bars: Bars, as_of_epoch: int) -> BlockQuality:
    present = ~np.isnan(bars.close)
    expected = bars.shape[1]
    rows = []
    for i, key in enumerate(bars.keys):
        count = int(present[i].sum())
        last = int(bars.minutes[np.flatnonzero(present[i])[-1]]) if count else None
        status = NO_DATA if count == 0 and expected else (COMPLETE if count == expected else GAPS)
        rows.append(
            InstrumentQuality(
                key=key,
                status=status,
                expected_bars=expected,
                present_bars=count,
                missing_bars=expected - count,
                rejected_bars=len(bars.rejected.get(key, ())),
                last_bar=_ist(last) if last is not None else None,
                minutes_since_last_bar=round((as_of_epoch - last - BAR_SECONDS) / 60, 1) if last is not None else None,
            )
        )
    total_expected = expected * len(bars.keys)
    return BlockQuality(
        instruments=len(bars.keys),
        expected_bars_each=expected,
        complete=sum(q.status == COMPLETE for q in rows),
        with_gaps=sum(q.status == GAPS for q in rows),
        no_data=sum(q.status == NO_DATA for q in rows),
        coverage=round(int(present.sum()) / total_expected, 4) if total_expected else None,
        rejected_bars=sum(q.rejected_bars for q in rows),
        per_instrument=tuple(rows),
    )


def frame_quality(frame: MarketFrame) -> FrameQuality:
    epoch = frame.as_of_epoch
    return FrameQuality(
        session_date=frame.session_date,
        as_of=frame.as_of.strftime("%Y-%m-%d %H:%M:%S IST"),
        equity=block_quality(frame.equity, epoch),
        indices=block_quality(frame.indices, epoch) if frame.indices is not None else None,
    )
