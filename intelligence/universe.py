"""Static structure of the universe: sector membership and the index each sector is measured against.

Sectors come from the hand-maintained ``sector_taxonomy.py``. Its ``OTHER``
bucket (about three quarters of the equity universe today) is treated as
"sector unknown", never as a sector of its own, so sector comparisons are only
made where a real peer group exists.
"""

from __future__ import annotations

from functools import cache

from sector_taxonomy import sector_for_symbol

UNKNOWN_SECTOR = "OTHER"
BROAD_INDEX = "nifty500"  # the benchmark every stock is compared with
MARKET_INDEX = "nifty"

# Only mappings where the sectoral index plainly tracks the taxonomy sector.
SECTOR_INDEX = {
    "BANKING": "banknifty",
    "FINANCIAL_SERVICES": "finnifty",
    "INFORMATION_TECHNOLOGY": "niftyit",
    "AUTOMOBILE": "niftyauto",
    "PHARMA_HEALTHCARE": "niftypharma",
    "METALS_MINING": "niftymetal",
    "CONSUMER_FMCG": "niftyfmcg",
    "REALTY": "niftyrealty",
    "ENERGY": "niftyenergy",
}


@cache
def sector_of(symbol: str) -> str | None:
    """The symbol's sector, or None when the taxonomy has no real sector for it."""
    sector = sector_for_symbol(symbol)
    return None if sector == UNKNOWN_SECTOR else sector


def sector_index_of(sector: str | None) -> str | None:
    return SECTOR_INDEX.get(sector) if sector else None
