"""Shared synthetic archives for intelligence tests: written once per session, read many times."""

import pytest

from intelligence.synthetic import Injection, SyntheticMarket, trading_dates

DATES = trading_dates("2026-09-01", 8)
TODAY = DATES[-1]
# Known answers for detectors: on the last day, a volume surge and a price shock in different stocks.
INJECTIONS = [
    Injection(TODAY, "TCS", minute=120, length=1, volume_multiplier=12.0),
    Injection(TODAY, "HDFCBANK", minute=150, length=1, extra_return=0.02),
    Injection(TODAY, "SUNPHARMA", minute=200, length=30, extra_return=0.0012),  # slow divergence from sector
]


@pytest.fixture(scope="session")
def market_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("archive")
    SyntheticMarket(injections=list(INJECTIONS)).write_days(root, DATES)
    return root


@pytest.fixture(scope="session")
def store_root(tmp_path_factory):
    return tmp_path_factory.mktemp("store")
