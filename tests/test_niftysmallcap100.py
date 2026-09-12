import unittest
from types import SimpleNamespace

from niftysmallcap100 import (
    NIFTYSMALLCAP100_EXCHANGE_SEGMENT,
    NIFTYSMALLCAP100_INSTRUMENT,
    NIFTYSMALLCAP100_SYMBOL,
    NiftySmallcap100State,
    niftysmallcap100_json,
)


class NiftySmallcap100EndpointTests(unittest.TestCase):
    def test_niftysmallcap100_contract(self):
        state = NiftySmallcap100State(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 25000.0
        state.last_ltt = 1757667600
        payload = niftysmallcap100_json(state, "TEST-ID")
        self.assertEqual(payload["symbol"], NIFTYSMALLCAP100_SYMBOL)
        self.assertEqual(payload["security_id"], "TEST-ID")
        self.assertEqual(payload["exchange_segment"], NIFTYSMALLCAP100_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYSMALLCAP100_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
