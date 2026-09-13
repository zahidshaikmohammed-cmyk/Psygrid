import unittest
from types import SimpleNamespace

from niftyit import NIFTYIT_EXCHANGE_SEGMENT, NIFTYIT_INSTRUMENT, NIFTYIT_SYMBOL, NiftyItState, niftyit_json


class NiftyItEndpointTests(unittest.TestCase):
    def test_niftyit_contract(self):
        state = NiftyItState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 28890.90
        state.last_ltt = 1789290000
        payload = niftyit_json(state, "19")
        self.assertEqual(payload["symbol"], NIFTYIT_SYMBOL)
        self.assertEqual(payload["security_id"], "19")
        self.assertEqual(payload["exchange_segment"], NIFTYIT_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYIT_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
