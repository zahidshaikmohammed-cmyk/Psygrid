import unittest
from types import SimpleNamespace

from niftyauto import NIFTYAUTO_EXCHANGE_SEGMENT, NIFTYAUTO_INSTRUMENT, NIFTYAUTO_SYMBOL, NiftyAutoState, niftyauto_json


class NiftyAutoEndpointTests(unittest.TestCase):
    def test_niftyauto_contract(self):
        state = NiftyAutoState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 28890.90
        state.last_ltt = 1789290000
        payload = niftyauto_json(state, "20")
        self.assertEqual(payload["symbol"], NIFTYAUTO_SYMBOL)
        self.assertEqual(payload["security_id"], "20")
        self.assertEqual(payload["exchange_segment"], NIFTYAUTO_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYAUTO_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
