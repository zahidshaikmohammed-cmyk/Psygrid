import unittest
from types import SimpleNamespace

from niftyenergy import NIFTYENERGY_EXCHANGE_SEGMENT, NIFTYENERGY_INSTRUMENT, NIFTYENERGY_SYMBOL, NiftyEnergyState, niftyenergy_json


class NiftyEnergyEndpointTests(unittest.TestCase):
    def test_niftyenergy_contract(self):
        state = NiftyEnergyState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 1000.0
        state.last_ltt = 1789290000
        payload = niftyenergy_json(state, "28")
        self.assertEqual(payload["symbol"], NIFTYENERGY_SYMBOL)
        self.assertEqual(payload["security_id"], "28")
        self.assertEqual(payload["exchange_segment"], NIFTYENERGY_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYENERGY_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
