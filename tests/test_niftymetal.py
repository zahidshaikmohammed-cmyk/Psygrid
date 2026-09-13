import unittest
from types import SimpleNamespace

from niftymetal import NIFTYMETAL_EXCHANGE_SEGMENT, NIFTYMETAL_INSTRUMENT, NIFTYMETAL_SYMBOL, NiftyMetalState, niftymetal_json


class NiftyMetalEndpointTests(unittest.TestCase):
    def test_niftymetal_contract(self):
        state = NiftyMetalState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 13000.60
        state.last_ltt = 1789290000
        payload = niftymetal_json(state, "23")
        self.assertEqual(payload["symbol"], NIFTYMETAL_SYMBOL)
        self.assertEqual(payload["security_id"], "23")
        self.assertEqual(payload["exchange_segment"], NIFTYMETAL_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYMETAL_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
