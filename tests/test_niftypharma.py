import unittest
from types import SimpleNamespace

from niftypharma import NIFTYPHARMA_EXCHANGE_SEGMENT, NIFTYPHARMA_INSTRUMENT, NIFTYPHARMA_SYMBOL, NiftyPharmaState, niftypharma_json


class NiftyPharmaEndpointTests(unittest.TestCase):
    def test_niftypharma_contract(self):
        state = NiftyPharmaState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 28890.90
        state.last_ltt = 1789290000
        payload = niftypharma_json(state, "TEST-ID")
        self.assertEqual(payload["symbol"], NIFTYPHARMA_SYMBOL)
        self.assertEqual(payload["security_id"], "TEST-ID")
        self.assertEqual(payload["exchange_segment"], NIFTYPHARMA_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYPHARMA_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
