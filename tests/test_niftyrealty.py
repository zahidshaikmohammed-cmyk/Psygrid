import unittest
from types import SimpleNamespace

from niftyrealty import NIFTYREALTY_EXCHANGE_SEGMENT, NIFTYREALTY_INSTRUMENT, NIFTYREALTY_SYMBOL, NiftyRealtyState, niftyrealty_json


class NiftyRealtyEndpointTests(unittest.TestCase):
    def test_niftyrealty_contract(self):
        state = NiftyRealtyState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 1000.0
        state.last_ltt = 1789290000
        payload = niftyrealty_json(state, "24")
        self.assertEqual(payload["symbol"], NIFTYREALTY_SYMBOL)
        self.assertEqual(payload["security_id"], "24")
        self.assertEqual(payload["exchange_segment"], NIFTYREALTY_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYREALTY_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
