import unittest
from types import SimpleNamespace

from nifty import NIFTY_EXCHANGE_SEGMENT, NIFTY_INSTRUMENT, NIFTY_SECURITY_ID, NIFTY_SYMBOL, NiftyState, nifty_json


class NiftyEndpointTests(unittest.TestCase):
    def test_nifty_contract(self):
        state = NiftyState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 25000.0
        state.last_ltt = 1757667600
        payload = nifty_json(state)
        self.assertEqual(payload["symbol"], NIFTY_SYMBOL)
        self.assertEqual(payload["security_id"], NIFTY_SECURITY_ID)
        self.assertEqual(payload["exchange_segment"], NIFTY_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTY_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
