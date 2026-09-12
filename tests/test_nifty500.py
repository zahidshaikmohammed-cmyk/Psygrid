import unittest
from types import SimpleNamespace

from nifty500 import (
    NIFTY500_EXCHANGE_SEGMENT,
    NIFTY500_INSTRUMENT,
    NIFTY500_SECURITY_ID,
    NIFTY500_SYMBOL,
    Nifty500State,
    nifty500_json,
)


class Nifty500EndpointTests(unittest.TestCase):
    def test_nifty500_contract(self):
        state = Nifty500State(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 25000.0
        state.last_ltt = 1757667600
        payload = nifty500_json(state)
        self.assertEqual(payload["symbol"], NIFTY500_SYMBOL)
        self.assertEqual(payload["security_id"], NIFTY500_SECURITY_ID)
        self.assertEqual(payload["exchange_segment"], NIFTY500_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTY500_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
