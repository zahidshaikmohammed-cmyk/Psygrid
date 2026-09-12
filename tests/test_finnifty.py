import unittest
from types import SimpleNamespace

from finnifty import FINNIFTY_EXCHANGE_SEGMENT, FINNIFTY_INSTRUMENT, FINNIFTY_SYMBOL, FinNiftyState, finnifty_json


class FinNiftyEndpointTests(unittest.TestCase):
    def test_finnifty_contract(self):
        state = FinNiftyState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 25545.40
        state.last_ltt = 1757667600
        payload = finnifty_json(state, "27")
        self.assertEqual(payload["symbol"], FINNIFTY_SYMBOL)
        self.assertEqual(payload["security_id"], "27")
        self.assertEqual(payload["exchange_segment"], FINNIFTY_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], FINNIFTY_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
