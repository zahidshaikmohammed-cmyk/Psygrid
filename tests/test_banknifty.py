import unittest
from types import SimpleNamespace

from banknifty import (
    BANKNIFTY_EXCHANGE_SEGMENT,
    BANKNIFTY_INSTRUMENT,
    BANKNIFTY_SECURITY_ID,
    BANKNIFTY_SYMBOL,
    BankNiftyState,
    banknifty_json,
)


class BankNiftyEndpointTests(unittest.TestCase):
    def test_banknifty_contract(self):
        state = BankNiftyState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 55000.0
        state.last_ltt = 1757667600
        payload = banknifty_json(state)
        self.assertEqual(payload["symbol"], BANKNIFTY_SYMBOL)
        self.assertEqual(payload["security_id"], BANKNIFTY_SECURITY_ID)
        self.assertEqual(payload["exchange_segment"], BANKNIFTY_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], BANKNIFTY_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
