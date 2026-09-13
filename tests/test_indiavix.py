import unittest
from types import SimpleNamespace

from indiavix import INDIAVIX_EXCHANGE_SEGMENT, INDIAVIX_INSTRUMENT, INDIAVIX_SYMBOL, IndiaVixState, indiavix_json


class IndiaVixEndpointTests(unittest.TestCase):
    def test_indiavix_contract(self):
        state = IndiaVixState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 12.29
        state.last_ltt = 1757667600
        payload = indiavix_json(state, "33")
        self.assertEqual(payload["symbol"], INDIAVIX_SYMBOL)
        self.assertEqual(payload["security_id"], "33")
        self.assertEqual(payload["exchange_segment"], INDIAVIX_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], INDIAVIX_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
