import unittest
from types import SimpleNamespace

from niftymidcap100 import (
    NIFTYMIDCAP100_EXCHANGE_SEGMENT,
    NIFTYMIDCAP100_INSTRUMENT,
    NIFTYMIDCAP100_SYMBOL,
    NiftyMidcap100State,
    niftymidcap100_json,
)


class NiftyMidcap100EndpointTests(unittest.TestCase):
    def test_niftymidcap100_contract(self):
        state = NiftyMidcap100State(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 62000.0
        state.last_ltt = 1757667600
        payload = niftymidcap100_json(state, "TEST_SECURITY_ID")
        self.assertEqual(payload["symbol"], NIFTYMIDCAP100_SYMBOL)
        self.assertEqual(payload["security_id"], "TEST_SECURITY_ID")
        self.assertEqual(payload["exchange_segment"], NIFTYMIDCAP100_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYMIDCAP100_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertEqual(payload["candle_source"]["1m"], "DHAN_WEBSOCKET_FULL")
        self.assertEqual(payload["candle_source"]["5m"], "DHAN_NATIVE_HISTORICAL")
        self.assertEqual(payload["candle_source"]["15m"], "DHAN_NATIVE_HISTORICAL")
        self.assertEqual(payload["candle_source"]["1h"], "DHAN_NATIVE_HISTORICAL")
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
