import unittest
from types import SimpleNamespace

from sensex import (
    SENSEX_EXCHANGE_SEGMENT,
    SENSEX_INSTRUMENT,
    SENSEX_SECURITY_ID,
    SENSEX_SYMBOL,
    SensexState,
    sensex_json,
)


class SensexEndpointTests(unittest.TestCase):
    def test_sensex_contract(self):
        state = SensexState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-12"
        state.session_status = "LIVE"
        state.last_ltp = 82000.0
        state.last_ltt = 1757667600
        payload = sensex_json(state)
        self.assertEqual(payload["symbol"], SENSEX_SYMBOL)
        self.assertEqual(payload["security_id"], SENSEX_SECURITY_ID)
        self.assertEqual(payload["exchange_segment"], SENSEX_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], SENSEX_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
