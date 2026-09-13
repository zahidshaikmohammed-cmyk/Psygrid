import unittest
from types import SimpleNamespace

from niftyinfra import NIFTYINFRA_EXCHANGE_SEGMENT, NIFTYINFRA_INSTRUMENT, NIFTYINFRA_SYMBOL, NiftyInfraState, niftyinfra_json


class NiftyInfraEndpointTests(unittest.TestCase):
    def test_niftyinfra_contract(self):
        state = NiftyInfraState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 1000.0
        state.last_ltt = 1789290000
        payload = niftyinfra_json(state, "10")
        self.assertEqual(payload["symbol"], NIFTYINFRA_SYMBOL)
        self.assertEqual(payload["security_id"], "10")
        self.assertEqual(payload["exchange_segment"], NIFTYINFRA_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYINFRA_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
