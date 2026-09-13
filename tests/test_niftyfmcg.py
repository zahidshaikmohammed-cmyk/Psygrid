import unittest
from types import SimpleNamespace

from niftyfmcg import NIFTYFMCG_EXCHANGE_SEGMENT, NIFTYFMCG_INSTRUMENT, NIFTYFMCG_SYMBOL, NiftyFmcgState, niftyfmcg_json


class NiftyFmcgEndpointTests(unittest.TestCase):
    def test_niftyfmcg_contract(self):
        state = NiftyFmcgState(SimpleNamespace(timezone="Asia/Kolkata"))
        state.session_date = "2026-09-13"
        state.session_status = "LIVE"
        state.last_ltp = 45185.60
        state.last_ltt = 1789290000
        payload = niftyfmcg_json(state, "22")
        self.assertEqual(payload["symbol"], NIFTYFMCG_SYMBOL)
        self.assertEqual(payload["security_id"], "22")
        self.assertEqual(payload["exchange_segment"], NIFTYFMCG_EXCHANGE_SEGMENT)
        self.assertEqual(payload["instrument"], NIFTYFMCG_INSTRUMENT)
        self.assertEqual(payload["timeframes"], ["1m", "5m", "15m", "1h"])
        self.assertFalse(payload["synthetic_candles"])


if __name__ == "__main__":
    unittest.main()
