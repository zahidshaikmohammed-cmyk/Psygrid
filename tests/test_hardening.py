import unittest
from types import SimpleNamespace

from state import PsygridState


class HardeningTests(unittest.TestCase):
    def _settings(self):
        return SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=30)

    def test_late_packet_cannot_move_active_candle_backwards(self):
        state = PsygridState(self._settings())
        instrument = SimpleNamespace(symbol="TEST", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY")
        state.begin("2026-09-03", [instrument])
        state.seed_cumulative_volume("1", 100)
        state.update_quote("1", {"LTP": 100, "LTT_EPOCH": 1000, "volume": 101, "LTQ": 1})
        state.update_quote("1", {"LTP": 99, "LTT_EPOCH": 940, "volume": 102, "LTQ": 1})
        self.assertEqual(state.current_1m["1"]["epoch"], 960)
        self.assertEqual(state.current_1m["1"]["close"], 100)

    def test_new_minute_creates_only_real_candles_no_gap_fill(self):
        state = PsygridState(self._settings())
        instrument = SimpleNamespace(symbol="TEST", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY")
        state.begin("2026-09-03", [instrument])
        state.seed_cumulative_volume("1", 100)
        state.update_quote("1", {"LTP": 100, "LTT_EPOCH": 960, "volume": 101, "LTQ": 1})
        state.update_quote("1", {"LTP": 103, "LTT_EPOCH": 1140, "volume": 102, "LTQ": 1})
        self.assertEqual(len(state.live_candles["1"]), 1)
        self.assertEqual(state.live_candles["1"][0]["epoch"], 960)
        self.assertEqual(state.current_1m["1"]["epoch"], 1140)

    def test_runtime_state_has_no_higher_timeframe_or_depth_store(self):
        state = PsygridState(self._settings())
        self.assertFalse(hasattr(state, "historical"))
        self.assertFalse(hasattr(state, "indicator_seed_1m"))
        self.assertFalse(hasattr(state, "raw_tick_ring"))
        self.assertFalse(hasattr(state, "market_context"))


if __name__ == "__main__":
    unittest.main()
