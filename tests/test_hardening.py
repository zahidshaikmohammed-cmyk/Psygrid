import unittest
from types import SimpleNamespace

from indicators import enrich
from state import PsygridState


class HardeningTests(unittest.TestCase):
    def _settings(self):
        return SimpleNamespace(
            timezone="Asia/Kolkata",
            ma_period=9,
            ema_period=20,
            rsi_period=14,
            max_live_age_seconds=30,
        )

    def test_enrich_returns_none_until_indicator_warmup(self):
        rows = [
            {"timestamp": 1000 + i * 60, "open": 100 + i, "high": 101 + i,
             "low": 99 + i, "close": 100 + i, "volume": 10, "complete": True}
            for i in range(20)
        ]
        enriched = enrich(rows, 9, 20, 14)
        self.assertIsNone(enriched[7]["ma9"])
        self.assertIsNotNone(enriched[8]["ma9"])
        self.assertIsNotNone(enriched[19]["ema20"])
        self.assertIsNotNone(enriched[14]["rsi14"])

    def test_late_packet_cannot_move_active_candle_backwards(self):
        state = PsygridState(self._settings())
        instrument = SimpleNamespace(
            symbol="TEST", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY"
        )
        state.begin("2026-09-03", [instrument])
        state.seed_cumulative_volume("1", 100)
        state.update_quote("1", {"LTP": 100, "LTT_EPOCH": 1000, "volume": 101, "LTQ": 1})
        state.update_quote("1", {"LTP": 99, "LTT_EPOCH": 940, "volume": 102, "LTQ": 1})
        self.assertEqual(state.current_1m["1"]["epoch"], 960)

    def test_new_minute_creates_only_real_candles_no_gap_fill(self):
        state = PsygridState(self._settings())
        instrument = SimpleNamespace(
            symbol="TEST", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY"
        )
        state.begin("2026-09-03", [instrument])
        state.seed_cumulative_volume("1", 100)
        state.update_quote("1", {"LTP": 100, "LTT_EPOCH": 960, "volume": 101, "LTQ": 1})
        state.update_quote("1", {"LTP": 103, "LTT_EPOCH": 1140, "volume": 102, "LTQ": 1})
        self.assertEqual(len(state.live_candles["1"]), 1)
        self.assertEqual(state.live_candles["1"][0]["epoch"], 960)
        self.assertEqual(state.current_1m["1"]["epoch"], 1140)


if __name__ == "__main__":
    unittest.main()
