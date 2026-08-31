import unittest
from types import SimpleNamespace

from config import Instrument
from state import PsygridState


class CandleStateTests(unittest.TestCase):
    def setUp(self):
        settings = SimpleNamespace(
            timezone="Asia/Kolkata",
            ma_period=9,
            ema_period=20,
            rsi_period=14,
        )
        self.state = PsygridState(settings)
        self.instrument = Instrument("TEST", "123")
        self.state.begin("2026-09-01", [self.instrument])
        self.state.seed_cumulative_volume("123", 1000)

    def test_first_quote_after_seed_creates_real_current_candle(self):
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234300, "volume": 1001, "LTQ": 1, "ATP": 100.0},
        )
        rows = self.state.live_enriched("123")
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]["complete"])
        self.assertEqual(rows[0]["open"], 100.0)
        self.assertEqual(rows[0]["volume"], 1)

    def test_new_minute_completes_previous_without_gap_fill(self):
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234300, "volume": 1001, "LTQ": 1, "ATP": 100.0},
        )
        self.state.update_quote(
            "123",
            {"LTP": 101.0, "LTT_EPOCH": 1788234365, "volume": 1002, "LTQ": 1, "ATP": 100.5},
        )
        rows = self.state.live_enriched("123")
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0]["complete"])
        self.assertFalse(rows[1]["complete"])
        self.assertEqual([r["volume"] for r in rows], [1, 1])

    def test_negative_cumulative_volume_reset_is_not_converted_to_fake_volume(self):
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234300, "volume": 1001, "LTQ": 1},
        )
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234360, "volume": 10, "LTQ": 1},
        )
        rows = self.state.live_enriched("123")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["volume"], 1)


if __name__ == "__main__":
    unittest.main()
