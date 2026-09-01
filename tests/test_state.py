import unittest
from types import SimpleNamespace

from state import PsygridState


class CandleStateTests(unittest.TestCase):
    def setUp(self):
        settings = SimpleNamespace(
            timezone="Asia/Kolkata",
            ma_period=9,
            ema_period=20,
            rsi_period=14,
            max_live_age_seconds=60,
        )
        self.state = PsygridState(settings)
        self.instrument = SimpleNamespace(
            symbol="TEST",
            security_id="123",
            exchange_segment="NSE_EQ",
            instrument="EQUITY",
        )
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

    def test_raw_tick_ring_is_flushed_when_minute_closes(self):
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234301, "volume": 1001, "LTQ": 1},
        )
        self.state.update_quote(
            "123",
            {"LTP": 100.5, "LTT_EPOCH": 1788234305, "volume": 1002, "LTQ": 1},
        )
        self.assertEqual(len(self.state.raw_tick_ring["123"]), 2)

        self.state.update_quote(
            "123",
            {"LTP": 101.0, "LTT_EPOCH": 1788234360, "volume": 1003, "LTQ": 1},
        )

        self.assertEqual(len(self.state.raw_tick_ring["123"]), 1)
        self.assertEqual(self.state.raw_tick_ring["123"][0]["epoch"], 1788234360)
        self.assertEqual(len(self.state.live_candles["123"]), 1)
        self.assertTrue(self.state.live_candles["123"][0]["complete"])
        self.assertEqual(self.state.live_candles["123"][0]["open"], 100.0)
        self.assertEqual(self.state.live_candles["123"][0]["high"], 100.5)
        self.assertEqual(self.state.live_candles["123"][0]["low"], 100.0)
        self.assertEqual(self.state.live_candles["123"][0]["close"], 100.5)
        self.assertEqual(self.state.live_candles["123"][0]["volume"], 2)

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

    def test_freshness_turns_stale_after_sixty_seconds(self):
        self.state.update_quote(
            "123",
            {"LTP": 100.0, "LTT_EPOCH": 1788234300, "volume": 1001, "LTQ": 1},
        )
        last = self.state.last_tick_epoch
        self.assertIsNotNone(last)
        fresh = self.state.freshness("123", now_epoch=last + 59.9)
        stale = self.state.freshness("123", now_epoch=last + 60.1)
        self.assertEqual(fresh["status"], "LIVE")
        self.assertTrue(fresh["live_data_valid"])
        self.assertEqual(stale["status"], "STALE")
        self.assertFalse(stale["live_data_valid"])


if __name__ == "__main__":
    unittest.main()
