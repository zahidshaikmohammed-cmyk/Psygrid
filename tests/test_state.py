import unittest
from types import SimpleNamespace

from state import PsygridState


class CandleStateTests(unittest.TestCase):
    def setUp(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=30)
        self.state = PsygridState(settings)
        self.instrument = SimpleNamespace(
            symbol="TEST", security_id="123", exchange_segment="NSE_EQ", instrument="EQUITY"
        )
        self.state.begin("2026-09-01", [self.instrument])
        self.state.seed_cumulative_volume("123", 1000)

    def _quote(self, ltt, ltp=100.0, volume=1001):
        self.state.update_quote(
            "123", {"LTP": ltp, "LTT_EPOCH": ltt, "volume": volume, "LTQ": 1}
        )
        self.state.record_live_quote("123", ltt)

    def test_first_quote_creates_current_1m_candle_only(self):
        self._quote(1788234300)
        self.assertIsNotNone(self.state.current_1m["123"])
        self.assertEqual(len(self.state.live_candles["123"]), 0)
        self.assertEqual(self.state.current_1m["123"]["open"], 100.0)
        self.assertEqual(self.state.current_1m["123"]["volume"], 1)

    def test_new_minute_completes_previous_without_gap_fill(self):
        self._quote(1788234300, 100.0, 1001)
        self._quote(1788234365, 101.0, 1002)
        self.assertEqual(len(self.state.live_candles["123"]), 1)
        candle = self.state.live_candles["123"][0]
        self.assertTrue(candle["complete"])
        self.assertEqual(candle["open"], 100.0)
        self.assertEqual(candle["high"], 100.0)
        self.assertEqual(candle["low"], 100.0)
        self.assertEqual(candle["close"], 100.0)
        self.assertEqual(candle["volume"], 1)
        self.assertIsNotNone(self.state.current_1m["123"])
        self.assertFalse(self.state.current_1m["123"]["complete"])

    def test_cumulative_volume_reset_does_not_create_fake_volume(self):
        self._quote(1788234300, 100.0, 1001)
        self._quote(1788234360, 100.0, 10)
        self.assertEqual(self.state.live_candles["123"][0]["volume"], 1)
        self.assertEqual(self.state.current_1m["123"]["volume"], 0)

    def test_freshness_uses_packet_receipt_time(self):
        self._quote(1788234300)
        received = self.state.last_tick_received_epoch
        self.assertIsNotNone(received)
        fresh = self.state.freshness("123", now_epoch=received + 29.9)
        stale = self.state.freshness("123", now_epoch=received + 30.1)
        self.assertEqual(fresh["status"], "LIVE")
        self.assertTrue(fresh["live_data_valid"])
        self.assertEqual(stale["status"], "STALE")
        self.assertFalse(stale["live_data_valid"])


if __name__ == "__main__":
    unittest.main()
