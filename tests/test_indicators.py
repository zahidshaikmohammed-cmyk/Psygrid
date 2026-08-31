import unittest

from indicators import ema, rsi, sma, vwap


class IndicatorTests(unittest.TestCase):
    def test_sma(self):
        self.assertEqual(sma([1, 2, 3, 4], 3), 3.0)

    def test_ema_requires_period(self):
        self.assertIsNone(ema([1, 2, 3], 4))
        self.assertAlmostEqual(ema([1, 2, 3, 4], 3), 3.0)

    def test_rsi_flat_is_50(self):
        self.assertEqual(rsi([10] * 15, 14), 50.0)

    def test_vwap(self):
        candles = [
            {"high": 12, "low": 10, "close": 11, "volume": 100},
            {"high": 14, "low": 12, "close": 13, "volume": 100},
        ]
        self.assertAlmostEqual(vwap(candles), 12.0)


if __name__ == "__main__":
    unittest.main()
