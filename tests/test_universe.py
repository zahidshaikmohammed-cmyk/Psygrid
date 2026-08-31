import json
import unittest
from pathlib import Path


class UniverseTests(unittest.TestCase):
    def test_exactly_90_unique_symbols(self):
        path = Path(__file__).resolve().parents[1] / "stocks.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        symbols = payload["symbols"]
        self.assertEqual(len(symbols), 90)
        self.assertEqual(len(set(symbols)), 90)
        self.assertEqual(payload["exchange"], "NSE")
        self.assertEqual(payload["instrument"], "EQUITY")


if __name__ == "__main__":
    unittest.main()
