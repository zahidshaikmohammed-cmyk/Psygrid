import json
import unittest
from pathlib import Path


class UniverseTests(unittest.TestCase):
    def test_exactly_990_unique_symbols(self):
        # Universe grew 90 -> 180 -> 270 -> 360 -> 450 -> 990 over this
        # repo's history (see git log); this assertion just tracks the
        # current canonical size documented in config.UNIVERSE_SIZE and
        # verified live by tests/test_universe_contract.py.
        path = Path(__file__).resolve().parents[1] / "stocks.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        symbols = payload["symbols"]
        self.assertEqual(len(symbols), 990)
        self.assertEqual(len(set(symbols)), 990)
        self.assertEqual(payload["exchange"], "NSE")
        self.assertEqual(payload["instrument"], "EQUITY")


if __name__ == "__main__":
    unittest.main()
