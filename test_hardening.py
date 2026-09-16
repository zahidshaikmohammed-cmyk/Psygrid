from __future__ import annotations

import unittest

from config import load_instruments
from output import market_live_json


class UniverseShardTests(unittest.TestCase):
    def test_canonical_universe_is_450_unique(self):
        instruments = load_instruments()
        self.assertEqual(len(instruments), 450)
        symbols = [meta["symbol"] for meta in instruments.values()]
        self.assertEqual(len(symbols), 450)
        self.assertEqual(len(set(symbols)), 450)

    def test_ten_45_stock_shards_are_disjoint_and_complete(self):
        instruments = load_instruments()
        symbols = [meta["symbol"] for meta in instruments.values()]
        shards = [symbols[i:i + 45] for i in range(0, 450, 45)]

        self.assertEqual(len(shards), 10)
        self.assertTrue(all(len(shard) == 45 for shard in shards))

        flattened = [symbol for shard in shards for symbol in shard]
        self.assertEqual(len(flattened), 450)
        self.assertEqual(len(set(flattened)), 450)
        self.assertEqual(set(flattened), set(symbols))

    def test_shard_slices_match_canonical_order(self):
        instruments = load_instruments()
        symbols = [meta["symbol"] for meta in instruments.values()]
        expected = {
            "a": symbols[0:45],
            "b": symbols[45:90],
            "c": symbols[90:135],
            "d": symbols[135:180],
            "e": symbols[180:225],
            "f": symbols[225:270],
            "g": symbols[270:315],
            "h": symbols[315:360],
            "i": symbols[360:405],
            "j": symbols[405:450],
        }
        flattened = []
        for shard in expected.values():
            flattened.extend(shard)
        self.assertEqual(flattened, symbols)


if __name__ == "__main__":
    unittest.main()
