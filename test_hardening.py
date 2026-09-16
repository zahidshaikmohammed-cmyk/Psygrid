from __future__ import annotations

import unittest

from config import load_instruments


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

    def test_shard_boundaries_are_non_overlapping(self):
        instruments = load_instruments()
        symbols = [meta["symbol"] for meta in instruments.values()]
        shards = [symbols[i:i + 45] for i in range(0, 450, 45)]

        for left, right in zip(shards, shards[1:]):
            self.assertTrue(set(left).isdisjoint(right))


if __name__ == "__main__":
    unittest.main()
