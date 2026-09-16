import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ShardContractTests(unittest.TestCase):
    def setUp(self):
        payload = json.loads((ROOT / "stocks.json").read_text(encoding="utf-8"))
        self.symbols = payload["symbols"]
        self.ranges = [(chr(ord("a") + i), i * 45, (i + 1) * 45) for i in range(10)]

    def test_a_to_j_are_exactly_10_disjoint_45_stock_slices(self):
        shards = {name: self.symbols[start:end] for name, start, end in self.ranges}
        self.assertEqual(len(shards), 10)
        self.assertTrue(all(len(symbols) == 45 for symbols in shards.values()))
        flattened = [symbol for symbols in shards.values() for symbol in symbols]
        self.assertEqual(len(flattened), 450)
        self.assertEqual(len(set(flattened)), 450)
        self.assertEqual(flattened, self.symbols)

    def test_every_pair_of_shards_is_disjoint(self):
        shards = [self.symbols[start:end] for _, start, end in self.ranges]
        for index, left in enumerate(shards):
            for right in shards[index + 1:]:
                self.assertTrue(set(left).isdisjoint(right))

    def test_shard_union_matches_canonical_universe_exactly(self):
        shards = [self.symbols[start:end] for _, start, end in self.ranges]
        union = {symbol for shard in shards for symbol in shard}
        canonical = set(self.symbols)
        self.assertEqual(union, canonical)
        self.assertEqual(len(union), 450)


if __name__ == "__main__":
    unittest.main()
