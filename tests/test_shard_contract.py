import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = 990
SHARD_NAMES = "abcdefghijklmnopqrstuv"

class ShardContractTests(unittest.TestCase):
    def setUp(self):
        payload = json.loads((ROOT / "stocks.json").read_text(encoding="utf-8"))
        self.symbols = payload["symbols"]
        self.ranges = [(name, i * 45, (i + 1) * 45) for i, name in enumerate(SHARD_NAMES)]

    def test_a_to_v_are_exactly_22_disjoint_45_stock_slices(self):
        shards = {name: self.symbols[start:end] for name, start, end in self.ranges}
        self.assertEqual(len(shards), 22)
        self.assertTrue(all(len(symbols) == 45 for symbols in shards.values()))
        flattened = [symbol for symbols in shards.values() for symbol in symbols]
        self.assertEqual(len(flattened), EXPECTED)
        self.assertEqual(len(set(flattened)), EXPECTED)
        self.assertEqual(flattened, self.symbols)

    def test_every_pair_of_shards_is_disjoint(self):
        shards = [self.symbols[start:end] for _, start, end in self.ranges]
        for index, left in enumerate(shards):
            for right in shards[index + 1:]:
                self.assertTrue(set(left).isdisjoint(right))

    def test_shard_union_matches_canonical_universe_exactly(self):
        shards = [self.symbols[start:end] for _, start, end in self.ranges]
        union = {symbol for shard in shards for symbol in shard}
        self.assertEqual(union, set(self.symbols))
        self.assertEqual(len(union), EXPECTED)

if __name__ == "__main__":
    unittest.main()
