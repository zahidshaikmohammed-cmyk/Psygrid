import struct
import unittest
from types import SimpleNamespace

from nifty_depth import NiftyDepthContract, NiftyDepthState, _parse_depth_message, _select_contracts


class NiftyDepthTests(unittest.TestCase):
    def test_parses_20_level_bid_packet(self):
        header = struct.pack("<HBBiI", 332, 41, 2, 49081, 1)
        levels = b"".join(struct.pack("<dII", 100.5 + i, 10 + i, 2 + i) for i in range(20))
        parsed = _parse_depth_message(header + levels)
        self.assertEqual(len(parsed), 1)
        security_id, side, rows = parsed[0]
        self.assertEqual(security_id, "49081")
        self.assertEqual(side, "bid")
        self.assertEqual(len(rows), 20)
        self.assertEqual(rows[0], {"level": 1, "price": 100.5, "quantity": 10, "orders": 2})
        self.assertEqual(rows[-1]["level"], 20)

    def test_selects_nearest_25_strikes_and_both_sides(self):
        strikes = []
        for strike in range(23000, 24001, 50):
            strikes.append({
                "strike": float(strike),
                "ce": {"security_id": str(100000 + strike)},
                "pe": {"security_id": str(200000 + strike)},
            })
        state = SimpleNamespace()
        state.snapshot = lambda: {"expiry": "2026-09-17", "underlying_ltp": 23500.0, "strikes": strikes}
        contracts, expiry, ltp = _select_contracts(state)
        self.assertEqual(expiry, "2026-09-17")
        self.assertEqual(ltp, 23500.0)
        self.assertEqual(len(contracts), 50)
        self.assertEqual(len({contract.strike for contract in contracts}), 25)
        self.assertEqual({contract.option_type for contract in contracts}, {"CE", "PE"})

    def test_state_is_ram_only_and_exposes_depth_metadata(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata")
        state = NiftyDepthState(settings)
        contracts = [NiftyDepthContract("49081", 23500.0, "CE", "2026-09-17")]
        state.set_contracts(contracts, "2026-09-17")
        state.update_depth("49081", "bid", [{"level": 1, "price": 100.0, "quantity": 500, "orders": 3}])
        state.update_quotes("49081" if False else {"49081": {"last_price": 101.0, "volume": 1234, "oi": 5678}})
        payload = state.snapshot()
        self.assertEqual(payload["storage"], "RAM_ONLY")
        self.assertFalse(payload["synthetic_data"])
        self.assertEqual(payload["depth_levels"], 20)
        self.assertEqual(payload["contracts"][0]["bid"][0]["price"], 100.0)
        self.assertEqual(payload["contracts"][0]["volume"], 1234)
        self.assertEqual(payload["contracts"][0]["oi"], 5678)


if __name__ == "__main__":
    unittest.main()
