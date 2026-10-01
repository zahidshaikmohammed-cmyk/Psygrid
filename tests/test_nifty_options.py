import unittest
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from index_options import (
    NIFTY,
    IndexOptionsState,
    _is_market_open,
    _normalize_chain,
    index_options_json,
)


class NiftyOptionsTests(unittest.TestCase):
    def test_contract_is_isolated_and_native(self):
        state = IndexOptionsState(SimpleNamespace(timezone="Asia/Kolkata"), NIFTY)
        state.set_snapshot(
            {
                "last_price": 25000.0,
                "oc": _normalize_chain(
                    {
                        "oc": {
                            "25000.000000": {
                                "ce": {"last_price": 120.0, "oi": 1000, "volume": 2000, "security_id": 101},
                                "pe": {"last_price": 110.0, "oi": 900, "volume": 1800, "security_id": 102},
                            }
                        }
                    }
                ),
            },
            ["2026-09-17", "2026-09-24"],
            "2026-09-17",
        )
        payload = index_options_json(state)
        self.assertEqual(payload["symbol"], "NIFTY")
        self.assertEqual(payload["security_id"], "13")
        self.assertEqual(payload["exchange_segment"], "IDX_I")
        self.assertEqual(payload["instrument"], "INDEX")
        self.assertEqual(payload["status"], "LIVE")
        self.assertEqual(payload["underlying_ltp"], 25000.0)
        self.assertEqual(payload["expiry"], "2026-09-17")
        self.assertFalse(payload["synthetic_data"])
        self.assertEqual(payload["storage"], "RAM_ONLY")
        self.assertIn(payload["market_status"], ("OPEN", "CLOSED"))
        self.assertIsInstance(payload["market_open"], bool)
        self.assertEqual(len(payload["strikes"]), 1)
        self.assertEqual(payload["strikes"][0]["strike"], 25000.0)
        self.assertEqual(payload["strikes"][0]["ce"]["security_id"], 101)
        self.assertEqual(payload["strikes"][0]["pe"]["security_id"], 102)

    def test_market_session_hours(self):
        tz = ZoneInfo("Asia/Kolkata")
        self.assertFalse(_is_market_open(datetime(2026, 9, 13, 12, 0, tzinfo=tz)))  # Sunday
        self.assertFalse(_is_market_open(datetime(2026, 9, 14, 9, 14, tzinfo=tz)))
        self.assertTrue(_is_market_open(datetime(2026, 9, 14, 9, 15, tzinfo=tz)))
        self.assertTrue(_is_market_open(datetime(2026, 9, 14, 15, 29, tzinfo=tz)))
        self.assertFalse(_is_market_open(datetime(2026, 9, 14, 15, 30, tzinfo=tz)))

    def test_normalize_chain_sorts_strikes(self):
        rows = _normalize_chain(
            {
                "oc": {
                    "25100": {"ce": {}, "pe": {}},
                    "24900": {"ce": {}, "pe": {}},
                }
            }
        )
        self.assertEqual([row["strike"] for row in rows], [24900.0, 25100.0])


if __name__ == "__main__":
    unittest.main()
