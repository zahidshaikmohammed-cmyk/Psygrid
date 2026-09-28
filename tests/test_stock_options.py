import unittest
from types import SimpleNamespace
from unittest.mock import patch

from dhan_auth import DhanTokenRateLimited
from stock_options import (
    NIFTY50_SYMBOLS,
    StockOptionsManager,
    StockOptionState,
    _is_market_open,
    _normalize_chain,
    stock_options_json,
    stock_options_listing_json,
)


class FakeDhanAPI:
    def __init__(self):
        self.settings = None
        self.expiry_calls = {}
        self.chain_calls = {}
        self.expiry_error_once = set()

    def option_expiry_list(self, item):
        self.expiry_calls[item.symbol] = self.expiry_calls.get(item.symbol, 0) + 1
        if item.symbol in self.expiry_error_once and self.expiry_calls[item.symbol] == 1:
            raise RuntimeError("Dhan API HTTP error: 401 Client Error: Unauthorized")
        return ["2026-09-30"]

    def option_chain(self, item, expiry):
        self.chain_calls[item.symbol] = self.chain_calls.get(item.symbol, 0) + 1
        return {
            "data": {
                "last_price": 1000.0 + self.chain_calls[item.symbol],
                "oc": {"1000": {"ce": {"security_id": "1"}, "pe": {"security_id": "2"}}},
            }
        }


def _manager(dhan_api, resolved: dict[str, str], settings=None):
    """Builds a StockOptionsManager with a controlled resolution result,
    bypassing the real Dhan instrument-master network fetch entirely."""
    settings = settings or SimpleNamespace(timezone="Asia/Kolkata", access_token="tok")
    with patch("stock_options.fetch_nse_equity_security_ids", return_value=resolved):
        return StockOptionsManager(settings, dhan_api)


class NIFTY50UniverseTests(unittest.TestCase):
    def test_exactly_50_unique_symbols(self):
        self.assertEqual(len(NIFTY50_SYMBOLS), 50)
        self.assertEqual(len(set(NIFTY50_SYMBOLS)), 50)


class StockOptionStateTests(unittest.TestCase):
    def test_contract_shape(self):
        state = StockOptionState("RELIANCE", SimpleNamespace(timezone="Asia/Kolkata"), security_id="500", exchange_segment="NSE_EQ")
        state.set_snapshot(
            {"last_price": 3000.0, "oc": _normalize_chain({"oc": {"3000": {"ce": {}, "pe": {}}}})},
            ["2026-09-30"], "2026-09-30", analytics={"pcr": 1.1},
        )
        payload = stock_options_json(SimpleNamespace(snapshot=lambda symbol: state.snapshot()), "RELIANCE")
        self.assertEqual(payload["symbol"], "RELIANCE")
        self.assertEqual(payload["security_id"], "500")
        self.assertEqual(payload["exchange_segment"], "NSE_EQ")
        self.assertEqual(payload["status"], "LIVE")
        self.assertFalse(payload["synthetic_data"])
        self.assertEqual(payload["storage"], "RAM_ONLY")
        self.assertEqual(payload["analytics"], {"pcr": 1.1})
        self.assertEqual(len(payload["strikes"]), 1)

    def test_market_session_hours(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Kolkata")
        self.assertFalse(_is_market_open(datetime(2026, 9, 13, 12, 0, tzinfo=tz)))  # Sunday
        self.assertTrue(_is_market_open(datetime(2026, 9, 14, 9, 15, tzinfo=tz)))
        self.assertFalse(_is_market_open(datetime(2026, 9, 14, 15, 30, tzinfo=tz)))


class StockOptionsManagerResolutionTests(unittest.TestCase):
    def test_resolves_symbols_independently_of_the_equity_universe(self):
        resolved = {symbol: str(i) for i, symbol in enumerate(NIFTY50_SYMBOLS[:3])}
        manager = _manager(FakeDhanAPI(), resolved)

        self.assertEqual(len(manager.instruments), 3)
        self.assertEqual(len(manager.states), 50)  # every NIFTY 50 symbol gets a state, resolved or not
        unresolved = [s for s in NIFTY50_SYMBOLS if s not in resolved]
        for symbol in unresolved:
            self.assertEqual(manager.states[symbol].status, "UNRESOLVED")
            self.assertIn(symbol, manager.resolution_errors)

    def test_a_symbol_missing_from_stocks_json_can_still_resolve(self):
        # SBILIFE/SHRIRAMFIN are real NIFTY 50 constituents not present in
        # the unrelated 989-equity universe - resolution must not depend on
        # that list at all.
        resolved = {"SBILIFE": "21808", "SHRIRAMFIN": "4306"}
        manager = _manager(FakeDhanAPI(), resolved)

        self.assertEqual(manager.instruments["SBILIFE"].security_id, "21808")
        self.assertEqual(manager.states["SBILIFE"].status, "PENDING")
        self.assertNotIn("SBILIFE", manager.resolution_errors)

    def test_instrument_master_fetch_failure_marks_every_symbol_unresolved_not_crashed(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", access_token="tok")
        with patch("stock_options.fetch_nse_equity_security_ids", side_effect=RuntimeError("network down")):
            manager = StockOptionsManager(settings, FakeDhanAPI())

        self.assertEqual(len(manager.instruments), 0)
        self.assertEqual(len(manager.resolution_errors), 50)
        self.assertTrue(all("network down" in err for err in manager.resolution_errors.values()))

    def test_listing_reports_resolution_and_rotation_metadata(self):
        resolved = {symbol: str(i) for i, symbol in enumerate(NIFTY50_SYMBOLS[:5])}
        manager = _manager(FakeDhanAPI(), resolved)

        listing = stock_options_listing_json(manager)
        self.assertEqual(listing["universe"], "NIFTY_50_STOCK_OPTIONS")
        self.assertEqual(listing["symbol_count"], 50)
        self.assertEqual(listing["resolved_count"], 5)
        self.assertEqual(listing["rotation_cycle_seconds"], round(5 * 3.2, 1))
        self.assertEqual(len(listing["stocks"]), 50)
        self.assertFalse(listing["synthetic_data"])


class StockOptionsManagerPollingTests(unittest.TestCase):
    def test_poll_one_populates_state_with_real_chain_data(self):
        api = FakeDhanAPI()
        manager = _manager(api, {"RELIANCE": "500"})

        manager._poll_one("RELIANCE")

        snap = manager.snapshot("RELIANCE")
        self.assertEqual(snap["status"], "LIVE")
        self.assertEqual(snap["expiry"], "2026-09-30")
        self.assertEqual(len(snap["strikes"]), 1)
        self.assertIn("analytics", snap)

    def test_poll_one_records_error_without_crashing_other_symbols(self):
        class FlakyAPI(FakeDhanAPI):
            def option_expiry_list(self, item):
                if item.symbol == "RELIANCE":
                    raise RuntimeError("DHAN_RELIANCE_OPTIONS_NO_ACTIVE_EXPIRIES")
                return super().option_expiry_list(item)

        manager = _manager(FlakyAPI(), {"RELIANCE": "500", "TCS": "501"})
        manager._poll_one("RELIANCE")
        manager._poll_one("TCS")

        reliance = manager.snapshot("RELIANCE")
        tcs = manager.snapshot("TCS")
        self.assertEqual(reliance["status"], "ERROR")
        self.assertIn("error", reliance)
        self.assertEqual(tcs["status"], "LIVE")

    def test_snapshot_for_unknown_symbol_never_fabricates_data(self):
        manager = _manager(FakeDhanAPI(), {})
        snap = manager.snapshot("NOTASYMBOL")
        self.assertEqual(snap["status"], "UNKNOWN_SYMBOL")
        self.assertNotIn("strikes", snap)


class StockOptionsManagerAuthRetryTests(unittest.TestCase):
    def test_401_refreshes_token_and_retries(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", access_token="stale")
        api = FakeDhanAPI()
        api.expiry_error_once.add("RELIANCE")
        manager = _manager(api, {"RELIANCE": "500"}, settings=settings)

        def refresh(current_settings, force=False):
            self.assertTrue(force)
            current_settings.access_token = "fresh"

        with patch("stock_options.refresh_access_token", side_effect=refresh) as refresh_mock:
            manager._poll_one("RELIANCE")

        self.assertEqual(manager.snapshot("RELIANCE")["status"], "LIVE")
        self.assertEqual(settings.access_token, "fresh")
        refresh_mock.assert_called_once()

    def test_token_generation_rate_limit_enters_cooldown(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", access_token="stale")
        api = FakeDhanAPI()
        api.expiry_error_once.add("RELIANCE")
        manager = _manager(api, {"RELIANCE": "500"}, settings=settings)

        with patch(
            "stock_options.refresh_access_token",
            side_effect=DhanTokenRateLimited("rate limited", retry_after=120),
        ):
            manager._poll_one("RELIANCE")

        snap = manager.snapshot("RELIANCE")
        self.assertEqual(snap["status"], "ERROR")


if __name__ == "__main__":
    unittest.main()
