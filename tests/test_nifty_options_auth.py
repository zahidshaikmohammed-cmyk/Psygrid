import unittest
from types import SimpleNamespace
from unittest.mock import patch

from dhan_auth import DhanTokenRateLimited
from nifty_options import NiftyOptionsManager


class FakeDhanAPI:
    def __init__(self):
        self.settings = None
        self.expiry_calls = 0

    def option_expiry_list(self, _instrument):
        self.expiry_calls += 1
        if self.expiry_calls == 1:
            raise RuntimeError("Dhan API HTTP error: 401 Client Error: Unauthorized")
        return ["2026-09-17"]


class NiftyOptionsAuthTests(unittest.TestCase):
    def test_401_refreshes_token_and_retries(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", access_token="stale")
        api = FakeDhanAPI()
        manager = NiftyOptionsManager(settings, api)

        def refresh(current_settings, force=False):
            self.assertTrue(force)
            current_settings.access_token = "fresh"

        with patch("nifty_options.refresh_access_token", side_effect=refresh) as refresh_mock:
            expiries = manager._load_expiries()

        self.assertEqual(expiries, ["2026-09-17"])
        self.assertEqual(settings.access_token, "fresh")
        self.assertEqual(api.expiry_calls, 2)
        refresh_mock.assert_called_once()

    def test_token_generation_rate_limit_enters_cooldown(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata", access_token="stale")
        api = FakeDhanAPI()
        manager = NiftyOptionsManager(settings, api)

        with patch(
            "nifty_options.refresh_access_token",
            side_effect=DhanTokenRateLimited("rate limited", retry_after=120),
        ) as refresh_mock:
            with self.assertRaises(DhanTokenRateLimited):
                manager._load_expiries()
            with self.assertRaises(RuntimeError) as raised:
                manager._load_expiries()

        self.assertIn("cooldown active", str(raised.exception))
        self.assertEqual(refresh_mock.call_count, 1)
        self.assertEqual(api.expiry_calls, 2)


if __name__ == "__main__":
    unittest.main()
