import unittest
from types import SimpleNamespace
from datetime import datetime
from zoneinfo import ZoneInfo

from session import SessionManager


class SessionBoundaryTests(unittest.TestCase):
    def setUp(self):
        settings = SimpleNamespace(
            timezone="Asia/Kolkata",
            market_start="09:15",
            market_end="15:15",
        )
        self.manager = SessionManager(settings, None, None, None, [])
        self.tz = ZoneInfo("Asia/Kolkata")

    def test_market_opens_at_0915(self):
        self.assertTrue(self.manager.in_market(datetime(2026, 9, 1, 9, 15, tzinfo=self.tz)))

    def test_market_closed_before_0915(self):
        self.assertFalse(self.manager.in_market(datetime(2026, 9, 1, 9, 14, 59, tzinfo=self.tz)))

    def test_market_closes_at_1515(self):
        self.assertFalse(self.manager.in_market(datetime(2026, 9, 1, 15, 15, tzinfo=self.tz)))

    def test_market_open_before_1515(self):
        self.assertTrue(self.manager.in_market(datetime(2026, 9, 1, 15, 14, 59, tzinfo=self.tz)))


if __name__ == "__main__":
    unittest.main()
