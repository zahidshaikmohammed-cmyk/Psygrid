from __future__ import annotations

import time
import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from feed import LiveFeed


class DhanLttNormalizationTests(unittest.TestCase):
    def test_current_dhan_utc_epoch_is_preserved(self):
        now = int(time.time())
        normalized = LiveFeed._parse_ltt(now, "Asia/Kolkata")
        self.assertIsNotNone(normalized)
        self.assertLessEqual(abs(normalized - now), 1)

    def test_exchange_wall_clock_epoch_is_corrected_by_timezone_offset(self):
        now = time.time()
        local = datetime.fromtimestamp(now, ZoneInfo("Asia/Kolkata"))
        wall_clock_epoch = int(
            local.replace(tzinfo=None).replace(tzinfo=timezone.utc).timestamp()
        )

        normalized = LiveFeed._parse_ltt(wall_clock_epoch, "Asia/Kolkata")
        self.assertIsNotNone(normalized)
        self.assertLessEqual(abs(normalized - int(now)), 2)

    def test_sdk_utc_clock_string_with_exchange_wall_clock_is_corrected(self):
        now = time.time()
        local = datetime.fromtimestamp(now, ZoneInfo("Asia/Kolkata"))
        sdk_clock = local.strftime("%H:%M:%S")

        normalized = LiveFeed._parse_ltt(sdk_clock, "Asia/Kolkata")
        self.assertIsNotNone(normalized)
        self.assertLessEqual(abs(normalized - int(now)), 2)

    def test_genuinely_future_timestamp_is_rejected(self):
        future = int(time.time()) + 3600
        normalized = LiveFeed._parse_ltt(future, "Asia/Kolkata")
        self.assertIsNone(normalized)


if __name__ == "__main__":
    unittest.main()
