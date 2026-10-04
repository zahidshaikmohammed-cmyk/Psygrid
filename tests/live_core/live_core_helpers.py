"""Helpers shared by the Live Core tests: a fixed IST clock, fake Dhan endpoints and tick drivers."""

from __future__ import annotations

from datetime import datetime
from typing import ClassVar
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
# Monday 2026-10-05 is an ordinary trading day.
MONDAY = (2026, 10, 5)


def ist(hour: int, minute: int, second: int = 0, day=MONDAY) -> datetime:
    return datetime(*day, hour, minute, second, tzinfo=IST)


class Clock:
    """A settable wall clock shared by the runtime (aware IST datetime) and the state (epoch)."""

    def __init__(self, when: datetime):
        self.when = when

    def now(self) -> datetime:
        return self.when

    def epoch(self) -> float:
        return self.when.timestamp()

    def set(self, when: datetime) -> None:
        self.when = when


class FakeDhanAPI:
    def __init__(self, settings=None, history=None):
        self.settings = settings
        self.history = history or {}
        self.history_calls: list[str] = []
        self.verify_calls = 0
        self.verify_error: Exception | None = None
        self.snapshot: dict = {}

    def verify_data_access(self):
        self.verify_calls += 1
        if self.verify_error is not None:
            raise self.verify_error
        return {"dataPlan": "Active"}

    def quote_snapshot(self, instruments):
        return dict(self.snapshot)

    def load_today_completed_intraday(self, item, interval):
        self.history_calls.append(item.symbol)
        return list(self.history.get(item.symbol, []))


class FakeFeed:
    """Stands in for LiveCoreFeed where a test only needs the lifecycle calls."""

    instances: ClassVar[list[FakeFeed]] = []

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = list(instruments)
        self.started = False
        self.stopped = False
        FakeFeed.instances.append(self)

    def start(self):
        self.started = True
        self.state.mark_websocket_connected(len(self.instruments))

    def stop(self):
        self.stopped = True
        self.state.set_feed_status("STOPPED")

    def thread_alive(self):
        return self.started and not self.stopped

    def lifecycle(self):
        return {
            "feed_thread_alive": self.thread_alive(),
            "connection_cycles": 1,
            "feeds_closed": 1 if self.stopped else 0,
            "event_loops_closed": 1 if self.stopped else 0,
            "event_loops_leaked": 0,
        }


class FakeSettings:
    def __init__(self):
        self.client_id = "1000000001"
        self.access_token = "test-token"
        self.timezone = "Asia/Kolkata"
        self.max_live_age_seconds = 30


def tick_payload(security_id: str, ltt_epoch: int, ltp: float, cumulative_volume: int, ltq: int = 1) -> dict:
    return {"LTT_EPOCH": ltt_epoch, "LTP": ltp, "volume": cumulative_volume, "LTQ": ltq}


def feed_minutes(state, series_ids, start_epoch: int, minutes: int, base_price: float = 100.0) -> None:
    """Drive real trades through the state's feed interface: three trades per stock per minute."""
    cumulative = dict.fromkeys(series_ids, 1000)
    for minute in range(minutes):
        for offset, second in enumerate((5, 25, 50)):
            ltt = start_epoch + minute * 60 + second
            for position, security_id in enumerate(series_ids):
                cumulative[security_id] += 10 + offset
                price = base_price + position * 0.01 + minute * 0.05 + offset * 0.02
                state.update_quote(security_id, tick_payload(security_id, ltt, price, cumulative[security_id]))
                state.record_live_quote(security_id, ltt)
