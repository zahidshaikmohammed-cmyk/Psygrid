from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Optional
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed

from output import LIVE_TIMEFRAMES, _completed_rows, _ist_timestamp, _normalize_ohlcv, _price


NIFTY_SYMBOL = "NIFTY"
NIFTY_SECURITY_ID = "13"
NIFTY_EXCHANGE_SEGMENT = "IDX_I"
NIFTY_INSTRUMENT = "INDEX"


@dataclass(frozen=True)
class NiftyInstrument:
    security_id: str = NIFTY_SECURITY_ID
    exchange_segment: str = NIFTY_EXCHANGE_SEGMENT
    instrument: str = NIFTY_INSTRUMENT


class NiftyState:
    """RAM-only state for the NIFTY 50 index endpoint."""

    def __init__(self, settings):
        self.settings = settings
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.session_date: Optional[str] = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        self.last_ltp: Optional[float] = None
        self.last_ltt: Optional[int] = None
        self.last_tick_received_epoch: Optional[float] = None
        self.live_candles: list[dict] = []
        self.current_1m: Optional[dict] = None
        self.historical: dict[str, list[dict]] = {}
        self.prev_cumulative_volume = 0
        self.last_trade_key: Optional[tuple] = None
        self.feed_messages = 0
        self.quote_packets = 0
        self.websocket_reconnects = 0
        self.websocket_connected_at: Optional[str] = None

    def begin(self, session_date: str) -> None:
        with self.lock:
            self.session_date = session_date
            self.session_status = "LIVE"
            self.feed_status = "STARTING"
            self.last_feed_error = ""
            self.last_ltp = None
            self.last_ltt = None
            self.last_tick_received_epoch = None
            self.live_candles = []
            self.current_1m = None
            self.historical = {}
            self.prev_cumulative_volume = 0
            self.last_trade_key = None
            self.feed_messages = 0
            self.quote_packets = 0
            self.websocket_reconnects = 0
            self.websocket_connected_at = None

    def reset(self) -> None:
        with self.lock:
            self.session_date = None
            self.session_status = "CLOSED"
            self.feed_status = "STOPPED"
            self.last_feed_error = ""
            self.last_ltp = None
            self.last_ltt = None
            self.last_tick_received_epoch = None
            self.live_candles = []
            self.current_1m = None
            self.historical = {}
            self.prev_cumulative_volume = 0
            self.last_trade_key = None
            self.feed_messages = 0
            self.quote_packets = 0
            self.websocket_reconnects = 0
            self.websocket_connected_at = None

    def set_feed_status(self, status: str, error: str = "") -> None:
        with self.lock:
            self.feed_status = status
            if error:
                self.last_feed_error = error

    def record_message(self, packet_type: str) -> None:
        with self.lock:
            self.feed_messages += 1
            if packet_type.lower() in {"quote data", "quote", "full data", "full"}:
                self.quote_packets += 1

    def update_quote(self, quote: dict) -> None:
        try:
            ltp = float(quote["LTP"])
            ltt_epoch = int(quote["LTT_EPOCH"])
            cumulative_volume = int(quote.get("volume", 0) or 0)
            ltq = int(quote.get("LTQ", quote.get("ltq", 0)) or 0)
        except (KeyError, TypeError, ValueError):
            return
        if ltp <= 0 or ltt_epoch <= 0 or cumulative_volume < 0:
            return

        with self.lock:
            if self.session_status != "LIVE":
                return
            minute_key = ltt_epoch - (ltt_epoch % 60)
            current = self.current_1m
            if current is not None:
                current_minute = int(current["epoch"])
                if minute_key < current_minute:
                    return
                if minute_key > current_minute:
                    current["complete"] = True
                    self.live_candles.append(dict(current))
                    self.current_1m = None

            delta_volume = max(0, cumulative_volume - self.prev_cumulative_volume)
            self.prev_cumulative_volume = cumulative_volume
            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            self.last_ltp = ltp
            self.last_ltt = ltt_epoch
            self.last_tick_received_epoch = time.time()
            if trade_key == self.last_trade_key:
                return
            self.last_trade_key = trade_key

            candle = self.current_1m
            if candle is None:
                self.current_1m = {
                    "timestamp": minute_key,
                    "epoch": minute_key,
                    "open": ltp,
                    "high": ltp,
                    "low": ltp,
                    "close": ltp,
                    "volume": delta_volume,
                    "source": "DHAN_WEBSOCKET_FULL",
                    "complete": False,
                }
            else:
                candle["high"] = max(float(candle["high"]), ltp)
                candle["low"] = min(float(candle["low"]), ltp)
                candle["close"] = ltp
                candle["volume"] = int(candle["volume"]) + delta_volume

    def merge_today_1m_history(self, candles: list[dict]) -> None:
        with self.lock:
            existing = {}
            for candle in self.live_candles:
                if candle.get("complete", True):
                    item = dict(candle)
                    epoch = int(item.get("epoch", item["timestamp"]))
                    item["epoch"] = epoch
                    existing[epoch // 60] = item
            for candle in candles:
                if candle.get("complete", True):
                    item = dict(candle)
                    epoch = int(item.get("epoch", item["timestamp"]))
                    item["epoch"] = epoch
                    existing[epoch // 60] = item
            self.live_candles = [existing[key] for key in sorted(existing)]

    def set_historical(self, timeframe: str, candles: list[dict]) -> None:
        with self.lock:
            self.historical[timeframe] = sorted(
                (dict(c) for c in candles),
                key=lambda c: int(c["timestamp"]),
            )

    def finalize_current(self) -> None:
        with self.lock:
            if self.current_1m is not None:
                candle = dict(self.current_1m)
                candle["complete"] = True
                self.live_candles.append(candle)
                self.current_1m = None


class NiftyFeed:
    NORMAL_INITIAL_BACKOFF = 5.0
    NORMAL_MAX_BACKOFF = 120.0
    NO_MESSAGE_WATCHDOG_SECONDS = 25.0

    def __init__(self, settings, state: NiftyState):
        self.settings = settings
        self.state = state
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._watchdog: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connection_stop = threading.Event()
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._started = 0.0
        self._baseline = 0

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        return MarketFeed(
            context,
            [(MarketFeed.IDX, NIFTY_SECURITY_ID, MarketFeed.Full)],
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

    @staticmethod
    def _parse_ltt(value) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)) or str(value).isdigit():
            return int(value)
        text = str(value).strip()
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt).replace(
                    year=datetime.now().year,
                    month=datetime.now().month,
                    day=datetime.now().day,
                    tzinfo=ZoneInfo("UTC"),
                )
                return int(parsed.timestamp())
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
            return int(parsed.timestamp())
        except ValueError:
            return None

    def _on_connect(self, _feed) -> None:
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._started = time.time()
        self._baseline = self.state.feed_messages
        self._connection_stop.clear()
        self.state.set_feed_status("CONNECTED")
        with self.state.lock:
            self.state.websocket_connected_at = datetime.now(ZoneInfo("UTC")).isoformat()

    def _on_close(self, _feed) -> None:
        if not self._stop.is_set():
            self.state.set_feed_status("RECONNECTING", "NIFTY websocket closed by peer")
            self.state.websocket_reconnects += 1

    def _on_error(self, _feed, error) -> None:
        if not self._stop.is_set():
            self.state.set_feed_status("ERROR", f"NIFTY websocket:{type(error).__name__}:{error}")

    def _on_message(self, _feed, data) -> None:
        if not isinstance(data, dict):
            return
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        self.state.record_message(packet_type)
        if packet_type == "error":
            self.state.set_feed_status(
                "ERROR",
                f"Dhan NIFTY feed error code={data.get('error_code', data.get('code'))}: "
                f"{data.get('message', data.get('error_message', 'feed error'))}",
            )
            return
        if packet_type in {"previous close", "prev close", "previous day"}:
            return
        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return
        try:
            security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
            if security_id != NIFTY_SECURITY_ID:
                return
            ltt = self._parse_ltt(data.get("LTT", data.get("ltt", data.get("last_trade_time"))))
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
        except (TypeError, ValueError):
            return
        if ltt is None:
            return
        self.state.update_quote({
            "LTT_EPOCH": ltt,
            "LTP": ltp,
            "volume": volume,
            "LTQ": ltq,
        })

    def _watch(self, feed) -> None:
        started = self._started
        baseline = self._baseline
        while not self._stop.is_set() and not self._connection_stop.wait(2.0):
            if time.time() - started < self.NO_MESSAGE_WATCHDOG_SECONDS:
                continue
            if self.state.feed_messages > baseline:
                return
            self.state.set_feed_status(
                "ERROR",
                f"NIFTY websocket:No market-feed messages received for "
                f"{int(self.NO_MESSAGE_WATCHDOG_SECONDS)}s after connect",
            )
            try:
                feed.close_connection()
            except Exception:
                pass
            return

    def _run(self) -> None:
        while not self._stop.is_set():
            feed = None
            try:
                feed = self._build_feed()
                self._feed = feed
                self.state.set_feed_status("CONNECTING")
                self._connection_stop.clear()
                self._watchdog = threading.Thread(
                    target=self._watch, args=(feed,), daemon=True, name="psygrid-nifty-watchdog"
                )
                self._watchdog.start()
                feed.run()
                if self._stop.is_set():
                    break
                self.state.set_feed_status("RECONNECTING", "NIFTY feed loop ended; reconnecting")
            except Exception as exc:
                self.state.set_feed_status("RECONNECTING", f"NIFTY websocket:{type(exc).__name__}:{exc}")
            finally:
                self._connection_stop.set()
                try:
                    if feed is not None:
                        feed.close_connection()
                except Exception:
                    pass
                self._feed = None
            if self._stop.wait(self._backoff):
                break
            self._backoff = min(self._backoff * 2.0, self.NORMAL_MAX_BACKOFF)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-nifty-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._connection_stop.set()
        feed = self._feed
        if feed is not None:
            try:
                feed.close_connection()
            except Exception:
                pass
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=8)
        self._thread = None
        self._feed = None
        self.state.set_feed_status("STOPPED")


class NiftyManager:
    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self.state = NiftyState(settings)
        self.feed = NiftyFeed(settings, self.state)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.htf_threads: dict[str, threading.Thread] = {}
        self.instrument = NiftyInstrument()

    def now(self) -> datetime:
        return datetime.now(self.state.tz)

    def in_market(self, now: Optional[datetime] = None) -> bool:
        now = now or self.now()
        sh, sm = map(int, self.settings.market_start.split(":"))
        eh, em = map(int, self.settings.market_end.split(":"))
        return dt_time(sh, sm) <= now.time() < dt_time(eh, em)

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-nifty-session")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            self.feed.stop()
        except Exception:
            pass
        for worker in list(self.htf_threads.values()):
            if worker and worker is not threading.current_thread():
                worker.join(timeout=10)
        self.htf_threads.clear()
        self.state.reset()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=3)
        self.thread = None

    def _loop(self) -> None:
        started_for_date = None
        while not self.stop_event.is_set():
            now = self.now()
            if self.in_market(now):
                if started_for_date != now.date().isoformat():
                    self._start_session(now)
                    started_for_date = now.date().isoformat() if self.state.session_status == "LIVE" else None
            elif started_for_date is not None:
                self._end_session()
                started_for_date = None
            self.stop_event.wait(2.0)

    def _start_session(self, now: datetime) -> None:
        session_date = now.date().isoformat()
        self.state.begin(session_date)
        try:
            snapshot = self.dhan_api.quote_snapshot([self.instrument])
            self.state.prev_cumulative_volume = max(
                0, int(snapshot.get(NIFTY_SECURITY_ID, {}).get("volume", 0) or 0)
            )
        except Exception as exc:
            self.state.set_feed_status("STARTING", f"NIFTY quote seed: {exc}")
        try:
            self.state.merge_today_1m_history(
                self.dhan_api.load_today_completed_intraday(self.instrument, 1)
            )
        except Exception as exc:
            self.state.set_feed_status("STARTING", f"NIFTY history bootstrap: {exc}")
        self.feed.start()
        self._start_htf_workers()

    def _start_htf_workers(self) -> None:
        for interval, key in ((5, "5m"), (15, "15m"), (60, "1h")):
            worker = threading.Thread(
                target=self._htf_loop,
                args=(interval, key),
                daemon=True,
                name=f"psygrid-nifty-{key}",
            )
            self.htf_threads[key] = worker
            worker.start()

    def _htf_loop(self, interval: int, key: str) -> None:
        last_slot = -1
        while not self.stop_event.is_set() and self.state.session_status == "LIVE":
            now = self.now()
            if not self.in_market(now):
                return
            sh, sm = map(int, self.settings.market_start.split(":"))
            market_start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
            elapsed = int((now - market_start).total_seconds() // 60)
            if elapsed >= interval:
                slot = elapsed // interval
                if slot > last_slot:
                    last_slot = slot
                    try:
                        rows = self.dhan_api.load_recent_completed_intraday(
                            self.instrument,
                            interval,
                            4,
                        )
                        if rows:
                            self.state.set_historical(key, rows)
                    except Exception as exc:
                        self.state.last_feed_error = f"NIFTY history refresh {key}: {exc}"
            self.stop_event.wait(1.0)

    def _end_session(self) -> None:
        self.feed.stop()
        for worker in list(self.htf_threads.values()):
            if worker and worker is not threading.current_thread():
                worker.join(timeout=10)
        self.htf_threads.clear()
        self.state.finalize_current()
        self.state.reset()


def nifty_json(state: NiftyState) -> dict:
    with state.lock:
        candles_1m = _completed_rows([dict(c) for c in state.live_candles])
        historical = {
            timeframe: _completed_rows(
                [dict(c) for c in state.historical.get(timeframe, [])]
            )
            for timeframe in ("5m", "15m", "1h")
        }
        ltp = state.last_ltp
        ltt = state.last_ltt
        session_date = state.session_date
        session_status = state.session_status

    return {
        "service": "PSYGRID",
        "schema_version": "3.0",
        "symbol": NIFTY_SYMBOL,
        "security_id": NIFTY_SECURITY_ID,
        "exchange_segment": NIFTY_EXCHANGE_SEGMENT,
        "instrument": NIFTY_INSTRUMENT,
        "session": {
            "status": session_status,
            "date": session_date,
            "timezone": "Asia/Kolkata",
            "current_time_ist": datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
                "%Y-%m-%d %H:%M:%S IST"
            ),
        },
        "ltp": _price(ltp),
        "ltp_timestamp": _ist_timestamp(ltt),
        "timeframes": list(LIVE_TIMEFRAMES),
        "candle_source": {
            "1m": "DHAN_WEBSOCKET_FULL",
            "5m": "DHAN_NATIVE_HISTORICAL",
            "15m": "DHAN_NATIVE_HISTORICAL",
            "1h": "DHAN_NATIVE_HISTORICAL",
        },
        "synthetic_candles": False,
        "1m": [_normalize_ohlcv(row) for row in candles_1m],
        "5m": [_normalize_ohlcv(row) for row in historical["5m"]],
        "15m": [_normalize_ohlcv(row) for row in historical["15m"]],
        "1h": [_normalize_ohlcv(row) for row in historical["1h"]],
    }
