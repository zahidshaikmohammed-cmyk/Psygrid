from __future__ import annotations

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed


class DhanConnectionLimited(RuntimeError):
    """Dhan rejected the market-feed connection because of limits."""


class LiveFeed:
    """One persistent Dhan Full WebSocket feeding native 1m OHLCV."""

    NORMAL_INITIAL_BACKOFF = 5.0
    NORMAL_MAX_BACKOFF = 120.0
    RATE_LIMIT_COOLDOWN = 300.0
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS = 5
    NO_MESSAGE_WATCHDOG_SECONDS = 25.0

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed: Optional[MarketFeed] = None
        self._thread: Optional[threading.Thread] = None
        self._watchdog: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()
        self._connection_stop = threading.Event()
        self._lock = threading.Lock()
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._connection_started_epoch = 0.0
        self._connection_message_baseline = 0

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        subscriptions = [(MarketFeed.NSE, item.security_id, MarketFeed.Full) for item in self.instruments]
        return MarketFeed(
            context,
            subscriptions,
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

    @staticmethod
    def _describe_error(error) -> str:
        cls = type(error).__name__
        text = str(error).strip() or repr(error)
        details = []
        for attr in ("code", "reason", "status_code"):
            value = getattr(error, attr, None)
            if value not in (None, ""):
                details.append(f"{attr}={value}")
        return f"{cls}: {text}" + (f"; {', '.join(details)}" if details else "")

    @staticmethod
    def _is_rate_limited_error(error) -> bool:
        text = str(error).lower()
        return (
            "429" in text or "805" in text or "too many requests" in text
            or ("too many" in text and "connection" in text)
            or "connection limit" in text
        )

    def _on_connect(self, _feed) -> None:
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._connection_started_epoch = time.time()
        self._connection_message_baseline = self.state.feed_messages
        self._connection_stop.clear()
        self.state.mark_websocket_connected(len(self.instruments))

    def _on_close(self, _feed) -> None:
        if not self._stop_requested.is_set():
            self.state.mark_websocket_reconnecting("websocket closed by peer")

    def _on_error(self, _feed, error) -> None:
        if not self._stop_requested.is_set():
            self.state.mark_websocket_error("websocket:" + self._describe_error(error))

    @staticmethod
    def _timezone_offset_seconds(timezone_name: str, now_epoch: float) -> int:
        try:
            tz = ZoneInfo(timezone_name)
            offset = datetime.fromtimestamp(now_epoch, timezone.utc).astimezone(tz).utcoffset()
            return int(offset.total_seconds()) if offset is not None else 0
        except Exception:
            return 0

    @classmethod
    def _normalize_future_epoch(cls, epoch: int, timezone_name: str, now_epoch: float) -> int | None:
        if epoch <= int(now_epoch) + cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            return epoch
        offset = cls._timezone_offset_seconds(timezone_name, now_epoch)
        if offset <= 0:
            return None
        if abs((epoch - now_epoch) - offset) <= cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            corrected = epoch - offset
            return corrected if corrected <= int(now_epoch) + cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS else None
        return None

    @classmethod
    def _parse_ltt(cls, value, timezone_name: str = "Asia/Kolkata") -> int | None:
        if value in (None, ""):
            return None
        now_epoch = time.time()
        if isinstance(value, (int, float)):
            epoch = int(value)
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch) if epoch > 0 else None
        text = str(value).strip()
        if text.isdigit():
            epoch = int(text)
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch) if epoch > 0 else None
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt)
                tz = ZoneInfo(timezone_name)
                local_now = datetime.fromtimestamp(now_epoch, tz)
                candidates = []
                for day_delta in (-1, 0, 1):
                    local_date = local_now.date() + timedelta(days=day_delta)
                    local = parsed.replace(year=local_date.year, month=local_date.month, day=local_date.day, tzinfo=tz)
                    candidates.append(int(local.timestamp()))
                nearest = min(candidates, key=lambda epoch: abs(epoch - now_epoch))
                return nearest if nearest <= int(now_epoch) + cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS else None
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return cls._normalize_future_epoch(int(parsed.timestamp()), timezone_name, now_epoch)
        except ValueError:
            return None

    def _on_message(self, feed, data) -> None:
        if not isinstance(data, dict):
            return
        self.state.record_feed_message(str(data.get("type", "UNKNOWN")))
        if str(data.get("type", "")).strip().lower() == "error":
            code = data.get("error_code", data.get("code"))
            message = str(data.get("message", data.get("error_message", "feed error")))
            description = f"Dhan feed error code={code}: {message}"
            self.state.mark_websocket_error(description)
            if self._is_rate_limited_error(f"{code} {message}"):
                self.state.mark_websocket_reconnecting("Dhan rate/connection limit; closing for clean reconnect")
            try:
                feed.close_connection()
            except Exception as exc:
                self.state.mark_websocket_error(description + "; close_request:" + self._describe_error(exc))
            return
        self._handle_packet(data)

    def _handle_packet(self, data) -> None:
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
        if not security_id or security_id not in self.state.instruments:
            return

        if packet_type in {"previous close", "prev close", "previous day"}:
            self.state.set_market_reference(security_id, previous_close=data.get("prev_close", data.get("previous_close")))
            return

        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return

        ltt_epoch = self._parse_ltt(
            data.get("LTT", data.get("ltt", data.get("last_trade_time"))),
            self.settings.timezone,
        )
        try:
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
        except (TypeError, ValueError):
            return
        if ltt_epoch is None or ltp <= 0 or volume < 0:
            return

        # Keep only fields belonging to the 1m OHLCV/reference contract.
        open_value = data.get("open")
        if open_value is None and isinstance(data.get("ohlc"), dict):
            open_value = data["ohlc"].get("open")
        if open_value is not None:
            self.state.set_market_reference(security_id, today_open=open_value)

        quote = {"LTT_EPOCH": ltt_epoch, "LTP": ltp, "volume": volume, "LTQ": ltq}
        self.state.update_quote(security_id, quote)
        self.state.record_live_quote(security_id, ltt_epoch)

    def _watch_connection(self, feed) -> None:
        started = self._connection_started_epoch
        baseline = self._connection_message_baseline
        while not self._stop_requested.is_set() and not self._connection_stop.wait(2.0):
            if self.state.session_status != "LIVE":
                continue
            if time.time() - started < self.NO_MESSAGE_WATCHDOG_SECONDS:
                continue
            if self.state.feed_messages > baseline:
                return
            self.state.mark_websocket_error(
                f"websocket:No market-feed messages received for {int(self.NO_MESSAGE_WATCHDOG_SECONDS)}s after connect"
            )
            try:
                feed.close_connection()
            except Exception as exc:
                self.state.mark_websocket_error("websocket watchdog close failed: " + self._describe_error(exc))
            return

    def _run_connected_session(self, feed) -> None:
        self._connection_stop.clear()
        self._watchdog = threading.Thread(
            target=self._watch_connection,
            args=(feed,),
            daemon=True,
            name="psygrid-dhan-watchdog",
        )
        self._watchdog.start()
        try:
            feed.run()
        finally:
            self._connection_stop.set()
            watchdog = self._watchdog
            self._watchdog = None
            if watchdog is not None and watchdog is not threading.current_thread():
                watchdog.join(timeout=2)

    def _close_feed(self, feed) -> None:
        if feed is None:
            return
        self._connection_stop.set()
        try:
            feed.close_connection()
        except Exception:
            pass
        try:
            if feed.loop and not feed.loop.is_closed():
                feed.loop.close()
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop_requested.is_set():
            feed = None
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.set_feed_status("CONNECTING")
                self._run_connected_session(feed)
                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_reconnecting("Dhan feed loop ended; reconnecting")
            except Exception as exc:
                message = self._describe_error(exc)
                self.state.mark_websocket_error("websocket:" + message)
                if self._is_rate_limited_error(message):
                    self.state.mark_websocket_reconnecting(
                        f"Dhan rate/connection limit; retrying in {int(self.RATE_LIMIT_COOLDOWN)}s"
                    )
                    self._backoff = self.RATE_LIMIT_COOLDOWN
                else:
                    self.state.mark_websocket_reconnecting(
                        f"websocket reconnect in {int(self._backoff)}s; cause={message}"
                    )
            finally:
                self._close_feed(feed)
                with self._lock:
                    if self._feed is feed:
                        self._feed = None
            if self._stop_requested.is_set():
                break
            self._stop_requested.wait(self._backoff)
            if self._backoff >= self.RATE_LIMIT_COOLDOWN:
                self._backoff = self.NORMAL_INITIAL_BACKOFF
            else:
                self._backoff = min(self._backoff * 2.0, self.NORMAL_MAX_BACKOFF)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-dhan-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self._connection_stop.set()
        self.state.set_feed_status("STOPPING")
        with self._lock:
            feed = self._feed
        if feed is not None:
            try:
                feed.close_connection()
            except Exception:
                pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=8)
        self._thread = None
        with self._lock:
            self._feed = None
        self.state.set_feed_status("STOPPED")
