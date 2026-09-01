from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed


class DhanConnectionLimited(RuntimeError):
    """Dhan rejected the market-feed connection because of connection/rate limits."""


class LiveFeed:
    """One persistent Dhan v2 Quote WebSocket; live data flows continuously into RAM.

    Important: the DhanHQ SDK's high-level ``run()`` loop performs its own
    one-second reconnect cycle. That is dangerous when Dhan returns HTTP 429 or
    feed error 805 because it can create a reconnect storm. Psygrid therefore
    owns the reconnect loop and uses the SDK only for connection, subscription,
    binary parsing and keep-alive handling.
    """

    NORMAL_INITIAL_BACKOFF = 5.0
    NORMAL_MAX_BACKOFF = 120.0
    RATE_LIMIT_COOLDOWN = 300.0
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS = 5

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()
        self._backoff = self.NORMAL_INITIAL_BACKOFF

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        subscriptions = [
            (MarketFeed.NSE, item.security_id, MarketFeed.Quote)
            for item in self.instruments
        ]
        return MarketFeed(
            context,
            subscriptions,
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

    def _on_connect(self, _feed) -> None:
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self.state.mark_websocket_connected(len(self.instruments))

    def _on_close(self, _feed) -> None:
        if not self._stop_requested.is_set():
            self.state.mark_websocket_reconnecting("websocket closed")

    def _on_error(self, _feed, error) -> None:
        if not self._stop_requested.is_set():
            message = str(error)
            self.state.mark_websocket_error(f"websocket:{message}")

    @staticmethod
    def _timezone_offset_seconds(timezone_name: str, now_epoch: float) -> int:
        """Return the exchange timezone UTC offset at the current instant."""
        try:
            tz = ZoneInfo(timezone_name)
            offset = datetime.fromtimestamp(now_epoch, timezone.utc).astimezone(tz).utcoffset()
            return int(offset.total_seconds()) if offset is not None else 0
        except Exception:
            # TIMEZONE is validated by normal application startup. If a bad
            # value somehow reaches this helper, keep the strict future check
            # rather than inventing a timezone correction.
            return 0

    @classmethod
    def _normalize_future_epoch(cls, epoch: int, timezone_name: str, now_epoch: float) -> int | None:
        """Normalize a Dhan wall-clock epoch when it is exactly one TZ offset ahead.

        DhanHQ-py's MarketFeed currently converts the wire LTT epoch to a UTC
        HH:MM:SS string before returning the packet. Some live NSE packets have
        been observed with the exchange-local wall clock encoded as the epoch,
        which makes that returned clock appear exactly one Asia/Kolkata offset
        (19,800 seconds) in the future. We correct only that narrow, provable
        case. A genuinely future timestamp is still rejected.
        """
        tolerance = cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        if epoch <= int(now_epoch) + tolerance:
            return epoch

        offset = cls._timezone_offset_seconds(timezone_name, now_epoch)
        if offset <= 0:
            return None

        corrected = epoch - offset
        if corrected <= int(now_epoch) + tolerance:
            return corrected

        return None

    @classmethod
    def _parse_ltt(cls, value, timezone_name: str = "Asia/Kolkata") -> int | None:
        """Normalize Dhan LTT into a real UTC epoch.

        Dhan's wire protocol carries LTT as epoch seconds, while the current
        DhanHQ Python client converts that value to a UTC HH:MM:SS string before
        returning it from ``get_instrument_data``. Psygrid accepts both forms.

        A known Dhan/exchange-local wall-clock representation can appear one
        timezone offset ahead of real UTC. That representation is corrected only
        when subtracting the configured exchange offset brings the timestamp
        back to now (within the strict future tolerance). All other future
        timestamps are rejected, preserving the anti-future-timestamp safety gate.
        """
        if value in (None, ""):
            return None

        now_epoch = time.time()

        if isinstance(value, (int, float)):
            epoch = int(value)
            if epoch <= 0:
                return None
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch)

        text = str(value).strip()
        if not text:
            return None

        if text.isdigit():
            epoch = int(text)
            if epoch <= 0:
                return None
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch)

        # DhanHQ-py currently returns LTT as UTC HH:MM:SS. Parse it as UTC first,
        # then apply the same narrow future-offset correction used for raw epochs.
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                parsed = parsed.replace(
                    year=datetime.now(timezone.utc).year,
                    month=datetime.now(timezone.utc).month,
                    day=datetime.now(timezone.utc).day,
                )
                epoch = int(parsed.timestamp())
                return cls._normalize_future_epoch(epoch, timezone_name, now_epoch)
            except ValueError:
                continue

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            epoch = int(parsed.timestamp())
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch)
        except ValueError:
            return None

    @staticmethod
    def _is_rate_limited_error(error) -> bool:
        text = str(error).lower()
        return (
            "429" in text
            or "too many requests" in text
            or "too many" in text and "connection" in text
            or "connection limit" in text
            or "805" in text
        )

    def _on_message(self, _feed, data) -> None:
        if not isinstance(data, dict):
            return
        self.state.record_feed_message(str(data.get("type", "UNKNOWN")))

        # Dhan feed-disconnect packets are parsed by the SDK as Error packets.
        # Stop the read loop immediately so the outer loop applies a deliberate
        # cooldown instead of the SDK's one-second reconnect storm.
        if str(data.get("type", "")).strip().lower() == "error":
            code = data.get("error_code")
            message = str(data.get("message", "feed error"))
            if str(code) == "805" or self._is_rate_limited_error(f"{code} {message}"):
                raise DhanConnectionLimited(f"Dhan feed error 805/rate-limit: {message}")
            raise RuntimeError(f"Dhan feed error {code}: {message}")

        self._handle_packet(data)

    def _handle_packet(self, data) -> None:
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return

        security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
        if not security_id or security_id not in self.state.instruments:
            return

        ltt_epoch = self._parse_ltt(
            data.get("LTT", data.get("ltt", data.get("last_trade_time"))),
            self.settings.timezone,
        )
        try:
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
            avg_price = data.get(
                "avg_price",
                data.get("ATP", data.get("atp", data.get("average_price"))),
            )
            avg_price = float(avg_price) if avg_price not in (None, "") else None
        except (TypeError, ValueError):
            return

        if ltt_epoch is None or ltp <= 0 or volume < 0:
            return

        quote = dict(data)
        quote["LTT_EPOCH"] = ltt_epoch
        quote["LTP"] = ltp
        quote["volume"] = volume
        quote["LTQ"] = ltq
        if avg_price is not None:
            quote["average_price"] = avg_price
        self.state.update_quote(security_id, quote)
        self.state.record_live_quote(security_id, ltt_epoch)

    def _run_connected_session(self, feed) -> None:
        """Connect once and receive until the socket fails or shutdown is requested."""
        # Do NOT call feed.run(): DhanHQ-py's run() contains its own 1-second
        # reconnect loop. Psygrid owns reconnect/backoff to prevent 429 storms.
        feed.loop.run_until_complete(feed.connect())
        while not self._stop_requested.is_set():
            data = feed.loop.run_until_complete(feed.get_instrument_data())
            self._on_message(feed, data)

    def _close_feed(self, feed) -> None:
        if feed is None:
            return
        try:
            if feed.loop and not feed.loop.is_closed():
                feed.loop.run_until_complete(
                    asyncio.wait_for(feed.disconnect(), timeout=5.0)
                )
        except Exception:
            try:
                if feed.ws is not None:
                    feed.loop.run_until_complete(
                        asyncio.wait_for(feed.ws.close(), timeout=3.0)
                    )
            except Exception:
                pass
        finally:
            try:
                if feed.loop and not feed.loop.is_closed():
                    feed.loop.close()
            except Exception:
                pass

    def _run(self) -> None:
        while not self._stop_requested.is_set():
            feed = None
            connected = False
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.set_feed_status("CONNECTING")
                self._run_connected_session(feed)
                connected = True
                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_reconnecting("feed connection ended")
            except DhanConnectionLimited as exc:
                self.state.mark_websocket_error(str(exc))
                self.state.mark_websocket_reconnecting(
                    f"Dhan rate/connection limit; retrying in {int(self.RATE_LIMIT_COOLDOWN)}s"
                )
                self._backoff = self.RATE_LIMIT_COOLDOWN
            except Exception as exc:
                message = str(exc)
                self.state.mark_websocket_error(f"websocket:{message}")
                if self._is_rate_limited_error(message):
                    self.state.mark_websocket_reconnecting(
                        f"Dhan rate/connection limit; retrying in {int(self.RATE_LIMIT_COOLDOWN)}s"
                    )
                    self._backoff = self.RATE_LIMIT_COOLDOWN
                else:
                    self.state.mark_websocket_reconnecting(
                        f"websocket reconnect in {int(self._backoff)}s"
                    )
            finally:
                self._close_feed(feed)
                with self._lock:
                    if self._feed is feed:
                        self._feed = None

            if self._stop_requested.is_set():
                break

            delay = self._backoff
            self._stop_requested.wait(delay)
            if self._backoff >= self.RATE_LIMIT_COOLDOWN:
                # After a rate-limit cooldown, return to a conservative normal
                # retry schedule rather than repeatedly hammering the endpoint.
                self._backoff = self.NORMAL_INITIAL_BACKOFF
            elif connected:
                self._backoff = self.NORMAL_INITIAL_BACKOFF
            else:
                self._backoff = min(self._backoff * 2.0, self.NORMAL_MAX_BACKOFF)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._backoff = self.NORMAL_INITIAL_BACKOFF
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="psygrid-dhan-feed",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.state.set_feed_status("STOPPING")
        with self._lock:
            feed = self._feed
        if feed is not None:
            try:
                feed.loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout=8)
        self._thread = None
        with self._lock:
            self._feed = None
        self.state.set_feed_status("STOPPED")
