from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed


class DhanConnectionLimited(RuntimeError):
    """Dhan rejected the market-feed connection because of limits."""


class LiveFeed:
    """One persistent Dhan v2 Full WebSocket feeding genuine market data into RAM."""

    NORMAL_INITIAL_BACKOFF = 5.0
    NORMAL_MAX_BACKOFF = 120.0
    RATE_LIMIT_COOLDOWN = 300.0
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS = 5
    NO_MESSAGE_WATCHDOG_SECONDS = 25.0

    def __init__(self, settings, state, instruments):
        self.settings = settings
        self.state = state
        self.instruments = instruments
        self._feed = None
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
        tolerance = cls.FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        now_int = int(now_epoch)
        if epoch <= now_int + tolerance:
            return epoch
        offset = cls._timezone_offset_seconds(timezone_name, now_epoch)
        if offset <= 0:
            return None
        if abs((epoch - now_epoch) - offset) <= tolerance:
            corrected = epoch - offset
            return corrected if corrected <= now_int + tolerance else None
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
        if not text:
            return None
        if text.isdigit():
            epoch = int(text)
            return cls._normalize_future_epoch(epoch, timezone_name, now_epoch) if epoch > 0 else None
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                now_utc = datetime.now(timezone.utc)
                parsed = parsed.replace(year=now_utc.year, month=now_utc.month, day=now_utc.day)
                return cls._normalize_future_epoch(int(parsed.timestamp()), timezone_name, now_epoch)
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

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _store_market_context(self, security_id: str, data: dict, ltt_epoch: int | None = None) -> None:
        depth = data.get("depth")
        normalized_depth = []
        if isinstance(depth, list):
            for level in depth[:5]:
                if not isinstance(level, dict):
                    continue
                bid = self._to_float(level.get("bid_price"))
                ask = self._to_float(level.get("ask_price"))
                try:
                    bid_qty = int(level.get("bid_quantity", 0) or 0)
                    ask_qty = int(level.get("ask_quantity", 0) or 0)
                    bid_orders = int(level.get("bid_orders", 0) or 0)
                    ask_orders = int(level.get("ask_orders", 0) or 0)
                except (TypeError, ValueError):
                    continue
                normalized_depth.append({
                    "bid_price": bid if bid and bid > 0 else None,
                    "ask_price": ask if ask and ask > 0 else None,
                    "bid_qty": max(0, bid_qty),
                    "ask_qty": max(0, ask_qty),
                    "bid_orders": max(0, bid_orders),
                    "ask_orders": max(0, ask_orders),
                })
        context = getattr(self.state, "market_context", None)
        if context is None:
            context = {}
            self.state.market_context = context
        existing = dict(context.get(security_id, {}))
        for src, dst in (("open", "day_open"), ("high", "day_high"), ("low", "day_low")):
            value = self._to_float(data.get(src))
            if value is not None and value > 0:
                existing[dst] = value
        ltp = self._to_float(data.get("LTP"))
        if ltp is not None and ltp > 0:
            existing["ltp"] = ltp
        if normalized_depth:
            existing["depth"] = normalized_depth
            best = normalized_depth[0]
            existing["best_bid"] = best.get("bid_price")
            existing["best_ask"] = best.get("ask_price")
            existing["bid_qty"] = best.get("bid_qty")
            existing["ask_qty"] = best.get("ask_qty")
            existing["bid_orders"] = best.get("bid_orders")
            existing["ask_orders"] = best.get("ask_orders")
        if ltt_epoch is not None:
            existing["ltt_epoch"] = ltt_epoch
        existing["received_epoch"] = time.time()
        existing["source"] = "DHAN_WEBSOCKET_FULL"
        context[security_id] = existing

    def _store_prev_close(self, security_id: str, value) -> None:
        close = self._to_float(value)
        if close is None or close <= 0:
            return
        context = getattr(self.state, "market_context", None)
        if context is None:
            context = {}
            self.state.market_context = context
        existing = dict(context.get(security_id, {}))
        existing["prev_close"] = close
        existing["received_epoch"] = time.time()
        existing["source"] = "DHAN_WEBSOCKET_FULL"
        context[security_id] = existing

    def _handle_packet(self, data) -> None:
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        if packet_type in {"previous close", "prev close", "previous day"}:
            security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
            if security_id in self.state.instruments:
                self._store_prev_close(security_id, data.get("prev_close"))
            return
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
            avg_price = data.get("avg_price", data.get("ATP", data.get("atp", data.get("average_price"))))
            avg_price = float(avg_price) if avg_price not in (None, "") else None
        except (TypeError, ValueError):
            return
        if ltt_epoch is None or ltp <= 0 or volume < 0:
            return
        self._store_market_context(security_id, data, ltt_epoch)
        quote = dict(data)
        quote.update({"LTT_EPOCH": ltt_epoch, "LTP": ltp, "volume": volume, "LTQ": ltq})
        if avg_price is not None:
            quote["average_price"] = avg_price
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
            # Let the official SDK own its asyncio receive loop and ping/pong.
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
        self.state.market_context = {}
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
