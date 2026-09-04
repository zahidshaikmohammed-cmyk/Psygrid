from __future__ import annotations

import asyncio
import json
import random
import threading
import time

from dhan_api import DhanAPI
from feed import DhanConnectionLimited, LiveFeed as BaseLiveFeed
from self_keepalive import SelfKeepAlive


class LiveFeed(BaseLiveFeed):
    """Runtime WebSocket feed with bounded quote/depth recovery."""

    SOCKET_READ_CHECK_SECONDS = 5.0
    PING_INTERVAL_SECONDS = 15.0
    PONG_TIMEOUT_SECONDS = 30.0
    REST_FALLBACK_AFTER_SECONDS = 5.0
    RESUBSCRIBE_COOLDOWN_SECONDS = 30.0
    BACKOFF_INITIAL_SECONDS = 1.0
    BACKOFF_MAX_SECONDS = 15.0
    NORMAL_RETRY_JITTER_SECONDS = 0.75
    RATE_LIMIT_COOLDOWN = 120.0

    def __init__(self, settings, state, instruments, dhan_api=None):
        super().__init__(settings, state, instruments)
        self.dhan_api = dhan_api or DhanAPI(settings)
        self._last_resubscribe: dict[str, float] = {}
        self._last_rest_fallback = 0.0
        self.self_keepalive = SelfKeepAlive("https://psygrid.onrender.com/public/live-a.json")

    def _market_hours(self) -> bool:
        return self.state.session_status == "LIVE"

    async def _heartbeat_and_health(self, feed) -> None:
        """Run health checks without injecting client-side ping traffic into Dhan's socket."""
        while not self._stop_requested.is_set():
            if self._market_hours():
                await self._health_pass(feed)
            await asyncio.sleep(1.0)

    async def _health_pass(self, feed) -> None:
        now = time.time()
        stale = []
        for item in self.instruments:
            health = self.state.freshness(item.security_id, now)
            age = health.get("data_age_seconds")
            if age is None or age > self.state.settings.max_live_age_seconds:
                stale.append(item)

        for item in stale:
            key = str(item.security_id)
            if now - self._last_resubscribe.get(key, 0.0) < self.RESUBSCRIBE_COOLDOWN_SECONDS:
                continue
            try:
                await feed.ws.send(json.dumps({
                    "RequestCode": 21,
                    "InstrumentCount": 1,
                    "InstrumentList": [{
                        "ExchangeSegment": item.exchange_segment,
                        "SecurityId": str(item.security_id),
                    }],
                }))
                self._last_resubscribe[key] = now
            except Exception as exc:
                raise RuntimeError(f"LIVE_HEALTH: failed to re-subscribe {item.symbol}") from exc

        # Dhan Quote API is a real-time snapshot, capped at one request/sec and
        # supporting up to 1000 instruments. Polling only every 5 seconds avoids
        # hammering the quote endpoint while still recovering well inside the
        # 30-second freshness contract.
        if stale and now - self._last_rest_fallback >= self.REST_FALLBACK_AFTER_SECONDS:
            try:
                snapshot = self.dhan_api.quote_snapshot(self.instruments)
                self.state.apply_rest_snapshot(snapshot)
                self._apply_rest_market_context(snapshot)
                self._last_rest_fallback = time.time()
            except Exception as exc:
                self.state.set_feed_status(self.state.feed_status, f"REST_RECOVERY:{exc}")

    def _apply_rest_market_context(self, snapshot: dict) -> None:
        context = getattr(self.state, "market_context", None)
        if context is None:
            context = {}
            self.state.market_context = context
        now = time.time()
        for security_id, row in snapshot.items():
            if not isinstance(row, dict) or security_id not in self.state.instruments:
                continue
            existing = dict(context.get(str(security_id), {}))
            mapping = {
                "last_price": "ltp",
                "open": "day_open",
                "high": "day_high",
                "low": "day_low",
                "close": "prev_close",
            }
            for source, target in mapping.items():
                value = row.get(source)
                if value is None and isinstance(row.get("ohlc"), dict):
                    value = row["ohlc"].get(source)
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                if value > 0:
                    existing[target] = value
            depth = row.get("depth")
            if isinstance(depth, list) and depth:
                normalized = []
                for level in depth[:5]:
                    if not isinstance(level, dict):
                        continue
                    def num(*keys):
                        for key in keys:
                            if key in level:
                                try:
                                    return float(level[key])
                                except (TypeError, ValueError):
                                    pass
                        return None
                    bid = num("bid_price", "bidPrice")
                    ask = num("ask_price", "askPrice")
                    bq = num("bid_quantity", "bidQty", "bid_qty")
                    aq = num("ask_quantity", "askQty", "ask_qty")
                    bo = num("bid_orders", "bidOrders")
                    ao = num("ask_orders", "askOrders")
                    normalized.append({
                        "bid_price": bid if bid and bid > 0 else None,
                        "ask_price": ask if ask and ask > 0 else None,
                        "bid_qty": int(bq or 0),
                        "ask_qty": int(aq or 0),
                        "bid_orders": int(bo or 0),
                        "ask_orders": int(ao or 0),
                    })
                if normalized:
                    existing["depth"] = normalized
                    existing["best_bid"] = normalized[0]["bid_price"]
                    existing["best_ask"] = normalized[0]["ask_price"]
                    existing["bid_qty"] = normalized[0]["bid_qty"]
                    existing["ask_qty"] = normalized[0]["ask_qty"]
                    existing["bid_orders"] = normalized[0]["bid_orders"]
                    existing["ask_orders"] = normalized[0]["ask_orders"]
            existing["received_epoch"] = now
            existing["source"] = "DHAN_REST_QUOTE_RECOVERY"
            context[str(security_id)] = existing

    def _run_connected_session(self, feed) -> None:
        feed.loop.run_until_complete(feed.connect())
        heartbeat = feed.loop.create_task(self._heartbeat_and_health(feed))
        try:
            while not self._stop_requested.is_set():
                if heartbeat.done():
                    error = heartbeat.exception()
                    if error is not None:
                        raise error
                    raise RuntimeError("LIVE_HEARTBEAT: watchdog stopped unexpectedly")
                try:
                    data = feed.loop.run_until_complete(
                        asyncio.wait_for(feed.get_instrument_data(), timeout=self.SOCKET_READ_CHECK_SECONDS)
                    )
                except asyncio.TimeoutError:
                    continue
                self._on_message(feed, data)
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
                try:
                    feed.loop.run_until_complete(heartbeat)
                except BaseException:
                    pass

    def _run(self) -> None:
        """Reconnect with bounded exponential backoff plus jitter."""
        backoff = self.BACKOFF_INITIAL_SECONDS
        while not self._stop_requested.is_set():
            feed = None
            rate_limited = False
            try:
                feed = self._build_feed()
                with self._lock:
                    self._feed = feed
                self.state.set_feed_status("CONNECTING")
                self._run_connected_session(feed)
                if self._stop_requested.is_set():
                    break
                self.state.mark_websocket_reconnecting("feed connection ended")
            except DhanConnectionLimited as exc:
                rate_limited = True
                self.state.mark_websocket_error(str(exc))
                self.state.mark_websocket_reconnecting(
                    f"Dhan rate/connection limit; retrying in {int(self.RATE_LIMIT_COOLDOWN)}s"
                )
            except Exception as exc:
                message = str(exc)
                self.state.mark_websocket_error(f"websocket:{message}")
                self.state.mark_websocket_reconnecting(f"websocket reconnect scheduled: {message}")
            finally:
                self._close_feed(feed)
                with self._lock:
                    if self._feed is feed:
                        self._feed = None

            if self._stop_requested.is_set():
                break
            if rate_limited or "805" in str(getattr(self.state, "last_feed_error", "")):
                delay = self.RATE_LIMIT_COOLDOWN
                backoff = self.BACKOFF_INITIAL_SECONDS
            else:
                delay = min(backoff, self.BACKOFF_MAX_SECONDS) + random.uniform(
                    0.0, self.NORMAL_RETRY_JITTER_SECONDS
                )
                backoff = min(backoff * 2.0, self.BACKOFF_MAX_SECONDS)
            self._stop_requested.wait(delay)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self.self_keepalive.start()
        self.state.market_context = {}
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-dhan-feed")
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        self.self_keepalive.stop()
        super().stop()