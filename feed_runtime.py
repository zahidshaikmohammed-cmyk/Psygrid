from __future__ import annotations

import asyncio
import json
import threading
import time

from dhan_api import DhanAPI
from feed import LiveFeed as BaseLiveFeed
from self_keepalive import SelfKeepAlive


class LiveFeed(BaseLiveFeed):
    """Runtime Dhan feed with bounded REST recovery, without owning the socket loop."""

    REST_FALLBACK_AFTER_SECONDS = 5.0
    RESUBSCRIBE_COOLDOWN_SECONDS = 30.0

    def __init__(self, settings, state, instruments, dhan_api=None):
        super().__init__(settings, state, instruments)
        self.dhan_api = dhan_api or DhanAPI(settings)
        self._last_resubscribe: dict[str, float] = {}
        self._last_rest_fallback = 0.0
        self._health_stop = threading.Event()
        self._health_thread: threading.Thread | None = None
        self.self_keepalive = SelfKeepAlive("https://psygrid.onrender.com/public/live-a.json")

    def _market_hours(self) -> bool:
        return self.state.session_status == "LIVE"

    async def _resubscribe_one(self, feed, item) -> None:
        await feed.ws.send(json.dumps({
            "RequestCode": 21,
            "InstrumentCount": 1,
            "InstrumentList": [{
                "ExchangeSegment": item.exchange_segment,
                "SecurityId": str(item.security_id),
            }],
        }))

    def _health_pass(self, feed) -> None:
        if not self._market_hours() or self._stop_requested.is_set():
            return
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
                if feed.ws is None or feed.loop.is_closed():
                    return
                future = asyncio.run_coroutine_threadsafe(self._resubscribe_one(feed, item), feed.loop)
                future.result(timeout=3.0)
                self._last_resubscribe[key] = now
            except Exception as exc:
                self.state.mark_websocket_error(f"RESUBSCRIBE:{type(exc).__name__}:{exc}")

        # Dhan Quote API is real data, not synthetic data. Use it only as bounded
        # recovery for missing/stale quotes; the primary feed remains Full WebSocket.
        if stale and now - self._last_rest_fallback >= self.REST_FALLBACK_AFTER_SECONDS:
            try:
                snapshot = self.dhan_api.quote_snapshot(self.instruments)
                self.state.apply_rest_snapshot(snapshot)
                self._apply_rest_market_context(snapshot)
                self._last_rest_fallback = time.time()
            except Exception as exc:
                self.state.set_feed_status(
                    self.state.feed_status,
                    f"REST_RECOVERY:{type(exc).__name__}:{exc}",
                )

    def _health_loop(self, feed) -> None:
        while not self._stop_requested.is_set() and not self._health_stop.wait(1.0):
            try:
                self._health_pass(feed)
            except Exception as exc:
                self.state.mark_websocket_error(f"LIVE_HEALTH:{type(exc).__name__}:{exc}")

    def _apply_rest_market_context(self, snapshot: dict) -> None:
        context = getattr(self.state, "market_context", None)
        if context is None:
            context = {}
            self.state.market_context = context
        now = time.time()
        for security_id, row in snapshot.items():
            if not isinstance(row, dict) or str(security_id) not in self.state.instruments:
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
        # The official SDK owns its event loop. Its websocket client handles
        # Dhan server ping/pong; we never inject client-side ping traffic.
        self._health_stop.clear()
        self._health_thread = threading.Thread(
            target=self._health_loop,
            args=(feed,),
            daemon=True,
            name="psygrid-dhan-health",
        )
        self._health_thread.start()
        try:
            feed.run()
        finally:
            self._health_stop.set()
            health_thread = self._health_thread
            self._health_thread = None
            if health_thread is not None and health_thread is not threading.current_thread():
                health_thread.join(timeout=3)

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
        self._health_stop.set()
        self.self_keepalive.stop()
        super().stop()
