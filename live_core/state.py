"""Compact, RAM-only 1-minute equity state for one Live Core partition.

It implements the same market-state interface ``feed.LiveFeed`` drives (``update_quote``,
``record_live_quote``, ``set_market_reference``, ``mark_websocket_*`` ...) and builds candles by
the same rules as ``state.PsygridState``: the candle minute comes from the exchange LTT, volume is
the delta of Dhan's cumulative day volume, a repeated trade is ignored, and a Dhan historical
candle is authoritative over a WebSocket-built one for the same minute.

It differs only in representation and publication:

* candles are stored column-wise in ``array`` buffers (48 bytes per candle instead of a ~1 KB dict),
  so a whole session for ~495 stocks stays in single-digit megabytes;
* the minute a stock's candle covers is published once that minute has ended (plus a grace
  period) instead of waiting for the stock's next trade; a late trade for that minute is folded
  into it. No candle is ever created without a real trade or a Dhan historical bar.

Nothing in this module touches the disk. ``reset`` drops every market value.
"""

from __future__ import annotations

import threading
import time
import uuid
from array import array
from bisect import bisect_left
from collections import deque
from datetime import UTC, datetime

SOURCE_WEBSOCKET = 0
SOURCE_HISTORICAL = 1
_QUOTE_TYPES = frozenset({"quote data", "quote", "full data", "full"})


class StockSeries:
    """One stock's session: reference prices, completed 1m candles and the forming candle."""

    __slots__ = (
        "_fragment",
        "_fragment_count",
        "_fragment_generation",
        "_fragment_refs",
        "_fragment_revision",
        "closes",
        "current",
        "epochs",
        "generation",
        "highs",
        "index",
        "last_ltt",
        "last_received",
        "last_trade_key",
        "lows",
        "opens",
        "previous_close",
        "previous_cumulative_volume",
        "revision",
        "security_id",
        "sources",
        "symbol",
        "today_open",
        "volumes",
    )

    def __init__(self, index: int, symbol: str, security_id: str):
        self.index = index
        self.symbol = symbol
        self.security_id = security_id
        self.previous_close: float | None = None
        self.today_open: float | None = None
        self.epochs = array("q")
        self.opens = array("d")
        self.highs = array("d")
        self.lows = array("d")
        self.closes = array("d")
        self.volumes = array("q")
        self.sources = bytearray()
        # [minute_epoch, open, high, low, close, volume] while the minute is still forming.
        self.current: list | None = None
        self.previous_cumulative_volume: int | None = None
        self.last_trade_key: tuple | None = None
        self.last_received: float | None = None
        self.last_ltt: int | None = None
        # ``revision`` changes whenever anything published changes; ``generation`` only when a
        # published candle is rewritten or inserted (not a plain append), see live_core.render.
        self.revision = 0
        self.generation = 0
        # One cached JSON encoding of this stock (see live_core.render.stock_fragment).
        self._fragment: bytes | None = None
        self._fragment_revision = -1
        self._fragment_generation = -1
        self._fragment_count = 0
        self._fragment_refs: tuple | None = None

    def candle_count(self) -> int:
        return len(self.epochs)

    def _set_at(self, position: int, row, source: int) -> None:
        self.opens[position], self.highs[position], self.lows[position], self.closes[position] = row[1:5]
        self.volumes[position] = row[5]
        self.sources[position] = source

    def _insert_at(self, position: int, row, source: int) -> None:
        self.epochs.insert(position, row[0])
        self.opens.insert(position, row[1])
        self.highs.insert(position, row[2])
        self.lows.insert(position, row[3])
        self.closes.insert(position, row[4])
        self.volumes.insert(position, row[5])
        self.sources[position:position] = bytes((source,))

    def put_completed(self, row, source: int) -> bool:
        """Store a completed candle ``(epoch, o, h, l, c, v)``. Returns True when the published data changed."""
        epoch = row[0]
        count = len(self.epochs)
        if count == 0 or epoch > self.epochs[-1]:
            self.epochs.append(epoch)
            self.opens.append(row[1])
            self.highs.append(row[2])
            self.lows.append(row[3])
            self.closes.append(row[4])
            self.volumes.append(row[5])
            self.sources.append(source)
            self.revision += 1
            return True
        position = bisect_left(self.epochs, epoch)
        if position < count and self.epochs[position] == epoch:
            if self.sources[position] == SOURCE_HISTORICAL and source != SOURCE_HISTORICAL:
                return False
            self._set_at(position, row, source)
        else:
            self._insert_at(position, row, source)
        self.revision += 1
        self.generation += 1
        return True


class NodeState:
    """RAM-only market state for one partition, shaped for ``feed.LiveFeed``."""

    def __init__(self, timezone: str = "Asia/Kolkata", max_live_age_seconds: float = 30.0, clock=time.time):
        self.timezone = timezone
        self.max_live_age_seconds = float(max_live_age_seconds)
        self.clock = clock
        self.lock = threading.RLock()
        self.errors: deque[dict] = deque(maxlen=20)
        # ``version`` changes whenever anything published changes; ``instance`` tells a restarted
        # process apart, so a peer never mistakes a fresh state for the one it cached.
        self.version = 0
        self.instance = uuid.uuid4().hex[:12]
        self._init_runtime()

    # ------------------------------------------------------------------ lifecycle

    def _init_runtime(self) -> None:
        self.session_date: str | None = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        # ``instruments`` is what feed.LiveFeed checks a packet's security id against.
        self.instruments: dict[str, StockSeries] = {}
        self.by_symbol: dict[str, StockSeries] = {}
        self.ordered: list[StockSeries] = []
        self.feed_messages = 0
        self.quote_packets = 0
        self.live_quotes = 0
        self.websocket_reconnects = 0
        self.subscribed_count = 0
        self.last_message_type: str | None = None
        self.last_message_epoch: float | None = None
        self.last_tick_received_epoch: float | None = None
        self.last_tick_ltt: int | None = None
        self.websocket_connected_epoch: float | None = None

    def reset(self) -> None:
        """Drop every market value; nothing is carried into the next session or written anywhere."""
        with self.lock:
            self._init_runtime()
            self.version += 1

    def begin(self, session_date: str, instruments, start_index: int = 0) -> None:
        """Start a session for ``instruments`` (canonical order; ``start_index`` is the first's universe index)."""
        with self.lock:
            self._init_runtime()
            self.session_date = session_date
            self.session_status = "STARTING"
            self.feed_status = "STARTING"
            for offset, item in enumerate(instruments):
                series = StockSeries(start_index + offset, str(item.symbol), str(item.security_id))
                self.instruments[series.security_id] = series
                self.by_symbol[series.symbol] = series
                self.ordered.append(series)
            self.version += 1

    def content_version(self) -> str:
        with self.lock:
            return f"{self.instance}:{self.version}"

    def set_session_status(self, status: str) -> None:
        with self.lock:
            self.session_status = status
            self.version += 1

    def record_error(self, error: str) -> None:
        with self.lock:
            self.errors.append({"at": datetime.now(UTC).isoformat(timespec="seconds"), "error": str(error)[:500]})

    # ------------------------------------------------------------------ feed.LiveFeed interface

    def set_feed_status(self, status: str, error: str = "") -> None:
        with self.lock:
            self.feed_status = status
            if error:
                self.last_feed_error = error
                self.record_error(error)

    def mark_websocket_connected(self, subscribed_count: int) -> None:
        with self.lock:
            self.feed_status = "CONNECTED"
            self.websocket_connected_epoch = self.clock()
            self.last_feed_error = ""
            self.subscribed_count = int(subscribed_count)

    def mark_websocket_reconnecting(self, reason: str) -> None:
        with self.lock:
            self.feed_status = "RECONNECTING"
            self.websocket_reconnects += 1
            self.last_feed_error = reason
            self.record_error(reason)

    def mark_websocket_error(self, error: str) -> None:
        with self.lock:
            self.feed_status = "ERROR"
            self.last_feed_error = error
            self.record_error(error)

    def record_feed_message(self, packet_type: str) -> None:
        with self.lock:
            if self.session_status != "LIVE":
                return
            self.feed_messages += 1
            self.last_message_type = packet_type
            self.last_message_epoch = self.clock()
            if packet_type.lower() in _QUOTE_TYPES:
                self.quote_packets += 1

    def set_market_reference(self, security_id, *, previous_close=None, today_open=None) -> None:
        with self.lock:
            series = self.instruments.get(str(security_id))
            if series is None:
                return
            changed = False
            for name, value in (("previous_close", previous_close), ("today_open", today_open)):
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    continue
                if value > 0 and getattr(series, name) != value:
                    setattr(series, name, value)
                    changed = True
            if changed:
                series.revision += 1
                self.version += 1

    def record_live_quote(self, security_id, ltt_epoch: int) -> None:
        with self.lock:
            series = self.instruments.get(str(security_id))
            if series is None or self.session_status != "LIVE":
                return
            now = self.clock()
            self.live_quotes += 1
            self.last_tick_received_epoch = now
            self.last_tick_ltt = int(ltt_epoch)
            series.last_received = now
            series.last_ltt = int(ltt_epoch)

    def update_quote(self, security_id, quote: dict) -> None:
        with self.lock:
            series = self.instruments.get(str(security_id))
            if series is None or self.session_status != "LIVE":
                return
            try:
                ltp = float(quote["LTP"])
                ltt_epoch = int(quote["LTT_EPOCH"])
                cumulative_volume = int(quote.get("volume", 0) or 0)
                ltq = int(quote.get("LTQ", quote.get("ltq", 0)) or 0)
            except (KeyError, TypeError, ValueError):
                return
            if ltt_epoch <= 0 or ltp <= 0 or cumulative_volume < 0:
                return

            minute = ltt_epoch - (ltt_epoch % 60)
            current = series.current
            late_into_last = False
            if current is not None:
                if minute < current[0]:
                    return
                if minute > current[0]:
                    self._complete_current(series)
                    current = None
            elif len(series.epochs) and minute <= series.epochs[-1]:
                # The minute was already published by the timer; fold a late trade into it, never
                # into an older minute and never into an authoritative historical bar.
                if minute != series.epochs[-1] or series.sources[-1] != SOURCE_WEBSOCKET:
                    return
                late_into_last = True

            previous_volume = series.previous_cumulative_volume
            if previous_volume is None:
                previous_volume = cumulative_volume
            delta_volume = max(0, cumulative_volume - previous_volume)
            series.previous_cumulative_volume = cumulative_volume
            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            if trade_key == series.last_trade_key:
                return
            series.last_trade_key = trade_key

            if late_into_last:
                position = len(series.epochs) - 1
                series.highs[position] = max(series.highs[position], ltp)
                series.lows[position] = min(series.lows[position], ltp)
                series.closes[position] = ltp
                series.volumes[position] += delta_volume
                series.revision += 1
                series.generation += 1
                self.version += 1
            elif current is None:
                series.current = [minute, ltp, ltp, ltp, ltp, delta_volume]
            else:
                if ltp > current[2]:
                    current[2] = ltp
                if ltp < current[3]:
                    current[3] = ltp
                current[4] = ltp
                current[5] += delta_volume

    # ------------------------------------------------------------------ candles

    def _complete_current(self, series: StockSeries) -> None:
        current = series.current
        series.current = None
        if current is not None and series.put_completed(current, SOURCE_WEBSOCKET):
            self.version += 1

    def finalize_due(self, now_epoch: float | None = None, grace_seconds: float = 3.0) -> int:
        """Publish every forming candle whose minute ended at least ``grace_seconds`` ago."""
        now_epoch = self.clock() if now_epoch is None else now_epoch
        cutoff = now_epoch - 60 - grace_seconds
        finalized = 0
        with self.lock:
            for series in self.ordered:
                current = series.current
                if current is not None and current[0] <= cutoff:
                    self._complete_current(series)
                    finalized += 1
        return finalized

    def finalize_all(self) -> None:
        with self.lock:
            for series in self.ordered:
                if series.current is not None:
                    self._complete_current(series)

    def merge_history(self, security_id, rows) -> int:
        """Merge genuine Dhan historical 1m bars (dicts from ``dhan_api``). Returns how many were stored."""
        stored = 0
        with self.lock:
            series = self.instruments.get(str(security_id))
            if series is None:
                return 0
            for candle in rows:
                if not isinstance(candle, dict) or candle.get("complete", True) is False:
                    continue
                try:
                    epoch = int(candle.get("epoch", candle["timestamp"]))
                    row = (
                        epoch - (epoch % 60),
                        float(candle["open"]),
                        float(candle["high"]),
                        float(candle["low"]),
                        float(candle["close"]),
                        int(candle.get("volume", 0) or 0),
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                if min(row[1:5]) <= 0 or row[5] < 0:
                    continue
                if series.put_completed(row, SOURCE_HISTORICAL):
                    stored += 1
            current = series.current
            if current is not None and len(series.epochs):
                position = bisect_left(series.epochs, current[0])
                if position < len(series.epochs) and series.epochs[position] == current[0]:
                    # Dhan's historical bar is authoritative for a completed minute.
                    series.current = None
            if stored:
                self.version += 1
        return stored

    def seed_from_snapshot(self, snapshot: dict) -> None:
        """Apply a Dhan REST quote snapshot: reference prices and the cumulative-volume baseline."""
        with self.lock:
            for security_id, row in snapshot.items():
                series = self.instruments.get(str(security_id))
                if series is None or not isinstance(row, dict):
                    continue
                ohlc = row.get("ohlc") if isinstance(row.get("ohlc"), dict) else {}
                self.set_market_reference(
                    series.security_id,
                    previous_close=row.get("previous_close", row.get("prev_close", ohlc.get("close"))),
                    today_open=row.get("open", ohlc.get("open")),
                )
                try:
                    volume = int(row.get("volume", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if series.previous_cumulative_volume is None:
                    series.previous_cumulative_volume = max(0, volume)

    def last_completed_epoch(self, series: StockSeries) -> int | None:
        with self.lock:
            return int(series.epochs[-1]) if len(series.epochs) else None

    # ------------------------------------------------------------------ observability

    def freshness(self, now_epoch: float | None = None) -> dict:
        now_epoch = self.clock() if now_epoch is None else now_epoch
        with self.lock:
            live = stale = never = 0
            for series in self.ordered:
                received = series.last_received
                if received is None:
                    never += 1
                elif now_epoch - received <= self.max_live_age_seconds:
                    live += 1
                else:
                    stale += 1
            last = self.last_tick_received_epoch
            age = round(max(0.0, now_epoch - last), 3) if last is not None else None
            fresh = bool(self.session_status == "LIVE" and age is not None and age <= self.max_live_age_seconds)
            if self.session_status != "LIVE":
                stream = self.session_status
            elif live == len(self.ordered) and self.ordered:
                stream = "FULL_LIVE"
            elif live == 0:
                stream = "NO_LIVE_QUOTES"
            else:
                stream = "PARTIAL_LIVE"
            return {
                "fresh": fresh,
                "stale": bool(self.session_status == "LIVE" and not fresh),
                "stream_health": stream,
                "max_live_age_seconds": self.max_live_age_seconds,
                "last_tick_age_seconds": age,
                "last_market_timestamp": datetime.fromtimestamp(self.last_tick_ltt, UTC).isoformat()
                if self.last_tick_ltt
                else None,
                "live_stock_count": live,
                "stale_stock_count": stale,
                "no_quote_stock_count": never,
            }

    def memory_summary(self) -> dict:
        with self.lock:
            candles = sum(len(series.epochs) for series in self.ordered)
            forming = sum(1 for series in self.ordered if series.current is not None)
            return {
                "stocks_in_ram": len(self.ordered),
                "completed_candles_in_ram": candles,
                "forming_candles_in_ram": forming,
                "approx_candle_bytes": candles * 49,
            }

    def snapshot(self) -> dict:
        now = self.clock()
        with self.lock:
            last_message_age = (
                round(max(0.0, now - self.last_message_epoch), 3) if self.last_message_epoch is not None else None
            )
            return {
                "session_date": self.session_date,
                "session_status": self.session_status,
                "feed_status": self.feed_status,
                "last_feed_error": self.last_feed_error,
                "subscribed_instrument_count": self.subscribed_count,
                "instrument_count": len(self.ordered),
                "feed_messages": self.feed_messages,
                "quote_packets": self.quote_packets,
                "live_quotes": self.live_quotes,
                "websocket_reconnects": self.websocket_reconnects,
                "websocket_connected_at": _iso(self.websocket_connected_epoch),
                "last_message_type": self.last_message_type,
                "last_feed_message_at": _iso(self.last_message_epoch),
                "last_feed_message_age_seconds": last_message_age,
            }


def _iso(epoch: float | None) -> str | None:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds") if epoch is not None else None
