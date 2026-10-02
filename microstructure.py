"""Preserve the Full-mode information the equity feed receives, as per-minute microstructure features.

Dhan's Full packet carries 5-level depth (quantities, order counts, prices),
total buy and sell quantity and the average traded price for every one of the
989 stocks. PSYGRID builds 1m OHLCV from LTP and volume; this module keeps the
rest, without touching that path.

Contract with the live feed:

- ``observe`` is called once per Full packet on the feed thread. It does a fixed
  amount of arithmetic on one stock's accumulator, takes no lock shared with
  ``PsygridState``, never blocks and never raises (any error disables the
  recorder and is counted).
- Accumulators are bucketed by the *receive* minute. Only the feed thread
  writes a bucket, and only the flush thread removes one, after its minute
  has closed plus a grace period, so no lock is needed (single dict
  operations are atomic in CPython).
- The flush thread writes each closed minute as one block to an append-only
  stream (``<archive>/.live/<date>/micro_1m.csv`` and ``bars_1m.csv``) that
  the intelligence service reads within seconds, and compresses the day into
  ``<archive>/<date>/microstructure_1m.csv.gz`` after the session.
- Memory is bounded: buckets older than ``MAX_PENDING_MINUTES`` are dropped
  and counted. Disk is bounded: writing pauses below a free-space floor, raw
  snapshots stop at a daily byte budget, stream files are deleted after a
  retention period.

Features are measured from Dhan packets, which are snapshots of the book at
Dhan's dissemination cadence, not exchange events: quantities between two
packets are unobserved, means are packet-weighted, and the trade-side
classification is a packet-level approximation. ``packets`` and
``max_gap_ms`` per stock-minute record the cadence so analysis can judge
what each metric can support.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

FEATURE_VERSION = "1"
GRACE_SECONDS = 3.0
CPU_BUDGET = 0.15  # the recorder may use at most this share of one core, measured each second
CPU_BUDGET_SECONDS = 5  # consecutive seconds over budget before it switches itself off
MAX_PENDING_MINUTES = 10
STREAM_DIR = ".live"
MICRO_STREAM = "micro_1m.csv"
BARS_STREAM = "bars_1m.csv"
MICRO_ARCHIVE = "microstructure_1m.csv.gz"
RAW_ARCHIVE = "depth_snapshots.csv.gz"
STATUS_FILE = "status.json"
END_MARKER = "#END"
INDEX_PREFIX = "IDX:"  # bar rows for indices carry security_id IDX:<key>

MICRO_COLUMNS = (
    "symbol", "security_id", "timestamp", "packets", "trade_packets", "quote_changes", "invalid_book",
    "first_recv_ms", "last_recv_ms", "max_gap_ms",
    "ltp", "ltt", "cum_volume", "volume", "buy_volume", "sell_volume", "unclassified_volume", "avg_price",
    "bid1", "ask1", "mid_first", "mid_last", "mid_high", "mid_low",
    "spread_bps_mean", "spread_bps_max", "spread_bps_last",
    "bid1_qty_mean", "ask1_qty_mean", "bid5_qty_mean", "ask5_qty_mean", "bid5_qty_last", "ask5_qty_last",
    "bid1_orders_last", "ask1_orders_last", "imbalance1_mean", "imbalance5_mean",
    "total_buy_qty_first", "total_buy_qty_last", "total_sell_qty_first", "total_sell_qty_last",
    "ofi", "bid1_depletion", "bid1_replenish", "ask1_depletion", "ask1_replenish",
    "bid_up", "bid_down", "ask_up", "ask_down",
)  # fmt: skip
BAR_COLUMNS = ("symbol", "security_id", "timestamp", "open", "high", "low", "close", "volume")
RAW_COLUMNS = ("symbol", "recv_ms", "ltt", "ltp", "volume", "total_buy_qty", "total_sell_qty",
               *[f"{side}{lvl}_{f}" for lvl in range(1, 6) for side in ("bid", "ask") for f in ("price", "qty", "orders")])  # fmt: skip


def _f(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class _Bucket:
    """One stock's accumulator for one receive-minute."""

    __slots__ = (
        "ao1",
        "aq1_sum",
        "aq5_last",
        "aq5_sum",
        "ask1",
        "ask_dep",
        "ask_down",
        "ask_rep",
        "ask_up",
        "avg_price",
        "bid1",
        "bid_dep",
        "bid_down",
        "bid_rep",
        "bid_up",
        "bo1",
        "bq1_sum",
        "bq5_last",
        "bq5_sum",
        "buy",
        "cum_volume",
        "first_ms",
        "imb1_sum",
        "imb5_sum",
        "invalid_book",
        "last_ms",
        "ltp",
        "ltt",
        "max_gap_ms",
        "mid_first",
        "mid_high",
        "mid_last",
        "mid_low",
        "ofi",
        "packets",
        "quote_changes",
        "sell",
        "spread_last",
        "spread_max",
        "spread_n",
        "spread_sum",
        "tbq_first",
        "tbq_last",
        "trade_packets",
        "tsq_first",
        "tsq_last",
        "unclassified",
        "volume",
    )  # fmt: skip

    def __init__(self):
        for name in self.__slots__:
            setattr(self, name, 0)
        self.mid_first = self.mid_high = self.mid_low = None
        self.first_ms = None


class _Last:
    """The previous packet's book and trade state for one stock (carried across minutes)."""

    __slots__ = ("ap", "aq", "bp", "bq", "ltp", "mid", "recv_ms", "volume")

    def __init__(self):
        self.bp = self.bq = self.ap = self.aq = self.mid = self.ltp = None
        self.volume = None
        self.recv_ms = None


class MicrostructureRecorder:
    def __init__(
        self,
        archive_root: Path,
        symbols: dict[str, str],
        timezone: str = "Asia/Kolkata",
        bars_for_minute: Callable[[int], list[tuple]] | None = None,
        raw_symbols: set[str] | None = None,
        raw_bytes_per_day: int = 300_000_000,
        min_free_bytes: int = 2_000_000_000,
        stream_retention_days: int = 2,
        clock: Callable[[], float] = time.time,
    ):
        self.root = Path(archive_root)
        self.symbols = dict(symbols)  # security_id -> symbol
        self.tz = ZoneInfo(timezone)
        self.bars_for_minute = bars_for_minute
        self.raw_ids = {sid for sid, sym in self.symbols.items() if sym in (raw_symbols or set())}
        self.raw_budget = raw_bytes_per_day
        self.min_free = min_free_bytes
        self.retention = stream_retention_days
        self.clock = clock
        self.enabled = True
        self.paused_reason: str | None = None
        self._buckets: dict[int, dict[str, _Bucket]] = {}
        self._last: dict[str, _Last] = {}
        self._raw: list[tuple] = []
        self._raw_bytes: dict[str, int] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._budget_mark = 0
        self._over_budget = 0
        self.stats = {"packets": 0, "observe_errors": 0, "minutes_written": 0, "rows_written": 0,
                      "dropped_minutes": 0, "write_errors": 0, "last_minute": None, "last_written_at": None,
                      "observe_ns_total": 0, "compactions": 0, "last_error": None}  # fmt: skip

    # --- feed thread -----------------------------------------------------------------------

    def observe(self, security_id: str, data: dict) -> None:
        """Account one Full packet. Constant time; never raises."""
        if not self.enabled:
            return
        started = time.perf_counter_ns()
        try:
            self._observe(security_id, data, self.clock())
        except Exception as exc:  # never let research recording touch the live feed
            self.stats["observe_errors"] += 1
            self.stats["last_error"] = f"observe: {type(exc).__name__}: {exc}"[:300]
            if self.stats["observe_errors"] > 1000:
                self.enabled = False
                self.paused_reason = "too many observe errors"
        self.stats["packets"] += 1
        self.stats["observe_ns_total"] += time.perf_counter_ns() - started

    def _observe(self, sid: str, data: dict, now: float) -> None:
        recv_ms = int(now * 1000)
        minute = int(now // 60 * 60)
        bucket = self._buckets.get(minute)
        if bucket is None:
            bucket = self._buckets.setdefault(minute, {})
        b = bucket.get(sid)
        if b is None:
            b = bucket[sid] = _Bucket()
        last = self._last.get(sid)
        if last is None:
            last = self._last[sid] = _Last()

        depth = data.get("depth") or ()
        if len(depth) >= 1:
            top = depth[0]
            bp, ap = _f(top.get("bid_price")), _f(top.get("ask_price"))
            bq, aq = _i(top.get("bid_quantity")), _i(top.get("ask_quantity"))
            bq5 = aq5 = 0
            for level in depth[:5]:
                bq5 += level.get("bid_quantity") or 0
                aq5 += level.get("ask_quantity") or 0
            bo1, ao1 = _i(top.get("bid_orders")), _i(top.get("ask_orders"))
        else:
            bp = ap = 0.0
            bq = aq = bq5 = aq5 = bo1 = ao1 = 0
        ltp = _f(data.get("LTP"))
        volume = _i(data.get("volume"))

        b.packets += 1
        if b.first_ms is None:
            b.first_ms = recv_ms
        elif recv_ms - b.last_ms > b.max_gap_ms:
            b.max_gap_ms = recv_ms - b.last_ms
        b.last_ms = recv_ms
        b.ltp, b.ltt, b.cum_volume = ltp, data.get("LTT"), volume
        b.avg_price = _f(data.get("avg_price"))
        tbq, tsq = _i(data.get("total_buy_quantity")), _i(data.get("total_sell_quantity"))
        if b.packets == 1:
            b.tbq_first, b.tsq_first = tbq, tsq
        b.tbq_last, b.tsq_last = tbq, tsq

        valid_book = bp > 0 and ap > 0 and ap > bp
        if valid_book:
            mid = (bp + ap) / 2
            spread = (ap - bp) / mid * 10_000
            b.bid1, b.ask1 = bp, ap
            if b.mid_first is None:
                b.mid_first = b.mid_high = b.mid_low = mid
            else:
                b.mid_high = max(b.mid_high, mid)
                b.mid_low = min(b.mid_low, mid)
            b.mid_last = mid
            b.spread_sum += spread
            b.spread_n += 1
            b.spread_max = max(b.spread_max, spread)
            b.spread_last = spread
            b.bq1_sum += bq
            b.aq1_sum += aq
            b.bq5_sum += bq5
            b.aq5_sum += aq5
            b.bq5_last, b.aq5_last, b.bo1, b.ao1 = bq5, aq5, bo1, ao1
            b.imb1_sum += (bq - aq) / (bq + aq) if bq + aq else 0.0
            b.imb5_sum += (bq5 - aq5) / (bq5 + aq5) if bq5 + aq5 else 0.0
            if last.bp is not None:
                changed = bp != last.bp or ap != last.ap or bq != last.bq or aq != last.aq
                if changed:
                    b.quote_changes += 1
                # Order-flow imbalance at the best quotes (Cont, Kukanov and Stoikov), between packets.
                b.ofi += ((bq if bp >= last.bp else 0) - (last.bq if bp <= last.bp else 0)
                          - (aq if ap <= last.ap else 0) + (last.aq if ap >= last.ap else 0))  # fmt: skip
                if bp == last.bp:
                    delta = bq - last.bq
                    if delta < 0:
                        b.bid_dep -= delta
                    else:
                        b.bid_rep += delta
                elif bp > last.bp:
                    b.bid_up += 1
                else:
                    b.bid_down += 1
                if ap == last.ap:
                    delta = aq - last.aq
                    if delta < 0:
                        b.ask_dep -= delta
                    else:
                        b.ask_rep += delta
                elif ap < last.ap:
                    b.ask_down += 1
                else:
                    b.ask_up += 1
        else:
            b.invalid_book += 1

        if last.volume is not None and volume > last.volume:
            traded = volume - last.volume
            b.trade_packets += 1
            b.volume += traded
            reference = last.mid
            if reference is not None and ltp > reference:
                b.buy += traded
            elif reference is not None and ltp < reference:
                b.sell += traded
            elif last.ltp is not None and ltp > last.ltp:
                b.buy += traded
            elif last.ltp is not None and ltp < last.ltp:
                b.sell += traded
            else:
                b.unclassified += traded

        if valid_book:
            last.bp, last.bq, last.ap, last.aq, last.mid = bp, bq, ap, aq, (bp + ap) / 2
        if ltp > 0:
            last.ltp = ltp
        if volume >= (last.volume or 0):
            last.volume = volume
        last.recv_ms = recv_ms

        if sid in self.raw_ids:
            row = [self.symbols.get(sid, sid), recv_ms, data.get("LTT"), ltp, volume, tbq, tsq]
            for level in range(5):
                entry = depth[level] if level < len(depth) else {}
                row += [entry.get("bid_price"), entry.get("bid_quantity"), entry.get("bid_orders"),
                        entry.get("ask_price"), entry.get("ask_quantity"), entry.get("ask_orders")]  # fmt: skip
            self._raw.append(tuple(row))

    # --- flush thread ------------------------------------------------------------------------

    def _day(self, epoch: float) -> str:
        return datetime.fromtimestamp(epoch, self.tz).strftime("%Y-%m-%d")

    def _stamp(self, epoch: float) -> str:
        return datetime.fromtimestamp(epoch, self.tz).strftime("%Y-%m-%d %H:%M:%S IST")

    def stream_dir(self, day: str) -> Path:
        return self.root / STREAM_DIR / day

    def _space_ok(self) -> bool:
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            free = shutil.disk_usage(self.root).free
        except OSError as exc:
            self.paused_reason = f"disk check failed: {exc}"
            return False
        if free < self.min_free:
            self.paused_reason = f"free disk {free // 1_000_000} MB below the {self.min_free // 1_000_000} MB floor"
            return False
        self.paused_reason = None
        return True

    @staticmethod
    def _row(symbol: str, sid: str, stamp: str, b: _Bucket) -> tuple:
        n = b.spread_n or 1
        valid = b.spread_n > 0

        def mean(total):
            return round(total / n, 4) if valid else ""

        return (
            symbol, sid, stamp, b.packets, b.trade_packets, b.quote_changes, b.invalid_book,
            b.first_ms or "", b.last_ms or "", b.max_gap_ms,
            b.ltp or "", b.ltt or "", b.cum_volume, b.volume, b.buy, b.sell, b.unclassified, b.avg_price or "",
            b.bid1 or "", b.ask1 or "", _r(b.mid_first), _r(b.mid_last), _r(b.mid_high), _r(b.mid_low),
            mean(b.spread_sum), round(b.spread_max, 4) if valid else "", round(b.spread_last, 4) if valid else "",
            mean(b.bq1_sum), mean(b.aq1_sum), mean(b.bq5_sum), mean(b.aq5_sum), b.bq5_last, b.aq5_last,
            b.bo1, b.ao1, mean(b.imb1_sum), mean(b.imb5_sum),
            b.tbq_first, b.tbq_last, b.tsq_first, b.tsq_last,
            b.ofi, b.bid_dep, b.bid_rep, b.ask_dep, b.ask_rep, b.bid_up, b.bid_down, b.ask_up, b.ask_down,
        )  # fmt: skip

    def flush(self, now: float | None = None) -> int:
        """Write every minute that closed at least ``GRACE_SECONDS`` ago. Returns minutes written."""
        now = self.clock() if now is None else now
        written = 0
        ready = sorted(m for m in list(self._buckets) if m + 60 + GRACE_SECONDS <= now)
        for minute in ready:
            bucket = self._buckets.pop(minute, None)
            if bucket is None:
                continue
            if minute < now - MAX_PENDING_MINUTES * 60 - 60:
                self.stats["dropped_minutes"] += 1
                continue
            if not self._space_ok():
                self.stats["dropped_minutes"] += 1
                continue
            try:
                self._write_minute(minute, bucket)
                written += 1
            except Exception as exc:
                self.stats["write_errors"] += 1
                self.stats["last_error"] = f"write: {type(exc).__name__}: {exc}"[:300]
        self._write_raw(now)
        return written

    def _append_block(self, path: Path, columns: tuple, rows: list[tuple], minute: int) -> None:
        import csv
        import io

        path.parent.mkdir(parents=True, exist_ok=True)
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        if not path.exists() or path.stat().st_size == 0:
            writer.writerow(columns)
        writer.writerows(rows)
        buffer.write(f"{END_MARKER} {minute} {len(rows)}\n")
        with open(path, "a", encoding="utf-8") as handle:  # one write per minute: readers see whole blocks
            handle.write(buffer.getvalue())

    def _write_minute(self, minute: int, bucket: dict[str, _Bucket]) -> None:
        day, stamp = self._day(minute), self._stamp(minute)
        rows = [self._row(self.symbols.get(sid, sid), sid, stamp, b) for sid, b in sorted(
            bucket.items(), key=lambda item: self.symbols.get(item[0], item[0]))]  # fmt: skip
        folder = self.stream_dir(day)
        self._append_block(folder / MICRO_STREAM, MICRO_COLUMNS, rows, minute)
        if self.bars_for_minute is not None:
            bars = self.bars_for_minute(minute)
            self._append_block(folder / BARS_STREAM, BAR_COLUMNS, bars, minute)
        self.stats["minutes_written"] += 1
        self.stats["rows_written"] += len(rows)
        self.stats["last_minute"] = stamp
        self.stats["last_written_at"] = self._stamp(self.clock())

    def _write_raw(self, now: float) -> None:
        if not self._raw:
            return
        rows, self._raw = self._raw, []
        day = self._day(now)
        if self._raw_bytes.get(day, 0) >= self.raw_budget:
            return
        import csv
        import io

        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="\n").writerows(rows)
        data = gzip.compress(buffer.getvalue().encode(), compresslevel=6)
        path = self.stream_dir(day) / RAW_ARCHIVE
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "ab") as handle:  # gzip members concatenate into one valid stream
            handle.write(data)
        self._raw_bytes[day] = self._raw_bytes.get(day, 0) + len(data)

    # --- end of day ----------------------------------------------------------------------------

    def compact(self, day: str) -> Path | None:
        """Compress a finished day's stream into the archive, with a manifest. Idempotent."""
        folder = self.stream_dir(day)
        source = folder / MICRO_STREAM
        if not source.exists():
            return None
        target = self.root / day / MICRO_ARCHIVE
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp")
        rows, header_written = 0, False
        with open(source, encoding="utf-8") as src, gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as out:
            for line in src:
                if line.startswith(END_MARKER) or not line.endswith("\n"):
                    continue  # block markers, and a torn last line from a crash mid-write
                if line.startswith("symbol,"):
                    if header_written:
                        continue
                    header_written = True
                else:
                    rows += 1
                out.write(line)
        os.replace(tmp, target)
        files = {MICRO_ARCHIVE: {"rows": rows, "sha256": _sha256(target)}}
        if (folder / RAW_ARCHIVE).exists():
            shutil.copyfile(folder / RAW_ARCHIVE, self.root / day / RAW_ARCHIVE)
            files[RAW_ARCHIVE] = {"sha256": _sha256(self.root / day / RAW_ARCHIVE), "columns": list(RAW_COLUMNS)}
        manifest = {"session_date": day, "feature_version": FEATURE_VERSION, "source": "DHAN_FULL_PACKETS",
                    "files": files, "compacted_at": self._stamp(self.clock())}  # fmt: skip
        (self.root / day / "microstructure_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
        self.stats["compactions"] += 1
        return target

    def housekeeping(self, now: float | None = None) -> None:
        """Compact earlier days' streams and delete streams past retention."""
        now = self.clock() if now is None else now
        today = self._day(now)
        base = self.root / STREAM_DIR
        if not base.exists():
            return
        for folder in sorted(p for p in base.iterdir() if p.is_dir()):
            day = folder.name
            if day >= today:
                continue
            if not (self.root / day / MICRO_ARCHIVE).exists():
                self.compact(day)
            age_days = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(day, "%Y-%m-%d")).days
            if age_days > self.retention and (self.root / day / MICRO_ARCHIVE).exists():
                shutil.rmtree(folder, ignore_errors=True)

    def status(self) -> dict:
        packets = self.stats["packets"] or 1
        return {
            **{k: v for k, v in self.stats.items() if k != "observe_ns_total"},
            "enabled": self.enabled,
            "paused_reason": self.paused_reason,
            "observe_us_mean": round(self.stats["observe_ns_total"] / packets / 1000, 2),
            "pending_minutes": len(self._buckets),
            "feature_version": FEATURE_VERSION,
        }

    def write_status(self) -> None:
        path = self.root / STREAM_DIR / STATUS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(".status.tmp")
        tmp.write_text(json.dumps({**self.status(), "updated_at": self._stamp(self.clock())}))
        os.replace(tmp, path)

    def check_cpu_budget(self, elapsed: float) -> None:
        """Switch the recorder off if it spent more than CPU_BUDGET of a core for CPU_BUDGET_SECONDS running."""
        spent = self.stats["observe_ns_total"] - self._budget_mark
        self._budget_mark = self.stats["observe_ns_total"]
        if elapsed > 0 and spent / 1e9 / elapsed > CPU_BUDGET:
            self._over_budget += 1
        else:
            self._over_budget = 0
        if self._over_budget >= CPU_BUDGET_SECONDS and self.enabled:
            self.enabled = False
            self.paused_reason = f"switched off: over the {CPU_BUDGET:.0%} CPU budget for {CPU_BUDGET_SECONDS}s"

    def _run(self) -> None:
        last_housekeeping = 0.0
        last_check = time.monotonic()
        while not self._stop.wait(1.0):
            now = time.monotonic()
            self.check_cpu_budget(now - last_check)
            last_check = now
            try:
                self.flush()
                if self.clock() - last_housekeeping > 600:
                    self.housekeeping()
                    last_housekeeping = self.clock()
                self.write_status()
            except Exception as exc:
                self.stats["write_errors"] += 1
                self.stats["last_error"] = f"flush: {type(exc).__name__}: {exc}"[:300]

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-microstructure")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._thread = None
        try:
            self.flush(self.clock() + 120)  # write whatever has accumulated
            self.write_status()
        except Exception:
            pass


def state_bars_for_minute(live_state, symbols: dict[str, str], minute: int) -> list[tuple]:
    """Completed 1m bars for one minute from PsygridState, in archive form (one short hold of its lock)."""
    from output import _ist_timestamp, _price

    stamp = _ist_timestamp(minute)
    rows = []
    with live_state.lock:
        for security_id, symbol in symbols.items():
            candle = None
            done = live_state.live_candles.get(security_id)
            if done and int(done[-1].get("epoch", done[-1].get("timestamp", 0)) or 0) == minute:
                candle = done[-1]
            else:
                current = live_state.current_1m.get(security_id)
                if current is not None and int(current.get("epoch", 0)) == minute:
                    candle = current
            if candle is not None:
                rows.append((symbol, security_id, stamp, _price(candle["open"]), _price(candle["high"]),
                             _price(candle["low"]), _price(candle["close"]), int(candle.get("volume") or 0)))  # fmt: skip
    return rows


def index_bars_for_minute(index_states: dict, minute: int) -> list[tuple]:
    """Completed index 1m bars for one minute, in stream form: ``security_id`` is ``IDX:<key>``."""
    from output import _ist_timestamp, _price

    stamp = _ist_timestamp(minute)
    rows = []
    for key, st in sorted((index_states or {}).items()):
        with st.lock:
            candle = None
            done = st.live_candles
            if done and int(done[-1].get("timestamp", done[-1].get("epoch", 0)) or 0) == minute:
                candle = done[-1]
            elif st.current_1m is not None and int(st.current_1m.get("epoch", 0)) == minute:
                candle = st.current_1m
            if candle is not None:
                rows.append((st.symbol, f"{INDEX_PREFIX}{key}", stamp, _price(candle["open"]), _price(candle["high"]),
                             _price(candle["low"]), _price(candle["close"]), int(candle.get("volume") or 0)))  # fmt: skip
    return rows


def _r(value) -> float | str:
    return round(value, 4) if value is not None else ""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()
