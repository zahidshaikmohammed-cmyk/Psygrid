"""Preserve the 20-level option depth the depth feeds receive, as per-minute order-book features.

``index_depth`` and ``stock_depth`` keep only the latest 20-level book for each
option contract they watch. This module keeps what happened in between: one
row per contract per minute with the book's cadence, top-of-book order-flow
imbalance, how much liquidity was added and removed across all 20 levels,
the largest resting order ("wall") on each side, and the REST-quote OI and
volume the depth state also carries.

Contract with the depth feeds:

- ``observe`` is called once per side packet, from the depth state's
  ``update_depth`` while that state holds its own lock. It does a fixed amount
  of arithmetic, takes only this recorder's lock (never a depth-state lock),
  never blocks on I/O and never raises: an error is counted, and too many
  disable the recorder.
- Accumulators are bucketed by the *receive* minute. The flush thread writes
  each closed minute as one block to ``<archive>/.live/<date>/option_depth_1m.csv``
  (ending with ``#END <minute> <rows>`` in the same write call), and compacts the
  day into ``<archive>/<date>/option_depth_1m.csv.gz`` with a manifest afterwards.
- Memory is bounded (buckets older than ``MAX_PENDING_MINUTES`` are dropped and
  counted) and so is disk (writing pauses below a free-space floor; stream files
  are deleted after a retention period once their day is compacted).

Dhan's 20-level feed sends each side as a separate snapshot packet, not
exchange events: quantities between packets are unobserved, means are
packet-weighted, and the add/remove totals are net changes between snapshots
(a cancel and an equal new order at the same level net to zero). ``packets``
and ``max_gap_ms`` record the cadence so analysis can judge what each metric
supports.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import os
import shutil
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from microstructure import END_MARKER, STREAM_DIR, _r, _sha256

FEATURE_VERSION = "1"
GRACE_SECONDS = 3.0
MAX_PENDING_MINUTES = 10
MAX_OBSERVE_ERRORS = 1000
CPU_BUDGET = 0.10  # share of one core the recorder may use, measured each second
CPU_BUDGET_SECONDS = 5  # consecutive seconds over budget before it switches itself off
TOP_LEVELS = 5
STREAM_FILE = "option_depth_1m.csv"
ARCHIVE_FILE = "option_depth_1m.csv.gz"
MANIFEST_FILE = "option_depth_manifest.json"
STATUS_FILE = "option_depth_status.json"

COLUMNS = (
    "underlying", "security_id", "expiry", "strike", "option_type", "timestamp",
    "packets", "bid_packets", "ask_packets", "book_updates", "invalid_book", "first_recv_ms", "last_recv_ms",
    "max_gap_ms", "underlying_ltp",
    "bid1", "ask1", "mid_first", "mid_last", "mid_high", "mid_low",
    "spread_bps_mean", "spread_bps_max", "spread_bps_last",
    "bid5_qty_mean", "ask5_qty_mean", "bid20_qty_mean", "ask20_qty_mean", "bid20_qty_last", "ask20_qty_last",
    "bid20_orders_last", "ask20_orders_last", "imbalance5_mean", "imbalance20_mean",
    "ofi", "bid_added", "bid_removed", "ask_added", "ask_removed",
    "bid_wall_price", "bid_wall_qty", "ask_wall_price", "ask_wall_qty",
    "ltp", "volume", "oi_first", "oi_last",
)  # fmt: skip


def _num(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


class _Side:
    """One side of the last packet seen for a contract (carried across minutes)."""

    __slots__ = ("orders", "price", "qty1", "qty5", "qty20", "wall_price", "wall_qty")

    def __init__(self, levels: list[dict]):
        price = qty1 = qty5 = qty20 = orders = 0
        wall_price, wall_qty = 0.0, 0
        for index, level in enumerate(levels):
            p = _num(level.get("price"))
            q = int(level.get("quantity") or 0)
            if p <= 0 or q <= 0:
                continue  # Dhan pads an empty level with zeros
            if index == 0:
                price, qty1 = p, q
            if index < TOP_LEVELS:
                qty5 += q
            qty20 += q
            orders += int(level.get("orders") or 0)
            if q > wall_qty:
                wall_price, wall_qty = p, q
        self.price, self.qty1, self.qty5, self.qty20, self.orders = price, qty1, qty5, qty20, orders
        self.wall_price, self.wall_qty = wall_price, wall_qty


class _Contract:
    """The previous packet's state for one contract."""

    __slots__ = ("ask", "bid", "recv_ms")

    def __init__(self):
        self.bid: _Side | None = None
        self.ask: _Side | None = None
        self.recv_ms: int | None = None


class _Bucket:
    """One contract's accumulators for one minute."""

    __slots__ = (
        "ask1",
        "ask5_sum",
        "ask20_orders",
        "ask20_qty_last",
        "ask20_sum",
        "ask_added",
        "ask_packets",
        "ask_removed",
        "ask_wall",
        "bid1",
        "bid5_sum",
        "bid20_orders",
        "bid20_qty_last",
        "bid20_sum",
        "bid_added",
        "bid_packets",
        "bid_removed",
        "bid_wall",
        "book_n",
        "expiry",
        "first_ms",
        "imb5_sum",
        "imb20_sum",
        "invalid",
        "last_ms",
        "ltp",
        "max_gap_ms",
        "mid_first",
        "mid_high",
        "mid_last",
        "mid_low",
        "ofi",
        "oi_first",
        "oi_last",
        "option_type",
        "spread_last",
        "spread_max",
        "spread_sum",
        "strike",
        "underlying_ltp",
        "volume",
    )  # fmt: skip

    def __init__(self, row: dict):
        for name in self.__slots__:
            setattr(self, name, 0)
        self.strike, self.option_type, self.expiry = row.get("strike"), row.get("option_type"), row.get("expiry")
        self.mid_first = self.mid_last = self.mid_high = self.mid_low = None
        self.first_ms = self.oi_first = self.oi_last = self.ltp = self.volume = self.underlying_ltp = None
        self.bid_wall = self.ask_wall = (0.0, 0)


class OptionDepthRecorder:
    def __init__(
        self,
        archive_root: Path,
        timezone: str = "Asia/Kolkata",
        min_free_bytes: int = 2_000_000_000,
        stream_retention_days: int = 2,
        clock: Callable[[], float] = time.time,
    ):
        self.root = Path(archive_root)
        self.tz = ZoneInfo(timezone)
        self.min_free = min_free_bytes
        self.retention = stream_retention_days
        self.clock = clock
        self.enabled = True
        self.paused_reason: str | None = None
        self._lock = threading.Lock()
        self._buckets: dict[int, dict[tuple[str, str], _Bucket]] = {}
        self._last: dict[tuple[str, str], _Contract] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._budget_mark = 0
        self._over_budget = 0
        self.stats = {"packets": 0, "observe_errors": 0, "minutes_written": 0, "rows_written": 0,
                      "dropped_minutes": 0, "write_errors": 0, "last_minute": None, "last_written_at": None,
                      "observe_ns_total": 0, "compactions": 0, "last_error": None}  # fmt: skip

    # --- depth-feed threads --------------------------------------------------------------------

    def observe(self, underlying: str, row: dict, side: str, levels: list[dict], underlying_ltp=None) -> None:
        """Account one side packet for one contract. Constant time; never raises."""
        if not self.enabled:
            return
        started = time.perf_counter_ns()
        with self._lock:
            try:
                self._observe(underlying, row, side, levels, underlying_ltp, self.clock())
            except Exception as exc:  # never let research recording touch a live feed
                self.stats["observe_errors"] += 1
                self.stats["last_error"] = f"observe: {type(exc).__name__}: {exc}"[:300]
                if self.stats["observe_errors"] > MAX_OBSERVE_ERRORS:
                    self.enabled = False
                    self.paused_reason = "too many observe errors"
            self.stats["packets"] += 1
            self.stats["observe_ns_total"] += time.perf_counter_ns() - started

    def _observe(self, underlying: str, row: dict, side: str, levels: list[dict], underlying_ltp, now: float):
        if side not in ("bid", "ask"):
            return
        key = (underlying, str(row.get("security_id")))
        recv_ms = int(now * 1000)
        minute = int(now // 60 * 60)
        bucket = self._buckets.setdefault(minute, {})
        b = bucket.get(key)
        if b is None:
            b = bucket[key] = _Bucket(row)
        last = self._last.get(key)
        if last is None:
            last = self._last[key] = _Contract()

        if b.first_ms is None:
            b.first_ms = recv_ms
        elif last.recv_ms is not None:
            b.max_gap_ms = max(b.max_gap_ms, recv_ms - last.recv_ms)
        b.last_ms = recv_ms
        last.recv_ms = recv_ms

        new = _Side(levels)
        previous = last.bid if side == "bid" else last.ask
        if side == "bid":
            b.bid_packets += 1
            last.bid = new
            b.bid20_qty_last, b.bid20_orders, b.bid_wall = new.qty20, new.orders, (new.wall_price, new.wall_qty)
        else:
            b.ask_packets += 1
            last.ask = new
            b.ask20_qty_last, b.ask20_orders, b.ask_wall = new.qty20, new.orders, (new.wall_price, new.wall_qty)

        if previous is not None:
            change = new.qty20 - previous.qty20
            if side == "bid":
                b.bid_added += max(change, 0)
                b.bid_removed += max(-change, 0)
            else:
                b.ask_added += max(change, 0)
                b.ask_removed += max(-change, 0)
            # Top-of-book order-flow imbalance (Cont, Kukanov and Stoikov), one side at a time.
            if previous.price > 0 and new.price > 0:
                if side == "bid":
                    if new.price > previous.price:
                        b.ofi += new.qty1
                    elif new.price == previous.price:
                        b.ofi += new.qty1 - previous.qty1
                    else:
                        b.ofi -= previous.qty1
                else:
                    if new.price < previous.price:
                        b.ofi -= new.qty1
                    elif new.price == previous.price:
                        b.ofi -= new.qty1 - previous.qty1
                    else:
                        b.ofi += previous.qty1

        for name, field in (("ltp", "last_price"), ("volume", "volume")):
            value = row.get(field)
            if value not in (None, ""):
                setattr(b, name, value)
        oi = row.get("oi")
        if oi not in (None, ""):
            if b.oi_first is None:
                b.oi_first = oi
            b.oi_last = oi
        if underlying_ltp not in (None, ""):
            b.underlying_ltp = underlying_ltp

        bid, ask = last.bid, last.ask
        if bid is None or ask is None or bid.price <= 0 or ask.price <= 0:
            return
        if bid.price >= ask.price:
            b.invalid += 1
            return
        mid = (bid.price + ask.price) / 2
        spread = (ask.price - bid.price) / mid * 10_000
        b.book_n += 1
        b.bid1, b.ask1 = bid.price, ask.price
        if b.mid_first is None:
            b.mid_first = b.mid_high = b.mid_low = mid
        b.mid_last = mid
        b.mid_high = max(b.mid_high, mid)
        b.mid_low = min(b.mid_low, mid)
        b.spread_sum += spread
        b.spread_max = max(b.spread_max, spread)
        b.spread_last = spread
        b.bid5_sum += bid.qty5
        b.ask5_sum += ask.qty5
        b.bid20_sum += bid.qty20
        b.ask20_sum += ask.qty20
        total5, total20 = bid.qty5 + ask.qty5, bid.qty20 + ask.qty20
        b.imb5_sum += (bid.qty5 - ask.qty5) / total5 if total5 else 0.0
        b.imb20_sum += (bid.qty20 - ask.qty20) / total20 if total20 else 0.0

    # --- flush thread --------------------------------------------------------------------------

    def _day(self, epoch: float) -> str:
        return datetime.fromtimestamp(epoch, self.tz).strftime("%Y-%m-%d")

    def _stamp(self, epoch: float) -> str:
        return datetime.fromtimestamp(epoch, self.tz).strftime("%Y-%m-%d %H:%M:%S IST")

    def stream_path(self, day: str) -> Path:
        return self.root / STREAM_DIR / day / STREAM_FILE

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
        if self.enabled:
            self.paused_reason = None
        return True

    @staticmethod
    def _row(underlying: str, security_id: str, stamp: str, b: _Bucket) -> tuple:
        n = b.book_n or 1
        valid = b.book_n > 0

        def mean(total, digits=4):
            return round(total / n, digits) if valid else ""

        def blank(value):
            return "" if value is None else value

        return (
            underlying, security_id, blank(b.expiry), blank(b.strike), blank(b.option_type), stamp,
            b.bid_packets + b.ask_packets, b.bid_packets, b.ask_packets, b.book_n, b.invalid,
            blank(b.first_ms), b.last_ms or "", b.max_gap_ms, blank(b.underlying_ltp),
            b.bid1 or "", b.ask1 or "", _r(b.mid_first), _r(b.mid_last), _r(b.mid_high), _r(b.mid_low),
            mean(b.spread_sum), round(b.spread_max, 4) if valid else "", round(b.spread_last, 4) if valid else "",
            mean(b.bid5_sum, 1), mean(b.ask5_sum, 1), mean(b.bid20_sum, 1), mean(b.ask20_sum, 1),
            b.bid20_qty_last, b.ask20_qty_last, b.bid20_orders, b.ask20_orders, mean(b.imb5_sum), mean(b.imb20_sum),
            b.ofi, b.bid_added, b.bid_removed, b.ask_added, b.ask_removed,
            b.bid_wall[0] or "", b.bid_wall[1] or "", b.ask_wall[0] or "", b.ask_wall[1] or "",
            blank(b.ltp), blank(b.volume), blank(b.oi_first), blank(b.oi_last),
        )  # fmt: skip

    def flush(self, now: float | None = None) -> int:
        """Write every minute that closed at least ``GRACE_SECONDS`` ago. Returns minutes written."""
        now = self.clock() if now is None else now
        with self._lock:
            ready = sorted(m for m in self._buckets if m + 60 + GRACE_SECONDS <= now)
            closed = [(m, self._buckets.pop(m)) for m in ready]
        written = 0
        for minute, bucket in closed:
            if minute < now - MAX_PENDING_MINUTES * 60 - 60 or not self._space_ok():
                self.stats["dropped_minutes"] += 1
                continue
            try:
                self._write_minute(minute, bucket)
                written += 1
            except Exception as exc:
                self.stats["write_errors"] += 1
                self.stats["last_error"] = f"write: {type(exc).__name__}: {exc}"[:300]
        return written

    def _write_minute(self, minute: int, bucket: dict[tuple[str, str], _Bucket]) -> None:
        import csv
        import io

        stamp = self._stamp(minute)
        rows = [self._row(underlying, sid, stamp, b) for (underlying, sid), b in sorted(
            bucket.items(), key=lambda item: (item[0][0], _num(item[1].strike), str(item[1].option_type)))]  # fmt: skip
        path = self.stream_path(self._day(minute))
        path.parent.mkdir(parents=True, exist_ok=True)
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        if not path.exists() or path.stat().st_size == 0:
            writer.writerow(COLUMNS)
        writer.writerows(rows)
        buffer.write(f"{END_MARKER} {minute} {len(rows)}\n")
        with open(path, "a", encoding="utf-8") as handle:  # one write per minute: readers see whole blocks
            handle.write(buffer.getvalue())
        self.stats["minutes_written"] += 1
        self.stats["rows_written"] += len(rows)
        self.stats["last_minute"] = stamp
        self.stats["last_written_at"] = self._stamp(self.clock())

    # --- end of day ----------------------------------------------------------------------------

    def compact(self, day: str) -> Path | None:
        """Compress a finished day's stream into the archive, with a manifest. Idempotent."""
        source = self.stream_path(day)
        if not source.exists():
            return None
        target = self.root / day / ARCHIVE_FILE
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp")
        rows, header_written = 0, False
        with open(source, encoding="utf-8") as src, gzip.open(tmp, "wt", encoding="utf-8", compresslevel=6) as out:
            for line in src:
                if line.startswith(END_MARKER) or not line.endswith("\n"):
                    continue  # block markers, and a torn last line from a crash mid-write
                if line.startswith("underlying,"):
                    if header_written:
                        continue
                    header_written = True
                else:
                    rows += 1
                out.write(line)
        os.replace(tmp, target)
        manifest = {"session_date": day, "feature_version": FEATURE_VERSION,
                    "source": "DHAN_20_LEVEL_DEPTH_WEBSOCKET", "columns": list(COLUMNS),
                    "files": {ARCHIVE_FILE: {"rows": rows, "sha256": _sha256(target)}},
                    "compacted_at": self._stamp(self.clock())}  # fmt: skip
        (self.root / day / MANIFEST_FILE).write_text(json.dumps(manifest, indent=2, sort_keys=True))
        self.stats["compactions"] += 1
        return target

    def housekeeping(self, now: float | None = None) -> None:
        """Compact earlier days' streams and delete streams past retention once compacted."""
        now = self.clock() if now is None else now
        today = self._day(now)
        base = self.root / STREAM_DIR
        if not base.exists():
            return
        for folder in sorted(p for p in base.iterdir() if p.is_dir()):
            day = folder.name
            if day >= today or not (folder / STREAM_FILE).exists():
                continue
            if not (self.root / day / ARCHIVE_FILE).exists():
                self.compact(day)
            age_days = (datetime.strptime(today, "%Y-%m-%d") - datetime.strptime(day, "%Y-%m-%d")).days
            if age_days > self.retention and (self.root / day / ARCHIVE_FILE).exists():
                (folder / STREAM_FILE).unlink(missing_ok=True)

    def status(self) -> dict:
        packets = self.stats["packets"] or 1
        return {
            **{k: v for k, v in self.stats.items() if k != "observe_ns_total"},
            "enabled": self.enabled,
            "paused_reason": self.paused_reason,
            "observe_us_mean": round(self.stats["observe_ns_total"] / packets / 1000, 2),
            "pending_minutes": len(self._buckets),
            "tracked_contracts": len(self._last),
            "feature_version": FEATURE_VERSION,
        }

    def write_status(self) -> None:
        path = self.root / STREAM_DIR / STATUS_FILE
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{STATUS_FILE}.tmp")
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
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-option-depth-recorder")
        self._thread.start()

    def stop(self) -> None:
        """Write every pending minute (including the open one) and stop."""
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=5)
        self._thread = None
        with contextlib.suppress(Exception):
            self.flush(self.clock() + 120)
            self.write_status()
