"""The 20-level option-depth recorder: features, stream blocks, compaction, budgets, and feed isolation."""

import csv
import gzip
import json
from types import SimpleNamespace

from index_depth import DepthContract, IndexDepthState
from index_options import IndexDerivativesSpec
from microstructure import END_MARKER, STREAM_DIR
from option_depth_recorder import (
    ARCHIVE_FILE,
    COLUMNS,
    MANIFEST_FILE,
    STREAM_FILE,
    OptionDepthRecorder,
)
from stock_depth import StockDepthContract, StockDepthState

T0 = 1_790_000_040  # a minute boundary (UTC epoch divisible by 60)
ROW = {"security_id": "5001", "strike": 25000.0, "option_type": "CE", "expiry": "2026-10-07",
       "last_price": 101.5, "volume": 12000, "oi": 50000}  # fmt: skip


def levels(top, qty, step=0.05, count=20, direction=-1, wall=None):
    """20 levels from ``top`` moving by ``step`` (down for bids, up for asks); ``wall`` = (level, qty)."""
    out = []
    for i in range(count):
        q = wall[1] if wall and wall[0] == i else qty
        out.append({"level": i + 1, "price": round(top + direction * i * step, 2), "quantity": q, "orders": 2})
    return out


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def recorder(tmp_path, clock, **kw):
    return OptionDepthRecorder(tmp_path, clock=clock, min_free_bytes=0, **kw)


def read_blocks(path):
    blocks, rows = [], []
    with open(path) as handle:
        reader = csv.reader(handle)
        header = next(reader)
        for row in reader:
            if row[0].startswith(END_MARKER):
                blocks.append((int(row[0].split()[1]), int(row[0].split()[2]), rows))
                rows = []
            else:
                rows.append(dict(zip(header, row, strict=True)))
    return header, blocks


def stream_file(tmp_path, rec, minute):
    return tmp_path / STREAM_DIR / rec._day(minute) / STREAM_FILE


def test_features_from_a_known_packet_sequence(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50), 25010.0)
    clock.t += 1
    rec.observe("NIFTY", ROW, "ask", levels(100.5, 40, direction=1, wall=(3, 900)), 25010.0)
    clock.t += 2
    # The bid moves up a tick: OFI adds the whole new top quantity; the 20-level total grows by 19 lots.
    rec.observe("NIFTY", {**ROW, "oi": 50600}, "bid", levels(100.05, 69), 25012.0)
    clock.t = T0 + 70
    assert rec.flush() == 1

    header, blocks = read_blocks(stream_file(tmp_path, rec, T0))
    assert tuple(header) == COLUMNS
    (minute, count, rows), *_ = blocks
    assert (minute, count) == (T0, 1)
    row = rows[0]
    assert (row["underlying"], row["security_id"], row["option_type"], row["strike"]) == (
        "NIFTY",
        "5001",
        "CE",
        "25000.0",
    )
    assert (row["packets"], row["bid_packets"], row["ask_packets"], row["book_updates"]) == ("3", "2", "1", "2")
    assert row["max_gap_ms"] == "2000"
    assert row["ofi"] == "69"
    assert (row["bid_added"], row["bid_removed"]) == (str(20 * 69 - 20 * 50), "0")
    assert (row["bid1"], row["ask1"]) == ("100.05", "100.5")
    assert (row["ask_wall_price"], row["ask_wall_qty"]) == ("100.65", "900")
    assert (row["oi_first"], row["oi_last"]) == ("50000", "50600")
    assert row["underlying_ltp"] == "25012.0"
    assert float(row["imbalance20_mean"]) != 0.0
    assert float(row["spread_bps_last"]) < float(row["spread_bps_max"])


def test_the_ask_side_ofi_follows_cont_sign_conventions(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    rec.observe("NIFTY", ROW, "ask", levels(101.0, 40, direction=1))
    rec.observe("NIFTY", ROW, "ask", levels(100.95, 30, direction=1))  # ask improves: sell pressure, -30
    rec.observe("NIFTY", ROW, "ask", levels(100.95, 10, direction=1))  # same price, 20 fewer: +20
    rec.observe("NIFTY", ROW, "ask", levels(101.0, 40, direction=1))  # ask lifted away: +10 (old top)
    clock.t = T0 + 70
    rec.flush()
    _, [(_, _, rows)] = read_blocks(stream_file(tmp_path, rec, T0))
    assert rows[0]["ofi"] == str(-30 + 20 + 10)


def test_crossed_and_empty_books_are_counted_not_measured(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    rec.observe("NIFTY", ROW, "bid", levels(101.0, 50))
    rec.observe("NIFTY", ROW, "ask", levels(100.0, 40, direction=1))  # crossed
    rec.observe("NIFTY", {**ROW, "security_id": "5002"}, "bid", [{"price": 0.0, "quantity": 0, "orders": 0}] * 20)
    clock.t = T0 + 70
    rec.flush()
    _, [(_, _, rows)] = read_blocks(stream_file(tmp_path, rec, T0))
    by_id = {r["security_id"]: r for r in rows}
    assert by_id["5001"]["invalid_book"] == "1" and by_id["5001"]["book_updates"] == "0"
    assert by_id["5001"]["spread_bps_mean"] == "" and by_id["5001"]["mid_last"] == ""
    assert by_id["5002"]["bid20_qty_last"] == "0" and by_id["5002"]["bid1"] == ""


def test_minutes_are_written_once_after_the_grace_period(tmp_path):
    clock = Clock(T0 + 5)
    rec = recorder(tmp_path, clock)
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    clock.t = T0 + 61
    assert rec.flush() == 0  # inside the grace period
    clock.t = T0 + 64
    assert rec.flush() == 1
    assert rec.flush() == 0
    _, blocks = read_blocks(stream_file(tmp_path, rec, T0))
    assert len(blocks) == 1


def test_garbage_never_raises_and_errors_are_counted(tmp_path):
    rec = recorder(tmp_path, Clock(T0 + 1))
    rec.observe("NIFTY", ROW, "bid", [{"price": "x", "quantity": "y"}])
    rec.observe("NIFTY", ROW, "bid", None)
    rec.observe("NIFTY", None, "bid", [])
    assert rec.stats["observe_errors"] == 3 and rec.enabled
    rec.observe("NIFTY", ROW, "sideways", levels(100.0, 1))  # unknown side: ignored, not an error
    assert rec.stats["observe_errors"] == 3


def test_old_minutes_are_dropped_and_disk_floor_pauses(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    clock.t = T0 + 60 * 30
    assert rec.flush() == 0 and rec.stats["dropped_minutes"] == 1

    full = OptionDepthRecorder(tmp_path, clock=clock, min_free_bytes=10**18)
    full.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    clock.t += 70
    assert full.flush() == 0 and "below" in full.paused_reason


def test_compaction_and_retention(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock, stream_retention_days=0)
    for minute in range(3):
        clock.t = T0 + minute * 60 + 1
        rec.observe("NIFTY", ROW, "bid", levels(100.0 + minute, 50))
        rec.observe("RELIANCE", {**ROW, "security_id": "7001"}, "ask", levels(20.0, 10, direction=1))
    clock.t = T0 + 5 * 60
    assert rec.flush() == 3
    day = rec._day(T0)
    with open(stream_file(tmp_path, rec, T0), "a") as handle:
        handle.write("NIFTY,torn")  # a crash mid-write leaves an unterminated line

    clock.t = T0 + 2 * 86_400
    rec.housekeeping()
    with gzip.open(tmp_path / day / ARCHIVE_FILE, "rt") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6 and {r["underlying"] for r in rows} == {"NIFTY", "RELIANCE"}
    manifest = json.loads((tmp_path / day / MANIFEST_FILE).read_text())
    assert manifest["files"][ARCHIVE_FILE]["rows"] == 6 and manifest["columns"] == list(COLUMNS)
    assert not stream_file(tmp_path, rec, T0).exists()  # past retention, and safely compacted


def test_retention_spares_the_other_recorders_stream(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock, stream_retention_days=0)
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    clock.t = T0 + 70
    rec.flush()
    other = stream_file(tmp_path, rec, T0).with_name("micro_1m.csv")
    other.write_text("symbol\n")
    clock.t = T0 + 2 * 86_400
    rec.housekeeping()
    assert other.exists()  # the microstructure recorder's file shares the folder and is not ours to delete


def test_stop_writes_the_open_minute(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    rec.start()
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    rec.stop()
    _, blocks = read_blocks(stream_file(tmp_path, rec, T0))
    assert len(blocks) == 1 and rec.status()["minutes_written"] == 1


def test_the_recorder_switches_itself_off_over_its_cpu_budget(tmp_path):
    rec = recorder(tmp_path, Clock(T0 + 1))
    for _ in range(5):
        rec.stats["observe_ns_total"] += 900_000_000  # 0.9 s of a 1 s window
        rec.check_cpu_budget(1.0)
    assert not rec.enabled and "CPU budget" in rec.paused_reason
    rec.observe("NIFTY", ROW, "bid", levels(100.0, 50))
    assert rec.stats["packets"] == 0


def test_index_depth_state_calls_the_observer_and_survives_a_broken_one():
    settings = SimpleNamespace(timezone="Asia/Kolkata")
    spec = IndexDerivativesSpec(symbol="NIFTY", security_id="13", fno_segment="NSE_FNO")
    state = IndexDepthState(settings, spec)
    state.set_contracts([DepthContract("5001", 25000.0, "CE", "2026-10-07")], "2026-10-07")
    state.set_underlying_ltp(25010.0)
    seen = []
    state.observer = lambda *args: seen.append(args)
    state.update_depth("5001", "bid", levels(100.0, 50))
    state.update_depth("9999", "bid", levels(100.0, 50))  # not tracked: not observed
    assert len(seen) == 1
    underlying, row, side, lv, ltp = seen[0]
    assert (underlying, row["security_id"], side, ltp) == ("NIFTY", "5001", "bid", 25010.0) and len(lv) == 20

    real = OptionDepthRecorder("/nonexistent", min_free_bytes=0)
    state.observer = real.observe
    state.update_depth("5001", "ask", None)  # a malformed packet reaches the recorder, which absorbs it
    assert real.stats["observe_errors"] == 1 and state.status == "LIVE"


def test_stock_depth_state_calls_the_observer():
    state = StockDepthState("RELIANCE", SimpleNamespace(timezone="Asia/Kolkata"))
    state.set_contracts([StockDepthContract("7001", "RELIANCE", 1400.0, "PE", "2026-10-28")], "2026-10-28")
    seen = []
    state.observer = lambda *args: seen.append(args)
    state.update_depth("7001", "ask", levels(20.0, 10, direction=1))
    assert seen and seen[0][0] == "RELIANCE" and seen[0][1]["option_type"] == "PE"
