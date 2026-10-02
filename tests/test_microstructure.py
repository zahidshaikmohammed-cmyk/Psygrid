"""The Full-packet research recorder: features, stream blocks, compaction, budgets, and feed isolation."""

import csv
import gzip
import json

import pytest

from microstructure import (
    BARS_STREAM,
    END_MARKER,
    MICRO_ARCHIVE,
    MICRO_COLUMNS,
    MICRO_STREAM,
    MicrostructureRecorder,
    state_bars_for_minute,
)

T0 = 1_790_000_040  # a minute boundary (UTC epoch divisible by 60)


def packet(bid, ask, bq, aq, ltp, volume, tbq=1000, tsq=900):
    """Shaped like dhanhq MarketFeed.process_full output (prices are formatted strings)."""
    depth = [{"bid_quantity": bq + i * 10, "ask_quantity": aq + i * 10, "bid_orders": 3, "ask_orders": 4,
              "bid_price": f"{bid - i * 0.05:.2f}", "ask_price": f"{ask + i * 0.05:.2f}"} for i in range(5)]  # fmt: skip
    return {"type": "Full Data", "security_id": 101, "LTP": f"{ltp:.2f}", "LTQ": 1, "LTT": "10:15:00",
            "avg_price": f"{ltp:.2f}", "volume": volume, "total_buy_quantity": tbq, "total_sell_quantity": tsq,
            "depth": depth}  # fmt: skip


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def recorder(tmp_path, clock, **kw):
    return MicrostructureRecorder(tmp_path, {"101": "TCS", "102": "INFY"}, clock=clock, min_free_bytes=0, **kw)


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


def test_features_from_a_known_packet_sequence(tmp_path):
    clock = Clock(T0 + 1)
    rec = recorder(tmp_path, clock)
    sequence = [
        packet(100.00, 100.10, 500, 400, 100.05, 1000),  # first packet: book only
        packet(100.00, 100.10, 300, 400, 100.10, 1200),  # bid depleted by 200; 200 traded at the ask (buy)
        packet(100.00, 100.10, 600, 400, 100.00, 1300),  # bid replenished by 300; 100 traded at the bid (sell)
        packet(100.05, 100.10, 200, 400, 100.05, 1300),  # bid moves up: OFI +200
        packet(0, 0, 0, 0, 100.05, 1300),  # an empty book: counted as invalid, not used
    ]
    for i, p in enumerate(sequence):
        clock.t = T0 + 1 + i
        rec.observe("101", p)
    assert rec.flush(T0 + 60 + 4) == 1
    header, blocks = read_blocks(rec.stream_dir(rec._day(T0)) / MICRO_STREAM)
    assert tuple(header) == MICRO_COLUMNS
    minute, count, rows = blocks[0]
    assert minute == T0 and count == 1
    r = rows[0]
    assert r["symbol"] == "TCS" and int(r["packets"]) == 5 and int(r["invalid_book"]) == 1
    assert int(r["volume"]) == 300 and int(r["buy_volume"]) == 200 and int(r["sell_volume"]) == 100
    assert int(r["bid1_depletion"]) == 200 and int(r["bid1_replenish"]) == 300 and int(r["bid_up"]) == 1
    # OFI between packets: (-200) + (+300) + (+200, the bid price rose) = +300
    assert int(r["ofi"]) == 300
    assert float(r["spread_bps_max"]) == pytest.approx((100.10 - 100.00) / 100.05 * 1e4, rel=1e-3)
    assert int(r["max_gap_ms"]) == 1000 and int(r["quote_changes"]) == 3


def test_minutes_are_written_once_after_the_grace_period(tmp_path):
    clock = Clock(T0 + 10)
    rec = recorder(tmp_path, clock)
    rec.observe("101", packet(100, 100.1, 10, 10, 100.05, 10))
    assert rec.flush(T0 + 61) == 0  # inside the grace period
    assert rec.flush(T0 + 64) == 1 and rec.flush(T0 + 65) == 0
    clock.t = T0 + 70
    rec.observe("102", packet(50, 50.1, 10, 10, 50.05, 10))
    rec.flush(T0 + 130)
    _, blocks = read_blocks(rec.stream_dir(rec._day(T0)) / MICRO_STREAM)
    assert [b[0] for b in blocks] == [T0, T0 + 60]


def test_garbage_never_raises_and_errors_are_counted(tmp_path):
    rec = recorder(tmp_path, Clock(T0))
    rec.observe("101", {"depth": [{"bid_price": object()}], "volume": "x"})
    rec.observe("101", None)  # not even a dict
    assert rec.stats["observe_errors"] >= 1 and rec.enabled


def test_old_minutes_are_dropped_and_disk_floor_pauses(tmp_path):
    rec = recorder(tmp_path, Clock(T0))
    rec.observe("101", packet(100, 100.1, 10, 10, 100.05, 10))
    rec.flush(T0 + 60 * 30)
    assert rec.stats["dropped_minutes"] == 1 and rec.stats["minutes_written"] == 0
    full = MicrostructureRecorder(tmp_path, {"101": "TCS"}, clock=Clock(T0), min_free_bytes=10**18)
    full.observe("101", packet(100, 100.1, 10, 10, 100.05, 10))
    full.flush(T0 + 64)
    assert full.stats["minutes_written"] == 0 and "below" in full.paused_reason


def test_compaction_and_retention(tmp_path):
    clock = Clock(T0 + 5)
    rec = recorder(tmp_path, clock, raw_symbols={"TCS"})
    for m in range(3):
        clock.t = T0 + 60 * m + 5
        rec.observe("101", packet(100, 100.1, 10, 10, 100.05, 10 + m))
        rec.flush(T0 + 60 * m + 1)
    rec.flush(T0 + 60 * 3 + 5)
    day = rec._day(T0)
    with open(rec.stream_dir(day) / MICRO_STREAM, "a") as handle:
        handle.write("TCS,101,torn")  # a crash mid-write leaves a partial line
    target = rec.compact(day)
    with gzip.open(target, "rt") as handle:
        lines = handle.read().splitlines()
    assert lines[0].startswith("symbol,") and len(lines) == 4 and not any(END_MARKER in x for x in lines)
    manifest = json.loads((tmp_path / day / "microstructure_manifest.json").read_text())
    assert manifest["files"][MICRO_ARCHIVE]["rows"] == 3 and "depth_snapshots.csv.gz" in manifest["files"]
    with gzip.open(tmp_path / day / "depth_snapshots.csv.gz", "rt") as handle:
        assert len(handle.read().splitlines()) == 3
    rec.housekeeping(T0 + 86400 * 5)
    assert not rec.stream_dir(day).exists() and target.exists()


def test_bars_stream_comes_from_psygrid_state(tmp_path):
    from config import Settings
    from state import PsygridState

    settings = Settings(client_id="c", access_token="t")
    live = PsygridState(settings)
    live.set_instruments([type("I", (), {"symbol": "TCS", "security_id": "101"})()]) if hasattr(
        live, "set_instruments") else live.instruments.update({"101": {"symbol": "TCS"}})  # fmt: skip
    live.session_status = "LIVE"
    live.update_quote("101", {"LTT_EPOCH": T0 + 5, "LTP": 100.0, "volume": 10, "LTQ": 1})
    live.update_quote("101", {"LTT_EPOCH": T0 + 30, "LTP": 101.0, "volume": 25, "LTQ": 1})
    rows = state_bars_for_minute(live, {"101": "TCS"}, T0)
    assert rows and rows[0][0] == "TCS" and rows[0][3:7] == (100.0, 101.0, 100.0, 101.0)
    rec = MicrostructureRecorder(tmp_path, {"101": "TCS"}, clock=Clock(T0 + 5), min_free_bytes=0,
                                 bars_for_minute=lambda m: state_bars_for_minute(live, {"101": "TCS"}, m))  # fmt: skip
    rec.observe("101", packet(100, 100.1, 10, 10, 100.05, 10))
    rec.flush(T0 + 64)
    _, blocks = read_blocks(rec.stream_dir(rec._day(T0)) / BARS_STREAM)
    assert blocks[0][2][0]["close"] == "101.0"


def test_the_feed_calls_the_observer_and_survives_a_broken_one():
    from config import Instrument, Settings
    from feed import LiveFeed
    from state import PsygridState

    settings = Settings(client_id="c", access_token="t")
    live = PsygridState(settings)
    live.instruments["101"] = {"symbol": "TCS"}
    live.session_status = "LIVE"
    feed = LiveFeed(settings, live, [Instrument("TCS", "101")])
    seen = []
    feed.observer = lambda sid, data: seen.append(sid)
    data = packet(100, 100.1, 10, 10, 100.05, 10)
    data["LTT"] = T0
    feed._handle_packet(data)
    feed._handle_packet({"type": "Ticker Data", "security_id": 101, "LTP": "1"})
    assert seen == ["101"]

    def broken(sid, data):
        raise RuntimeError("bug")

    feed.observer = broken
    feed._handle_packet(data)  # must not raise
    assert feed.observer is None


def test_the_recorder_switches_itself_off_over_its_cpu_budget(tmp_path):
    rec = recorder(tmp_path, Clock(T0))
    for second in range(4):
        rec.stats["observe_ns_total"] += 300_000_000  # 30% of a core in this second
        rec.check_cpu_budget(1.0)
        assert rec.enabled, second
    rec.stats["observe_ns_total"] += 300_000_000
    rec.check_cpu_budget(1.0)
    assert not rec.enabled and "CPU budget" in rec.paused_reason
    rec.observe("101", packet(100, 100.1, 10, 10, 100.05, 10))  # off: a no-op
    assert rec.stats["packets"] == 0
