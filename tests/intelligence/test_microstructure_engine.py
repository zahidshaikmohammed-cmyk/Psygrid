"""Microstructure engine: cadence gates every metric; classification of moves; reading the recorder's stream."""

import pytest

from intelligence.microstructure_engine import classify, index_rows, micro_state
from intelligence.stream import micro_rows
from microstructure import MicrostructureRecorder
from output import _ist_timestamp

T0 = 1_790_000_040  # a minute boundary


def row(i, packets=40, trades=10, buy=800, sell=200, mid0=100.0, mid1=100.05, ask5_last=5000, gap=2000):
    return {"symbol": "TCS", "timestamp": _ist_timestamp(T0 + 60 * i), "packets": packets, "trade_packets": trades,
            "max_gap_ms": gap, "invalid_book": 0, "buy_volume": buy, "sell_volume": sell, "unclassified_volume": 0,
            "volume": buy + sell, "mid_first": mid0, "mid_last": mid1, "spread_bps_mean": 2.0,
            "bid1_qty_mean": 500, "ask1_qty_mean": 500, "bid5_qty_mean": 3000, "ask5_qty_mean": 3000,
            "bid5_qty_last": 5000, "ask5_qty_last": ask5_last, "ofi": 100, "bid1_depletion": 100,
            "bid1_replenish": 150, "ask1_depletion": 300, "ask1_replenish": 100, "imbalance1_mean": 0.2}  # fmt: skip


def test_buying_that_moves_price_is_aggressive_consumption():
    rows = [row(i, mid0=100 + 0.05 * i, mid1=100 + 0.05 * (i + 1)) for i in range(15)]
    state = micro_state(index_rows(rows)["TCS"], "TCS", T0 + 15 * 60)
    assert state.minutes == 15 and state.support["quote"] and state.support["trade"]
    assert state.signed_flow == pytest.approx(0.6) and state.ofi_norm == pytest.approx(1500 / 500)
    assert state.replenish_ratio == pytest.approx(250 / 400) and state.move_bps > 50
    assert state.classification == "AGGRESSIVE_CONSUMPTION"
    assert micro_state(index_rows(rows)["TCS"], "TCS", T0 + 5 * 60).minutes == 5  # minutes 0..4 have closed


def test_sparse_packets_make_metrics_unsupported_not_estimated():
    rows = [row(i, packets=3, trades=1, gap=30_000) for i in range(15)]
    state = micro_state(index_rows(rows)["TCS"], "TCS", T0 + 15 * 60)
    assert not state.support["quote"] and not state.support["trade"]
    assert state.ofi_norm is None and state.signed_flow is None and state.classification == "UNSUPPORTED"
    assert any("unsupported" in n for n in state.notes)


def test_classification_rules():
    assert classify(0.5, 2.0, 0.6, None, True) == "ABSORPTION"
    assert classify(0.5, 2.0, 0.0, None, True) == "QUIET"
    assert classify(-30, 2.0, -0.5, None, True) == "AGGRESSIVE_CONSUMPTION"
    assert classify(30, 2.0, 0.05, 0.4, True) == "LIQUIDITY_WITHDRAWAL"
    assert classify(30, 2.0, -0.5, 0.9, True) == "MIXED"
    assert classify(30, 2.0, 0.5, 0.9, False) == "UNSUPPORTED"


def test_reads_the_recorders_stream(tmp_path):
    from tests.test_microstructure import packet

    now = [T0]
    rec = MicrostructureRecorder(tmp_path, {"101": "TCS"}, clock=lambda: now[0], min_free_bytes=0)
    volume, price = 1000, 100.0
    for minute in range(16):
        for k in range(30):
            now[0] = T0 + 60 * minute + k * 2
            volume += 10
            price += 0.01
            rec.observe("101", packet(price - 0.05, price + 0.05, 500, 300, price + 0.05, volume))
        rec.flush(now[0] + 60)
    day = _ist_timestamp(T0)[:10]
    rows = micro_rows(tmp_path, day)
    state = micro_state(index_rows(rows)["TCS"], "TCS", T0 + 15 * 60)
    assert state.minutes == 15 and state.cadence["packets_per_minute"] == 30
    assert state.support["quote"] and state.move_bps > 0 and state.signed_flow > 0.9  # every trade at the ask
