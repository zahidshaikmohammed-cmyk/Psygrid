"""Whole-pipeline replay: determinism, no look-ahead, and live/replay equivalence."""

import dataclasses
import json
import shutil
from datetime import timedelta

import numpy as np
import pytest

from intelligence.archive import EQUITY_FILE, INDEX_FILE, _read_rows, day_from_payloads, load_day
from intelligence.derivatives import DerivativesRecorder, snapshot_from_payloads
from intelligence.engine import IntelligenceEngine, replay_session
from intelligence.event_store import EventStore
from intelligence.frame import as_of_time, frame_at
from intelligence.replay import replay
from tests.intelligence.conftest import TODAY


@pytest.fixture(scope="module")
def day(market_root):
    return load_day(market_root, TODAY)


def engine(market_root, store_root, db):
    return IntelligenceEngine(market_root, store_root, event_store=EventStore(db))


def poison(day, cutoff_epoch):
    """The same day with every bar that completes after ``cutoff`` replaced by wild values."""

    def mangle(bars):
        future = bars.minutes + 60 > cutoff_epoch
        arrays = {}
        for name in ("open", "high", "low", "close"):
            a = bars.__dict__[name].copy()
            a[:, future] *= 3.7
            arrays[name] = a
        volume = bars.volume.copy()
        volume[:, future] *= 250
        rejected = {k: list(v) for k, v in bars.rejected.items()}
        for key in bars.keys[:5]:
            rejected.setdefault(key, []).append((int(bars.minutes[-1]), "invalid_ohlc"))
        return dataclasses.replace(bars, volume=volume, rejected=rejected, **arrays)

    return dataclasses.replace(day, equity=mangle(day.equity), indices=mangle(day.indices))


def test_full_session_replay_is_deterministic(day, market_root, store_root, tmp_path):
    first = replay_session(engine(market_root, store_root, tmp_path / "a.db"), day)
    second = replay_session(engine(market_root, store_root, tmp_path / "b.db"), day)
    assert first.steps == 361 and first.events > 0
    assert first.events_by_type == second.events_by_type
    a = EventStore(tmp_path / "a.db").search(after_seq=0, limit=1000)
    b = EventStore(tmp_path / "b.db").search(after_seq=0, limit=1000)
    assert [e["event_id"] for e in a] == [e["event_id"] for e in b]
    assert {"volume_surge", "return_shock", "sector_divergence"} <= set(first.events_by_type)


@pytest.mark.parametrize("minute", [45, 150, 229, 330])
def test_no_engine_sees_the_future(day, market_root, store_root, tmp_path, minute):
    as_of = as_of_time(TODAY, "09:15") + timedelta(minutes=minute + 1)
    cutoff = int(as_of.timestamp())
    for store, future in (("clean", False), ("dirty", True)):
        recorder = DerivativesRecorder(tmp_path / store)
        for m in range(0, 361):  # the dirty store also holds later, wild snapshots, which must stay invisible
            epoch = int(as_of_time(TODAY, "09:15").timestamp()) + 60 * (m + 1)
            if epoch > cutoff and not future:
                break
            wild = 50.0 if epoch > cutoff else 1.0
            futures = {
                "nifty": {
                    "last_price": (25100.0 + m % 5) * wild,
                    "top_bid_price": 25099.0,
                    "top_ask_price": 25101.0 * wild,
                }
            }
            options = {"nifty": {"analytics": {"pcr_oi": 1.0 + 0.001 * (m % 7) * wild, "iv_skew": 0.5}}}
            recorder.append(TODAY, snapshot_from_payloads(epoch, {"nifty": 25000.0}, futures, options))
        for name in ("summaries", "states"):
            if (store_root / name).exists():
                shutil.copytree(store_root / name, tmp_path / store / name, dirs_exist_ok=True)
    clean_engine = IntelligenceEngine(market_root, tmp_path / "clean", event_store=EventStore(tmp_path / "c.db"))
    dirty_engine = IntelligenceEngine(market_root, tmp_path / "dirty", event_store=EventStore(tmp_path / "d.db"))
    dirty_day = poison(day, cutoff)
    for clean_frame, dirty_frame in zip(replay(day, every=7, end=as_of.strftime("%H:%M")),
                                        replay(dirty_day, every=7, end=as_of.strftime("%H:%M")), strict=True):  # fmt: skip
        clean_engine.step(clean_frame), dirty_engine.step(dirty_frame)
    a, b = clean_engine.step(frame_at(day, as_of)), dirty_engine.step(frame_at(dirty_day, as_of))
    assert a.as_of == b.as_of == as_of.strftime("%Y-%m-%d %H:%M:%S IST")
    for name in a.features.values:
        np.testing.assert_array_equal(a.features.values[name], b.features.values[name])
    for name, measure in a.report.measures.items():
        np.testing.assert_array_equal(measure.z, b.report.measures[name].z)
        assert list(measure.classification) == list(b.report.measures[name].classification)
    assert a.relationships == b.relationships
    if minute >= 45:  # enough snapshots for the derivatives series to be judged
        assert any(r.kind == "spot_futures_basis" and r.z is not None for r in a.relationships)
    stored_a = EventStore(tmp_path / "c.db").search(after_seq=0, limit=1000)
    stored_b = EventStore(tmp_path / "d.db").search(after_seq=0, limit=1000)
    assert [e["event_id"] for e in stored_a] == [e["event_id"] for e in stored_b]
    assert all(e["bar_epoch"] + 60 <= cutoff for e in stored_a)


def live_payloads(market_root, session_date, until_epoch):
    """What /public/live.json and /public/<index>.json would have served: candles opened before ``until``."""
    from intelligence.archive import parse_timestamp

    stocks = {}
    for row in _read_rows(market_root / session_date / EQUITY_FILE):
        if parse_timestamp(row["timestamp"]) >= until_epoch:
            continue
        stock = stocks.setdefault(row["symbol"], {"candles_1m": []})
        stock["candles_1m"].append({k: row[k] for k in ("timestamp", "open", "high", "low", "close", "volume")})
    reference = _read_rows(market_root / session_date / "equity_reference.csv.gz")
    for row in reference:
        stocks.setdefault(row["symbol"], {"candles_1m": []}).update(
            previous_close=row["previous_close"], today_open=row["today_open"]
        )
    indices = {}
    for row in _read_rows(market_root / session_date / INDEX_FILE):
        if parse_timestamp(row["timestamp"]) >= until_epoch:
            continue
        snap = indices.setdefault(row["index"], {"symbol": row["symbol"], "session": {"date": session_date}, "1m": []})
        snap["1m"].append({k: row[k] for k in ("timestamp", "open", "high", "low", "close", "volume")})
    return {"session": {"date": session_date}, "stocks": stocks}, indices


def test_live_payloads_give_the_same_frame_as_the_archive(day, market_root):
    as_of = as_of_time(TODAY, "11:00")
    # The live payload also carries the candle still forming at 11:00; the frame must drop it.
    live, indices = live_payloads(market_root, TODAY, int(as_of.timestamp()) + 60)
    live_frame = frame_at(day_from_payloads(live, indices), as_of)
    archived = frame_at(day, as_of)
    assert live_frame.equity.keys == archived.equity.keys
    for name in ("open", "high", "low", "close", "volume"):
        np.testing.assert_array_equal(getattr(live_frame.equity, name), getattr(archived.equity, name))
        np.testing.assert_array_equal(getattr(live_frame.indices, name), getattr(archived.indices, name))
    assert live_frame.reference == archived.reference
    with pytest.raises(ValueError):
        day_from_payloads({"stocks": {}})


def test_snapshot_views_are_strict_json(day, market_root, store_root, tmp_path):
    eng = engine(market_root, store_root, tmp_path / "e.db")
    snap = eng.step(frame_at(day, as_of_time(TODAY, "13:00")))
    for view in (snap.market(), snap.instrument("SUNPHARMA"), snap.observations(["TCS", "NOPE"]),
                 snap.anomalies(), snap.relationship_views(kind="stock_sector"), snap.events):  # fmt: skip
        json.dumps(view, allow_nan=False)
    assert snap.instrument("NOPE") is None
    assert snap.market()["top_anomalies"] is not None
    assert len(snap.observations(["TCS", "NOPE"])) == 1
    assert set(snap.timings_ms) >= {"features", "anomalies", "relationships", "events", "total"}
