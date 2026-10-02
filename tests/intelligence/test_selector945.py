"""PSYGRID 945: exactly one decision, determinism, zero look-ahead (poisoned futures), immutability, outcomes."""

import csv
import gzip
import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from daily_archive import EQUITY_FILE, INDEX_FILE
from intelligence.archive import load_day, parse_timestamp, session_days
from intelligence.derivatives import ChainRecorder, chain_rows
from intelligence.frame import as_of_time
from intelligence.matrix import COLUMNS
from intelligence.pipeline945 import decide_day, finish_day, inputs_at_0945, reconstruct, summarise_decisions
from intelligence.selector945 import (
    CALIBRATION_MIN_DAYS,
    HORIZONS,
    Decision,
    DecisionStore,
    TrainingDay,
    calibration_v2,
    choose_model,
    decide,
    decision_outcome,
    eligibility,
    oos_history,
    outcomes,
    render,
    wilson,
)
from intelligence.synthetic import SyntheticMarket, trading_dates

DATES = trading_dates("2026-08-03", 9)
DAY = DATES[-1]


@pytest.fixture(scope="module")
def archive(tmp_path_factory):
    root = tmp_path_factory.mktemp("archive945")
    SyntheticMarket(seed=11).write_days(root, DATES)
    return root


@pytest.fixture(scope="module")
def replayed(archive, tmp_path_factory):
    store = tmp_path_factory.mktemp("store945")
    report = reconstruct(archive, store, warm_sessions=5)
    return store, report


def _decide(archive, store, namespace="t"):
    day = load_day(archive, DAY)
    earlier = [d for d in session_days(archive) if d < DAY]
    return decide_day(archive, store, day, DecisionStore(store, namespace), earlier=earlier, computed_at="test")


# --- the decision -----------------------------------------------------------------------------


def test_reconstruction_decides_exactly_one_stock_per_session(replayed):
    store, report = replayed
    decisions = DecisionStore(store, "replay").decisions()
    assert decisions == DATES[5:]  # the first five sessions only build history
    for session in decisions:
        d = DecisionStore(store, "replay").load_decision(session)
        assert isinstance(d["selected"]["symbol"], str) and d["selected"]["direction"] in ("UP", "DOWN")
        assert d["decision_time"] == f"{session} 09:45:00 IST" and d["immutable"] is True
        assert d["eligible"] >= 1 and d["universe"] >= d["eligible"]
        assert 0 <= d["selection_score"] <= 100
        assert d["probability"]["status"] in ("UNCALIBRATED", "CALIBRATED_OOS")
    assert report["decisions"] == len(decisions) and report["verdict"] == "UNPROVEN"  # 4 decisions prove nothing
    assert set(report["horizons"]) == {"5m", "15m", "30m"}


def test_decision_is_deterministic(archive, replayed):
    store, _ = replayed
    a, _ = _decide(archive, store, "det-a")
    b, _ = _decide(archive, store, "det-b")
    assert a.payload["hashes"]["decision"] == b.payload["hashes"]["decision"]
    assert a.payload["hashes"]["input"] == b.payload["hashes"]["input"]
    replay = DecisionStore(store, "replay").load_decision(DAY)
    assert replay["hashes"]["decision"] == a.payload["hashes"]["decision"]  # replay == a direct decision


def _poison(src: Path, dst: Path, add_late_symbol: bool = True) -> None:
    """Copy the archive and corrupt everything on DAY from 09:45 onward: prices, volume, the index, a new stock."""
    shutil.copytree(src, dst)
    cut = as_of_time(DAY, "09:45").timestamp()
    for name, key in ((EQUITY_FILE, "symbol"), (INDEX_FILE, "index")):
        path = dst / DAY / name
        with gzip.open(path, "rt", newline="") as handle:
            reader = csv.DictReader(handle)
            columns, rows = reader.fieldnames, list(reader)
        late = []
        for r in rows:
            if parse_timestamp(r["timestamp"]) >= cut:
                factor = 3.0 if hash(r[key]) % 2 else 0.2  # enormous moves both ways
                for f in ("open", "high", "low", "close"):
                    r[f] = f"{float(r[f]) * factor:.2f}"
                r["volume"] = str(int(float(r["volume"]) * 1000 + 10**7))
                if add_late_symbol and name == EQUITY_FILE and r[key] == "TCS":
                    late.append({**r, "symbol": "ZZLATE", "security_id": "999999"})
        with gzip.open(path, "wt", newline="") as handle:
            writer = csv.DictWriter(handle, columns)
            writer.writeheader()
            writer.writerows(rows + late)


def test_poisoned_future_cannot_change_the_decision(archive, replayed, tmp_path):
    store, _ = replayed
    clean, clean_matrix = _decide(archive, store, "clean")
    poisoned_root = tmp_path / "poisoned"
    _poison(archive, poisoned_root)
    poisoned_store = tmp_path / "store"
    shutil.copytree(store, poisoned_store)
    # future option data, a future training record and a future decision context
    ChainRecorder(poisoned_store).append(
        DAY,
        chain_rows(
            int(as_of_time(DAY, "09:50").timestamp()),
            "nifty",
            {
                "expiry": "2026-08-20",
                "underlying_ltp": 99999.0,
                "strikes": [
                    {"strike": 99999, "ce": {"last_price": 1e6, "implied_volatility": 900}, "pe": {"last_price": 1e6}}
                ],
            },
        ),
    )
    ds = DecisionStore(poisoned_store, "replay")
    future = ds.load_training(before="2099-01-01")[-1]
    future.session_date = "2099-01-01"
    for h in HORIZONS:
        future.forward[h] = np.full(len(future.keys), 0.5)
    ds.save_training(future)
    # the poison really is there: later bars differ enormously and a late-only symbol exists in the day's data
    clean_day, bad_day = load_day(archive, DAY), load_day(poisoned_root, DAY)
    later = clean_day.equity.minutes >= as_of_time(DAY, "09:45").timestamp()
    i = clean_day.equity.keys.index("TCS")
    j = bad_day.equity.keys.index("TCS")
    assert not np.allclose(clean_day.equity.close[i, later], bad_day.equity.close[j, later])
    assert "ZZLATE" in bad_day.equity.keys
    earlier_bars = ~later
    np.testing.assert_array_equal(clean_day.equity.close[i, earlier_bars], bad_day.equity.close[j, earlier_bars])
    poisoned, poisoned_matrix = _decide(poisoned_root, poisoned_store, "poisoned")
    assert poisoned.payload["hashes"]["decision"] == clean.payload["hashes"]["decision"]
    assert poisoned.payload["hashes"]["input"] == clean.payload["hashes"]["input"]
    assert poisoned.key == clean.key and poisoned.direction == clean.direction
    assert "ZZLATE" not in poisoned_matrix.keys  # a stock that first trades after 09:45 does not exist at 09:45
    for name in COLUMNS:
        np.testing.assert_array_equal(poisoned_matrix.values[name], clean_matrix.values[name], err_msg=name)


def test_training_data_from_the_decision_day_or_later_is_refused(archive, replayed):
    store, _ = replayed
    matrix = inputs_at_0945(archive, store, load_day(archive, DAY))
    same_day = DecisionStore(store, "replay").load_training(before="2099-01-01")[-1]
    assert same_day.session_date == DAY
    with pytest.raises(ValueError, match="before the decision day"):
        decide(matrix, [same_day])


def test_decisions_are_immutable(archive, replayed, tmp_path):
    store, _ = replayed
    decision, _ = _decide(archive, store, "immutable")
    ds = DecisionStore(store, "immutable")
    path = ds.decision_path(DAY)
    assert not ds.save_decision(decision)  # the same decision again: a no-op
    assert oct(path.stat().st_mode & 0o777) == "0o444"
    changed = Decision(json.loads(json.dumps(decision.payload)))
    changed.payload["selected"]["direction"] = "DOWN" if decision.direction == "UP" else "UP"
    changed.payload["hashes"]["decision"] = "different"
    with pytest.raises(ValueError, match="immutable"):
        ds.save_decision(changed)
    assert ds.load_decision(DAY)["selected"] == decision.payload["selected"]


def test_outcome_is_measured_from_the_0945_open(archive, replayed):
    store, _ = replayed
    day = load_day(archive, DAY)
    decision = DecisionStore(store, "replay").load_decision(DAY)
    outcome = decision_outcome(decision, day)
    key, sign = decision["selected"]["symbol"], 1 if decision["selected"]["direction"] == "UP" else -1
    i = day.equity.keys.index(key)
    start = as_of_time(DAY, "09:45").timestamp()
    cols = np.flatnonzero((day.equity.minutes >= start) & (day.equity.minutes < start + 15 * 60))
    entry, exit_ = day.equity.open[i, cols[0]], day.equity.close[i, cols[-1]]
    assert outcome["entry"] == pytest.approx(entry)
    assert outcome["horizons"]["15m"]["return_pct"] == pytest.approx(sign * np.log(exit_ / entry) * 100, abs=1e-3)
    high, low = np.nanmax(day.equity.high[i, cols]), np.nanmin(day.equity.low[i, cols])
    best = np.log(high / entry) if sign > 0 else -np.log(low / entry)
    assert outcome["horizons"]["15m"]["mfe_pct"] == pytest.approx(max(best, 0) * 100, abs=1e-3)
    assert outcome["horizons"]["15m"]["net_return_pct"] < outcome["horizons"]["15m"]["return_pct"]
    assert "SELECTED" in render(decision, outcome) and "IMMUTABLE" in render(decision, outcome)


def test_outcomes_handle_missing_bars(archive):
    day = load_day(archive, DAY)
    o = outcomes(day, ("NOT_A_STOCK",))
    assert np.isnan(o["entry"][0]) and np.isnan(o["forward"][15][0])


def test_eligibility_always_leaves_a_candidate(archive, replayed):
    store, _ = replayed
    matrix = inputs_at_0945(archive, store, load_day(archive, DAY))
    mask, tier = eligibility(matrix)
    assert tier == "STRICT" and mask.sum() > 10
    matrix.values["turnover_session_cr"][:] = 0.0  # nothing passes the liquidity floor
    mask, tier = eligibility(matrix)
    assert tier == "RELAXED" and mask.any()
    decision = decide(matrix, [])
    assert decision.payload["data_quality"]["status"] == "DEGRADED"
    matrix.values["ltp"][:] = np.nan
    with pytest.raises(ValueError, match="no usable data"):
        eligibility(matrix)


def test_finish_day_writes_a_training_record_with_outcomes(archive, replayed):
    store, _ = replayed
    record = DecisionStore(store, "replay").load_training(before="2099-01-01")[-1]
    assert record.session_date == DAY and set(record.values) == set(COLUMNS)
    assert set(record.forward) == set(HORIZONS) and np.isfinite(record.forward[15]).sum() > 10


# --- model choice and calibration on constructed data ------------------------------------------


def _days(count: int, signal: float, seed: int = 0) -> list[TrainingDay]:
    """Training days where the 15-minute forward return depends on ret_15m with strength ``signal``."""
    rng = np.random.default_rng(seed)
    out = []
    n = 300
    keys = tuple(f"S{i:03d}" for i in range(n))
    for d in range(count):
        values = {name: rng.normal(0, 1, n) for name in COLUMNS}
        values.update(ltp=np.full(n, 100.0), prev_close=np.full(n, 100.0), completeness=np.ones(n),
                      last_bar_age_min=np.zeros(n), frozen=np.zeros(n), turnover_session_cr=np.full(n, 5.0),
                      rvol_session=np.full(n, 0.001), rel_volume=np.exp(rng.normal(0, 0.3, n)),
                      market_ret_open=np.zeros(n), breadth=np.full(n, 0.5), dispersion=np.full(n, 0.01))  # fmt: skip
        y = signal * 0.002 * values["ret_15m"] + rng.normal(0, 0.002, n)
        fwd = dict.fromkeys(HORIZONS, y)
        out.append(TrainingDay(f"2026-0{1 + d // 28}-{1 + d % 28:02d}", keys, values, np.ones(n, bool), fwd,
                               {h: np.abs(y) for h in HORIZONS}, {h: -np.abs(y) for h in HORIZONS}))  # fmt: skip
    return out


def test_v2_must_earn_its_place():
    strong = oos_history(_days(45, signal=1.0))
    model, evidence = choose_model(strong)
    assert model == "v2" and evidence["v2_oos_ic"] > 0.2 and evidence["common_oos_days"] >= 15
    noise = oos_history(_days(45, signal=0.0, seed=3))
    model, evidence = choose_model(noise)
    assert model == "v1" and "v2 needs" in evidence["rule"]


def test_probability_is_calibrated_only_with_enough_oos_evidence():
    history = oos_history(_days(45, signal=1.0))
    cal = calibration_v2(history["v2_points"])
    assert cal["days"] >= CALIBRATION_MIN_DAYS and cal["status"] == "CALIBRATED_OOS" and cal["ece"] <= 0.05
    assert cal["b"] > 0  # a larger |prediction| means a likelier direction
    few = calibration_v2([p for p in history["v2_points"] if p[0] < history["v2_points"][0][0][:8] + "99"][:500])
    assert few["status"] == "UNCALIBRATED"
    assert calibration_v2([])["status"] == "UNCALIBRATED"


def test_wilson_interval():
    lo, hi = wilson(30, 50)
    assert lo < 0.6 < hi and lo > 0.45 and hi < 0.75
    assert wilson(0, 0) == (0.0, 1.0)


def test_summary_verdicts(replayed):
    store, _ = replayed
    report = summarise_decisions(DecisionStore(store, "replay"))
    assert report["horizons"]["15m"]["n"] == 4 and report["verdict"] == "UNPROVEN"
    assert summarise_decisions(DecisionStore(store, "empty"))["verdict"] == "NO_DECISIONS"


def test_finish_day_is_repeatable(archive, replayed):
    store, _ = replayed
    day = load_day(archive, DAY)
    ds = DecisionStore(store, "replay")
    decision = Decision(ds.load_decision(DAY))
    matrix = inputs_at_0945(archive, store, day)
    first = finish_day(ds, decision, matrix, day)
    assert finish_day(ds, decision, matrix, day) == first == ds.load_outcome(DAY)


def test_live_decision_from_the_stream_equals_the_replayed_decision(archive, replayed, tmp_path):
    """The live runner decides at 09:45 from PSYGRID's minute stream; replay decides from the archive."""
    from datetime import timedelta

    from intelligence.live import LiveRunner
    from intelligence.settings import Settings
    from intelligence.stream import stream_path
    from microstructure import BAR_COLUMNS, MicrostructureRecorder

    store, _ = replayed
    live_archive, live_store = tmp_path / "archive", tmp_path / "store"
    shutil.copytree(archive, live_archive)
    shutil.copytree(store, live_store)
    shutil.rmtree(live_store / "945" / "replay")
    # Today: no archived bars yet (the reference file is there, as PSYGRID writes it with its first archive).
    blocks: dict[int, list[tuple]] = {}
    cut = as_of_time(DAY, "09:45").timestamp()
    for name, key in ((EQUITY_FILE, "symbol"), (INDEX_FILE, "index")):
        path = live_archive / DAY / name
        with gzip.open(path, "rt", newline="") as handle:
            reader = csv.DictReader(handle)
            columns, rows = reader.fieldnames, list(reader)
        for r in rows:
            minute = parse_timestamp(r["timestamp"])
            if minute < cut:
                sid = r.get("security_id", "") if key == "symbol" else f"IDX:{r['index']}"
                blocks.setdefault(minute, []).append(
                    (r["symbol"], sid, r["timestamp"], r["open"], r["high"], r["low"], r["close"], r["volume"])
                )
        if name == EQUITY_FILE:
            path.unlink()
        else:
            with gzip.open(path, "wt", newline="") as handle:
                csv.DictWriter(handle, columns).writeheader()
    for minute in sorted(blocks):
        MicrostructureRecorder._append_block(None, stream_path(live_archive, DAY), BAR_COLUMNS, blocks[minute], minute)
    settings = Settings(live_archive, live_store, "http://127.0.0.1:9", "127.0.0.1", 18101, False, 600, 100, 3, 2,
                        False, False, 60, 2)  # fmt: skip
    now = as_of_time(DAY, "09:45") + timedelta(seconds=6)
    runner = LiveRunner(settings, fetch=lambda url: {}, clock=lambda: now)
    runner.follow_archive(now)
    live = runner.run_945(now)
    assert live is not None and runner.status["945"]["symbol"] == live["selected"]["symbol"]
    replay = DecisionStore(store, "replay").load_decision(DAY)
    assert live["hashes"]["decision"] == replay["hashes"]["decision"]
    assert runner.run_945(now + timedelta(minutes=5))["hashes"] == live["hashes"]  # decided once, never again
    before = as_of_time(DAY, "09:44") + timedelta(seconds=30)
    assert LiveRunner(settings, fetch=lambda url: {}, clock=lambda: before).run_945(before) is None
