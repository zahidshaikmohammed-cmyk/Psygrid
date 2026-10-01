from datetime import timedelta

import numpy as np
import pytest

from intelligence.anomaly import EXTREME, INSUFFICIENT_DATA, NORMAL, UNUSUAL, detect
from intelligence.archive import load_day
from intelligence.derivatives import DerivativesRecorder, load_derivatives, snapshot_from_payloads
from intelligence.features import compute_features
from intelligence.frame import as_of_time, frame_at
from intelligence.history import build_baselines
from intelligence.relationships import (
    NOT_ESTABLISHED,
    all_relationships,
    derivatives_relationships,
    judge_against,
    price_volume_relationships,
    sector_relationships,
    stock_relationships,
)
from tests.intelligence.conftest import TODAY


def minute_time(minute: int):
    return as_of_time(TODAY, "09:15") + timedelta(minutes=minute + 1)


@pytest.fixture(scope="module")
def day(market_root):
    return load_day(market_root, TODAY)


def at(day, minute):
    frame = frame_at(day, minute_time(minute))
    return frame, compute_features(frame)


def test_slow_divergence_from_sector_is_flagged(day):
    frame, features = at(day, 229)  # the last 15 minutes are all inside the injected drift
    rels = stock_relationships(frame, features, only_flagged=False)
    sun = next(r for r in rels if r.kind == "stock_sector" and r.subject == "SUNPHARMA")
    assert sun.counterpart == "PHARMA_HEALTHCARE"
    assert sun.classification in (UNUSUAL, EXTREME) and sun.z > 3
    assert sun.evidence["correlation"] >= 0.3 and sun.evidence["recent_minutes"] == 15
    assert sun.evidence["subject_return"] > sun.evidence["counterpart_return"]
    before, before_features = at(day, 190)  # same stock before the drift began
    earlier = stock_relationships(before, before_features, only_flagged=False)
    assert next(r for r in earlier if r.kind == "stock_sector" and r.subject == "SUNPHARMA").classification == NORMAL


def test_stock_relationships_cover_index_and_sector_index(day):
    frame, features = at(day, 229)
    kinds = {(r.kind, r.subject): r for r in stock_relationships(frame, features, only_flagged=False)}
    assert kinds[("stock_index", "INFY")].counterpart == "nifty500"
    assert kinds[("stock_sector_index", "INFY")].counterpart == "niftyit"
    flagged = stock_relationships(frame, features)
    assert all(r.classification in (UNUSUAL, EXTREME) for r in flagged)


def test_short_sessions_are_insufficient(day):
    frame, features = at(day, 30)  # 31 bars < 15 recent + 40 estimation
    rels = stock_relationships(frame, features, only_flagged=False)
    assert rels and {r.classification for r in rels} == {INSUFFICIENT_DATA}


def test_uncorrelated_series_is_not_established():
    rng = np.random.default_rng(1)
    r = rng.normal(0, 1e-3, (1, 120))
    bench = rng.normal(0, 1e-3, (1, 120))
    assert judge_against(r, bench)["classification"][0] == NOT_ESTABLISHED
    tracked = judge_against(bench + rng.normal(0, 2e-4, (1, 120)), bench)
    assert tracked["classification"][0] == NORMAL and tracked["correlation"][0] > 0.9


def test_judgement_ignores_the_future():
    rng = np.random.default_rng(2)
    bench = rng.normal(0, 1e-3, (1, 200))
    r = bench + rng.normal(0, 3e-4, (1, 200))
    first = judge_against(r[:, :120], bench[:, :120])
    r2 = r.copy()
    r2[:, 120:] += 0.05  # a later shock must not change the earlier judgement
    second = judge_against(r2[:, :120], bench[:, :120])
    assert first["z"][0] == second["z"][0]


def test_sector_pairs(day):
    _, features = at(day, 229)
    rels = sector_relationships(features, only_flagged=False)
    assert rels and all(r.kind == "sector_sector" and r.subject < r.counterpart for r in rels)
    assert all(r.evidence["history_points"] >= 10 for r in rels)


def test_price_and_volume_disagreements():
    from types import SimpleNamespace

    def measure(z):
        return SimpleNamespace(z=np.array(z), baseline=np.array(["HISTORICAL"] * len(z)))

    report = SimpleNamespace(
        keys=("A", "B", "C", "D"),
        measures={"volume": measure([6.0, 0.2, 4.0, np.nan]), "return": measure([0.5, -3.5, 4.0, 9.0])},
    )
    rels = {r.subject: r for r in price_volume_relationships(report)}
    assert rels["A"].counterpart == "volume_without_move" and rels["A"].classification == EXTREME
    assert rels["B"].counterpart == "move_without_volume" and rels["B"].classification == UNUSUAL
    assert "C" not in rels and "D" not in rels  # agreeing, or not scored


def test_price_volume_on_real_report(day, market_root, store_root):
    baselines = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    frame, features = at(day, 120)
    rels = price_volume_relationships(detect(frame, features, baselines))
    assert all(r.counterpart in ("volume_without_move", "move_without_volume") for r in rels)


# --- derivatives ---------------------------------------------------------------------------


def _record(root, minutes=40, shock_at=None):
    recorder = DerivativesRecorder(root)
    start = int(as_of_time(TODAY, "09:15").timestamp())
    rng = np.random.default_rng(3)
    for m in range(minutes):
        spot = 25000 + m
        basis = 0.004 + rng.normal(0, 0.0001) + (0.003 if m == shock_at else 0)
        futures = {"nifty": {"last_price": spot * (1 + basis), "top_bid_price": spot * (1 + basis) - 1,
                             "top_ask_price": spot * (1 + basis) + 1, "oi": 1e6, "volume": 1000 * m}}  # fmt: skip
        options = {"nifty": {"underlying_ltp": spot,
                             "analytics": {"pcr_oi": 1.0 + rng.normal(0, 0.01), "iv_skew": "bad", "atm_strike": 25000}}}  # fmt: skip
        recorder.append(TODAY, snapshot_from_payloads(start + 60 * (m + 1), {"nifty": spot}, futures, options))
    return start


def test_snapshot_cleans_values():
    snap = snapshot_from_payloads(60, {"nifty": "25000"}, {"nifty": {"last_price": float("nan"), "oi": True}},
                                  {"nifty": None})  # fmt: skip
    assert snap["spot"]["nifty"] == 25000.0
    assert snap["futures"]["nifty"]["last_price"] is None and snap["futures"]["nifty"]["oi"] is None
    assert snap["options"]["nifty"]["pcr_oi"] is None


def test_basis_shock_is_flagged_and_bad_values_are_skipped(tmp_path):
    start = _record(tmp_path, minutes=40, shock_at=39)
    derivs = load_derivatives(tmp_path, TODAY)
    rels = derivatives_relationships(derivs, start + 60 * 40, only_flagged=False)
    by_kind = {r.kind: r for r in rels}
    assert by_kind["spot_futures_basis"].classification == EXTREME
    assert by_kind["option_pcr_oi"].classification in (NORMAL, UNUSUAL)
    assert by_kind["option_iv_skew"].classification == INSUFFICIENT_DATA  # every value was non-numeric
    # One minute earlier the shock had not been recorded yet.
    earlier = {r.kind: r for r in derivatives_relationships(derivs, start + 60 * 39, only_flagged=False)}
    assert earlier["spot_futures_basis"].classification == NORMAL


def test_loader_skips_torn_lines_and_hides_future_snapshots(tmp_path):
    start = _record(tmp_path, minutes=5)
    path = tmp_path / "derivatives" / f"{TODAY}.jsonl"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write('{"minute": 12')  # crash mid-write
    derivs = load_derivatives(tmp_path, TODAY)
    assert len(derivs.snapshots) == 5
    assert len(derivs.known_at(start + 60 * 3)) == 3
    assert [m for m, _ in derivs.series(start + 120, "spot", "nifty", "x")] == [start + 60, start + 120]
    assert load_derivatives(tmp_path, "2020-01-01").snapshots == ()


def test_all_relationships_runs_end_to_end(day, market_root, store_root, tmp_path):
    baselines = build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)
    frame, features = at(day, 229)
    report = detect(frame, features, baselines)
    rels = all_relationships(frame, features, report, load_derivatives(tmp_path, TODAY))
    assert any(r.subject == "SUNPHARMA" for r in rels)
