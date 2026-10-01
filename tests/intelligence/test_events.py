from datetime import timedelta
from itertools import pairwise

import pytest

from intelligence.anomaly import detect
from intelligence.archive import load_day
from intelligence.event_store import EventStore
from intelligence.events import COOLDOWN_MINUTES, EVENT_TYPES, EventEngine, event_id, severity
from intelligence.features import compute_features
from intelligence.frame import as_of_time, frame_at
from intelligence.history import build_baselines
from intelligence.relationships import all_relationships
from tests.intelligence.conftest import DATES, TODAY

FORBIDDEN = ("bull", "bear", "buy", "sell", "long", "short ")


@pytest.fixture(scope="module")
def setup(market_root, store_root):
    day = load_day(market_root, TODAY)
    return day, build_baselines(market_root, TODAY, day.equity.keys, cache_root=store_root)


def run(day, baselines, engine, minutes):
    out = []
    for minute in minutes:
        frame = frame_at(day, as_of_time(TODAY, "09:15") + timedelta(minutes=minute + 1))
        features = compute_features(frame)
        report = detect(frame, features, baselines)
        out += engine.build(frame, features, report, all_relationships(frame, features, report), baselines)
    return out


def test_severity_bands_and_ids():
    assert [severity(z) for z in (3.0, -4.4, 4.5, 7.4, 7.5, -20)] == ["LOW", "LOW", "MEDIUM", "MEDIUM", "HIGH", "HIGH"]
    assert event_id("volume_surge", "TCS", "", 1) == event_id("volume_surge", "TCS", "", 1)
    assert event_id("volume_surge", "TCS", "", 1) != event_id("volume_surge", "TCS", "", 2)
    assert event_id("volume_surge", "TCS", "", 1).startswith("evt_") and len(event_id("a", "b", "", 1)) == 20


def test_injected_findings_become_events_with_evidence(setup, tmp_path):
    day, baselines = setup
    events = run(day, baselines, EventEngine(EventStore(tmp_path / "e.db")), [120, 150, 229])
    surge = next(e for e in events if e["event_type"] == "volume_surge" and e["subject"]["key"] == "TCS")
    assert surge["category"] == "ANOMALY" and surge["scope"] == "INSTRUMENT"
    assert surge["bar_time"] < surge["observed_at"] and surge["bar_time"].endswith("11:15:00 IST")
    assert surge["evidence"]["baseline"]["kind"] == "HISTORICAL"
    assert surge["data_quality"]["subject_status"] == "COMPLETE"
    assert "BASELINE_SHORT" in surge["data_quality"]["flags"]  # 7 earlier sessions < 20
    assert surge["evidence"]["context"]["sector"] == "INFORMATION_TECHNOLOGY"
    shock = next(e for e in events if e["event_type"] == "return_shock" and e["subject"]["key"] == "HDFCBANK")
    assert shock["severity"] in ("MEDIUM", "HIGH") and shock["magnitude"]["value"] > 0
    divergence = next(
        e for e in events if e["event_type"] == "sector_divergence" and e["subject"]["key"] == "SUNPHARMA"
    )
    assert divergence["subject"]["counterpart"] == "PHARMA_HEALTHCARE" and divergence["relationships"]
    for event in events:
        assert event["event_type"] in EVENT_TYPES
        assert event["schema_version"] == "event/1" and event["synthetic_data"] is True  # test archives are synthetic
        assert abs(event["magnitude"]["value"]) >= event["magnitude"]["threshold"]
        text = str({k: v for k, v in event.items() if k != "subject"}).lower()
        assert not any(word in text for word in FORBIDDEN), event["event_type"]


def test_replay_is_deterministic(setup, tmp_path):
    day, baselines = setup
    first = run(day, baselines, EventEngine(EventStore(tmp_path / "a.db")), range(110, 160))
    second = run(day, baselines, EventEngine(EventStore(tmp_path / "b.db")), range(110, 160))
    assert first == second and first


def test_cooldown_and_restart(setup, tmp_path):
    day, baselines = setup
    store = EventStore(tmp_path / "e.db")
    events = run(day, baselines, EventEngine(store), range(200, 260))
    divergences = [e for e in events if e["event_type"] == "sector_divergence" and e["subject"]["key"] == "SUNPHARMA"]
    assert divergences
    for a, b in pairwise(divergences):
        gap = b["bar_epoch"] - a["bar_epoch"]
        assert gap >= COOLDOWN_MINUTES * 60 or b["severity"] > a["severity"] or (a["severity"], b["severity"]) in (
            ("LOW", "MEDIUM"), ("LOW", "HIGH"), ("MEDIUM", "HIGH"))  # fmt: skip
    # A restarted engine reads the cooldown back from the store and does not repeat.
    again = run(day, baselines, EventEngine(store), range(250, 260))
    later = run(day, baselines, EventEngine(EventStore(tmp_path / "fresh.db")), range(250, 260))
    assert len(again) <= len(later)
    assert store.stats()["events"] == len(events) + len(again)


def test_store_search_and_idempotence(setup, tmp_path):
    day, baselines = setup
    store = EventStore(tmp_path / "e.db")
    events = run(day, baselines, EventEngine(store), [120, 150])
    assert store.add(events) == []  # same ids: nothing new
    assert store.get(events[0]["event_id"])["event_id"] == events[0]["event_id"]
    assert store.get("evt_missing") is None
    tcs = store.search(instrument="TCS")
    assert tcs and all("TCS" in e["affected_instruments"] for e in tcs)
    assert all(e["severity"] == "HIGH" for e in store.search(min_severity="HIGH"))
    assert {e["event_type"] for e in store.search(event_type="volume_surge")} == {"volume_surge"}
    newest = store.search(limit=1)[0]
    assert newest["bar_epoch"] == max(e["bar_epoch"] for e in events)
    stream = store.search(after_seq=0, limit=1000)
    assert [e["seq"] for e in stream] == sorted(e["seq"] for e in stream) and len(stream) == len(events)
    assert store.search(after_seq=store.latest_seq()) == []
    assert store.search(session_date=DATES[0]) == []
    backup = store.backup(tmp_path / "backup" / "e.db")
    assert EventStore(backup).stats() == store.stats()


def test_novelty_counts_earlier_sessions(setup, tmp_path):
    day, baselines = setup
    store = EventStore(tmp_path / "e.db")
    events = run(day, baselines, EventEngine(store), [120])
    surge = next(e for e in events if e["event_type"] == "volume_surge" and e["subject"]["key"] == "TCS")
    assert surge["novelty"]["prior_occurrences"] == 0
    earlier = {**surge, "event_id": "evt_earlier0000000", "session_date": DATES[-2]}
    store.add([earlier])
    assert store.prior_occurrences("volume_surge", "TCS", TODAY, 20) == 1
    assert store.prior_occurrences("volume_surge", "TCS", DATES[-2], 20) == 0  # never counts its own day or later
