"""The live runner, API keys, rate limits, the /v2 API and the event stream."""

import dataclasses
import json
import logging
import os
import shutil
import stat
import subprocess
import sys
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from intelligence.api import create_app
from intelligence.archive import EQUITY_FILE, load_day
from intelligence.derivatives import load_derivatives
from intelligence.engine import IntelligenceEngine, replay_session
from intelligence.event_store import EventStore
from intelligence.frame import as_of_time
from intelligence.keys import KeyStore, RateLimiter
from intelligence.live import LiveRunner
from intelligence.settings import Settings
from tests.intelligence.conftest import TODAY


class Clock:
    def __init__(self, moment):
        self.moment = moment

    def __call__(self):
        return self.moment


def make_settings(tmp_path, market_root, store_root, **overrides):
    archive = tmp_path / "archive"
    if not archive.exists():
        shutil.copytree(market_root, archive)
    store = tmp_path / "store"
    for name in ("summaries", "states"):
        if (store_root / name).exists():
            shutil.copytree(store_root / name, store / name, dirs_exist_ok=True)
    values = {
        "archive_dir": archive, "store_dir": store, "psygrid_url": "http://psygrid.test", "host": "127.0.0.1",
        "port": 18101, "require_keys": True, "rate_per_minute": 600, "rate_burst": 100, "max_streams": 3,
        "max_streams_per_key": 2, "live_enabled": False, "record_derivatives": True, "similarity_lookback": 60,
        "backup_keep": 2,
    }  # fmt: skip
    values.update(overrides)
    return Settings(**values)


def set_written(settings, hhmm_ss):
    """Pretend PSYGRID last wrote today's archive at ``HH:MM:SS``."""
    hh, mm, ss = map(int, hhmm_ss.split(":"))
    moment = as_of_time(TODAY, f"{hh:02d}:{mm:02d}") + timedelta(seconds=ss)
    os.utime(settings.archive_dir / TODAY / EQUITY_FILE, (moment.timestamp(), moment.timestamp()))


def payloads(open_market=True, fail=()):
    def fetch(url):
        for part in fail:
            if part in url:
                raise ConnectionError(f"refused: {url}")
        if url.endswith("-futures.json"):
            return {"last_price": 25100.5, "top_bid_price": 25100.0, "top_ask_price": 25101.0, "oi": 1e6,
                    "volume": 10.0, "market_open": open_market}  # fmt: skip
        if url.endswith("-options.json"):
            return {"underlying_ltp": 25000.0, "market_status": "OPEN" if open_market else "MARKET_CLOSED",
                    "analytics": {"pcr_oi": 0.9, "iv_skew": 1.2}}  # fmt: skip
        return {"ltp": 25000.0}

    return fetch


@pytest.fixture
def settings(tmp_path, market_root, store_root):
    return make_settings(tmp_path, market_root, store_root)


# --- keys and limits -------------------------------------------------------------------------


def test_keys_are_stored_hashed_and_can_be_revoked(tmp_path):
    keys = KeyStore(tmp_path / "keys.json")
    assert not keys.configured() and keys.verify("anything") is None
    key_id, key = keys.create("engine-a")
    assert key.startswith(f"psg_{key_id}_") and keys.configured()
    assert keys.verify(key)["name"] == "engine-a"
    assert key not in (tmp_path / "keys.json").read_text()
    assert stat.S_IMODE((tmp_path / "keys.json").stat().st_mode) == 0o600
    assert keys.verify(key[:-1] + ("A" if key[-1] != "A" else "B")) is None
    assert keys.verify(f"psg_{key_id}_short") is None and keys.verify(None) is None
    assert all("hash" not in entry for entry in keys.list())
    assert keys.revoke(key_id) and not keys.revoke(key_id)
    assert keys.verify(key) is None
    with pytest.raises(ValueError):
        keys.create("")


def test_rate_limiter_refills():
    now = [0.0]
    limiter = RateLimiter(per_minute=60, burst=2, clock=lambda: now[0])
    assert limiter.check("k")[0] and limiter.check("k")[0]
    allowed, _, wait = limiter.check("k")
    assert not allowed and wait == pytest.approx(1.0)
    now[0] += 1.0
    assert limiter.check("k")[0]
    assert limiter.check("other")[0]  # buckets are per key


# --- live runner ------------------------------------------------------------------------------


def test_live_follows_the_archive_and_matches_replay(settings, market_root, store_root, tmp_path):
    clock = Clock(as_of_time(TODAY, "11:00") + timedelta(seconds=5))
    runner = LiveRunner(settings, fetch=payloads(), clock=clock)
    set_written(settings, "10:30:20")
    assert runner.follow_archive(clock()) == 76  # 09:15 .. 10:30: only minutes closed before the file was written
    assert runner.snapshot.as_of.endswith("10:30:00 IST")
    assert runner.follow_archive(clock()) == 0  # file unchanged
    set_written(settings, "10:35:31")
    assert runner.follow_archive(clock()) == 5
    live_ids = [e["event_id"] for e in runner.store.search(after_seq=0, limit=1000)]
    replayed = EventStore(tmp_path / "replay.db")
    engine = IntelligenceEngine(settings.archive_dir, settings.store_dir, event_store=replayed)
    replay_session(engine, load_day(settings.archive_dir, TODAY), end="10:35")
    assert live_ids == [e["event_id"] for e in replayed.search(after_seq=0, limit=1000)]
    assert {e["provenance"]["source"] for e in runner.store.search(limit=1000)} == {"LIVE_SNAPSHOT"}


def test_restart_does_not_repeat_events(settings):
    clock = Clock(as_of_time(TODAY, "12:00"))
    set_written(settings, "11:59:40")
    first = LiveRunner(settings, fetch=payloads(), clock=clock)
    first.follow_archive(clock())
    count = first.store.stats()["events"]
    again = LiveRunner(settings, fetch=payloads(), clock=clock)
    again.follow_archive(clock())
    assert again.store.stats()["events"] == count and again.status["events_emitted"] == 0


def test_derivatives_recording(settings):
    clock = Clock(as_of_time(TODAY, "10:00") + timedelta(seconds=5))
    runner = LiveRunner(settings, fetch=payloads(fail=("banknifty-options",)), clock=clock)
    assert runner.record_derivatives(clock()) and not runner.record_derivatives(clock())  # once per minute
    snap = load_derivatives(settings.store_dir, TODAY).snapshots[0]
    assert snap["minute"] == int(as_of_time(TODAY, "10:00").timestamp())
    assert snap["futures"]["nifty"]["last_price"] == 25100.5
    assert "banknifty" not in snap["options"] and snap["spot"]["banknifty"] == 25000.0  # index fallback
    assert runner.status["derivatives_errors"] == 1
    closed = LiveRunner(settings, fetch=payloads(open_market=False), clock=Clock(clock() + timedelta(minutes=1)))
    assert not closed.record_derivatives(closed.clock())  # PSYGRID says the market is closed: record nothing


def test_a_failing_source_never_stops_the_loop(settings):
    def broken(url):
        raise ConnectionError("PSYGRID is down")

    clock = Clock(as_of_time(TODAY, "10:00") + timedelta(seconds=5))
    runner = LiveRunner(settings, fetch=broken, clock=clock)
    shutil.rmtree(settings.archive_dir)  # and the archive is missing
    runner.tick()
    runner.tick()
    health = runner.health()
    assert health["ticks"] == 2 and health["state"] == "WAITING_FOR_DATA"
    assert health["derivatives_errors"] >= 2


def test_after_the_close_it_backs_up_and_prunes(settings):
    clock = Clock(as_of_time(TODAY, "16:00"))
    set_written(settings, "15:31:00")
    runner = LiveRunner(settings, fetch=payloads(), clock=clock)
    runner.tick()
    assert runner.snapshot.as_of.endswith("15:15:00 IST") and runner.health()["state"] == "CLOSED"
    assert runner.status["last_backup"] and EventStore(runner.status["last_backup"]).stats()["events"] > 0
    for label in ("a", "b", "c"):
        runner.backup(label)
    assert len(list((settings.store_dir / "backups").glob("events-*.db"))) == settings.backup_keep


def test_thread_starts_and_stops_gracefully(settings):
    runner = LiveRunner(settings, fetch=payloads(), clock=Clock(as_of_time(TODAY, "16:00")))
    runner.start()
    runner.stop(timeout=30)
    assert runner._thread is None


# --- API --------------------------------------------------------------------------------------


@pytest.fixture
def api(settings):
    clock = Clock(as_of_time(TODAY, "13:00") + timedelta(seconds=5))
    set_written(settings, "12:59:30")
    runner = LiveRunner(settings, fetch=payloads(), clock=clock)
    runner.follow_archive(clock())
    _, key = KeyStore(settings.keys_file).create("tests")
    with TestClient(create_app(settings, runner)) as client:
        client.headers["X-API-Key"] = key
        yield client, key, runner


def test_public_health_and_auth(api):
    client, key, _ = api
    anonymous = TestClient(client.app)
    health = anonymous.get("/v2/health")
    assert health.status_code == 200 and health.json()["auth"] == {"required": True, "keys_configured": True}
    assert anonymous.get("/v2/ready").json() == {"ready": True}
    assert anonymous.get("/v2/market").status_code == 401
    assert anonymous.get("/v2/market", headers={"X-API-Key": "psg_00000000_" + "x" * 43}).status_code == 401
    assert anonymous.get("/v2/market", headers={"Authorization": f"Bearer {key}"}).status_code == 200
    response = client.get("/v2/market")
    assert response.headers["X-Content-Type-Options"] == "nosniff" and "X-RateLimit-Remaining" in response.headers


def test_data_routes(api):
    client, _, _ = api
    market = client.get("/v2/market").json()
    assert market["as_of"].endswith("12:59:00 IST") and market["minute_index"] == 223
    meta = client.get("/v2/meta").json()
    assert "volume_surge" in meta["event_types"] and "ret_1m" in meta["features"]
    obs = client.get("/v2/observations", params={"keys": "TCS,INFY", "fields": "ret_1m,volume_1m"}).json()
    assert [o["key"] for o in obs["observations"]] == ["TCS", "INFY"] and set(obs["observations"][0]) == {
        "key", "sector", "ret_1m", "volume_1m", "classification"}  # fmt: skip
    assert client.get("/v2/observations", params={"fields": "nope"}).status_code == 422
    assert client.get("/v2/observations", params={"keys": "bad key!"}).status_code == 422
    assert client.get("/v2/observations", params={"limit": 5000}).status_code == 422
    sector = client.get("/v2/observations", params={"sector": "BANKING"}).json()["observations"]
    assert sector and {o["sector"] for o in sector} == {"BANKING"}
    sun = client.get("/v2/instruments/SUNPHARMA").json()
    assert sun["sector"] == "PHARMA_HEALTHCARE" and sun["anomalies"]["volume"]["classification"]
    assert any(r["kind"] == "stock_sector" for r in sun["relationships"])
    assert client.get("/v2/instruments/NOPE").status_code == 404
    extreme = client.get("/v2/anomalies", params={"minimum": "EXTREME"}).json()
    assert all(a["classification"] == "EXTREME" for a in extreme["anomalies"])
    assert client.get("/v2/anomalies", params={"minimum": "HUGE"}).status_code == 422
    rels = client.get("/v2/relationships", params={"kind": "stock_sector", "flagged_only": False}).json()
    assert rels["total"] > 0 and all(r["kind"] == "stock_sector" for r in rels["relationships"])


def test_event_routes(api):
    client, _, runner = api
    listed = client.get("/v2/events", params={"date": TODAY, "limit": 1000}).json()
    assert listed["count"] == runner.store.stats()["events"] > 0
    first = listed["events"][0]
    assert client.get(f"/v2/events/{first['event_id']}").json()["event_id"] == first["event_id"]
    assert client.get("/v2/events/evt_0000000000000000").status_code == 404
    assert client.get("/v2/events/drop-table").status_code == 422
    assert client.get("/v2/events", params={"date": "yesterday"}).status_code == 422
    surge = client.get("/v2/events", params={"instrument": "TCS", "event_type": "volume_surge"}).json()["events"]
    assert surge and all(e["event_type"] == "volume_surge" for e in surge)
    page = client.get("/v2/events", params={"after_seq": 0, "limit": 2}).json()["events"]
    assert [e["seq"] for e in page] == [1, 2]


def test_historical_matches(api):
    client, _, _ = api
    market = client.get("/v2/historical-matches/market", params={"k": 5}).json()
    assert market["status"] == "OK" and len(market["matches"]) == 5 and "not a forecast" in market["caveat"]
    assert client.get("/v2/historical-matches/instruments/TCS").json()["status"] == "OK"
    assert client.get("/v2/historical-matches/instruments/NOPE").status_code == 404
    assert client.get("/v2/historical-matches/market", params={"k": 500}).status_code == 422


def test_no_snapshot_means_503(settings):
    runner = LiveRunner(settings, fetch=payloads(), clock=Clock(as_of_time(TODAY, "09:00")))
    _, key = KeyStore(settings.keys_file).create("tests")
    client = TestClient(create_app(settings, runner))
    assert client.get("/v2/market", headers={"X-API-Key": key}).status_code == 503
    assert client.get("/v2/ready").status_code == 503


def test_rate_limit(settings, tmp_path):
    tight = dataclasses.replace(settings, rate_per_minute=1, rate_burst=2)
    _, key = KeyStore(tight.keys_file).create("tests")
    client = TestClient(create_app(tight, LiveRunner(tight, fetch=payloads())))
    codes = [client.get("/v2/meta", headers={"X-API-Key": key}).status_code for _ in range(3)]
    assert codes == [200, 200, 429]
    assert client.get("/v2/meta", headers={"X-API-Key": key}).headers["Retry-After"]


def test_access_log_never_records_keys(api, caplog):
    client, key, _ = api
    with caplog.at_level(logging.INFO, logger="psygrid.intelligence.access"):
        client.get("/v2/meta")
        client.get("/v2/meta", params={"api_key": key})
    assert caplog.records and all(key not in r.getMessage() for r in caplog.records)
    assert json.loads(caplog.records[0].getMessage())["path"] == "/v2/meta"


def test_keys_can_be_switched_off(settings):
    open_settings = dataclasses.replace(settings, require_keys=False)
    client = TestClient(create_app(open_settings, LiveRunner(open_settings, fetch=payloads())))
    assert client.get("/v2/meta").status_code == 200


# --- stream -----------------------------------------------------------------------------------


def test_stream_backfills_filters_and_authenticates(api):
    client, key, runner = api
    with client.websocket_connect(f"/v2/stream?api_key={key}&after_seq=0&snapshots=true") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "hello" and hello["after_seq"] == 0
        messages = [ws.receive_json() for _ in range(runner.store.stats()["events"] + 1)]
        events = [m for m in messages if m["type"] == "event"]
        assert [m["seq"] for m in events] == sorted(m["seq"] for m in events)
        assert any(m["type"] == "snapshot" for m in messages)
        ws.send_text("ping")
        assert ws.receive_json()["type"] in ("pong", "heartbeat", "snapshot")
    with client.websocket_connect(
        f"/v2/stream?api_key={key}&after_seq=0&event_types=volume_surge&snapshots=false"
    ) as ws:
        ws.receive_json()
        message = ws.receive_json()
        assert message["type"] == "event" and message["event"]["event_type"] == "volume_surge"
    bare = TestClient(client.app)  # no default key header
    with pytest.raises(WebSocketDisconnect), bare.websocket_connect("/v2/stream?api_key=wrong") as ws:
        ws.receive_json()
    with pytest.raises(WebSocketDisconnect), bare.websocket_connect(f"/v2/stream?api_key={key}&min_severity=X") as ws:
        ws.receive_json()


def test_stream_connection_limits(api):
    client, key, _ = api
    with (
        client.websocket_connect(f"/v2/stream?api_key={key}") as a,
        client.websocket_connect(f"/v2/stream?api_key={key}") as b,
    ):
        a.receive_json(), b.receive_json()
        with pytest.raises(WebSocketDisconnect), client.websocket_connect(f"/v2/stream?api_key={key}") as c:
            c.receive_json()


# --- isolation --------------------------------------------------------------------------------


def test_the_service_never_imports_the_production_app():
    code = "import sys, intelligence.api, intelligence.live; sys.exit(1 if 'app' in sys.modules else 0)"
    assert subprocess.run([sys.executable, "-c", code], cwd=os.getcwd()).returncode == 0


def test_settings_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("PSYGRID_INTELLIGENCE_DIR", str(tmp_path))
    monkeypatch.setenv("PSYGRID_INTELLIGENCE_PORT", "not-a-number")
    monkeypatch.setenv("PSYGRID_INTELLIGENCE_RATE_PER_MINUTE", "0")
    monkeypatch.setenv("PSYGRID_INTELLIGENCE_REQUIRE_KEYS", "false")
    s = Settings.from_environment()
    assert s.port == 18101 and s.rate_per_minute == 1 and not s.require_keys and s.host == "127.0.0.1"
    assert s.events_db == tmp_path / "events.db"


def test_live_follows_the_minute_stream_and_matches_replay(settings, tmp_path):
    """Minutes the archive does not have yet come from PSYGRID's per-minute stream, with identical results."""
    import csv
    import gzip

    from daily_archive import INDEX_FILE
    from intelligence.archive import parse_timestamp
    from intelligence.stream import stream_day, stream_path
    from microstructure import BAR_COLUMNS, MicrostructureRecorder

    full = load_day(settings.archive_dir, TODAY)
    cut, last = as_of_time(TODAY, "10:30").timestamp(), as_of_time(TODAY, "10:41").timestamp()
    stream_rows: dict[int, list[tuple]] = {}
    for name, key_col in ((EQUITY_FILE, "symbol"), (INDEX_FILE, "index")):
        path = settings.archive_dir / TODAY / name
        with gzip.open(path, "rt", newline="") as handle:
            reader = csv.DictReader(handle)
            columns, rows = reader.fieldnames, list(reader)
        keep = [r for r in rows if parse_timestamp(r["timestamp"]) < cut]
        for r in rows:
            minute = parse_timestamp(r["timestamp"])
            if cut <= minute <= last:
                sid = r.get("security_id", "") if key_col == "symbol" else f"IDX:{r['index']}"
                stream_rows.setdefault(minute, []).append(
                    (r["symbol"], sid, r["timestamp"], r["open"], r["high"], r["low"], r["close"], r["volume"])
                )
        with gzip.open(path, "wt", newline="") as handle:
            writer = csv.DictWriter(handle, columns)
            writer.writeheader()
            writer.writerows(keep)
    set_written(settings, "10:30:20")
    target = stream_path(settings.archive_dir, TODAY)
    for minute in sorted(stream_rows):
        MicrostructureRecorder._append_block(None, target, BAR_COLUMNS, stream_rows[minute], minute)
    with open(target, "a") as handle:  # a torn block (no end marker yet) is never read
        handle.write("XYZ,1,2026-01-01 10:42:00 IST,1,1,1,1,1\n")
    merged, last_minute, _ = stream_day(settings.archive_dir, TODAY)
    assert last_minute == int(last) and merged.equity.close.shape[0] == full.equity.close.shape[0]
    assert "XYZ" not in merged.equity.keys

    clock = Clock(as_of_time(TODAY, "10:42") + timedelta(seconds=5))
    runner = LiveRunner(settings, fetch=payloads(), clock=clock)
    assert runner.follow_archive(clock()) == 88  # 09:15 .. 10:42
    assert runner.snapshot.as_of.endswith("10:42:00 IST") and runner.status["stream_minute"] == "10:41"
    assert runner.follow_archive(clock()) == 0  # nothing new in either source
    replayed = EventStore(tmp_path / "replay.db")
    engine = IntelligenceEngine(settings.archive_dir, settings.store_dir, event_store=replayed)
    replay_session(engine, full, end="10:42")
    assert [e["event_id"] for e in runner.store.search(after_seq=0, limit=1000)] == [
        e["event_id"] for e in replayed.search(after_seq=0, limit=1000)
    ]


def test_stream_can_be_switched_off(settings):
    from intelligence.stream import stream_path
    from microstructure import BAR_COLUMNS, MicrostructureRecorder

    off = dataclasses.replace(settings, use_stream=False)
    set_written(off, "10:00:20")
    minute = int(as_of_time(TODAY, "10:00").timestamp())
    MicrostructureRecorder._append_block(None, stream_path(off.archive_dir, TODAY), BAR_COLUMNS, [], minute)
    clock = Clock(as_of_time(TODAY, "10:30"))
    runner = LiveRunner(off, fetch=payloads(), clock=clock)
    assert runner.follow_archive(clock()) == 46 and "stream_minute" not in runner.status


def test_research_routes(api):
    client, _, _ = api
    anonymous = TestClient(client.app)
    assert anonymous.get("/v2/stocks/TCS").status_code == 401
    research = client.app.state.research
    first = client.get("/v2/stocks/TCS").json()  # the response model builds in the background
    assert first["expected_response"]["status"] in ("WARMING", "READY")
    assert research.wait_for_model(TODAY, timeout=300)
    research._cache.clear()
    tcs = client.get("/v2/stocks/TCS").json()
    resp = tcs["expected_response"]
    assert resp["status"] == "READY" and resp["window_minutes"] == 15
    assert set(resp["contributions"]) == {"market", "sector", "statistical", "residual"}
    assert resp["expected_response"] == pytest.approx(sum(resp["contributions"][k] for k in ("market", "sector", "statistical")), abs=1e-6)  # fmt: skip
    assert tcs["liquidity_microstructure"]["classification"] == "UNSUPPORTED"  # no Full packets in this archive
    assert tcs["market_state"]["regime"] and tcs["freshness"]["as_of"] == tcs["as_of"]
    assert tcs["data_quality"]["status"] and isinstance(tcs["events"], list)
    assert "not forecasts" in tcs["disclaimer"]
    assert client.get("/v2/stocks/NOPE").status_code == 404
    ranked = client.get("/v2/stocks", params={"by": "response_gap_sigma", "limit": 5}).json()
    assert ranked["status"] == "READY" and len(ranked["stocks"]) == 5
    values = [abs(s["response_gap_sigma"]) for s in ranked["stocks"]]
    assert values == sorted(values, reverse=True)
    assert client.get("/v2/stocks", params={"by": "price"}).status_code == 422
    state = client.get("/v2/market/state").json()
    assert state["market_state"]["percentiles"]["history_sessions"] >= 5
    assert set(state["derivatives_expectation"]) == {"nifty", "banknifty", "midcpnifty"}
