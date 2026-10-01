"""History bootstrap against an offline Dhan fake: plan, download, resume, integrity, and Intelligence on top."""

import csv
import gzip
import json
from datetime import datetime, timedelta

import pytest

from daily_archive import EQUITY_FILE, EQUITY_REFERENCE_FILE, INDEX_FILE, MANIFEST_FILE
from intelligence.archive import IST, available_days, load_day
from intelligence.archive_integrity import digests_match, verify_day
from intelligence.history_bootstrap import (
    SOURCE,
    Bootstrap,
    BootstrapBlocked,
    HistoryClient,
    RateLimiter,
    credentials,
    estimate,
    parse_candles,
    read_env_file,
    stage_complete,
)
from tests.intelligence.fake_dhan import SYMBOLS, FakeDhan, candles, master_lines

NOW = datetime(2026, 10, 2, 20, 0, tzinfo=IST)  # a Friday evening (and a market holiday)
UNIVERSE = [*sorted(SYMBOLS), "NOTLISTED"]


def make(root, fake=None, sessions=5, now=NOW, workers=2):
    fake = fake or FakeDhan()
    client = HistoryClient("cid", "token", rate=4.0, post=fake, sleep=lambda s: None)
    client.limiter = RateLimiter(4.0, sleep=lambda s: None)
    boot = Bootstrap(root, client, sessions=sessions, workers=workers, clock=lambda: now, log=lambda m: None,
                     master_lines=master_lines(), symbols=UNIVERSE)  # fmt: skip
    return boot, fake


def rows(path):
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.DictReader(handle))


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("bootstrapped")
    boot, fake = make(root)
    result = boot.run()
    return root, boot, fake, result


def test_plan_needs_no_credentials_or_api(tmp_path):
    boot = Bootstrap(tmp_path, None, sessions=20, clock=lambda: NOW, log=lambda m: None, symbols=UNIVERSE)
    plan = boot.plan(rate=2.0)
    assert plan["to_download"]["sessions"] == 20
    requests = plan["to_download"]["requests"]
    assert requests["intraday"] == len(UNIVERSE) + 16 and requests["daily"] == len(UNIVERSE)
    assert estimate(["2026-01-01", "2026-06-30"], 989, 16, 2.0)["requests"]["intraday"] == 1005 * 3  # 90-day spans


def test_downloads_the_last_sessions_from_the_real_calendar(built):
    root, _, _, result = built
    assert result["downloaded_sessions"] == ["2026-09-25", "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-01"]
    assert available_days(root) == result["downloaded_sessions"]  # Intelligence discovers them; no holiday, no today
    assert all(r["ok"] for r in result["verified"].values())


def test_values_are_dhans_exactly(built):
    root, *_ = built
    archived = [r for r in rows(root / "2026-09-30" / EQUITY_FILE) if r["symbol"] == "TCS"]
    day = datetime(2026, 9, 30).date()
    genuine = [c for c in candles(SYMBOLS["TCS"], day, False) if datetime.fromtimestamp(c[0], IST).hour * 60
               + datetime.fromtimestamp(c[0], IST).minute >= 555]  # fmt: skip
    assert len(archived) == len(genuine) == 375
    for got, want in zip(archived, genuine, strict=True):
        assert got["timestamp"] == datetime.fromtimestamp(want[0], IST).strftime("%Y-%m-%d %H:%M:%S IST")
        assert [float(got[k]) for k in ("open", "high", "low", "close")] == want[1:5]
        assert int(got["volume"]) == want[5]


def test_invalid_rows_are_excluded_and_counted_never_repaired(built):
    root, *_ = built
    first = [r for r in rows(root / "2026-09-25" / EQUITY_FILE) if r["symbol"] == "RELIANCE"]
    assert len(first) == 375 - 2  # the invalid row and the non-numeric row are left out; the duplicate kept once
    stamps = [r["timestamp"] for r in first]
    assert len(stamps) == len(set(stamps)) and all(" 09:1" not in s or s >= "2026-09-25 09:15" for s in stamps)
    manifest = json.loads((root / "2026-09-25" / MANIFEST_FILE).read_text())
    excluded = manifest["bootstrap"]["excluded_rows_in_batch"]
    assert excluded["invalid_ohlc"] == 1 and excluded["duplicate_minute"] == 1 and excluded["non_numeric"] == 1
    assert excluded["outside_session"] > 0  # every pre-open bar
    assert manifest["source"] == SOURCE and manifest["synthetic_candles"] is False


def test_manifest_and_reference(built):
    root, *_ = built
    manifest = json.loads((root / "2026-09-29" / MANIFEST_FILE).read_text())
    assert manifest["bootstrap"]["missing_equities"] == ["DELISTED"]  # Dhan had no data: recorded, not invented
    assert "NOTLISTED" in manifest["bootstrap"]["unresolved_instruments"]
    assert manifest["files"][EQUITY_FILE]["rows"] == 5 * 375
    reference = {r["symbol"]: r for r in rows(root / "2026-09-29" / EQUITY_REFERENCE_FILE)}
    previous = candles(SYMBOLS["INFY"], datetime(2026, 9, 28).date(), False)[-1][4]  # prior session's close
    assert float(reference["INFY"]["previous_close"]) == previous
    assert float(reference["INFY"]["today_open"]) == candles(SYMBOLS["INFY"], datetime(2026, 9, 29).date(), False)[1][1]
    indices = {r["index"] for r in rows(root / "2026-09-29" / INDEX_FILE)}
    assert {"nifty", "nifty500", "niftyit", "niftypharma"} <= indices


def test_intelligence_reads_bootstrapped_days(built):
    root, *_ = built
    day = load_day(root, "2026-10-01")
    assert len(day.equity.keys) == 5 and day.indices is not None
    assert day.reference["TCS"]["previous_close"] is not None


def test_rerun_is_a_no_op_and_staging_is_cleaned(built):
    root, boot, fake, _ = built
    before = len(fake.calls)
    again = boot.run()
    assert again["downloaded_sessions"] == [] and len(fake.calls) == before + 1  # only the calendar probe
    assert not any((root / ".bootstrap").iterdir())


def test_resume_after_a_crash_downloads_only_what_is_left(tmp_path):
    crash, _ = make(tmp_path, FakeDhan(stop_after=9), workers=1)
    with pytest.raises(KeyboardInterrupt):
        crash.run()
    batch = next((tmp_path / ".bootstrap").iterdir())
    staged = list((batch / "intraday").glob("*.csv.gz"))
    assert staged and all(stage_complete(p) for p in staged)
    (batch / "intraday" / ".partial.tmp").write_text("left by the crash")
    resume, fake = make(tmp_path, FakeDhan(fail_once=()))
    result = resume.run()
    assert len(result["downloaded_sessions"]) == 5
    intraday = [c for c in fake.calls if c[0] == "intraday"]
    assert len(intraday) == len(UNIVERSE) - 1 + 16 - len(staged)  # NOTLISTED never requested; staged not refetched


def test_a_corrupt_stage_is_downloaded_again(tmp_path):
    boot, _ = make(tmp_path, FakeDhan(stop_after=6), workers=1)
    with pytest.raises(KeyboardInterrupt):
        boot.run()
    stage = next((next((tmp_path / ".bootstrap").iterdir()) / "intraday").glob("*.csv.gz"))
    stage.write_bytes(stage.read_bytes()[:-10])
    assert not stage_complete(stage)
    resume, _ = make(tmp_path, FakeDhan(fail_once=()))
    assert len(resume.run()["downloaded_sessions"]) == 5


def test_expansion_downloads_only_older_sessions(built, tmp_path):
    root, *_ = built
    import shutil

    shutil.copytree(root, tmp_path / "a")
    deeper, _ = make(tmp_path / "a", FakeDhan(fail_once=()), sessions=8)
    result = deeper.run()
    assert result["downloaded_sessions"] == ["2026-09-22", "2026-09-23", "2026-09-24"]
    assert len(available_days(tmp_path / "a")) == 8


def test_live_archived_days_are_never_touched(tmp_path):
    live = tmp_path / "2026-09-30"
    live.mkdir(parents=True)
    (live / EQUITY_FILE).write_bytes(gzip.compress(b"symbol,security_id,timestamp,open,high,low,close,volume\n"))
    (live / MANIFEST_FILE).write_text(json.dumps({"session_date": "2026-09-30", "files": {}}))
    before = (live / EQUITY_FILE).read_bytes()
    boot, _ = make(tmp_path)
    result = boot.run()
    assert "2026-09-30" not in result["downloaded_sessions"] and len(result["downloaded_sessions"]) == 4
    assert (live / EQUITY_FILE).read_bytes() == before


def test_a_damaged_bootstrapped_day_is_rebuilt(built, tmp_path):
    root, *_ = built
    import shutil

    shutil.copytree(root, tmp_path / "a")
    path = tmp_path / "a" / "2026-09-29" / EQUITY_FILE
    path.write_bytes(path.read_bytes()[:-20])
    assert not digests_match(path.parent) and not verify_day(path.parent)["ok"]
    boot, _ = make(tmp_path / "a", FakeDhan(fail_once=()))
    assert boot.run()["downloaded_sessions"] == ["2026-09-29"]
    assert verify_day(path.parent)["ok"]


def test_market_hours_and_auth_failures_block(tmp_path):
    open_market = NOW.replace(day=1, hour=11)  # Thursday 11:00
    boot, fake = make(tmp_path, now=open_market)
    with pytest.raises(BootstrapBlocked, match="market hours"):
        boot.run()
    assert fake.calls == []
    denied, _ = make(tmp_path, FakeDhan(auth_fail=True))
    with pytest.raises(BootstrapBlocked, match="credentials"):
        denied.run()
    assert available_days(tmp_path) == []


def test_credentials_never_generate_a_token_implicitly(tmp_path):
    assert credentials({"DHAN_CLIENT_ID": "1", "DHAN_ACCESS_TOKEN": "t"})[1] == "t"
    assert credentials({"DHAN_CLIENT_ID": "1", "DHAN_TOKEN_VAR": "X", "X": "u"})[1] == "u"
    with pytest.raises(BootstrapBlocked, match="PIN"):
        credentials({"DHAN_CLIENT_ID": "1", "DHAN_PIN": "1", "DHAN_TOTP_SECRET": "ABC"})
    with pytest.raises(BootstrapBlocked, match="DHAN_CLIENT_ID"):
        credentials({})
    env = tmp_path / "psygrid.env"
    env.write_text("# comment\nexport DHAN_CLIENT_ID=\"12\"\nDHAN_ACCESS_TOKEN='abc=def'\n")
    assert read_env_file(env) == {"DHAN_CLIENT_ID": "12", "DHAN_ACCESS_TOKEN": "abc=def"}


def test_rate_limiter_spaces_requests_and_cools_down():
    now, slept = [0.0], []

    def sleep(s):
        slept.append(round(s, 3))
        now[0] += s

    limiter = RateLimiter(2.0, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        limiter.acquire()
    assert slept == [0.5, 0.5]
    limiter.cool_down(5)
    limiter.acquire()
    assert slept[-1] == pytest.approx(5.0)
    assert RateLimiter(100).interval == pytest.approx(0.25)  # never above MAX_RATE


def test_parse_candles_counts_every_exclusion():
    day = datetime(2026, 9, 30).date()
    data = candles("2885", day, True)
    body = {k: [r[i] for r in data] for i, k in enumerate(("timestamp", "open", "high", "low", "close", "volume"))}
    parsed, rejected = parse_candles(body)
    assert len(parsed) == 373 and rejected == {"outside_session": 1, "invalid_ohlc": 1, "duplicate_minute": 1,
                                               "non_numeric": 1}  # fmt: skip
    assert parse_candles(None) == ([], {})


def test_verify_detects_problems(built, tmp_path):
    root, *_ = built
    import shutil

    day = tmp_path / "2026-09-30"
    shutil.copytree(root / "2026-09-30", day)
    good = rows(day / EQUITY_FILE)
    broken = [*good, dict(good[0]), {**good[1], "high": "1"}, {**good[2], "timestamp": "2026-09-29 10:00:00 IST"}]
    with gzip.open(day / EQUITY_FILE, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(good[0]))
        writer.writeheader()
        writer.writerows(broken)
    report = verify_day(day, sorted(SYMBOLS))
    text = " ".join(report["problems"])
    assert not report["ok"]
    for expected in ("duplicates", "invalid ohlc", "wrong day", "manifest records", "SHA-256"):
        assert expected in text, expected
    assert report["files"][EQUITY_FILE]["missing_expected"] == ["DELISTED"]


def test_the_engine_starts_and_replays_deterministically_on_bootstrapped_data(built, tmp_path):
    from intelligence.engine import IntelligenceEngine, replay_session
    from intelligence.event_store import EventStore
    from intelligence.frame import as_of_time, frame_at
    from intelligence.live import LiveRunner
    from intelligence.settings import Settings

    root, *_ = built
    day = load_day(root, "2026-10-01")
    ids = []
    for name in ("a", "b"):
        engine = IntelligenceEngine(root, tmp_path / name, event_store=EventStore(tmp_path / name / "e.db"))
        replay_session(engine, day, every=5)
        ids.append([e["event_id"] for e in EventStore(tmp_path / name / "e.db").search(after_seq=0, limit=1000)])
    assert ids[0] == ids[1]
    frame = frame_at(day, as_of_time("2026-10-01", "11:00"))
    assert int(frame.grid[-1]) + 60 <= int(as_of_time("2026-10-01", "11:00").timestamp())  # no later bar
    settings = Settings(root, tmp_path / "live", "http://127.0.0.1:9", "127.0.0.1", 18101, True, 60, 10, 5, 2,
                        False, False, 60, 2)  # fmt: skip
    runner = LiveRunner(settings, fetch=lambda url: {}, clock=lambda: NOW + timedelta(minutes=1))
    assert runner.health()["state"] == "STARTING"
    runner.tick()
    assert runner.health()["state"] == "CLOSED" and runner.snapshot.session_date == "2026-10-01"


def test_cli_plan_status_verify(built, capsys):
    from intelligence.__main__ import main

    root, *_ = built
    assert main(["--archive-dir", str(root), "bootstrap-history", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["bootstrap_sessions"] == 5 and status["live_sessions"] == 0
    assert main(["--archive-dir", str(root), "bootstrap-history", "plan", "--sessions", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["already_archived"] == 5
    assert main(["--archive-dir", str(root), "bootstrap-history", "run"]) == 3  # no credentials: blocked, not crashed


def test_validate_replay_command(market_root, store_root, capsys):
    from intelligence.__main__ import main
    from tests.intelligence.conftest import TODAY

    assert main(["--archive-dir", str(market_root), "validate-replay", TODAY, "--store-dir", str(store_root)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["deterministic"] and report["no_look_ahead"] and report["events"] > 0
    assert all(not c["differences"] for c in report["checkpoints"].values())
