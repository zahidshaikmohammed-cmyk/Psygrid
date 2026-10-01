import csv
import gzip
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from daily_archive import (
    EQUITY_COLUMNS,
    EQUITY_FILE,
    EQUITY_REFERENCE_FILE,
    INDEX_COLUMNS,
    INDEX_FILE,
    MANIFEST_FILE,
    ArchiveManager,
    DailyArchive,
    archive_dir_from_environment,
)
from index_layer import IndexInstrument, IndexLayerManager, IndexState
from output import market_live_json
from session import SessionManager
from state import PsygridState

SESSION_DATE = "2026-10-01"
EPOCH_0915 = int(datetime(2026, 10, 1, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata")).timestamp())


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=60)


def _candle(minute: int, close: float) -> dict:
    epoch = EPOCH_0915 + 60 * minute
    return {
        "timestamp": epoch,
        "epoch": epoch,
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 100 + minute,
        "complete": True,
    }


def _equity_state(candles_per_stock: int = 3) -> PsygridState:
    state = PsygridState(_settings())
    instruments = [
        SimpleNamespace(symbol="BBB", security_id="2", exchange_segment="NSE_EQ", instrument="EQUITY"),
        SimpleNamespace(symbol="AAA", security_id="1", exchange_segment="NSE_EQ", instrument="EQUITY"),
    ]
    state.begin(SESSION_DATE, instruments)
    for security_id in ("1", "2"):
        state.set_market_reference(security_id, previous_close=100.0, today_open=101.0)
        state.live_candles[security_id].extend(_candle(m, 100.0 + m) for m in range(candles_per_stock))
    return state


def _read_csv_gz(path) -> list[list[str]]:
    with gzip.open(path, "rt", newline="") as handle:
        return list(csv.reader(handle))


def test_equity_archive_matches_the_public_payload(tmp_path):
    archive = DailyArchive(tmp_path)
    payload = market_live_json(_equity_state())

    assert archive.write_equity(payload)

    rows = _read_csv_gz(tmp_path / SESSION_DATE / EQUITY_FILE)
    assert tuple(rows[0]) == EQUITY_COLUMNS
    assert len(rows) == 1 + 6
    assert rows[1] == ["AAA", "1", "2026-10-01 09:15:00 IST", "99.0", "101.0", "98.0", "100.0", "100"]
    assert [r[0] for r in rows[1:]] == ["AAA"] * 3 + ["BBB"] * 3

    references = _read_csv_gz(tmp_path / SESSION_DATE / EQUITY_REFERENCE_FILE)
    assert references[1:] == [["AAA", "1", "100.0", "101.0"], ["BBB", "2", "100.0", "101.0"]]

    manifest = json.loads((tmp_path / SESSION_DATE / MANIFEST_FILE).read_text())
    assert manifest["files"][EQUITY_FILE]["rows"] == 6
    assert manifest["synthetic_candles"] is False


def test_a_smaller_snapshot_never_replaces_a_fuller_one(tmp_path):
    archive = DailyArchive(tmp_path)
    assert archive.write_equity(market_live_json(_equity_state(candles_per_stock=5)))
    # A process restarted mid-session that has only re-bootstrapped part of the day.
    assert not archive.write_equity(market_live_json(_equity_state(candles_per_stock=2)))
    assert len(_read_csv_gz(tmp_path / SESSION_DATE / EQUITY_FILE)) == 1 + 10
    # A later, fuller snapshot does replace it.
    assert archive.write_equity(market_live_json(_equity_state(candles_per_stock=7)))
    assert len(_read_csv_gz(tmp_path / SESSION_DATE / EQUITY_FILE)) == 1 + 14


def test_an_empty_session_writes_nothing(tmp_path):
    archive = DailyArchive(tmp_path)
    assert not archive.write_equity(market_live_json(_equity_state(candles_per_stock=0)))
    assert not (tmp_path / SESSION_DATE).exists()


def test_index_archive(tmp_path):
    archive = DailyArchive(tmp_path)
    snapshots = {
        "nifty": {"symbol": "NIFTY", "session": {"date": SESSION_DATE}, "1m": [{"timestamp": "t1", "close": 1.0}]},
        "banknifty": {"symbol": "BANKNIFTY", "session": {"date": SESSION_DATE}, "1m": [{"timestamp": "t1"}]},
    }
    assert archive.write_indices(snapshots)
    rows = _read_csv_gz(tmp_path / SESSION_DATE / INDEX_FILE)
    assert tuple(rows[0]) == INDEX_COLUMNS
    assert [r[:2] for r in rows[1:]] == [["banknifty", "BANKNIFTY"], ["nifty", "NIFTY"]]


def test_archive_failures_are_recorded_not_raised(tmp_path):
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("")
    manager = ArchiveManager(DailyArchive(blocker), _equity_state(), lambda: None)

    manager.archive_now()

    assert manager.write_count == 0
    assert manager.last_error.startswith("equity:")
    assert manager.status()["error"] == manager.last_error


def test_manager_skips_closed_sessions(tmp_path):
    state = _equity_state()
    state.reset()
    manager = ArchiveManager(DailyArchive(tmp_path), state, lambda: None)
    manager.archive_now()
    assert manager.write_count == 0
    assert not any(tmp_path.iterdir())


def test_archive_dir_is_configurable(monkeypatch, tmp_path):
    monkeypatch.setenv("PSYGRID_ARCHIVE_DIR", str(tmp_path / "x"))
    assert archive_dir_from_environment() == tmp_path / "x"
    monkeypatch.delenv("PSYGRID_ARCHIVE_DIR")
    assert archive_dir_from_environment().name == "psygrid-data"


def _session_manager(state) -> SessionManager:
    manager = SessionManager.__new__(SessionManager)
    manager.state = state
    manager.feed = MagicMock()
    manager.history_stop = MagicMock()
    manager.history_thread = None
    manager._lock = __import__("threading").RLock()
    manager._started_for_date = SESSION_DATE
    manager.on_session_end = None
    return manager


def test_equity_session_end_archives_the_final_candle_before_the_wipe(tmp_path):
    state = _equity_state()
    state.current_1m["1"] = {**_candle(3, 104.0), "complete": False}
    session = _session_manager(state)
    archive_manager = ArchiveManager(DailyArchive(tmp_path), state, lambda: None)
    session.on_session_end = archive_manager.archive_equity

    session._end_session()

    rows = _read_csv_gz(tmp_path / SESSION_DATE / EQUITY_FILE)
    assert [r[2] for r in rows[1:] if r[0] == "AAA"][-1] == "2026-10-01 09:18:00 IST"  # in-progress candle kept
    assert state.session_status == "CLOSED"  # state still wiped afterwards


def test_a_failing_archive_hook_never_blocks_the_session_end():
    state = _equity_state()
    session = _session_manager(state)
    session.on_session_end = MagicMock(side_effect=OSError("disk full"))

    session._end_session()

    session.on_session_end.assert_called_once()
    assert state.session_status == "CLOSED"


def test_index_session_end_runs_the_hook_after_finalizing_every_index():
    manager = IndexLayerManager.__new__(IndexLayerManager)
    manager.settings = _settings()
    manager.feed = MagicMock()
    states = {}
    for key, security_id in (("nifty", "13"), ("banknifty", "25")):
        state = IndexState(manager.settings, key, key.upper(), IndexInstrument(security_id, "IDX_I"))
        state.begin(SESSION_DATE)
        state.current_1m = {**_candle(0, 10.0), "complete": False}
        states[key] = state
    manager.states = states
    seen = {}
    manager.on_session_end = lambda: seen.update(
        {key: (len(s.live_candles), s.session_status) for key, s in manager.states.items()}
    )

    manager._end_session()

    assert seen == {"nifty": (1, "LIVE"), "banknifty": (1, "LIVE")}
    assert all(s.session_status == "CLOSED" for s in states.values())


@pytest.mark.parametrize("filename", [EQUITY_FILE, MANIFEST_FILE])
def test_writes_leave_no_temporary_files(tmp_path, filename):
    DailyArchive(tmp_path).write_equity(market_live_json(_equity_state()))
    day = tmp_path / SESSION_DATE
    assert (day / filename).exists()
    assert not [p for p in day.iterdir() if p.name.endswith(".tmp")]


def test_archive_is_readable_with_pandas(tmp_path):
    pandas = pytest.importorskip("pandas")
    DailyArchive(tmp_path).write_equity(market_live_json(_equity_state()))
    frame = pandas.read_csv(tmp_path / SESSION_DATE / EQUITY_FILE)
    assert list(frame.columns) == list(EQUITY_COLUMNS)
    assert len(frame) == 6
    assert frame["volume"].dtype.kind == "i"
