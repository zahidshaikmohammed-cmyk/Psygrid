"""Archive reader, MarketFrame, data quality and replay: correctness and no look-ahead."""

import csv
import gzip
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from daily_archive import EQUITY_COLUMNS, EQUITY_FILE, INDEX_COLUMNS, INDEX_FILE, DailyArchive
from intelligence.__main__ import main
from intelligence.archive import available_days, load_day
from intelligence.frame import as_of_time, frame_at, session_grid
from intelligence.quality import COMPLETE, GAPS, NO_DATA, frame_quality
from intelligence.replay import replay

IST = ZoneInfo("Asia/Kolkata")
DAY = "2026-10-05"


def stamp(hhmm: str) -> str:
    return f"{DAY} {hhmm}:00 IST"


def minute(hhmm: str, offset: int) -> str:
    base = datetime.strptime(f"{DAY} {hhmm}", "%Y-%m-%d %H:%M") + timedelta(minutes=offset)
    return base.strftime("%H:%M")


def bar(symbol: str, sid: str, hhmm: str, close: float, **overrides) -> list:
    row = {"symbol": symbol, "security_id": sid, "timestamp": stamp(hhmm), "open": close - 1,
           "high": close + 1, "low": close - 2, "close": close, "volume": 100}  # fmt: skip
    row.update(overrides)
    return [row[c] for c in EQUITY_COLUMNS]


def write_csv(path, columns, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def standard_rows(until: str = "10:30") -> list[list]:
    """AAA trades every minute 09:15-until; BBB misses 09:20 and 09:21; CCC never trades."""
    rows = []
    n = 0
    while minute("09:15", n) < until:
        hhmm = minute("09:15", n)
        rows.append(bar("AAA", "1", hhmm, 100.0 + n))
        if hhmm not in ("09:20", "09:21"):
            rows.append(bar("BBB", "2", hhmm, 200.0 + n))
        n += 1
    return rows


@pytest.fixture
def archive_root(tmp_path):
    rows = standard_rows()
    rows += [
        bar("CCC", "3", "09:16", 50.0, high=40.0),  # high below close: invalid geometry
        bar("AAA", "1", "09:15", 999.0),  # duplicate minute: first row wins
        bar("BBB", "2", "09:10", 1.0),  # before the session opens
        bar("BBB", "2", "10:00", 300.0, volume=""),  # missing value, rejected after 10:00
    ]
    write_csv(tmp_path / DAY / EQUITY_FILE, EQUITY_COLUMNS, rows)
    index_rows = [["nifty", "NIFTY", stamp(minute("09:15", n)), 1, 2, 0.5, 1.5, 10] for n in range(30)]
    write_csv(tmp_path / DAY / INDEX_FILE, INDEX_COLUMNS, index_rows)
    return tmp_path


def at(hhmm: str, seconds: int = 0) -> datetime:
    return as_of_time(DAY, hhmm) + timedelta(seconds=seconds)


# --- archive reader --------------------------------------------------------------------


def test_reader_keeps_values_exactly_and_leaves_gaps_empty(archive_root):
    day = load_day(archive_root, DAY)
    assert day.equity.keys == ("AAA", "BBB", "CCC")
    aaa = day.equity.keys.index("AAA")
    col_0915 = int(np.flatnonzero(day.equity.minutes == int(at("09:15").timestamp()))[0])
    assert day.equity.close[aaa, col_0915] == 100.0  # the duplicate 999.0 row was rejected, not used
    bbb = day.equity.keys.index("BBB")
    col_0920 = int(np.flatnonzero(day.equity.minutes == int(at("09:20").timestamp()))[0])
    assert np.isnan(day.equity.close[bbb, col_0920])  # missing stays missing
    assert np.isnan(day.equity.close[day.equity.keys.index("CCC")]).all()


def test_reader_rejects_bad_rows_with_reasons(archive_root):
    rejected = load_day(archive_root, DAY).equity.rejected
    assert [reason for _, reason in rejected["CCC"]] == ["invalid_ohlc"]
    assert [reason for _, reason in rejected["AAA"]] == ["duplicate_minute"]
    assert [reason for _, reason in rejected["BBB"]] == ["missing_value"]


def test_reader_round_trips_the_real_archive_writer(tmp_path):
    payload = {
        "session": {"date": DAY},
        "stocks": {
            "XYZ": {
                "security_id": "9",
                "previous_close": 10.0,
                "today_open": 10.5,
                "candles_1m": [
                    {"timestamp": stamp("09:15"), "open": 10.5, "high": 11.0, "low": 10.0, "close": 10.8, "volume": 7}
                ],
            },
        },
    }
    DailyArchive(tmp_path).write_equity(payload)
    day = load_day(tmp_path, DAY)
    assert day.equity.keys == ("XYZ",)
    assert day.equity.close[0, 0] == 10.8 and day.equity.volume[0, 0] == 7
    assert day.reference == {"XYZ": {"previous_close": 10.0, "today_open": 10.5}}
    assert available_days(tmp_path) == [DAY]


def test_missing_day_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_day(tmp_path, DAY)


# --- MarketFrame and look-ahead -------------------------------------------------------


def test_frame_holds_only_bars_completed_by_as_of(archive_root):
    day = load_day(archive_root, DAY)
    frame = frame_at(day, at("10:17"))
    assert frame.grid[-1] == int(at("10:16").timestamp())  # the 10:16 bar closed at 10:17
    assert frame.grid[0] == int(at("09:15").timestamp())
    assert frame_at(day, at("10:16", 30)).grid[-1] == int(at("10:15").timestamp())  # 10:16 still open


def test_frame_excludes_bars_outside_the_session(archive_root):
    day = load_day(archive_root, DAY)
    assert len(frame_at(day, at("09:15")).grid) == 0  # nothing has completed at the open
    full = frame_at(day, at("16:00")).grid
    assert full[-1] == int(at("15:14").timestamp())  # the 15:15 close ends the grid
    assert len(session_grid(DAY, at("09:10"))) == 0
    bbb = frame_at(day, at("09:20")).equity
    assert not np.isnan(bbb.close[bbb.keys.index("BBB"), 0])  # 09:15 bar present; the 09:10 bar is gone


def test_no_look_ahead_frame_is_unchanged_when_later_bars_are_deleted(archive_root, tmp_path_factory):
    as_of = at("10:00")
    truncated_root = tmp_path_factory.mktemp("truncated")
    with gzip.open(archive_root / DAY / EQUITY_FILE, "rt", newline="") as handle:
        rows = list(csv.reader(handle))
    known = [r for r in rows[1:] if datetime.strptime(r[2], "%Y-%m-%d %H:%M:%S IST").replace(tzinfo=IST) + timedelta(minutes=1) <= as_of]  # fmt: skip
    write_csv(truncated_root / DAY / EQUITY_FILE, EQUITY_COLUMNS, known)

    full = frame_at(load_day(archive_root, DAY), as_of).equity
    cut = frame_at(load_day(truncated_root, DAY), as_of).equity
    assert full.keys == cut.keys
    for name in ("open", "high", "low", "close", "volume"):
        assert np.array_equal(full.field(name), cut.field(name), equal_nan=True), name
    assert full.rejected == cut.rejected  # BBB's 10:00 rejection is not yet known at 10:00


def test_frame_arrays_are_copies_not_views(archive_root):
    day = load_day(archive_root, DAY)
    frame = frame_at(day, at("09:30"))
    frame.equity.close[0, 0] = -1.0  # AAA's 09:15 bar in the frame
    col_0915 = int(np.flatnonzero(day.equity.minutes == int(at("09:15").timestamp()))[0])
    assert day.equity.close[0, col_0915] == 100.0


def test_as_of_must_be_timezone_aware(archive_root):
    with pytest.raises(ValueError):
        frame_at(load_day(archive_root, DAY), datetime(2026, 10, 5, 10, 0))


# --- data quality -------------------------------------------------------------------------


def test_quality_classifies_each_instrument(archive_root):
    quality = frame_quality(frame_at(load_day(archive_root, DAY), at("09:30")))
    by_key = {q.key: q for q in quality.equity.per_instrument}
    assert by_key["AAA"].status == COMPLETE and by_key["AAA"].present_bars == 15
    assert by_key["BBB"].status == GAPS and by_key["BBB"].missing_bars == 2
    assert by_key["CCC"].status == NO_DATA and by_key["CCC"].rejected_bars == 1
    assert by_key["AAA"].minutes_since_last_bar == 0.0
    assert quality.equity.coverage == round(28 / 45, 4)
    assert [q.key for q in quality.equity.worst()] == ["CCC", "BBB"]
    assert quality.indices.complete == 1


def test_quality_reports_staleness(archive_root):
    quality = frame_quality(frame_at(load_day(archive_root, DAY), at("11:00")))
    aaa = next(q for q in quality.equity.per_instrument if q.key == "AAA")
    assert aaa.last_bar == stamp("10:29")
    assert aaa.minutes_since_last_bar == 30.0
    assert aaa.status == GAPS


# --- replay and command line ------------------------------------------------------------


def test_replay_steps_minute_by_minute_and_is_deterministic(archive_root):
    day = load_day(archive_root, DAY)
    frames = list(replay(day, "09:20", "09:30"))
    assert len(frames) == 11
    assert [len(f.grid) for f in frames] == list(range(5, 16))
    again = list(replay(day, "09:20", "09:30"))
    assert all(
        np.array_equal(a.equity.close, b.equity.close, equal_nan=True) for a, b in zip(frames, again, strict=True)
    )
    assert len(list(replay(day, "09:15", "10:15", every=15))) == 5


def test_cli_replay_reports_quality(archive_root, capsys):
    assert main(["--archive-dir", str(archive_root), "replay", DAY, "--until", "09:30"]) == 0
    out = capsys.readouterr().out
    assert "as of 2026-10-05 09:30:00 IST" in out and "3 instruments" in out and "CCC" in out


def test_cli_replay_json_and_missing_day(archive_root, capsys):
    assert main(["--archive-dir", str(archive_root), "replay", DAY, "--until", "09:30", "--json"]) == 0
    record = json.loads(capsys.readouterr().out)
    assert record["equity"]["instruments"] == 3
    assert main(["--archive-dir", str(archive_root), "replay", "2026-10-06"]) == 2
    assert main(["--archive-dir", str(archive_root), "days"]) == 0
    assert DAY in capsys.readouterr().out
