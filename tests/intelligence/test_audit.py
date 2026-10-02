"""Archive audit: integrity, coverage and missingness are reported, never repaired."""

import csv
import gzip
import shutil

from daily_archive import EQUITY_FILE
from intelligence.archive import parse_timestamp
from intelligence.audit import audit, audit_day
from intelligence.frame import as_of_time
from tests.intelligence.conftest import DATES, TODAY


def test_audit_reports_defects(market_root, tmp_path, monkeypatch):
    monkeypatch.setattr("intelligence.audit._universe", lambda: None)  # synthetic symbols are not the real universe
    root = tmp_path / "archive"
    shutil.copytree(market_root, root)
    path = root / TODAY / EQUITY_FILE
    with gzip.open(path, "rt", newline="") as handle:
        reader = csv.DictReader(handle)
        columns, rows = reader.fieldnames, list(reader)
    gap = (as_of_time(TODAY, "11:00").timestamp(), as_of_time(TODAY, "11:10").timestamp())
    kept = [r for r in rows if not gap[0] <= parse_timestamp(r["timestamp"]) < gap[1]]
    kept.append(dict(kept[5]))  # a duplicate
    bad = dict(kept[6])
    bad["high"] = str(float(bad["low"]) - 1)  # high below low
    kept[6] = bad
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, columns)
        writer.writeheader()
        writer.writerows(kept)
    day = audit_day(root, TODAY, None, True)
    assert day["duplicates"] == 1 and day["invalid_ohlc"] == 1 and not day["integrity_ok"]
    assert "11:00-11:09" in day["missing_intervals"]
    report = audit(root)
    assert report["sessions_archived"] == len(DATES) and TODAY in report["sessions_failed"]
    assert report["totals"]["duplicates"] == 1 and report["sessions_integrity_ok"] == len(DATES) - 1
