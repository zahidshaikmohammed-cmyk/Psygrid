"""Data-integrity audit and the confirmation driver. Synthetic data only: these tests prove the
machinery (nothing dropped silently, aborts make no verdicts, outputs carry no prices), not any edge."""

import contextlib
import csv
import datetime as dt
import gzip
import io
import json
import re

import numpy as np

from research.behaviour import confirm_run, lockcheck
from research.behaviour.data import EXCLUDED, KEPT, KEPT_FILLED, build, read_stream, write_instrument
from research.behaviour.synth import make_rows_1m, sessions

D0 = dt.date(2021, 10, 1)


def _days(n, start=D0):
    return sessions(start, start + dt.timedelta(days=n * 2))[:n]


def _defective(seed=3):
    """Six sessions, each with a different defect, plus extra rows outside the session."""
    days = _days(8)
    rows = make_rows_1m(seed, days)
    per = {d: rows[k * 375 : (k + 1) * 375].copy() for k, d in enumerate(days)}
    per[days[1]] = per[days[1]][1:]  # no 09:15
    per[days[2]] = np.delete(per[days[2]], range(100, 130), axis=0)  # 30 missing: incomplete
    per[days[3]] = np.delete(per[days[3]], [50, 51, 52], axis=0)  # 3 missing: kept, filled
    dup = per[days[4]][[10]].copy()
    per[days[4]] = np.vstack([per[days[4]], dup])  # identical duplicate: deduplicated, kept
    bad = per[days[5]][[20]].copy()
    bad[0, 4] *= 1.01
    per[days[5]] = np.vstack([per[days[5]], bad])  # conflicting duplicate: excluded
    late = per[days[6]][-31:].copy()
    late[:, 0] += 31 * 60
    per[days[6]] = np.vstack([per[days[6]], late])  # 31 minutes after 15:29 (as in 2021): kept
    muhurat = per[days[7]][:60].copy()
    muhurat[:, 0] += 9 * 3600  # 18:15-19:14 only
    per[days[7]] = muhurat
    return days, np.vstack(list(per.values()))


def test_every_session_is_audited_and_none_is_dropped_silently():
    days, rows = _defective()
    inst = build("X", "EQUITY", "BANKING", rows[np.random.default_rng(0).permutation(len(rows))], 1)
    a = inst.sessions
    st = dict(zip(a["date"].tolist(), a["status"].tolist(), strict=True))
    n = [(d - dt.date(1970, 1, 1)).days for d in days]
    assert len(st) == 8
    assert st[n[0]] == KEPT and st[n[4]] == KEPT and st[n[6]] == KEPT
    assert st[n[1]] == "EXCLUDED_NO_0915"
    assert st[n[2]] == "EXCLUDED_INCOMPLETE"
    assert st[n[3]] == KEPT_FILLED
    assert st[n[5]] == "EXCLUDED_DUPLICATE_CONFLICT"
    assert st[n[7]] == "EXCLUDED_NO_REGULAR_SESSION"
    k = {d: i for i, d in enumerate(a["date"].tolist())}
    assert a["missing"][k[n[3]]] == 3 and a["dup_identical"][k[n[4]]] == 1 and a["post_1530"][k[n[6]]] == 31
    assert a["first"][k[n[7]]] == "18:15" and a["last"][k[n[7]]] == "19:14"
    g = inst.audit["integrity"]
    assert g["sessions_kept"] == len(inst.dates) == 4
    assert sum(g["sessions_excluded"].values()) + g["sessions_kept"] == g["sessions_with_rows"] == 8
    assert set(g["sessions_excluded"]) == set(EXCLUDED) and all(v == 1 for v in g["sessions_excluded"].values())
    assert g["duplicate_timestamps_identical"] == 1 and g["duplicate_timestamps_conflicting"] == 1
    assert sorted(inst.dates.tolist()) == [n[0], n[3], n[4], n[6]]
    # the kept session with an identical duplicate is unchanged by the duplicate
    ref = build("Y", "EQUITY", "BANKING", make_rows_1m(3, days), 1)
    j = ref.dates.tolist().index(n[4])
    assert np.array_equal(inst.m1[inst.dates.tolist().index(n[4])], ref.m1[j])


def test_span_restriction_counts_rows_outside():
    days = _days(10)
    rows = make_rows_1m(1, days)
    inst = build("X", "EQUITY", "-", rows, 1, day_from=(days[3] - dt.date(1970, 1, 1)).days)
    assert len(inst.dates) == 7 and inst.audit["rows_out_of_span"] == 3 * 375


def _stream(insts, stop=None, end=True):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(b"META interval=1 start=2021-10-01 end=2026-10-12\n")
        for name, kind, rows in insts:
            write_instrument(gz, name, kind, "-", rows)
        if stop:
            gz.write(f"STOP {stop}\n".encode())
        if end:
            gz.write(b"END\n")
    buf.seek(0)
    return buf


def _universe(n_days=420, n_stocks=6):
    days = _days(n_days)
    mk = np.random.default_rng(7).normal(0, 0.0003, size=(len(days), 375))
    out = [("NIFTY", "INDEX", make_rows_1m(1, days, mk, sigma=0.0002, volume=False))]
    for k in range(n_stocks):
        out.append((f"S{k}", "EQUITY", make_rows_1m(10 + k, days, mk)))
    return out


def _run(tmp_path, stream, name="out"):
    p = tmp_path / f"{name}.gz"
    p.write_bytes(stream.read())
    out = tmp_path / name
    rc = confirm_run.main(["--stream", str(p), "--out", str(out)], sector_of=lambda s: "BANKING")
    return rc, out, json.loads((out / "summary.json").read_text())


def test_aborted_fetch_makes_no_verdicts_but_reports_integrity(tmp_path):
    rc, out, s = _run(tmp_path, _stream(_universe(60, 2), stop="live service health: slow (4.2s)"))
    assert rc == 4 and s["status"] == "ABORTED" and "live service health" in s["reason"]
    assert "verdicts" not in s and s["lock"]["ok"]
    assert (out / "integrity_summary.json").exists() and (out / "session_audit.csv.gz").exists()
    rc2, _, s2 = _run(tmp_path, _stream(_universe(60, 2), end=False), "trunc")
    assert rc2 == 4 and s2["reason"] == "stream ended before END"


def test_full_run_on_synthetic_data_writes_aggregates_only(tmp_path):
    uni = _universe()
    _, rows = _defective(seed=40)
    uni.append(("DEFECT", "EQUITY", rows))
    rc, out, s = _run(tmp_path, _stream(uni))
    assert rc == 0 and s["status"] == "COMPLETE"
    assert set(s["verdicts"]) == {"H1", "H2", "H3", "H4", "H5", "H6"}
    assert all(r["verdict"] != "CONFIRMED" for r in s["verdicts"].values())  # random walks: no edge
    assert "HISTORICAL CONFIRMATION PERIOD THAT PREDATES" in (out / "REPORT.md").read_text()
    assert "does not prove that no edge exists" in (out / "REPORT.md").read_text()
    assert s["not_applicable_instruments"]["confirmation"]["DEFECT"].startswith("only")
    with open(out / "session_anomalies.csv") as fh:
        anomalies = list(csv.DictReader(fh))
    assert {r["status"] for r in anomalies if r["instrument"] == "DEFECT"} >= set(EXCLUDED) | {KEPT_FILLED}
    with gzip.open(out / "session_audit.csv.gz", "rt") as fh:
        audit = list(csv.DictReader(fh))
    assert len(audit) == s["sessions_audited"] == 7 * 420 + 8
    # no price-like values anywhere in the outputs: every price in the synthetic data is > 100
    prices = np.unique(np.round(np.concatenate([r[:, 1:5].ravel() for _, _, r in uni]), 2))
    for f in out.iterdir():
        with gzip.open(f, "rt") if f.suffix == ".gz" else open(f) as fh:
            text = fh.read()
        nums = {round(float(x), 2) for x in re.findall(r"(?<![\w.])\d{3,}\.\d+", text)}
        assert not (nums & set(prices.tolist())), f.name
    assert {f.name for f in out.iterdir()} == {
        "summary.json",
        "integrity_summary.json",
        "session_anomalies.csv",
        "session_audit.csv.gz",
        "eligibility.json",
        "per_stock.csv",
        "per_month.csv",
        "REPORT.md",
    }


def test_refuses_when_preregistration_changed(tmp_path, monkeypatch):
    monkeypatch.setattr(confirm_run, "verify", lambda: {"ok": False, "files": {}})
    rc, _, s = _run(tmp_path, _stream(_universe(30, 1)))
    assert rc == 3 and s["status"] == "REFUSED"
    assert lockcheck.verify()["ok"]


def test_read_stream_guard_can_stop_the_parse():
    calls = []

    def guard(inst):
        calls.append(inst.name)
        if len(calls) == 2:
            raise RuntimeError("memory")

    with contextlib.suppress(RuntimeError):
        read_stream(_stream(_universe(5, 3)), on_instrument=guard)
    assert calls == ["NIFTY", "S0"]
