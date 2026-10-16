"""Market-data stream -> per-instrument day matrices.

Stream (produced by deploy/live-core/behaviour_fetch.py, gzip-compressed):
    META interval=<1|5> ...                       once, first line
    INST <name> <kind> <sector>                    starts an instrument
    ROWS <n>  + n*6 little-endian float64          (epoch, open, high, low, close, volume)
    DONE <name> <status>                           ends an instrument
    STOP <reason> | END                            last line
Nothing is written to disk; matrices live in RAM for the length of the run.
"""

# ruff: noqa: E741  (O/H/L/C/V are the conventional OHLCV names)

from __future__ import annotations

import gzip
import io
from dataclasses import dataclass, field

import numpy as np

IST_OFFSET = 19800
OPEN_MIN = 555  # 09:15 in minutes after midnight
SESSION = 375  # 09:15 .. 15:29, one-minute slots
BARS = 75  # 5-minute bars per session
MIN_COVERAGE = 0.95


@dataclass
class Instrument:
    name: str
    kind: str
    sector: str
    status: str = "OK"
    dates: np.ndarray = field(default_factory=lambda: np.zeros(0, np.int64))  # IST day numbers
    m1: np.ndarray | None = None  # [D, 375, 5] o h l c v (1-minute), when the stream was 1-minute
    O: np.ndarray | None = None  # [D, 75] 5-minute bars
    H: np.ndarray | None = None
    L: np.ndarray | None = None
    C: np.ndarray | None = None
    V: np.ndarray | None = None
    audit: dict = field(default_factory=dict)
    sessions: dict = field(default_factory=dict)  # per-session integrity audit (columns AUDIT_COLS)


def _fill(M: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Forward-fill missing slots of [D, S, 5] in place: o=h=l=c=previous close, v=0."""
    present = ~np.isnan(M[:, :, 3])
    idx = np.where(present, np.arange(M.shape[1])[None, :], 0)
    np.maximum.accumulate(idx, axis=1, out=idx)
    c = np.take_along_axis(M[:, :, 3], idx, axis=1)
    miss = ~present
    for k in range(4):
        M[:, :, k] = np.where(miss, c, M[:, :, k])
    M[:, :, 4] = np.where(miss, 0.0, np.nan_to_num(M[:, :, 4]))
    return M, miss.sum(axis=1)


# Per-session status in the integrity audit. Every session that has any row appears in the audit;
# only the KEPT* ones reach the analysis, and every exclusion is counted and reported.
KEPT, KEPT_FILLED = "KEPT", "KEPT_FILLED"
EXCLUDED = (
    "EXCLUDED_NO_REGULAR_SESSION",  # no minute inside 09:15-15:29 (e.g. the Diwali Muhurat session)
    "EXCLUDED_DUPLICATE_CONFLICT",  # one timestamp with two different candles
    "EXCLUDED_NO_0915",  # the first minute is missing (open price unknown)
    "EXCLUDED_INCOMPLETE",  # fewer than 95% of the session's slots
)
AUDIT_COLS = (
    "date",
    "status",
    "rows",
    "first",
    "last",
    "pre_0915",
    "core",
    "post_1530",
    "missing",
    "has_0915",
    "has_1529",
    "dup_identical",
    "dup_conflict",
    "off_minute",
)


def _hhmm(minute_of_day: np.ndarray) -> np.ndarray:
    return np.char.add(
        np.char.zfill((minute_of_day // 60).astype(str), 2),
        np.char.add(":", np.char.zfill((minute_of_day % 60).astype(str), 2)),
    )


def session_audit(rows: np.ndarray, interval: int) -> tuple[np.ndarray, dict]:
    """Deduplicate rows and audit every session that has any row.

    Returns (rows without identical duplicates, sorted by time; audit as column arrays, one entry per
    session, columns AUDIT_COLS). Only timestamps and counts are recorded, never prices."""
    order = np.argsort(rows[:, 0], kind="stable")
    rows = rows[order]
    ts = rows[:, 0].astype(np.int64)
    same = ts[1:] == ts[:-1]
    a, b = rows[1:, 1:6], rows[:-1, 1:6]
    equal = np.all((a == b) | (np.isnan(a) & np.isnan(b)), axis=1)
    keep = np.r_[True, ~(same & equal)]
    dup_ident_at, dup_conf_at = np.r_[False, same & equal], np.r_[False, same & ~equal]
    local = ts + IST_OFFSET
    day = local // 86400
    mod = (local % 86400) // 60
    off = (local % 60) != 0
    minute = mod - OPEN_MIN
    slots = SESSION // interval
    dates, di = np.unique(day, return_inverse=True)
    n = len(dates)

    def per(mask):
        return np.bincount(di, weights=mask.astype(float), minlength=n).astype(int)

    k = keep
    core_m = k & (minute >= 0) & (minute < SESSION)
    slot = np.where(core_m, minute // interval, 0)
    seen = np.zeros((n, slots), bool)
    seen[di[core_m], slot[core_m]] = True
    first = np.full(n, 10**6)
    last = np.full(n, -1)
    np.minimum.at(first, di, mod)
    np.maximum.at(last, di, mod)
    core = seen.sum(1)
    conflict = per(dup_conf_at)
    status = np.where(
        core == 0,
        EXCLUDED[0],
        np.where(
            conflict > 0,
            EXCLUDED[1],
            np.where(
                ~seen[:, 0],
                EXCLUDED[2],
                np.where(core < MIN_COVERAGE * slots, EXCLUDED[3], np.where(core < slots, KEPT_FILLED, KEPT)),
            ),
        ),
    )
    audit = {
        "date": dates,
        "status": status,
        "rows": per(k),
        "first": _hhmm(first),
        "last": _hhmm(last),
        "pre_0915": per(k & (minute < 0)),
        "core": core,
        "post_1530": per(k & (minute >= SESSION)),
        "missing": slots - core,
        "has_0915": seen[:, 0],
        "has_1529": seen[:, -1],
        "dup_identical": per(dup_ident_at),
        "dup_conflict": conflict,
        "off_minute": per(k & off),
    }
    return rows[keep], audit


def audit_summary(audit: dict, interval: int) -> dict:
    st = audit["status"]
    slots = SESSION // interval
    kept = np.isin(st, (KEPT, KEPT_FILLED))
    return {
        "sessions_with_rows": len(st),
        "sessions_kept": int(kept.sum()),
        "sessions_kept_complete": int((st == KEPT).sum()),
        "sessions_kept_filled": int((st == KEPT_FILLED).sum()),
        "slots_filled": int(audit["missing"][kept].sum()),
        "sessions_excluded": {e: int((st == e).sum()) for e in EXCLUDED},
        "sessions_missing_0915": int((~audit["has_0915"]).sum()),
        "sessions_missing_1529": int((~audit["has_1529"]).sum()),
        "sessions_unusual_length": int((audit["rows"] != slots).sum()),
        "sessions_with_rows_before_0915": int((audit["pre_0915"] > 0).sum()),
        "sessions_with_rows_from_1530": int((audit["post_1530"] > 0).sum()),
        "duplicate_timestamps_identical": int(audit["dup_identical"].sum()),
        "duplicate_timestamps_conflicting": int(audit["dup_conflict"].sum()),
        "timestamps_not_on_the_minute": int(audit["off_minute"].sum()),
    }


def build(
    name: str,
    kind: str,
    sector: str,
    rows: np.ndarray,
    interval: int,
    status: str = "OK",
    day_from: int | None = None,
    day_to: int | None = None,
) -> Instrument:
    """Day matrices for the sessions that pass the integrity audit (KEPT, KEPT_FILLED).

    day_from/day_to (IST day numbers, inclusive) restrict the span; rows outside it are counted in
    the audit summary as out_of_span and otherwise ignored."""
    inst = Instrument(name, kind, sector, status)
    out_of_span = 0
    if rows.size and (day_from is not None or day_to is not None):
        d = (rows[:, 0].astype(np.int64) + IST_OFFSET) // 86400
        sel = np.ones(len(rows), bool)
        if day_from is not None:
            sel &= d >= day_from
        if day_to is not None:
            sel &= d <= day_to
        out_of_span = int((~sel).sum())
        rows = rows[sel]
    if rows.size == 0:
        inst.sessions = {c: np.zeros(0) for c in AUDIT_COLS}
        inst.audit = {"rows": 0, "sessions_raw": 0, "sessions_kept": 0, "rows_out_of_span": out_of_span}
        return inst
    rows, inst.sessions = session_audit(rows, interval)
    ts = rows[:, 0].astype(np.int64) + IST_OFFSET
    day = ts // 86400
    minute = (ts % 86400) // 60 - OPEN_MIN
    keep = (minute >= 0) & (minute < SESSION)
    rows, day, minute = rows[keep], day[keep], minute[keep]
    slots = SESSION // interval
    slot = minute // interval
    dates, di = np.unique(day, return_inverse=True)
    M = np.full((len(dates), slots, 5), np.nan)
    M[di, slot] = rows[:, 1:6]
    ok = inst.sessions["date"][np.isin(inst.sessions["status"], (KEPT, KEPT_FILLED))]
    good = np.isin(dates, ok)
    M, filled = _fill(M[good])
    inst.dates = dates[good]
    if interval == 1:
        inst.m1 = M
        B = M.reshape(len(inst.dates), BARS, 5, 5)
        inst.O, inst.H, inst.L, inst.C = B[:, :, 0, 0], B[:, :, :, 1].max(2), B[:, :, :, 2].min(2), B[:, :, 4, 3]
        inst.V = B[:, :, :, 4].sum(2)
    else:
        inst.O, inst.H, inst.L, inst.C, inst.V = (M[:, :, k] for k in range(5))
    inst.audit = {
        "rows": len(rows),
        "rows_out_of_span": out_of_span,
        "interval_min": interval,
        "sessions_raw": len(inst.sessions["date"]),
        "sessions_kept": int(good.sum()),
        "sessions_dropped_incomplete": int(len(inst.sessions["date"]) - good.sum()),
        "slots_filled_pct": round(100 * float(filled.sum()) / max(1, filled.size * slots), 3),
        "first": str(np.datetime64(int(inst.dates[0]), "D")) if len(inst.dates) else None,
        "last": str(np.datetime64(int(inst.dates[-1]), "D")) if len(inst.dates) else None,
        "has_volume": bool(np.nansum(inst.V) > 0) if inst.V is not None else False,
        "integrity": audit_summary(inst.sessions, interval),
    }
    return inst


def read_stream(fh, day_from=None, day_to=None, on_instrument=None) -> tuple[dict, list[Instrument]]:
    """Parse a (gzip) stream from a binary file object.

    day_from/day_to restrict every instrument to that span (see build). on_instrument(inst), if
    given, is called after each instrument is built; an exception from it stops the parse."""
    if not hasattr(fh, "peek"):
        fh = io.BufferedReader(fh)
    if fh.peek(2)[:2] == b"\x1f\x8b":
        fh = gzip.GzipFile(fileobj=fh)
    meta: dict = {"interval": 5}
    out: list[Instrument] = []
    cur = None
    chunks: list[np.ndarray] = []
    while True:
        line = fh.readline()
        if not line:
            break
        parts = line.decode().strip().split(" ")
        tag = parts[0]
        if tag == "META":
            for p in parts[1:]:
                k, _, v = p.partition("=")
                meta[k] = int(v) if v.isdigit() else v
        elif tag == "INST":
            cur, chunks = parts[1:4], []
        elif tag == "ROWS":
            n = int(parts[1])
            buf = b""
            while len(buf) < n * 48:
                part = fh.read(n * 48 - len(buf))
                if not part:
                    raise EOFError("stream ended inside a ROWS block")
                buf += part
            chunks.append(np.frombuffer(buf, dtype="<f8").reshape(n, 6).copy())
        elif tag == "DONE" and cur:
            rows = np.concatenate(chunks) if chunks else np.zeros((0, 6))
            status = parts[2] if len(parts) > 2 else "OK"
            out.append(build(cur[0], cur[1], cur[2], rows, int(meta["interval"]), status, day_from, day_to))
            cur, chunks = None, []
            del rows
            if on_instrument is not None:
                on_instrument(out[-1])
        elif tag == "STOP":
            meta["stopped"] = " ".join(parts[1:])
        elif tag == "END":
            meta["complete"] = True
            break
    return meta, out


def write_instrument(fh, name: str, kind: str, sector: str, rows: np.ndarray, status: str = "OK") -> None:
    """Writer used by the node fetcher and by the tests (same format as read_stream)."""
    fh.write(f"INST {name} {kind} {sector}\n".encode())
    if len(rows):
        fh.write(f"ROWS {len(rows)}\n".encode())
        fh.write(np.ascontiguousarray(rows, dtype="<f8").tobytes())
    fh.write(f"DONE {name} {status}\n".encode())
