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


def build(name: str, kind: str, sector: str, rows: np.ndarray, interval: int, status: str = "OK") -> Instrument:
    inst = Instrument(name, kind, sector, status)
    if rows.size == 0:
        inst.audit = {"rows": 0, "sessions_raw": 0, "sessions_kept": 0}
        return inst
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
    present = ~np.isnan(M[:, :, 3])
    count = present.sum(axis=1)
    good = (count >= MIN_COVERAGE * slots) & present[:, 0]
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
        "interval_min": interval,
        "sessions_raw": len(dates),
        "sessions_kept": int(good.sum()),
        "sessions_dropped_incomplete": int((~good).sum()),
        "slots_filled_pct": round(100 * float(filled.sum()) / max(1, filled.size * slots), 3),
        "first": str(np.datetime64(int(inst.dates[0]), "D")) if len(inst.dates) else None,
        "last": str(np.datetime64(int(inst.dates[-1]), "D")) if len(inst.dates) else None,
        "has_volume": bool(np.nansum(inst.V) > 0) if inst.V is not None else False,
    }
    return inst


def read_stream(fh) -> tuple[dict, list[Instrument]]:
    """Parse a (gzip) stream from a binary file object."""
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
            out.append(build(cur[0], cur[1], cur[2], rows, int(meta["interval"]), parts[2] if len(parts) > 2 else "OK"))
            cur, chunks = None, []
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
