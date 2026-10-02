"""Per-minute derivatives snapshots recorded by the intelligence process.

The daily archive holds only 1m candles. To relate spot, futures and options
over time, the live intelligence process records one compact snapshot per
minute from PSYGRID's existing endpoints (index spot, front-month futures,
option-chain analytics) to ``<store>/derivatives/<date>.jsonl``. Replay reads
the same file, exposing only snapshots taken by ``as_of``. Values are copied as
served; nothing is interpolated between minutes.
"""

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass
from pathlib import Path

FUTURES_FIELDS = ("last_price", "oi", "top_bid_price", "top_ask_price", "volume")
OPTION_FIELDS = ("pcr_oi", "pcr_volume", "avg_call_iv", "avg_put_iv", "iv_skew", "atm_strike", "max_pain_strike")


def _clean(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def snapshot_from_payloads(minute_epoch: int, spot: dict, futures: dict, options: dict) -> dict:
    """Build one snapshot from endpoint payloads.

    ``spot`` maps index key to its last price; ``futures`` and ``options`` map
    index key to the ``/public/<key>-futures.json`` and ``-options.json``
    payloads. Missing or non-numeric fields become None.
    """
    snapshot = {
        "minute": int(minute_epoch),
        "spot": {k: _clean(v) for k, v in spot.items()},
        "futures": {},
        "options": {},
    }
    for key, payload in futures.items():
        snapshot["futures"][key] = {f: _clean((payload or {}).get(f)) for f in FUTURES_FIELDS}
    for key, payload in options.items():
        analytics = (payload or {}).get("analytics") or {}
        row = {f: _clean(analytics.get(f)) for f in OPTION_FIELDS}
        row["underlying_ltp"] = _clean((payload or {}).get("underlying_ltp"))
        snapshot["options"][key] = row
    return snapshot


class DerivativesRecorder:
    """Appends snapshots to the day's file; one line per minute, written whole."""

    def __init__(self, root: Path):
        self.root = Path(root) / "derivatives"
        self._lock = threading.Lock()

    def path(self, session_date: str) -> Path:
        return self.root / f"{session_date}.jsonl"

    def append(self, session_date: str, snapshot: dict) -> None:
        line = json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n"
        with self._lock:
            self.root.mkdir(parents=True, exist_ok=True)
            with open(self.path(session_date), "a", encoding="utf-8") as handle:
                handle.write(line)


@dataclass(frozen=True)
class DerivativesDay:
    session_date: str
    snapshots: tuple[dict, ...]  # ascending by minute

    def known_at(self, as_of_epoch: int) -> tuple[dict, ...]:
        """Snapshots taken at or before ``as_of`` (their minute is the time they were taken)."""
        return tuple(s for s in self.snapshots if s["minute"] <= as_of_epoch)

    def series(self, as_of_epoch: int, section: str, key: str, field: str) -> list[tuple[int, float]]:
        out = []
        for snap in self.known_at(as_of_epoch):
            value = (snap.get(section) or {}).get(key)
            value = value.get(field) if isinstance(value, dict) else value
            if value is not None:
                out.append((snap["minute"], float(value)))
        return out


def load_derivatives(root: Path, session_date: str) -> DerivativesDay:
    """The day's recorded snapshots; empty when nothing was recorded. A torn last line is skipped."""
    path = Path(root) / "derivatives" / f"{session_date}.jsonl"
    snapshots = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                snap = json.loads(line)
            except ValueError:
                continue
            snapshots[int(snap["minute"])] = snap  # a re-recorded minute replaces the earlier line
    return DerivativesDay(session_date, tuple(snapshots[m] for m in sorted(snapshots)))


CHAIN_COLUMNS = (
    "minute", "underlying", "expiry", "underlying_ltp", "strike", "side", "last_price", "oi", "previous_oi",
    "volume", "implied_volatility", "top_bid_price", "top_bid_quantity", "top_ask_price", "top_ask_quantity",
    "delta", "gamma", "theta", "vega",
)  # fmt: skip
CHAIN_BYTES_PER_DAY = 150_000_000


def chain_rows(minute_epoch: int, key: str, payload: dict) -> list[tuple]:
    """Every strike and side of one option-chain payload, as served (missing values stay empty)."""
    rows = []
    expiry, underlying = payload.get("expiry"), _clean(payload.get("underlying_ltp"))
    for row in payload.get("strikes") or []:
        strike = _clean(row.get("strike"))
        for side in ("ce", "pe"):
            leg = row.get(side)
            if not isinstance(leg, dict):
                continue
            greeks = leg.get("greeks") if isinstance(leg.get("greeks"), dict) else {}
            values = [_clean(leg.get(f)) for f in ("last_price", "oi", "previous_oi", "volume", "implied_volatility",
                                                    "top_bid_price", "top_bid_quantity", "top_ask_price",
                                                    "top_ask_quantity")]  # fmt: skip
            values += [_clean(greeks.get(g)) for g in ("delta", "gamma", "theta", "vega")]
            rows.append((minute_epoch, key, expiry, underlying, strike, side.upper(),
                         *["" if v is None else v for v in values]))  # fmt: skip
    return rows


class ChainRecorder:
    """Full option-chain snapshots, once a minute per underlying, within a daily byte budget.

    ``<root>/chains/<date>.csv.gz`` is a sequence of gzip members (one per
    snapshot), which reads back as one CSV stream; a torn last member from a
    crash is detected by the reader and skipped.
    """

    def __init__(self, root: Path, bytes_per_day: int = CHAIN_BYTES_PER_DAY):
        self.root = Path(root) / "chains"
        self.budget = bytes_per_day
        self._lock = threading.Lock()

    def path(self, session_date: str) -> Path:
        return self.root / f"{session_date}.csv.gz"

    def append(self, session_date: str, rows: list[tuple]) -> bool:
        if not rows:
            return False
        import csv
        import gzip
        import io

        path = self.path(session_date)
        with self._lock:
            self.root.mkdir(parents=True, exist_ok=True)
            size = path.stat().st_size if path.exists() else 0
            if size >= self.budget:
                return False
            buffer = io.StringIO()
            writer = csv.writer(buffer, lineterminator="\n")
            if size == 0:
                writer.writerow(CHAIN_COLUMNS)
            writer.writerows(rows)
            with open(path, "ab") as handle:
                handle.write(gzip.compress(buffer.getvalue().encode(), compresslevel=6))
        return True


def load_chains(root: Path, session_date: str) -> list[dict]:
    """Recorded chain rows for a day (a torn final gzip member is ignored)."""
    import csv
    import io
    import zlib

    path = Path(root) / "chains" / f"{session_date}.csv.gz"
    if not path.exists():
        return []
    data = path.read_bytes()
    text, offset = [], 0
    while offset < len(data):
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            chunk = decompressor.decompress(data[offset:])
        except zlib.error:
            break
        if not decompressor.eof:
            break  # a torn member
        text.append(chunk.decode())
        offset = len(data) - len(decompressor.unused_data)
    return list(csv.DictReader(io.StringIO("".join(text))))
