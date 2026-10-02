"""Archive audit: what the historical data actually contains, session by session, with nothing assumed.

``python -m intelligence audit`` reports for every archived day, and in total:

- integrity (``archive_integrity.verify_day``): duplicates, malformed rows (missing values, unreadable
  timestamps), OHLC violations, bars outside the session or not minute-aligned, wrong-day rows, checksums;
- coverage: equities present against the 989-symbol universe, the indices present, previous-close coverage;
- missing intervals: minutes where fewer than half the stocks that traded that day have a bar (a feed or
  download gap rather than illiquidity), as time ranges; the per-stock bar fill distribution;
- volume consistency: zero-volume bars, bars whose price moved on zero volume;
- timestamp consistency: first and last bar of the day against 09:15 and 15:29;
- whether the day qualifies as a session (``archive.session_days``) and why not.

Nothing is repaired: problems are reported. A day with integrity problems is listed under ``failed``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np

from intelligence.archive import IST, available_days, load_day, session_days
from intelligence.archive_integrity import verify_day

SESSION_MINUTES = 375
GAP_SHARE = 0.5
EXPECTED_INDICES = ("nifty", "nifty500", "banknifty")


def _universe() -> tuple[str, ...] | None:
    try:
        from config import _load_symbol_universe

        return tuple(_load_symbol_universe())
    except Exception:
        return None


def _ranges(minutes: list[int]) -> list[str]:
    """Contiguous minute epochs as 'HH:MM-HH:MM' ranges."""
    out, start, prev = [], None, None
    for m in minutes:
        if start is None:
            start = prev = m
        elif m == prev + 60:
            prev = m
        else:
            out.append((start, prev))
            start = prev = m
    if start is not None:
        out.append((start, prev))

    def hhmm(e):
        return datetime.fromtimestamp(e, IST).strftime("%H:%M")

    return [hhmm(a) if a == b else f"{hhmm(a)}-{hhmm(b)}" for a, b in out]


def audit_day(root: Path, session_date: str, universe: tuple[str, ...] | None, qualified: bool) -> dict:
    integrity = verify_day(Path(root) / session_date, list(universe) if universe else None)
    report = {"session_date": session_date, "qualified_session": qualified, "integrity_ok": integrity["ok"],
              "problems": integrity["problems"]}  # fmt: skip
    eq = integrity["files"].get("equity_1m.csv.gz", {})
    for name in ("rows", "duplicates", "bad_timestamp", "wrong_day", "unaligned", "outside_session", "invalid_ohlc",
                 "missing_value"):  # fmt: skip
        report[name] = eq.get(name)
    try:
        day = load_day(root, session_date)
    except FileNotFoundError:
        report["error"] = "no equity file"
        return report
    bars = day.equity
    _n, m = bars.shape
    present = np.isfinite(bars.close)
    traded = present.any(axis=1)
    report["equities"] = int(traded.sum())
    if universe:
        have = {k for k, t in zip(bars.keys, traded, strict=True) if t}
        report["universe"] = len(universe)
        report["equity_coverage"] = round(len(have & set(universe)) / len(universe), 4)
        report["missing_symbols"] = len(set(universe) - have)
    fill = present[traded].sum(axis=1) / SESSION_MINUTES if traded.any() else np.zeros(0)
    report["bar_fill"] = {"mean": round(float(fill.mean()), 4) if len(fill) else None,
                          "p05": round(float(np.quantile(fill, 0.05)), 4) if len(fill) else None,
                          "median": round(float(np.median(fill)), 4) if len(fill) else None}  # fmt: skip
    active = present[traded].mean(axis=0) if traded.any() else np.zeros(m)
    gaps = [int(bars.minutes[j]) for j in range(m) if active[j] < GAP_SHARE]
    start = int(datetime.strptime(f"{session_date} 09:15", "%Y-%m-%d %H:%M").replace(tzinfo=IST).timestamp())
    expected = set(range(start, start + SESSION_MINUTES * 60, 60))
    absent = sorted(expected - {int(x) for x in bars.minutes})  # minutes with no bar for any stock
    report["missing_intervals"] = _ranges(sorted(set(gaps) | set(absent)))
    report["minutes_with_any_bar"] = int(m)
    if m:
        report["first_bar"] = datetime.fromtimestamp(int(bars.minutes[0]), IST).strftime("%H:%M")
        report["last_bar"] = datetime.fromtimestamp(int(bars.minutes[-1]), IST).strftime("%H:%M")
        report["timestamp_consistent"] = report["first_bar"] == "09:15" and report["last_bar"] == "15:29"
    vol = bars.volume
    with np.errstate(invalid="ignore"):
        zero = present & (vol == 0)
        moved = zero & (bars.high > bars.low)
    report["zero_volume_bars"] = int(zero.sum())
    report["price_move_on_zero_volume"] = int(moved.sum())
    report["negative_or_missing_volume"] = int((present & ~(vol >= 0)).sum())
    report["indices"] = sorted(day.indices.keys) if day.indices is not None else []
    report["missing_expected_indices"] = [k for k in EXPECTED_INDICES if k not in report["indices"]]
    prev = sum(1 for v in day.reference.values() if v.get("previous_close"))
    report["previous_close_coverage"] = round(prev / max(report["equities"], 1), 4)
    report["source"] = day.manifest.get("source", "PSYGRID_LIVE_ARCHIVE")
    return report


def audit(root: Path, log=lambda m: None) -> dict:
    universe = _universe()
    qualified = set(session_days(root))
    days = available_days(root)
    rows = []
    for d in days:
        rows.append(audit_day(root, d, universe, d in qualified))
        log(f"audited {d}")
    good = [r for r in rows if r.get("integrity_ok")]
    q = [r for r in rows if r["qualified_session"]]

    def total(name):
        return int(sum(r.get(name) or 0 for r in rows))

    coverage = [r["equity_coverage"] for r in q if r.get("equity_coverage") is not None]
    return {
        "archive": str(root),
        "sessions_archived": len(rows),
        "sessions_integrity_ok": len(good),
        "sessions_failed": {r["session_date"]: r["problems"] for r in rows if not r.get("integrity_ok")},
        "qualified_sessions": len(q),
        "partial_or_unqualified": [r["session_date"] for r in rows if not r["qualified_session"]],
        "first_session": rows[0]["session_date"] if rows else None,
        "last_session": rows[-1]["session_date"] if rows else None,
        "universe": len(universe) if universe else None,
        "equity_coverage": {
            "min": min(coverage, default=None),
            "median": float(np.median(coverage)) if coverage else None,
            "max": max(coverage, default=None),
        },
        "sessions_with_missing_intervals": [r["session_date"] for r in q if r.get("missing_intervals")],
        "sessions_missing_indices": {
            r["session_date"]: r["missing_expected_indices"] for r in q if r.get("missing_expected_indices")
        },
        "timestamp_inconsistent_sessions": [r["session_date"] for r in q if r.get("timestamp_consistent") is False],
        "totals": {
            name: total(name)
            for name in (
                "rows",
                "duplicates",
                "bad_timestamp",
                "wrong_day",
                "unaligned",
                "outside_session",
                "invalid_ohlc",
                "missing_value",
                "zero_volume_bars",
                "price_move_on_zero_volume",
                "negative_or_missing_volume",
            )
        },
        "days": rows,
    }
