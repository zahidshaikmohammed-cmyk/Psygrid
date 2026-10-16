"""Support and resistance levels: major (daily/weekly/monthly) and intraday, from Dhan's own bars.

Every level is a fixed, published formula, so the numbers are reproducible and checkable. They are
reference points, not trading signals.

Major levels come from completed daily bars before today:
  * previous day high / low / close, classic daily floor pivots (PP, R1-R3, S1-S3) and the CPR
    (central pivot range: BC = (H+L)/2, TC = 2*PP - BC);
  * previous week high / low / close and weekly pivots (PP, R1-R2, S1-S2);
  * previous month high / low / close;
  * the 20-session high / low;
  * daily swing highs / lows over the last 60 sessions (a bar whose high/low is the extreme of the
    two bars either side), merged when within 0.5% and ranked by how often price returned to them.

Intraday levels come from today's completed 1-minute candles and update every minute:
  * today's open, high and low; this week's high / low including today;
  * the opening range (first 15 minutes, 09:15-09:29), marked provisional until 09:30;
  * VWAP (only when the instrument reports traded volume; indices do not);
  * 5-minute swing highs / lows (two 5-minute bars either side), merged when within 0.1% and
    ranked by touches (5-minute bars whose high/low came within that band).

Index option levels (when an option chain is available): the three strikes with the most call OI
(resistance), the three with the most put OI (support) and the max-pain strike.

``supports`` are levels at or below the last price (nearest first), ``resistances`` above it.
``confluence`` counts the other levels within 0.15% of a level: several independent formulas at
the same price.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime
from zoneinfo import ZoneInfo

DAILY_LOOKBACK_CALENDAR_DAYS = 100
DAILY_SWING_SESSIONS = 60
DAILY_SWING_SIDE = 2
DAILY_SWING_MERGE_PCT = 0.5
DAILY_SWING_KEEP = 3
INTRADAY_BAR_MINUTES = 5
INTRADAY_SWING_SIDE = 2
INTRADAY_SWING_MERGE_PCT = 0.1
INTRADAY_SWING_KEEP = 4
OPENING_RANGE_MINUTES = 15
CONFLUENCE_PCT = 0.15
RANGE_SESSIONS = 20

# Candle tuple layout used throughout: (epoch_seconds, open, high, low, close, volume).
Candle = tuple[int, float, float, float, float, float]


def _r(value: float | None) -> float | None:
    return None if value is None else round(float(value), 2)


def _floor_pivots(high: float, low: float, close: float, depth: int = 3) -> dict[str, float]:
    pp = (high + low + close) / 3
    out = {"PP": pp, "R1": 2 * pp - low, "S1": 2 * pp - high, "R2": pp + (high - low), "S2": pp - (high - low)}
    if depth >= 3:
        out["R3"] = high + 2 * (pp - low)
        out["S3"] = low - 2 * (high - pp)
    return out


def _swings(
    highs: Sequence[float], lows: Sequence[float], side: int
) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    """Confirmed fractal swing highs and lows: the extreme of ``side`` bars on either side."""
    tops, bottoms = [], []
    for i in range(side, len(highs) - side):
        window_h = highs[i - side : i + side + 1]
        window_l = lows[i - side : i + side + 1]
        if highs[i] == max(window_h) and window_h.count(highs[i]) == 1:
            tops.append((i, highs[i]))
        if lows[i] == min(window_l) and window_l.count(lows[i]) == 1:
            bottoms.append((i, lows[i]))
    return tops, bottoms


def _cluster(points: list[tuple[int, float]], merge_pct: float) -> list[dict]:
    """Merge swing points within ``merge_pct`` of each other: price = mean, last = latest bar index."""
    clusters: list[dict] = []
    for index, price in sorted(points, key=lambda p: p[1]):
        if clusters and abs(price - clusters[-1]["price"]) <= clusters[-1]["price"] * merge_pct / 100:
            c = clusters[-1]
            c["sum"] += price
            c["n"] += 1
            c["price"] = c["sum"] / c["n"]
            c["last"] = max(c["last"], index)
        else:
            clusters.append({"price": price, "sum": price, "n": 1, "last": index})
    return clusters


def _ranked_swings(
    highs: Sequence[float], lows: Sequence[float], side: int, merge_pct: float, keep: int
) -> tuple[list[dict], list[dict]]:
    """Swing-high and swing-low clusters ranked by touches (bars reaching the band), then recency."""
    tops, bottoms = _swings(highs, lows, side)
    ranked = []
    for points, series in ((tops, highs), (bottoms, lows)):
        clusters = _cluster(points, merge_pct)
        for c in clusters:
            band = c["price"] * merge_pct / 100
            c["touches"] = sum(1 for value in series if abs(value - c["price"]) <= band)
        clusters.sort(key=lambda c: (c["touches"], c["last"]), reverse=True)
        ranked.append([{"price": c["price"], "touches": c["touches"], "last_bar": c["last"]} for c in clusters[:keep]])
    return ranked[0], ranked[1]


def daily_rows_before(rows: Iterable[dict], today: date, tz: ZoneInfo) -> list[dict]:
    """Completed daily bars strictly before ``today``, oldest first, as plain dicts with a ``date``."""
    out = []
    for row in rows:
        try:
            day = datetime.fromtimestamp(int(row["timestamp"]), tz).date()
            bar = {k: float(row[k]) for k in ("open", "high", "low", "close")}
        except (KeyError, TypeError, ValueError):
            continue
        if day < today:
            out.append({"date": day, **bar})
    out.sort(key=lambda r: r["date"])
    dedup: dict[date, dict] = {r["date"]: r for r in out}
    return list(dedup.values())


def major_levels(daily: list[dict], today: date) -> dict:
    """Major levels from completed daily bars (``daily_rows_before``). Cheap; computed once a day."""
    if not daily:
        return {"levels": [], "reference": {}, "week_prior_days": None}
    levels: list[dict] = []
    ref: dict[str, float | str | None] = {}

    def add(name: str, price: float | None, **extra) -> None:
        if price is not None:
            levels.append({"name": name, "price": price, "scope": "MAJOR", **extra})

    prev = daily[-1]
    ref.update(PDH=prev["high"], PDL=prev["low"], PDC=prev["close"], previous_session=prev["date"].isoformat())
    add("PDH", prev["high"])
    add("PDL", prev["low"])
    add("PDC", prev["close"])
    for name, price in _floor_pivots(prev["high"], prev["low"], prev["close"]).items():
        add(f"DAILY_{name}", price)
        ref[f"DAILY_{name}"] = price
    pp = (prev["high"] + prev["low"] + prev["close"]) / 3
    bc = (prev["high"] + prev["low"]) / 2
    tc = 2 * pp - bc
    ref.update(CPR_TC=max(tc, bc), CPR_BC=min(tc, bc))
    add("CPR_TC", max(tc, bc))
    add("CPR_BC", min(tc, bc))

    this_week = today.isocalendar()[:2]
    week_prior = [r for r in daily if r["date"].isocalendar()[:2] == this_week]
    earlier = [r for r in daily if r["date"].isocalendar()[:2] < this_week]
    if earlier:
        last_week = earlier[-1]["date"].isocalendar()[:2]
        wk = [r for r in earlier if r["date"].isocalendar()[:2] == last_week]
        wh, wl, wc = max(r["high"] for r in wk), min(r["low"] for r in wk), wk[-1]["close"]
        ref.update(PWH=wh, PWL=wl, PWC=wc)
        add("PWH", wh)
        add("PWL", wl)
        add("PWC", wc)
        for name, price in _floor_pivots(wh, wl, wc, depth=2).items():
            add(f"WEEKLY_{name}", price)
            ref[f"WEEKLY_{name}"] = price

    this_month = (today.year, today.month)
    prior_months = [r for r in daily if (r["date"].year, r["date"].month) < this_month]
    if prior_months:
        last_month = (prior_months[-1]["date"].year, prior_months[-1]["date"].month)
        mo = [r for r in prior_months if (r["date"].year, r["date"].month) == last_month]
        mh, ml, mc = max(r["high"] for r in mo), min(r["low"] for r in mo), mo[-1]["close"]
        ref.update(PMH=mh, PML=ml, PMC=mc)
        add("PMH", mh)
        add("PML", ml)
        add("PMC", mc)

    recent = daily[-RANGE_SESSIONS:]
    ref.update(HIGH_20D=max(r["high"] for r in recent), LOW_20D=min(r["low"] for r in recent))
    add("HIGH_20D", ref["HIGH_20D"])
    add("LOW_20D", ref["LOW_20D"])

    window = daily[-DAILY_SWING_SESSIONS:]
    tops, bottoms = _ranked_swings(
        [r["high"] for r in window],
        [r["low"] for r in window],
        DAILY_SWING_SIDE,
        DAILY_SWING_MERGE_PCT,
        DAILY_SWING_KEEP,
    )
    for kind, found in (("DAILY_SWING_HIGH", tops), ("DAILY_SWING_LOW", bottoms)):
        for s in found:
            add(kind, s["price"], touches=s["touches"], last_seen=window[s["last_bar"]]["date"].isoformat())

    week_hl = (max(r["high"] for r in week_prior), min(r["low"] for r in week_prior)) if week_prior else None
    return {"levels": levels, "reference": ref, "week_prior_days": week_hl}


def _bars(candles: Sequence[Candle], minutes: int) -> list[list[float]]:
    """Aggregate 1-minute candles into ``minutes`` bars aligned to the 09:15 open: [o, h, l, c]."""
    out: dict[int, list[float]] = {}
    for epoch, o, h, low, c, _v in candles:
        key = (int(epoch) - 9 * 3600 - 15 * 60 + 19800) // (minutes * 60)  # IST-aligned bucket
        bar = out.get(key)
        if bar is None:
            out[key] = [o, h, low, c]
        else:
            bar[1] = max(bar[1], h)
            bar[2] = min(bar[2], low)
            bar[3] = c
    return [out[k] for k in sorted(out)]


def intraday_levels(candles: Sequence[Candle], week_prior_days: tuple[float, float] | None, tz: ZoneInfo) -> dict:
    """Intraday levels from today's completed 1-minute candles (epoch-sorted). Recomputed each minute."""
    if not candles:
        return {"levels": [], "reference": {}}
    levels: list[dict] = []
    ref: dict[str, float | bool | None] = {}

    def add(name: str, price: float | None, **extra) -> None:
        if price is not None:
            levels.append({"name": name, "price": price, "scope": "INTRADAY", **extra})

    day_high = max(c[2] for c in candles)
    day_low = min(c[3] for c in candles)
    first = datetime.fromtimestamp(candles[0][0], tz)
    day_open = candles[0][1] if (first.hour, first.minute) == (9, 15) else None
    ref.update(DAY_OPEN=day_open, DAY_HIGH=day_high, DAY_LOW=day_low)
    add("DAY_HIGH", day_high)
    add("DAY_LOW", day_low)
    add("DAY_OPEN", day_open)
    if week_prior_days is not None:
        ref.update(WEEK_HIGH=max(week_prior_days[0], day_high), WEEK_LOW=min(week_prior_days[1], day_low))
    else:
        ref.update(WEEK_HIGH=day_high, WEEK_LOW=day_low)  # first session of the week
    add("WEEK_HIGH", ref["WEEK_HIGH"])
    add("WEEK_LOW", ref["WEEK_LOW"])

    opening = []
    for c in candles:
        t = datetime.fromtimestamp(c[0], tz)
        minutes = (t.hour * 60 + t.minute) - (9 * 60 + 15)
        if 0 <= minutes < OPENING_RANGE_MINUTES:
            opening.append(c)
    if opening:
        provisional = len(opening) < OPENING_RANGE_MINUTES
        ref.update(ORH=max(c[2] for c in opening), ORL=min(c[3] for c in opening), OR_PROVISIONAL=provisional)
        add("ORH", ref["ORH"], provisional=provisional)
        add("ORL", ref["ORL"], provisional=provisional)

    volume = sum(c[5] for c in candles)
    if volume > 0:
        vwap = sum((c[2] + c[3] + c[4]) / 3 * c[5] for c in candles) / volume
        ref["VWAP"] = vwap
        add("VWAP", vwap)

    bars = _bars(candles, INTRADAY_BAR_MINUTES)
    tops, bottoms = _ranked_swings(
        [b[1] for b in bars], [b[2] for b in bars], INTRADAY_SWING_SIDE, INTRADAY_SWING_MERGE_PCT, INTRADAY_SWING_KEEP
    )
    for kind, found in (("INTRADAY_SWING_HIGH", tops), ("INTRADAY_SWING_LOW", bottoms)):
        for s in found:
            add(kind, s["price"], touches=s["touches"])
    return {"levels": levels, "reference": ref}


def option_levels(analytics: dict | None) -> list[dict]:
    if not analytics:
        return []
    out = []
    for rank, strike in enumerate(analytics.get("resistance_strikes") or [], 1):
        out.append({"name": f"CALL_OI_WALL_{rank}", "price": float(strike), "scope": "OPTIONS"})
    for rank, strike in enumerate(analytics.get("support_strikes") or [], 1):
        out.append({"name": f"PUT_OI_WALL_{rank}", "price": float(strike), "scope": "OPTIONS"})
    if analytics.get("max_pain_strike") is not None:
        out.append({"name": "MAX_PAIN", "price": float(analytics["max_pain_strike"]), "scope": "OPTIONS"})
    return out


def _nearest(levels: list[dict], scope: str | None) -> dict | None:
    pool = [lv for lv in levels if scope is None or lv["scope"] == scope]
    return pool[0] if pool else None


def build_levels(
    symbol: str,
    ltp: float | None,
    major: dict | None,
    intraday: dict | None,
    options: list[dict] | None = None,
    *,
    as_of: str | None = None,
) -> dict:
    """One instrument's levels payload: supports below / resistances above ``ltp``, nearest first."""
    major = major or {"levels": [], "reference": {}}
    intraday = intraday or {"levels": [], "reference": {}}
    every = [dict(lv) for lv in (*major["levels"], *intraday["levels"], *(options or []))]
    for lv in every:
        lv["price"] = float(lv["price"])
    for lv in every:
        band = lv["price"] * CONFLUENCE_PCT / 100
        lv["confluence"] = sum(1 for other in every if other is not lv and abs(other["price"] - lv["price"]) <= band)
    if ltp:
        for lv in every:
            lv["distance_points"] = _r(lv["price"] - ltp)
            lv["distance_pct"] = round((lv["price"] - ltp) / ltp * 100, 3)
    for lv in every:
        lv["price"] = _r(lv["price"])
    supports = sorted((lv for lv in every if ltp and lv["price"] <= ltp), key=lambda lv: -lv["price"])
    resistances = sorted((lv for lv in every if ltp and lv["price"] > ltp), key=lambda lv: lv["price"])
    reference = {
        k: (_r(v) if isinstance(v, float) else v) for k, v in {**major["reference"], **intraday["reference"]}.items()
    }
    return {
        "symbol": symbol,
        "ltp": _r(ltp),
        "as_of": as_of,
        "nearest": {
            "support": _nearest(supports, None),
            "resistance": _nearest(resistances, None),
            "intraday_support": _nearest(supports, "INTRADAY"),
            "intraday_resistance": _nearest(resistances, "INTRADAY"),
            "major_support": _nearest(supports, "MAJOR"),
            "major_resistance": _nearest(resistances, "MAJOR"),
        },
        "supports": supports,
        "resistances": resistances,
        "reference": reference,
        "method": "levels.py: fixed formulas on Dhan daily and 1-minute bars; reference points, not signals",
    }
