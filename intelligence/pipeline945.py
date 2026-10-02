"""The 945 pipeline: build the 09:45 information set, decide, store; and reconstruct history the same way.

``inputs_at_0945`` is the only place the decision's inputs are assembled. Live and historical replay both
call it, so a live decision and a replayed decision of the same data are the same decision (a test proves
it). Each input is restricted to what was known at 09:45:00 on the day:

- bars: ``frame_at(day, 09:45)``, the bars that closed by then;
- volume and volatility norms: day summaries of qualified sessions before the day;
- expected response: the model trained on the 20 qualified sessions before the day;
- market state: measured from today's bars by 09:45 and placed against earlier sessions;
- microstructure: recorder minutes that closed by 09:45; derivatives: chain snapshots taken by 09:45;
- training records: sessions before the day only (``DecisionStore.load_training(before=day)``).

``reconstruct`` walks the archive in date order. For each session it freezes at 09:45, decides, stores the
immutable decision, and only then evaluates the outcome and adds that day's training record for later days.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from pathlib import Path

import numpy as np

from intelligence.archive import load_day, session_days
from intelligence.expectation import expectation
from intelligence.frame import as_of_time, frame_at
from intelligence.history import DEFAULT_WINDOW_SESSIONS, load_summary
from intelligence.market_state import measure, session_profile, with_history
from intelligence.matrix import StateMatrix, build_matrix, history_from_summaries
from intelligence.microstructure_engine import index_rows, micro_state
from intelligence.response import align, compact, evaluate_state, fit, model_for, session_returns, universe_keys
from intelligence.selector945 import (
    DECISION_TIME,
    Decision,
    DecisionStore,
    decide,
    decision_outcome,
    training_day,
)
from intelligence.stream import micro_rows

log = logging.getLogger("psygrid.intelligence.945")
TRAIN_SESSIONS = 20


def configured_universe() -> tuple[str, ...] | None:
    """The 989 symbols PSYGRID subscribes to (``stocks.json``), or None if it cannot be read."""
    try:
        from config import _load_symbol_universe

        return tuple(_load_symbol_universe())
    except Exception:
        return None


def universe_frame(frame, universe: tuple[str, ...] | None):
    """The frame restricted to the decision universe: the configured symbols (rows of NaN where a symbol has no
    data) plus any other symbol with a bar by ``as_of``. A symbol that first trades after 09:45 must not even
    appear at 09:45: its existence is future information."""
    from dataclasses import replace

    from intelligence.archive import BAR_FIELDS, Bars

    bars = frame.equity
    has_bar = np.isfinite(bars.close).any(axis=1) if bars.close.size else np.zeros(len(bars.keys), dtype=bool)
    keys = set(universe or ()) | {k for k, h in zip(bars.keys, has_bar, strict=True) if h}
    keys = tuple(sorted(keys))
    index = {k: i for i, k in enumerate(bars.keys)}
    rows = np.array([index.get(k, -1) for k in keys], dtype=int)
    m = bars.close.shape[1]
    arrays = {}
    for name in BAR_FIELDS:
        source = bars.field(name)
        out = np.full((len(keys), m), np.nan)
        ok = rows >= 0
        out[ok] = source[rows[ok]]
        arrays[name] = out
    names = tuple(bars.names[index[k]] if k in index else k for k in keys)
    rejected = {k: v for k, v in bars.rejected.items() if k in set(keys)}
    return replace(frame, equity=Bars(keys, names, bars.minutes, rejected=rejected, **arrays))


def inputs_at_0945(archive_root: Path, store_root: Path, day, response_model=None,
                   earlier: list[str] | None = None) -> StateMatrix:  # fmt: skip
    """The 989-stock state matrix at 09:45:00 on ``day``, from information available then only."""
    session = day.session_date
    at = as_of_time(session, DECISION_TIME)
    frame = universe_frame(frame_at(day, at), configured_universe())
    earlier = earlier if earlier is not None else [d for d in session_days(archive_root) if d < session]
    summaries = [load_summary(archive_root, d, store_root) for d in earlier[-DEFAULT_WINDOW_SESSIONS:]]
    history = history_from_summaries(summaries)
    model = response_model if response_model is not None else model_for(archive_root, session, cache_root=store_root)
    response = None
    if model is not None:
        sr = align(session_returns(day, at), model.keys)
        if sr.r.shape[1]:
            response = evaluate_state(model, sr)
    try:
        market = with_history(measure(day, at), archive_root, store_root).view()
    except Exception as exc:  # the decision proceeds without market context; the matrix records the gap
        log.warning("945 market state failed: %s", exc)
        market = None
    micro = {}
    rows = micro_rows(archive_root, session)
    if rows:
        epoch = int(at.timestamp())
        micro = {key: micro_state(series, key, epoch).view() for key, series in index_rows(rows).items()}
    nifty = expectation(archive_root, store_root, session, "nifty", at).view()
    matrix = build_matrix(frame, history, response, market, micro, nifty)
    if market:
        matrix.context["market_state_detail"] = {
            k: market.get(k) for k in ("state", "regime", "persistence", "mean_correlation", "dispersion_bps")
        }
    return matrix


def decide_day(archive_root: Path, store_root: Path, day, store: DecisionStore, response_model=None,
               earlier: list[str] | None = None, computed_at: str | None = None) -> tuple[Decision, StateMatrix]:  # fmt: skip
    matrix = inputs_at_0945(archive_root, store_root, day, response_model, earlier)
    training = store.load_training(before=day.session_date)
    decision = decide(matrix, training, {"computed_at": computed_at})
    store.save_decision(decision)
    return decision, matrix


def finish_day(store: DecisionStore, decision: Decision, matrix: StateMatrix, day) -> dict:
    """After the session: the decision's outcome and the day's training record (for later days only)."""
    outcome = decision_outcome(decision.payload, day)
    store.save_outcome(day.session_date, outcome)
    store.save_training(training_day(matrix, day))
    return outcome


def reconstruct(archive_root: Path, store_root: Path, namespace: str = "replay", first: str | None = None,
                last: str | None = None, log_fn: Callable[[str], None] = lambda m: None,
                warm_sessions: int = 5) -> dict:  # fmt: skip
    """Historical 09:45 reconstruction over every qualified session (in date order), then a summary.

    Sessions without ``warm_sessions`` earlier sessions are used only to build history (summaries, market-state
    profiles, training records), never decided: there is nothing to judge them against.
    """
    store = DecisionStore(store_root, namespace)
    days = session_days(archive_root)
    loaded: dict[str, object] = {}  # rolling compact session returns for the response model
    timings = []
    for position, session in enumerate(days):
        if last and session > last:
            break
        started = time.perf_counter()
        day = load_day(archive_root, session)
        earlier = days[:position]
        decide_it = len(earlier) >= warm_sessions and (first is None or session >= first)
        if decide_it:
            training_sessions = earlier[-TRAIN_SESSIONS:]
            for old in [d for d in loaded if d not in training_sessions]:
                del loaded[old]
            for d in training_sessions:
                if d not in loaded:
                    loaded[d] = compact(session_returns(load_day(archive_root, d)))
            sessions = [loaded[d] for d in training_sessions]
            model = fit(sessions, universe_keys(sessions)) if len(sessions) >= 5 else None
            existing = store.load_decision(session)
            decision, matrix = decide_day(archive_root, store_root, day, store, model, earlier,
                                          computed_at=existing["computed_at"] if existing else "historical replay")  # fmt: skip
            outcome = finish_day(store, decision, matrix, day)
            h = outcome["horizons"]["15m"]
            log_fn(f"{session}: {decision.key} {decision.direction} model={decision.payload['model']['active']} "
                   f"+15m {h['return_pct']}% hit={h['direction_hit']} ({time.perf_counter() - started:.1f}s)")  # fmt: skip
        else:
            matrix = inputs_at_0945(archive_root, store_root, day, None, earlier)
            store.save_training(training_day(matrix, day))
            log_fn(f"{session}: history only ({len(earlier)} earlier sessions)")
        # cache this day's summary and market-state profile for later days (read from the loaded day)
        load_summary(archive_root, session, store_root, day=day)
        session_profile(archive_root, session, store_root, day=day)
        loaded[session] = compact(session_returns(day))
        timings.append(time.perf_counter() - started)
    report = summarise_decisions(store)
    report["seconds_per_session"] = round(float(np.median(timings)), 2) if timings else None
    return report


def summarise_decisions(store: DecisionStore) -> dict:
    """Out-of-sample performance of every stored decision, per horizon, with intervals and a verdict."""
    from intelligence.selector945 import COST_BPS_ROUND_TRIP, HORIZONS, wilson

    rows = []
    for session in store.decisions():
        decision, outcome = store.load_decision(session), store.load_outcome(session)
        if decision and outcome:
            rows.append((decision, outcome))
    out = {"decisions": len(rows), "cost_bps_round_trip": COST_BPS_ROUND_TRIP, "horizons": {}, "by_model": {},
           "by_market_state": {}, "probability_status": {}}  # fmt: skip
    if not rows:
        out["verdict"] = "NO_DECISIONS"
        return out
    for h in HORIZONS:
        r = np.array(
            [o["horizons"][f"{h}m"]["return_pct"] for _, o in rows if o["horizons"][f"{h}m"]["return_pct"] is not None]
        )
        net = r - COST_BPS_ROUND_TRIP / 100
        mfe = np.array(
            [o["horizons"][f"{h}m"]["mfe_pct"] for _, o in rows if o["horizons"][f"{h}m"]["mfe_pct"] is not None]
        )
        mae = np.array(
            [o["horizons"][f"{h}m"]["mae_pct"] for _, o in rows if o["horizons"][f"{h}m"]["mae_pct"] is not None]
        )
        hits = int((r > 0).sum())
        lo, hi = wilson(hits, len(r))
        se = r.std(ddof=1) / np.sqrt(len(r)) if len(r) > 1 else float("nan")
        t = float(net.mean() / se) if len(r) > 2 and se > 0 else float("nan")
        out["horizons"][f"{h}m"] = {
            "n": len(r), "hit_rate": round(hits / len(r), 4) if len(r) else None,
            "hit_rate_ci95": [round(lo, 4), round(hi, 4)],
            "mean_return_pct": _f(r.mean()), "median_return_pct": _f(np.median(r)) if len(r) else None,
            "return_sd_pct": _f(r.std(ddof=1)) if len(r) > 1 else None,
            "mean_net_return_pct": _f(net.mean()), "net_ci95_pct": [_f(net.mean() - 1.96 * se), _f(net.mean() + 1.96 * se)],
            "net_t": _f(t), "expectancy_net_pct": _f(net.mean()),
            "mean_mfe_pct": _f(mfe.mean()) if len(mfe) else None, "mean_mae_pct": _f(mae.mean()) if len(mae) else None,
            "cumulative_net_pct": _f(net.sum()),
        }  # fmt: skip
    primary = out["horizons"]["15m"]
    for label, key in (
        ("by_model", lambda d: d["model"]["active"]),
        ("by_market_state", lambda d: d["market"]["state"]),
    ):
        groups: dict[str, list[float]] = {}
        for d, o in rows:
            value = o["horizons"]["15m"]["return_pct"]
            if value is not None:
                groups.setdefault(str(key(d)), []).append(value)
        out[label] = {g: {"n": len(v), "hit_rate": round(float(np.mean(np.array(v) > 0)), 4),
                          "mean_net_return_pct": _f(np.mean(v) - COST_BPS_ROUND_TRIP / 100)} for g, v in groups.items()}  # fmt: skip
    for d, _ in rows:
        status = d["probability"]["status"]
        out["probability_status"][status] = out["probability_status"].get(status, 0) + 1
    lo_net = primary["net_ci95_pct"][0]
    if primary["n"] < 30:
        out["verdict"] = "UNPROVEN"
        out["verdict_reason"] = f"only {primary['n']} out-of-sample decisions; at least 30 are needed to judge"
    elif lo_net is not None and lo_net > 0:
        out["verdict"] = "SUPPORTED"
        out["verdict_reason"] = "mean net 15-minute return is positive with its 95% interval above zero"
    elif primary["net_ci95_pct"][1] is not None and primary["net_ci95_pct"][1] < 0:
        out["verdict"] = "REJECTED"
        out["verdict_reason"] = "mean net 15-minute return is negative with its 95% interval below zero"
    else:
        out["verdict"] = "UNPROVEN"
        out["verdict_reason"] = "the 95% interval of the mean net 15-minute return includes zero"
    out["first_session"], out["last_session"] = rows[0][0]["session_date"], rows[-1][0]["session_date"]
    return out


def _f(x) -> float | None:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return round(x, 4) if np.isfinite(x) else None
