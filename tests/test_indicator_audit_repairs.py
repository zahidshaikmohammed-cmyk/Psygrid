from datetime import datetime, timedelta
import math

import pytest

from psygrid_master_indicator import PsygridMasterIndicatorEngine, IndicatorConfig


def _assert_close(a, b, tol=1e-10):
    assert a is not None and b is not None
    assert math.isclose(float(a), float(b), rel_tol=tol, abs_tol=tol)


def _rows(count=45, start_price=100.0):
    start = datetime(2026, 1, 1, 9, 15)
    rows = []
    for i in range(count):
        close = start_price + i * 0.2 + (0.5 if i % 3 == 0 else -0.1)
        prev_close = rows[-1]["close"] if rows else close
        rows.append({
            "timestamp": (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S IST"),
            "open": prev_close,
            "high": max(prev_close, close) + 0.5,
            "low": min(prev_close, close) - 0.5,
            "close": close,
            "volume": 1000 + i,
            "complete": True,
        })
    return rows


def test_wilder_rma_and_adx_golden_reference():
    rows = _rows(45)
    engine = PsygridMasterIndicatorEngine(IndicatorConfig(min_history=14, include_series=True))
    out = engine.compute_stock(
        {"symbol": "GOLDEN", "security_id": "1", "candles_1m": rows},
        rows[-1]["timestamp"],
    )
    adx = out["indicators"]["adx_14"]
    plus = out["indicators"]["plus_di_14"]
    minus = out["indicators"]["minus_di_14"]
    assert adx is not None
    assert plus is not None
    assert minus is not None

    trs, p_dm, m_dm = [], [], []
    for i in range(1, len(rows)):
        h, l = rows[i]["high"], rows[i]["low"]
        ph, pl, pc = rows[i - 1]["high"], rows[i - 1]["low"], rows[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up = h - ph
        down = pl - l
        p_dm.append(up if up > down and up > 0 else 0.0)
        m_dm.append(down if down > up and down > 0 else 0.0)

    n = 14

    def rma(values):
        seed = sum(values[:n]) / n
        out = [None] * (n - 1) + [seed]
        for x in values[n:]:
            out.append((out[-1] * (n - 1) + x) / n)
        return out

    atr = rma(trs)
    ps = rma(p_dm)
    ms = rma(m_dm)
    dx = []
    for a, pp, mm in zip(atr, ps, ms):
        if a and a > 0:
            pdi = 100 * pp / a
            mdi = 100 * mm / a
            den = pdi + mdi
            dx.append(100 * abs(pdi - mdi) / den if den else 0.0)
        else:
            dx.append(None)

    valid = [x for x in dx if x is not None]
    assert len(valid) >= n
    expected = sum(valid[:n]) / n
    for x in valid[n:]:
        expected = (expected * (n - 1) + x) / n
    _assert_close(adx, expected, 1e-9)


def test_cmf_handles_flat_bars_and_zero_volume_without_fabrication():
    rows = _rows(25)
    for row in rows:
        row["open"] = row["close"] = 10.0
        row["high"] = row["low"] = 10.0
        row["volume"] = 100.0
    rows[20]["volume"] = 0.0

    engine = PsygridMasterIndicatorEngine(IndicatorConfig(min_history=20, include_series=True))
    out = engine.compute_stock(
        {"symbol": "CMF", "security_id": "2", "candles_1m": rows},
        rows[-1]["timestamp"],
    )
    cmf = out["indicators"]["cmf_20"]
    assert cmf is not None
    assert math.isclose(cmf, 0.0, abs_tol=1e-12)


def test_invalid_numeric_observation_is_rejected_not_fabricated():
    rows = _rows(25)
    rows[20]["close"] = None
    engine = PsygridMasterIndicatorEngine(IndicatorConfig(min_history=20, include_series=True))
    with pytest.raises(ValueError, match="close is not numeric"):
        engine.compute_stock(
            {"symbol": "INVALID", "security_id": "3", "candles_1m": rows},
            rows[-1]["timestamp"],
        )


def test_future_candle_does_not_change_prior_indicator_values():
    rows = _rows(45)
    engine = PsygridMasterIndicatorEngine(IndicatorConfig(min_history=20, include_series=True))
    base = engine.compute_stock(
        {"symbol": "NL", "security_id": "4", "candles_1m": rows},
        rows[-1]["timestamp"],
    )
    altered = list(rows)
    altered[-1] = dict(altered[-1], high=9999.0, low=1.0, close=5000.0, volume=999999.0)
    changed = engine.compute_stock(
        {"symbol": "NL", "security_id": "4", "candles_1m": altered},
        rows[-1]["timestamp"],
    )

    for name, series in base.get("series", {}).items():
        other = changed.get("series", {}).get(name, [])
        assert len(other) == len(series)
        for i in range(len(series) - 1):
            a = series[i].get("value") if isinstance(series[i], dict) else series[i]
            b = other[i].get("value") if isinstance(other[i], dict) else other[i]
            if a is None or b is None:
                assert a is b
            else:
                _assert_close(a, b, 1e-12)
