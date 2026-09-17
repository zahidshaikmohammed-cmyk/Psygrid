import math

from psygrid_master_indicator import PsygridMasterIndicatorEngine, IndicatorConfig


def _assert_close(a, b, tol=1e-10):
    assert a is not None and b is not None
    assert math.isclose(float(a), float(b), rel_tol=tol, abs_tol=tol)


def test_wilder_rma_and_adx_golden_reference():
    # Hand-checkable monotonic OHLC sequence with enough observations for ADX.
    rows = []
    closes = [10, 11, 12, 11, 13, 14, 15, 14, 16, 17, 18, 17, 19, 20, 21, 20, 22, 23, 24, 23, 25]
    for i, close in enumerate(closes):
        prev = closes[i - 1] if i else close
        rows.append({
            "timestamp": f"2026-01-01T09:{15+i:02d}:00+05:30",
            "open": prev,
            "high": max(prev, close) + 0.5,
            "low": min(prev, close) - 0.5,
            "close": close,
            "volume": 1000 + i,
            "complete": True,
        })
    engine = PsygridMasterIndicatorEngine(IndicatorConfig(min_history=14, include_series=True))
    out = engine.compute_stock({"symbol":"GOLDEN", "security_id":"1", "candles_1m":rows}, "2026-01-01T09:35:00+05:30")
    adx = out["indicators"]["adx_14"]
    plus = out["indicators"]["plus_di_14"]
    minus = out["indicators"]["minus_di_14"]
    assert adx is not None
    assert plus is not None
    assert minus is not None
    # Independent canonical Wilder reference calculation.
    trs=[]; p=[]; m=[]
    for i in range(1, len(rows)):
        h,l,c=rows[i]["high"],rows[i]["low"],rows[i]["close"]
        ph,pl,pc=rows[i-1]["high"],rows[i-1]["low"],rows[i-1]["close"]
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
        up=h-ph; dn=pl-l
        p.append(up if up>dn and up>0 else 0.0)
        m.append(dn if dn>up and dn>0 else 0.0)
    n=14
    def rma(vals):
        s=sum(vals[:n]); out=[None]*(n-1)+[s/n]
        for x in vals[n:]: out.append((out[-1]*(n-1)+x)/n)
        return out
    atr=rma(trs); ps=rma(p); ms=rma(m)
    dx=[]
    for a,pp,mm in zip(atr,ps,ms):
        if a and a>0:
            pdi=100*pp/a; mdi=100*mm/a
            den=pdi+mdi; dx.append(100*abs(pdi-mdi)/den if den else 0.0)
        else: dx.append(None)
    valid=[x for x in dx if x is not None]
    assert len(valid) >= n
    expected=sum(valid[:n])/n
    for x in valid[n:]: expected=(expected*(n-1)+x)/n
    _assert_close(adx, expected, 1e-9)


def test_cmf_handles_flat_bars_zero_volume_and_invalid_rows_without_fabrication():
    rows=[]
    for i in range(25):
        rows.append({"timestamp":f"2026-01-01T09:{15+i:02d}:00+05:30","open":10,"high":10,"low":10,"close":10,"volume":100,"complete":True})
    rows[20]["volume"] = 0
    rows[21]["close"] = None
    engine=PsygridMasterIndicatorEngine(IndicatorConfig(min_history=20, include_series=True))
    out=engine.compute_stock({"symbol":"CMF","security_id":"2","candles_1m":rows}, "2026-01-01T09:40:00+05:30")
    cmf=out["indicators"]["cmf_20"]
    assert cmf is not None
    assert math.isclose(cmf, 0.0, abs_tol=1e-12)


def test_future_candle_does_not_change_prior_indicator_values():
    rows=[]
    for i in range(40):
        c=100+i*0.1
        rows.append({"timestamp":f"2026-01-01T09:{15+i:02d}:00+05:30","open":c-0.1,"high":c+0.5,"low":c-0.5,"close":c,"volume":1000+i,"complete":True})
    engine=PsygridMasterIndicatorEngine(IndicatorConfig(min_history=20, include_series=True))
    base=engine.compute_stock({"symbol":"NL","security_id":"3","candles_1m":rows}, rows[-1]["timestamp"])
    altered=list(rows)
    altered[-1]=dict(altered[-1], high=9999, low=1, close=5000, volume=999999)
    changed=engine.compute_stock({"symbol":"NL","security_id":"3","candles_1m":altered}, rows[-1]["timestamp"])
    for name, series in base.get("series",{}).items():
        other=changed.get("series",{}).get(name,[])
        for i in range(max(0, len(series)-1)):
            a=series[i].get("value") if isinstance(series[i],dict) else series[i]
            b=other[i].get("value") if isinstance(other[i],dict) else other[i]
            if a is None or b is None: assert a is b
            else: _assert_close(a,b,1e-12)
