"""
PSYGRID MASTER INDICATOR ENGINE
Version: 1.0.0
Purpose:
    Convert the PSYGRID 1-minute OHLCV universe endpoint into a synchronized,
    non-synthetic, freshness-aware indicator snapshot for every stock.

Design rules:
    1. The endpoint is the source of truth for OHLCV.
    2. No missing 1-minute candles are fabricated.
    3. Indicators are calculated only from observed candles.
    4. A stock can be marked STALE; stale indicator values are not emitted as
       tradable values.
    5. Every stock is evaluated independently.
    6. Session VWAP is reset at the session boundary.
    7. Indicators requiring more history than is available are NOT guessed.
    8. The output is as-of the latest observed candle for each symbol.
    9. The engine never forward-fills OHLCV or indicator values.
   10. Data validation is strict and failures are explicit.

Dependencies: numpy, pandas (standard scientific Python stack).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import math

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class IndicatorConfig:
    ema_fast: int = 9
    ema_slow: int = 20
    sma: int = 20
    rsi: int = 14
    roc: int = 12
    momentum: int = 10
    cci: int = 20
    stochastic_k: int = 14
    stochastic_smooth: int = 3
    stochastic_d: int = 3
    mfi: int = 14
    atr: int = 14
    bollinger: int = 20
    bollinger_std: float = 2.0
    keltner_ema: int = 20
    keltner_atr: int = 14
    keltner_mult: float = 2.0
    supertrend_atr: int = 10
    supertrend_mult: float = 3.0
    adx: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    rvol: int = 20
    cmf: int = 20
    donchian: int = 20
    stale_after_seconds: int = 120
    include_series: bool = False


REQUIRED_CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def _parse_ist_timestamp(value: str) -> pd.Timestamp:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    value = value.strip()
    if value.endswith(" IST"):
        value = value[:-4]
    return pd.Timestamp(value).tz_localize("Asia/Kolkata")


def _normalise_candles(candles: list[dict]) -> pd.DataFrame:
    if not isinstance(candles, list):
        raise ValueError("candles_1m must be a list")
    rows = []
    for c in candles:
        if not isinstance(c, dict):
            raise ValueError("every candle must be an object")
        missing = [k for k in REQUIRED_CANDLE_COLUMNS if k not in c]
        if missing:
            raise ValueError(f"candle missing fields: {missing}")
        ts = _parse_ist_timestamp(c["timestamp"])
        vals = []
        for k in ["open", "high", "low", "close", "volume"]:
            v = c[k]
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError(f"{k} is not numeric at {c['timestamp']}")
            if not np.isfinite(v):
                raise ValueError(f"{k} is non-finite at {c['timestamp']}")
            vals.append(float(v))
        o, h, l, cl, vol = vals
        if h < max(o, cl) or l > min(o, cl) or h < l:
            raise ValueError(f"invalid OHLC relationship at {c['timestamp']}")
        if vol < 0:
            raise ValueError(f"negative volume at {c['timestamp']}")
        rows.append([ts, o, h, l, cl, vol])
    if not rows:
        return pd.DataFrame(columns=REQUIRED_CANDLE_COLUMNS).set_index(
            pd.DatetimeIndex([], name="timestamp")
        )
    df = pd.DataFrame(rows, columns=REQUIRED_CANDLE_COLUMNS)
    if df["timestamp"].duplicated().any():
        raise ValueError("duplicate 1-minute timestamps detected")
    if not df["timestamp"].is_monotonic_increasing:
        raise ValueError("1-minute candles are not chronological")
    return df.set_index("timestamp")


def _last(series: pd.Series) -> Optional[float]:
    if len(series) == 0 or pd.isna(series.iloc[-1]):
        return None
    return float(series.iloc[-1])


def _pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return float((a / b - 1.0) * 100.0)


def _clean_dict(obj):
    if isinstance(obj, dict):
        return {k: _clean_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_dict(v) for v in obj]
    if isinstance(obj, (np.floating, np.integer)):
        if isinstance(obj, np.floating) and not np.isfinite(obj):
            return None
        return obj.item()
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    if pd.isna(obj) if not isinstance(obj, (dict, list, tuple, str, int, bool, type(None))) else False:
        return None
    return obj


def sma(s: pd.Series, p: int) -> pd.Series:
    return s.rolling(p, min_periods=p).mean()


def ema_sma_seed(s: pd.Series, p: int) -> pd.Series:
    x = s.astype(float).to_numpy()
    out = np.full(len(x), np.nan, dtype=float)
    if len(x) < p:
        return pd.Series(out, index=s.index)
    first = x[:p]
    if np.any(~np.isfinite(first)):
        return pd.Series(out, index=s.index)
    alpha = 2.0 / (p + 1.0)
    out[p - 1] = float(np.mean(first))
    for i in range(p, len(x)):
        if not np.isfinite(x[i]):
            out[i] = np.nan
        elif np.isfinite(out[i - 1]):
            out[i] = alpha * x[i] + (1.0 - alpha) * out[i - 1]
    return pd.Series(out, index=s.index)


def wilder_rma(s: pd.Series, p: int) -> pd.Series:
    x = s.astype(float).to_numpy()
    out = np.full(len(x), np.nan, dtype=float)
    if len(x) < p:
        return pd.Series(out, index=s.index)
    first = x[:p]
    if np.any(~np.isfinite(first)):
        return pd.Series(out, index=s.index)
    out[p - 1] = float(np.mean(first))
    for i in range(p, len(x)):
        if np.isfinite(x[i]) and np.isfinite(out[i - 1]):
            out[i] = ((p - 1.0) * out[i - 1] + x[i]) / p
    return pd.Series(out, index=s.index)


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    a = df["high"] - df["low"]
    b = (df["high"] - prev_close).abs()
    c = (df["low"] - prev_close).abs()
    return pd.concat([a, b, c], axis=1).max(axis=1)


def atr_wilder(df: pd.DataFrame, p: int) -> pd.Series:
    return wilder_rma(true_range(df), p)


def rsi_wilder(close: pd.Series, p: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = wilder_rma(gain.iloc[1:], p)
    avg_loss = wilder_rma(loss.iloc[1:], p)
    avg_gain.index = gain.index[1:]
    avg_loss.index = loss.index[1:]
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.where(~((avg_loss == 0) & (avg_gain > 0)), 100.0)
    rsi = rsi.where(~((avg_loss == 0) & (avg_gain == 0)), 50.0)
    return rsi.reindex(close.index)


def macd(close: pd.Series, fast: int, slow: int, signal: int):
    ef = ema_sma_seed(close, fast)
    es = ema_sma_seed(close, slow)
    line = ef - es
    sig = ema_sma_seed(line.dropna(), signal).reindex(close.index)
    return line, sig, line - sig


def stochastic(df: pd.DataFrame, k_p: int, k_smooth: int, d_p: int):
    low_n = df["low"].rolling(k_p, min_periods=k_p).min()
    high_n = df["high"].rolling(k_p, min_periods=k_p).max()
    raw_k = 100.0 * (df["close"] - low_n) / (high_n - low_n).replace(0, np.nan)
    k = raw_k.rolling(k_smooth, min_periods=k_smooth).mean()
    d = k.rolling(d_p, min_periods=d_p).mean()
    return raw_k, k, d


def cci(df: pd.DataFrame, p: int):
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    mean = tp.rolling(p, min_periods=p).mean()
    mad = tp.rolling(p, min_periods=p).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
    )
    return (tp - mean) / (0.015 * mad.replace(0, np.nan))


def mfi(df: pd.DataFrame, p: int):
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    raw_flow = tp * df["volume"]
    direction = tp.diff()
    pos = raw_flow.where(direction > 0, 0.0)
    neg = raw_flow.where(direction < 0, 0.0)
    pos_sum = pos.rolling(p, min_periods=p).sum()
    neg_sum = neg.rolling(p, min_periods=p).sum()
    ratio = pos_sum / neg_sum.replace(0, np.nan)
    out = 100.0 - 100.0 / (1.0 + ratio)
    out = out.where(~((neg_sum == 0) & (pos_sum > 0)), 100.0)
    return out.where(~((neg_sum == 0) & (pos_sum == 0)), 50.0)


def adx_wilder(df: pd.DataFrame, p: int):
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr = true_range(df)
    atr = wilder_rma(tr, p)
    plus_sm = wilder_rma(plus_dm, p)
    minus_sm = wilder_rma(minus_dm, p)
    plus_di = 100.0 * plus_sm / atr.replace(0, np.nan)
    minus_di = 100.0 * minus_sm / atr.replace(0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return wilder_rma(dx, p), plus_di, minus_di


def cmf(df: pd.DataFrame, p: int):
    hl = df["high"] - df["low"]
    mf_multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / hl.replace(0, np.nan)
    mf_volume = mf_multiplier * df["volume"]
    return mf_volume.rolling(p, min_periods=p).sum() / df["volume"].rolling(p, min_periods=p).sum()


def obv(close: pd.Series, volume: pd.Series):
    return (np.sign(close.diff()).fillna(0.0) * volume).cumsum()


def supertrend(df: pd.DataFrame, atr_p: int, mult: float):
    atr = atr_wilder(df, atr_p)
    hl2 = (df["high"] + df["low"]) / 2.0
    basic_upper = hl2 + mult * atr
    basic_lower = hl2 - mult * atr
    fu = basic_upper.copy()
    fl = basic_lower.copy()
    direction = pd.Series(np.nan, index=df.index)
    st = pd.Series(np.nan, index=df.index)
    first_valid = atr.first_valid_index()
    if first_valid is None:
        return st, direction
    i0 = df.index.get_loc(first_valid)
    direction.iloc[i0] = 1.0
    st.iloc[i0] = fl.iloc[i0]
    for i in range(i0 + 1, len(df)):
        prev_close = df["close"].iloc[i - 1]
        if basic_upper.iloc[i] < fu.iloc[i - 1] or prev_close > fu.iloc[i - 1]:
            fu.iloc[i] = basic_upper.iloc[i]
        else:
            fu.iloc[i] = fu.iloc[i - 1]
        if basic_lower.iloc[i] > fl.iloc[i - 1] or prev_close < fl.iloc[i - 1]:
            fl.iloc[i] = basic_lower.iloc[i]
        else:
            fl.iloc[i] = fl.iloc[i - 1]
        if direction.iloc[i - 1] == -1.0:
            direction.iloc[i] = 1.0 if df["close"].iloc[i] > fu.iloc[i] else -1.0
        else:
            direction.iloc[i] = -1.0 if df["close"].iloc[i] < fl.iloc[i] else 1.0
        st.iloc[i] = fl.iloc[i] if direction.iloc[i] == 1.0 else fu.iloc[i]
    return st, direction


def calculate_indicators(df: pd.DataFrame, cfg: IndicatorConfig) -> Dict[str, pd.Series]:
    close, high, low, volume = df["close"], df["high"], df["low"], df["volume"]
    out: Dict[str, pd.Series] = {}
    out["sma_20"] = sma(close, cfg.sma)
    out["ema_9"] = ema_sma_seed(close, cfg.ema_fast)
    out["ema_20"] = ema_sma_seed(close, cfg.ema_slow)
    tp = (high + low + close) / 3.0
    out["vwap"] = (tp * volume).cumsum() / volume.cumsum().replace(0, np.nan)
    out["vwma_20"] = ((close * volume).rolling(cfg.sma, min_periods=cfg.sma).sum() / volume.rolling(cfg.sma, min_periods=cfg.sma).sum().replace(0, np.nan))
    mid = sma(close, cfg.bollinger)
    sd = close.rolling(cfg.bollinger, min_periods=cfg.bollinger).std(ddof=0)
    upper, lower = mid + cfg.bollinger_std * sd, mid - cfg.bollinger_std * sd
    out["bb_middle_20"], out["bb_upper_20"], out["bb_lower_20"] = mid, upper, lower
    out["bb_width_20"] = (upper - lower) / mid.replace(0, np.nan) * 100.0
    out["bb_percent_b_20"] = (close - lower) / (upper - lower).replace(0, np.nan)
    out["true_range"] = true_range(df)
    out["atr_14"] = atr_wilder(df, cfg.atr)
    out["natr_14"] = out["atr_14"] / close.replace(0, np.nan) * 100.0
    out["rsi_14"] = rsi_wilder(close, cfg.rsi)
    ml, ms, mh = macd(close, cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
    out["macd_line"], out["macd_signal"], out["macd_histogram"] = ml, ms, mh
    raw_k, k, d = stochastic(df, cfg.stochastic_k, cfg.stochastic_smooth, cfg.stochastic_d)
    out["stoch_raw_k"], out["stoch_k"], out["stoch_d"] = raw_k, k, d
    out["cci_20"] = cci(df, cfg.cci)
    out["roc_12"] = close.pct_change(cfg.roc) * 100.0
    out["momentum_10"] = close.diff(cfg.momentum)
    hh = high.rolling(cfg.stochastic_k, min_periods=cfg.stochastic_k).max()
    ll = low.rolling(cfg.stochastic_k, min_periods=cfg.stochastic_k).min()
    out["williams_r_14"] = -100.0 * (hh - close) / (hh - ll).replace(0, np.nan)
    adx, pdi, mdi = adx_wilder(df, cfg.adx)
    out["adx_14"], out["plus_di_14"], out["minus_di_14"] = adx, pdi, mdi
    out["obv"] = obv(close, volume)
    out["cmf_20"] = cmf(df, cfg.cmf)
    out["mfi_14"] = mfi(df, cfg.mfi)
    prior_mean_vol = volume.shift(1).rolling(cfg.rvol, min_periods=cfg.rvol).mean()
    out["rvol_20"] = volume / prior_mean_vol.replace(0, np.nan)
    out["donchian_upper_20"] = high.shift(1).rolling(cfg.donchian, min_periods=cfg.donchian).max()
    out["donchian_lower_20"] = low.shift(1).rolling(cfg.donchian, min_periods=cfg.donchian).min()
    out["donchian_middle_20"] = (out["donchian_upper_20"] + out["donchian_lower_20"]) / 2.0
    out["keltner_middle"] = ema_sma_seed(close, cfg.keltner_ema)
    k_atr = atr_wilder(df, cfg.keltner_atr)
    out["keltner_upper"] = out["keltner_middle"] + cfg.keltner_mult * k_atr
    out["keltner_lower"] = out["keltner_middle"] - cfg.keltner_mult * k_atr
    st, st_dir = supertrend(df, cfg.supertrend_atr, cfg.supertrend_mult)
    out["supertrend"], out["supertrend_direction"] = st, st_dir
    return out


def _freshness(endpoint_current_time: Optional[str], last_ts: pd.Timestamp, cfg: IndicatorConfig):
    if not endpoint_current_time:
        return {"status": "UNKNOWN", "age_seconds": None, "reason": "endpoint session.current_time_ist was not supplied"}
    now = _parse_ist_timestamp(endpoint_current_time)
    age = (now - last_ts).total_seconds()
    if age < 0:
        return {"status": "TIME_ERROR", "age_seconds": age, "reason": "latest candle is in the future relative to endpoint clock"}
    if age > cfg.stale_after_seconds:
        return {"status": "STALE", "age_seconds": age, "reason": f"latest observed candle is older than {cfg.stale_after_seconds}s"}
    return {"status": "FRESH", "age_seconds": age, "reason": None}


def _series_readiness(series: pd.Series, required: int) -> Dict[str, Any]:
    valid_count = int(series.notna().sum())
    last_valid = pd.notna(series.iloc[-1]) if len(series) else False
    return {"ready": bool(valid_count >= required and last_valid), "valid_observations": valid_count, "required_observations": required}


class PsygridMasterIndicatorEngine:
    def __init__(self, config: Optional[IndicatorConfig] = None):
        self.config = config or IndicatorConfig()

    def compute_stock(self, stock_payload: Dict[str, Any], endpoint_current_time: Optional[str]) -> Dict[str, Any]:
        symbol = stock_payload.get("symbol")
        if not symbol:
            raise ValueError("stock payload missing symbol")
        candles = stock_payload.get("candles_1m")
        if candles is None:
            raise ValueError(f"{symbol}: candles_1m missing")
        df = _normalise_candles(candles)
        result: Dict[str, Any] = {
            "symbol": symbol,
            "security_id": stock_payload.get("security_id"),
            "previous_close": stock_payload.get("previous_close"),
            "today_open": stock_payload.get("today_open"),
            "timeframe": "1m",
            "synthetic_candles": False,
            "bar_count": int(len(df)),
        }
        if df.empty:
            result.update({"status": "NO_DATA", "as_of": None, "freshness": None, "indicators": {}, "indicator_status": {}})
            return result
        last_ts = df.index[-1]
        freshness = _freshness(endpoint_current_time, last_ts, self.config)
        result["as_of"] = last_ts.isoformat()
        result["freshness"] = freshness
        series = calculate_indicators(df, self.config)
        required = {
            "sma_20": 20, "ema_9": 9, "ema_20": 20, "vwap": 1, "vwma_20": 20,
            "bb_middle_20": 20, "bb_upper_20": 20, "bb_lower_20": 20, "bb_width_20": 20,
            "bb_percent_b_20": 20, "true_range": 1, "atr_14": 14, "natr_14": 14,
            "rsi_14": 15, "macd_line": 26, "macd_signal": 34, "macd_histogram": 34,
            "stoch_raw_k": 14, "stoch_k": 16, "stoch_d": 18, "cci_20": 20, "roc_12": 13,
            "momentum_10": 11, "williams_r_14": 14, "adx_14": 27, "plus_di_14": 14,
            "minus_di_14": 14, "obv": 1, "cmf_20": 20, "mfi_14": 15, "rvol_20": 21,
            "donchian_upper_20": 21, "donchian_lower_20": 21, "donchian_middle_20": 21,
            "keltner_middle": 20, "keltner_upper": 20, "keltner_lower": 20,
            "supertrend": 10, "supertrend_direction": 10,
        }
        indicator_values: Dict[str, Any] = {}
        indicator_status: Dict[str, Any] = {}
        stale = freshness["status"] != "FRESH"
        for name, s in series.items():
            st = _series_readiness(s, required.get(name, 1))
            indicator_status[name] = st
            indicator_values[name] = None if stale or not st["ready"] else _last(s)
        last_close = float(df["close"].iloc[-1])
        result["last_price"] = last_close
        result["price_change_from_previous_close_pct"] = _pct_change(last_close, stock_payload.get("previous_close"))
        result["price_change_from_today_open_pct"] = _pct_change(last_close, stock_payload.get("today_open"))
        result["latest_bar"] = {
            "timestamp": last_ts.isoformat(), "open": float(df["open"].iloc[-1]),
            "high": float(df["high"].iloc[-1]), "low": float(df["low"].iloc[-1]),
            "close": last_close, "volume": float(df["volume"].iloc[-1]),
        }
        result["indicators"] = indicator_values
        result["indicator_status"] = indicator_status
        if self.config.include_series:
            result["series"] = {name: [None if pd.isna(v) else float(v) for v in s.tolist()] for name, s in series.items()}
        return _clean_dict(result)

    def compute_universe(self, endpoint_payload: Dict[str, Any]) -> Dict[str, Any]:
        if endpoint_payload.get("service") != "PSYGRID":
            raise ValueError("unexpected endpoint service; expected PSYGRID")
        if endpoint_payload.get("status") != "OK":
            raise ValueError(f"endpoint status is not OK: {endpoint_payload.get('status')}")
        if endpoint_payload.get("synthetic_candles") is not False:
            raise ValueError("refusing to process: endpoint synthetic_candles is not explicitly false")
        if endpoint_payload.get("data_policy") != "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN":
            raise ValueError("unexpected data_policy")
        stocks = endpoint_payload.get("stocks")
        if not isinstance(stocks, dict):
            raise ValueError("endpoint stocks must be an object")
        endpoint_now = endpoint_payload.get("session", {}).get("current_time_ist")
        results, errors = {}, {}
        for symbol, stock in stocks.items():
            try:
                results[symbol] = self.compute_stock(stock, endpoint_now)
            except Exception as exc:
                errors[symbol] = {"error_type": type(exc).__name__, "message": str(exc)}
        return _clean_dict({
            "service": "PSYGRID_MASTER_INDICATOR", "engine_version": "1.0.0",
            "source_service": endpoint_payload.get("service"),
            "source_schema_version": endpoint_payload.get("schema_version"),
            "timeframe": "1m",
            "timezone": endpoint_payload.get("session", {}).get("timezone", "Asia/Kolkata"),
            "endpoint_current_time_ist": endpoint_now,
            "source_universe_size": endpoint_payload.get("universe_size"),
            "processed_count": len(results), "error_count": len(errors),
            "fresh_count": sum(v.get("freshness", {}).get("status") == "FRESH" for v in results.values()),
            "stale_count": sum(v.get("freshness", {}).get("status") == "STALE" for v in results.values()),
            "results": results, "errors": errors,
        })


def run_psygrid(endpoint_payload: Dict[str, Any], include_series: bool = False) -> Dict[str, Any]:
    return PsygridMasterIndicatorEngine(IndicatorConfig(include_series=include_series)).compute_universe(endpoint_payload)


if __name__ == "__main__":
    import argparse
    import json
    parser = argparse.ArgumentParser(description="PSYGRID master 1-minute indicator engine")
    parser.add_argument("input_json", help="PSYGRID endpoint JSON file")
    parser.add_argument("-o", "--output", help="output JSON file", default=None)
    parser.add_argument("--series", action="store_true", help="include full indicator series")
    args = parser.parse_args()
    with open(args.input_json, "r", encoding="utf-8") as f:
        payload = json.load(f)
    output = run_psygrid(payload, include_series=args.series)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, allow_nan=False)
    else:
        print(json.dumps(output, indent=2, allow_nan=False))
