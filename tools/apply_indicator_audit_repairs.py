"""Apply only the audited indicator math repairs to the master engine.

This script is intentionally narrow: it replaces the existing ADX and CMF
implementations by exact function boundaries and fails closed if the expected
source shape is not present.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "psygrid_master_indicator.py"


def replace_function(text: str, name: str, replacement: str, next_name: str) -> str:
    pattern = rf"(?ms)^def {re.escape(name)}\(.*?(?=^def {re.escape(next_name)}\()"
    new_text, count = re.subn(pattern, replacement.rstrip() + "\n\n", text, count=1)
    if count != 1:
        raise RuntimeError(f"Expected exactly one {name} function before {next_name}; found {count}")
    return new_text


ADX = '''def adx_wilder(df: pd.DataFrame, p: int):
    """Canonical Wilder ADX using the first p transition observations as seeds."""
    if p <= 0:
        raise ValueError("ADX period must be positive")
    if len(df) < p + 1:
        empty = pd.Series(np.nan, index=df.index, dtype="float64")
        return empty, empty.copy(), empty.copy()

    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")

    # Wilder's directional movement is defined from candle-to-candle transitions.
    up = high.diff().iloc[1:]
    down = (-low.diff()).iloc[1:]
    plus_dm = pd.Series(
        np.where((up > down) & (up > 0.0), up, 0.0), index=up.index, dtype="float64"
    )
    minus_dm = pd.Series(
        np.where((down > up) & (down > 0.0), down, 0.0), index=down.index, dtype="float64"
    )
    prev_close = close.shift(1).iloc[1:]
    tr = pd.concat(
        [
            (high - low).iloc[1:],
            (high.iloc[1:] - prev_close).abs(),
            (low.iloc[1:] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1).astype("float64")

    atr = wilder_rma(tr, p)
    plus_sm = wilder_rma(plus_dm, p)
    minus_sm = wilder_rma(minus_dm, p)

    plus_di = (100.0 * plus_sm / atr.replace(0.0, np.nan)).astype("float64")
    minus_di = (100.0 * minus_sm / atr.replace(0.0, np.nan)).astype("float64")
    di_sum = plus_di + minus_di
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum.replace(0.0, np.nan)).astype("float64")

    # ADX is the Wilder RMA of DX, seeded by the first p valid DX values.
    adx = wilder_rma(dx.dropna(), p).reindex(df.index)
    return adx.astype("float64"), plus_di.reindex(df.index).astype("float64"), minus_di.reindex(df.index).astype("float64")
'''

CMF = '''def cmf(df: pd.DataFrame, p: int):
    """Chaikin Money Flow with explicit flat-bar and zero-volume handling.

    A flat bar (high == low) has a zero money-flow multiplier. Zero-volume
    bars contribute zero to both numerator and denominator. The result remains
    unavailable only when the entire rolling denominator is zero.
    """
    if p <= 0:
        raise ValueError("CMF period must be positive")
    high = df["high"].astype("float64")
    low = df["low"].astype("float64")
    close = df["close"].astype("float64")
    volume = df["volume"].astype("float64")

    hl = high - low
    multiplier = pd.Series(0.0, index=df.index, dtype="float64")
    non_flat = hl != 0.0
    multiplier.loc[non_flat] = (
        ((close.loc[non_flat] - low.loc[non_flat]) - (high.loc[non_flat] - close.loc[non_flat]))
        / hl.loc[non_flat]
    )
    money_flow_volume = (multiplier * volume).astype("float64")
    numerator = money_flow_volume.rolling(p, min_periods=p).sum()
    denominator = volume.rolling(p, min_periods=p).sum()
    return (numerator / denominator.replace(0.0, np.nan)).astype("float64")
'''


def main() -> None:
    text = TARGET.read_text(encoding="utf-8")
    text = replace_function(text, "adx_wilder", ADX, "cmf")
    text = replace_function(text, "cmf", CMF, "obv")
    TARGET.write_text(text, encoding="utf-8")
    print(f"Repaired {TARGET}")


if __name__ == "__main__":
    main()
