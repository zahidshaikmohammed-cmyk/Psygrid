"""Resolves NIFTY/BANKNIFTY index-futures contracts from Dhan's own
instrument master (the same CSV instrument_master.py already fetches for
equity resolution, requested independently here to keep the derivatives
domain isolated from the sealed equity-resolution code path).

RAW DATA ONLY: this module identifies which contract is the current
front-month future and its lot size / tick size. It does not compute or
suggest anything about direction, rollover timing signals, or strategy.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import date, datetime

import requests

DHAN_INSTRUMENT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

# Dhan's compact instrument master marks index futures with this instrument
# name; some exports have used FUTIDX consistently since Dhan v2 launched.
FUTURES_INSTRUMENT_NAMES = {"FUTIDX"}


@dataclass(frozen=True)
class FuturesContract:
    symbol: str
    security_id: str
    exchange_segment: str
    instrument: str
    trading_symbol: str
    expiry_date: str
    lot_size: int | None
    tick_size: float | None


def _parse_expiry(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    # Dhan's scrip master writes SEM_EXPIRY_DATE with a time component
    # ("2026-10-27 14:30:00"); only the date part identifies the expiry.
    value = value.replace("T", " ").split(" ")[0]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _int_or_none(value) -> int | None:
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _float_or_none(value) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def download_instrument_master(timeout: int = 30) -> str:
    """Download Dhan's instrument master CSV once; callers parse it for as
    many exchanges/symbols as they need instead of re-downloading it."""
    response = requests.get(DHAN_INSTRUMENT_MASTER_URL, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    return response.text


def fetch_front_month_index_futures(
    underlying_symbols: tuple[str, ...], timeout: int = 30, exchange: str = "NSE"
) -> dict[str, FuturesContract]:
    """Resolve the nearest-expiry index-futures contract for each of the
    given underlying symbols (e.g. "NIFTY", "BANKNIFTY" on NSE, "SENSEX" on
    BSE). Never fabricates a contract: a symbol with no matching, unexpired
    row is simply absent from the returned dict rather than guessed.
    """
    if not {s.strip().upper() for s in underlying_symbols if s.strip()}:
        return {}
    return parse_front_month_index_futures(download_instrument_master(timeout), underlying_symbols, exchange)


def parse_front_month_index_futures(
    csv_text: str, underlying_symbols: tuple[str, ...], exchange: str = "NSE"
) -> dict[str, FuturesContract]:
    """Same resolution as fetch_front_month_index_futures, on an already
    downloaded instrument master."""
    wanted = {s.strip().upper() for s in underlying_symbols if s.strip()}
    if not wanted:
        return {}
    exchange = exchange.strip().upper()
    exchange_segment = f"{exchange}_FNO"

    reader = csv.DictReader(io.StringIO(csv_text))
    today = datetime.now().date()
    candidates: dict[str, list[tuple[date, dict]]] = {symbol: [] for symbol in wanted}

    for row in reader:
        if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != exchange:
            continue
        if row.get("SEM_INSTRUMENT_NAME", "").strip().upper() not in FUTURES_INSTRUMENT_NAMES:
            continue
        underlying = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
        # Trading symbols embed the underlying + expiry, e.g. "NIFTY-Oct2026-FUT".
        matched_symbol = None
        for symbol in wanted:
            if underlying.startswith(symbol + "-") or underlying == symbol:
                # Avoid "NIFTY" matching a "BANKNIFTY-..." row.
                if symbol == "NIFTY" and underlying.startswith("BANKNIFTY"):
                    continue
                matched_symbol = symbol
                break
        if matched_symbol is None:
            continue
        expiry = _parse_expiry(row.get("SEM_EXPIRY_DATE", ""))
        if expiry is None or expiry < today:
            continue
        candidates[matched_symbol].append((expiry, row))

    result: dict[str, FuturesContract] = {}
    for symbol, rows in candidates.items():
        if not rows:
            continue
        rows.sort(key=lambda item: item[0])
        expiry, row = rows[0]
        security_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
        if not security_id:
            continue
        result[symbol] = FuturesContract(
            symbol=symbol,
            security_id=security_id,
            exchange_segment=exchange_segment,
            instrument="FUTIDX",
            trading_symbol=row.get("SEM_TRADING_SYMBOL", "").strip(),
            expiry_date=expiry.isoformat(),
            lot_size=_int_or_none(row.get("SEM_LOT_UNITS")),
            tick_size=_float_or_none(row.get("SEM_TICK_SIZE")),
        )
    return result
