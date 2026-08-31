from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

from instrument_master import fetch_nse_equity_security_ids


@dataclass(frozen=True)
class Instrument:
    symbol: str
    security_id: str
    exchange_segment: str = "NSE_EQ"
    instrument: str = "EQUITY"


@dataclass(frozen=True)
class Settings:
    client_id: str
    access_token: str
    timezone: str = "Asia/Kolkata"
    market_start: str = "09:15"
    market_end: str = "15:15"
    intraday_history_days: int = 5
    daily_lookback: int = 7
    daily_indicator_warmup: int = 30
    weekly_lookback: int = 7
    ma_period: int = 9
    ema_period: int = 20
    rsi_period: int = 14
    max_instruments: int = 500


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be greater than zero")
    return value


def _load_symbol_universe() -> list[str]:
    path = Path(os.getenv("PSYGRID_STOCKS_FILE", "stocks.json"))
    if not path.exists():
        raise RuntimeError(f"Stock universe file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Unable to read stock universe: {path}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("stocks.json must be a JSON object")
    if str(payload.get("exchange", "")).upper() != "NSE":
        raise RuntimeError("stocks.json exchange must be NSE")
    if str(payload.get("instrument", "")).upper() != "EQUITY":
        raise RuntimeError("stocks.json instrument must be EQUITY")

    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise RuntimeError("stocks.json must contain a non-empty 'symbols' array")
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    if len(normalized) != len(set(normalized)):
        raise RuntimeError("stocks.json contains duplicate symbols")
    return normalized


def load_instruments() -> List[Instrument]:
    symbols = _load_symbol_universe()
    max_instruments = _positive_int_env("MAX_INSTRUMENTS", 500)
    if len(symbols) > max_instruments:
        raise RuntimeError(f"Configured {len(symbols)} symbols; MAX_INSTRUMENTS={max_instruments}")
    security_ids = fetch_nse_equity_security_ids(symbols)
    return [
        Instrument(symbol=symbol, security_id=security_ids[symbol])
        for symbol in symbols
    ]


def load_settings() -> Settings:
    token_var = os.getenv("DHAN_TOKEN_VAR", "DHAN_ACCESS_TOKEN").strip()
    if not token_var:
        raise RuntimeError("DHAN_TOKEN_VAR cannot be empty")
    access_token = os.getenv(token_var, "").strip()
    if not access_token:
        raise RuntimeError(f"Missing Dhan token. Expected variable: {token_var}")

    market_start = os.getenv("MARKET_START", "09:15").strip()
    market_end = os.getenv("MARKET_END", "15:15").strip()
    if len(market_start) != 5 or len(market_end) != 5:
        raise RuntimeError("MARKET_START and MARKET_END must use HH:MM")

    return Settings(
        client_id=_required("DHAN_CLIENT_ID"),
        access_token=access_token,
        timezone=os.getenv("TIMEZONE", "Asia/Kolkata").strip(),
        market_start=market_start,
        market_end=market_end,
        intraday_history_days=_positive_int_env("INTRADAY_HISTORY_DAYS", 5),
        daily_lookback=_positive_int_env("DAILY_LOOKBACK", 7),
        daily_indicator_warmup=_positive_int_env("DAILY_INDICATOR_WARMUP", 30),
        weekly_lookback=_positive_int_env("WEEKLY_LOOKBACK", 7),
        ma_period=_positive_int_env("MA_PERIOD", 9),
        ema_period=_positive_int_env("EMA_PERIOD", 20),
        rsi_period=_positive_int_env("RSI_PERIOD", 14),
        max_instruments=_positive_int_env("MAX_INSTRUMENTS", 500),
    )
