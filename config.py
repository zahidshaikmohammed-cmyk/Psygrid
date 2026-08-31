import json
import os
from dataclasses import dataclass
from typing import List


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
    intraday_history_days: int = 7
    daily_lookback: int = 7
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


def load_instruments() -> List[Instrument]:
    raw = os.getenv("PSYGRID_INSTRUMENTS", "").strip()
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("PSYGRID_INSTRUMENTS must be valid JSON") from exc

    if not isinstance(parsed, list):
        raise RuntimeError("PSYGRID_INSTRUMENTS must be a JSON array")

    instruments: List[Instrument] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().upper()
        security_id = str(item.get("security_id", "")).strip()
        if not symbol or not security_id:
            continue
        instruments.append(
            Instrument(
                symbol=symbol,
                security_id=security_id,
                exchange_segment=str(item.get("exchange_segment", "NSE_EQ")),
                instrument=str(item.get("instrument", "EQUITY")),
            )
        )

    if len(instruments) > 5000:
        raise RuntimeError("PSYGRID_INSTRUMENTS exceeds Dhan's 5000-instrument single-connection limit")
    return instruments


def load_settings() -> Settings:
    # The Environment Group can expose the daily token under any name.
    # Copy its value into DHAN_ACCESS_TOKEN in Render, or set DHAN_TOKEN_VAR.
    token_var = os.getenv("DHAN_TOKEN_VAR", "DHAN_ACCESS_TOKEN").strip()
    access_token = os.getenv(token_var, "").strip()
    if not access_token:
        raise RuntimeError(f"Missing Dhan token. Expected variable: {token_var}")

    return Settings(
        client_id=_required("DHAN_CLIENT_ID"),
        access_token=access_token,
        timezone=os.getenv("TIMEZONE", "Asia/Kolkata"),
        market_start=os.getenv("MARKET_START", "09:15"),
        market_end=os.getenv("MARKET_END", "15:15"),
        intraday_history_days=int(os.getenv("INTRADAY_HISTORY_DAYS", "7")),
        daily_lookback=int(os.getenv("DAILY_LOOKBACK", "7")),
        weekly_lookback=int(os.getenv("WEEKLY_LOOKBACK", "7")),
        ma_period=int(os.getenv("MA_PERIOD", "9")),
        ema_period=int(os.getenv("EMA_PERIOD", "20")),
        rsi_period=int(os.getenv("RSI_PERIOD", "14")),
        max_instruments=int(os.getenv("MAX_INSTRUMENTS", "500")),
    )
