from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

from dhan_auth import generate_access_token, token_from_environment
from instrument_master import fetch_nse_equity_security_ids


# Psygrid is intentionally a fixed production configuration. Market-data
# behavior must not change because a stale/leftover Render environment variable
# is present from an earlier experiment.
UNIVERSE_SIZE = 270
TIMEZONE = "Asia/Kolkata"
MARKET_START = "09:15"
MARKET_END = "15:15"
INTRADAY_HISTORY_DAYS = 7
DAILY_LOOKBACK = 7
DAILY_INDICATOR_WARMUP = 30
WEEKLY_LOOKBACK = 7
MA_PERIOD = 9
EMA_PERIOD = 20
RSI_PERIOD = 14
MAX_INSTRUMENTS = 270
MAX_LIVE_AGE_SECONDS = 30


@dataclass
class Instrument:
    symbol: str
    security_id: str
    exchange_segment: str = "NSE_EQ"
    instrument: str = "EQUITY"


@dataclass
class Settings:
    client_id: str
    access_token: str
    token_source: str = "UNKNOWN"
    token_expiry: str | None = None
    timezone: str = TIMEZONE
    market_start: str = MARKET_START
    market_end: str = MARKET_END
    intraday_history_days: int = INTRADAY_HISTORY_DAYS
    daily_lookback: int = DAILY_LOOKBACK
    daily_indicator_warmup: int = DAILY_INDICATOR_WARMUP
    weekly_lookback: int = WEEKLY_LOOKBACK
    ma_period: int = MA_PERIOD
    ema_period: int = EMA_PERIOD
    rsi_period: int = RSI_PERIOD
    max_instruments: int = MAX_INSTRUMENTS
    max_live_age_seconds: int = MAX_LIVE_AGE_SECONDS


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
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
    if len(normalized) != UNIVERSE_SIZE:
        raise RuntimeError(
            f"stocks.json must contain exactly {UNIVERSE_SIZE} unique symbols; got {len(normalized)}"
        )
    if len(normalized) != len(set(normalized)):
        raise RuntimeError("stocks.json contains duplicate symbols")
    return normalized


def load_instruments() -> List[Instrument]:
    symbols = _load_symbol_universe()
    security_ids = fetch_nse_equity_security_ids(symbols)
    if len(security_ids) != UNIVERSE_SIZE:
        missing = sorted(set(symbols) - set(security_ids))
        raise RuntimeError(
            f"Dhan instrument master resolved {len(security_ids)}/{UNIVERSE_SIZE} symbols; missing={missing}"
        )
    instruments = [Instrument(symbol=symbol, security_id=security_ids[symbol]) for symbol in symbols]
    if len({item.security_id for item in instruments}) != UNIVERSE_SIZE:
        raise RuntimeError("Dhan instrument master returned duplicate security IDs")
    return instruments


def load_settings() -> Settings:
    client_id = _required("DHAN_CLIENT_ID")
    access_token, token_expiry, token_source = token_from_environment(client_id)
    return Settings(
        client_id=client_id,
        access_token=access_token,
        token_source=token_source,
        token_expiry=token_expiry,
    )


def _token_expiry_epoch(token: str) -> int | None:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        value = decoded.get("exp")
        return int(value) if value is not None else None
    except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
        return None


def refresh_access_token(settings: Settings) -> None:
    pin = os.getenv("DHAN_PIN", "").strip()
    totp_secret = os.getenv("DHAN_TOTP_SECRET", "").strip()
    if settings.access_token:
        expiry_epoch = _token_expiry_epoch(settings.access_token)
        if expiry_epoch is None or expiry_epoch > int(time.time()) + 1800:
            return
    if not pin or not totp_secret:
        if settings.access_token:
            return
        raise RuntimeError("No usable Dhan access token or TOTP credentials configured")
    token, expiry = generate_access_token(settings.client_id, pin, totp_secret)
    settings.access_token = token
    settings.token_expiry = expiry
    settings.token_source = "AUTO_GENERATED_TOTP"
