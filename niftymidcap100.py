from __future__ import annotations

import csv
import io
import threading
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from dhanhq import DhanContext, MarketFeed

from nifty import NiftyFeed, NiftyInstrument, NiftyManager, NiftyState
from output import LIVE_TIMEFRAMES, _completed_rows, _ist_timestamp, _normalize_ohlcv, _price


NIFTYMIDCAP100_SYMBOL = "NIFTY_MIDCAP_100"
NIFTYMIDCAP100_EXCHANGE_SEGMENT = "IDX_I"
NIFTYMIDCAP100_INSTRUMENT = "INDEX"
NIFTYMIDCAP100_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def _norm(value: object) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def resolve_niftymidcap100_security_id() -> str:
    """Resolve the current Dhan security ID from the public instrument master.

    The ID is deliberately resolved at startup rather than guessed/hardcoded because
    Dhan's index security master is the authoritative source for the current ID.
    """
    response = requests.get(NIFTYMIDCAP100_MASTER_URL, timeout=15)
    response.raise_for_status()
    reader = csv.DictReader(io.StringIO(response.text))

    candidates = {
        _norm("NIFTY MIDCAP 100"),
        _norm("NIFTY_MIDCAP_100"),
        _norm("NIFTYMIDCAP100"),
    }
    matches: list[str] = []
    for row in reader:
        exchange = str(row.get("SEM_EXM_EXCH_ID", "")).strip().upper()
        segment = str(row.get("SEM_SEGMENT", "")).strip().upper()
        instrument = str(row.get("SEM_INSTRUMENT_NAME", "")).strip().upper()
        if exchange != "NSE" or segment != "I" or instrument != NIFTYMIDCAP100_INSTRUMENT:
            continue
        values = (
            row.get("SEM_TRADING_SYMBOL", ""),
            row.get("SEM_CUSTOM_SYMBOL", ""),
            row.get("SM_SYMBOL_NAME", ""),
        )
        if any(_norm(value) in candidates for value in values):
            security_id = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
            if security_id:
                matches.append(security_id)

    unique = list(dict.fromkeys(matches))
    if len(unique) != 1:
        raise RuntimeError(
            f"Dhan instrument master could not uniquely resolve {NIFTYMIDCAP100_SYMBOL}: {unique}"
        )
    return unique[0]


class NiftyMidcap100Instrument(NiftyInstrument):
    def __init__(self, security_id: str):
        object.__setattr__(self, "security_id", str(security_id))
        object.__setattr__(self, "exchange_segment", NIFTYMIDCAP100_EXCHANGE_SEGMENT)
        object.__setattr__(self, "instrument", NIFTYMIDCAP100_INSTRUMENT)


class NiftyMidcap100State(NiftyState):
    """RAM-only state for the NIFTY Midcap 100 index endpoint."""


class NiftyMidcap100Feed(NiftyFeed):
    def __init__(self, settings, state: NiftyMidcap100State, security_id: str):
        super().__init__(settings, state)
        self.security_id = str(security_id)

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        return MarketFeed(
            context,
            [(MarketFeed.IDX, self.security_id, MarketFeed.Full)],
            version="v2",
            on_connect=self._on_connect,
            on_message=self._on_message,
            on_close=self._on_close,
            on_error=self._on_error,
        )

    def _on_message(self, _feed, data) -> None:
        if not isinstance(data, dict):
            return
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        self.state.record_message(packet_type)
        if packet_type == "error":
            self.state.set_feed_status(
                "ERROR",
                f"Dhan {NIFTYMIDCAP100_SYMBOL} feed error code={data.get('error_code', data.get('code'))}: "
                f"{data.get('message', data.get('error_message', 'feed error'))}",
            )
            return
        if packet_type in {"previous close", "prev close", "previous day"}:
            return
        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return
        try:
            security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
            if security_id != self.security_id:
                return
            ltt = self._parse_ltt(data.get("LTT", data.get("ltt", data.get("last_trade_time"))))
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
        except (TypeError, ValueError):
            return
        if ltt is None:
            return
        self.state.update_quote({"LTT_EPOCH": ltt, "LTP": ltp, "volume": volume, "LTQ": ltq})


class NiftyMidcap100Manager(NiftyManager):
    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        security_id = resolve_niftymidcap100_security_id()
        self.state = NiftyMidcap100State(settings)
        self.feed = NiftyMidcap100Feed(settings, self.state, security_id)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.htf_threads: dict[str, threading.Thread] = {}
        self.instrument = NiftyMidcap100Instrument(security_id)


def niftymidcap100_json(state: NiftyMidcap100State, security_id: str) -> dict:
    with state.lock:
        candles_1m = _completed_rows([dict(c) for c in state.live_candles])
        historical = {
            timeframe: _completed_rows([dict(c) for c in state.historical.get(timeframe, [])])
            for timeframe in ("5m", "15m", "1h")
        }
        ltp = state.last_ltp
        ltt = state.last_ltt
        session_date = state.session_date
        session_status = state.session_status

    return {
        "service": "PSYGRID",
        "schema_version": "3.0",
        "symbol": NIFTYMIDCAP100_SYMBOL,
        "security_id": str(security_id),
        "exchange_segment": NIFTYMIDCAP100_EXCHANGE_SEGMENT,
        "instrument": NIFTYMIDCAP100_INSTRUMENT,
        "session": {"status": session_status, "date": session_date, "timezone": "Asia/Kolkata", "current_time_ist": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")},
        "ltp": _price(ltp),
        "ltp_timestamp": _ist_timestamp(ltt),
        "timeframes": list(LIVE_TIMEFRAMES),
        "candle_source": {"1m": "DHAN_WEBSOCKET_FULL", "5m": "DHAN_NATIVE_HISTORICAL", "15m": "DHAN_NATIVE_HISTORICAL", "1h": "DHAN_NATIVE_HISTORICAL"},
        "synthetic_candles": False,
        "1m": [_normalize_ohlcv(row) for row in candles_1m],
        "5m": [_normalize_ohlcv(row) for row in historical["5m"]],
        "15m": [_normalize_ohlcv(row) for row in historical["15m"]],
        "1h": [_normalize_ohlcv(row) for row in historical["1h"]],
    }
