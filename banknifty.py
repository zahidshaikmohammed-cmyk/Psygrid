from __future__ import annotations

import threading
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from dhanhq import DhanContext, MarketFeed

from nifty import NiftyFeed, NiftyInstrument, NiftyManager, NiftyState
from output import LIVE_TIMEFRAMES, _completed_rows, _ist_timestamp, _normalize_ohlcv, _price


BANKNIFTY_SYMBOL = "BANKNIFTY"
BANKNIFTY_SECURITY_ID = "25"
BANKNIFTY_EXCHANGE_SEGMENT = "IDX_I"
BANKNIFTY_INSTRUMENT = "INDEX"


class BankNiftyInstrument(NiftyInstrument):
    security_id: str = BANKNIFTY_SECURITY_ID
    exchange_segment: str = BANKNIFTY_EXCHANGE_SEGMENT
    instrument: str = BANKNIFTY_INSTRUMENT

    def __init__(self):
        object.__setattr__(self, "security_id", BANKNIFTY_SECURITY_ID)
        object.__setattr__(self, "exchange_segment", BANKNIFTY_EXCHANGE_SEGMENT)
        object.__setattr__(self, "instrument", BANKNIFTY_INSTRUMENT)


class BankNiftyState(NiftyState):
    """RAM-only state for the BANKNIFTY index endpoint."""


class BankNiftyFeed(NiftyFeed):
    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        return MarketFeed(
            context,
            [(MarketFeed.IDX, BANKNIFTY_SECURITY_ID, MarketFeed.Full)],
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
                f"Dhan BANKNIFTY feed error code={data.get('error_code', data.get('code'))}: "
                f"{data.get('message', data.get('error_message', 'feed error'))}",
            )
            return
        if packet_type in {"previous close", "prev close", "previous day"}:
            return
        if packet_type not in {"quote data", "quote", "full data", "full"}:
            return
        try:
            security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
            if security_id != BANKNIFTY_SECURITY_ID:
                return
            ltt = self._parse_ltt(data.get("LTT", data.get("ltt", data.get("last_trade_time"))))
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
        except (TypeError, ValueError):
            return
        if ltt is None:
            return
        self.state.update_quote({
            "LTT_EPOCH": ltt,
            "LTP": ltp,
            "volume": volume,
            "LTQ": ltq,
        })


class BankNiftyManager(NiftyManager):
    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self.state = BankNiftyState(settings)
        self.feed = BankNiftyFeed(settings, self.state)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.htf_threads: dict[str, threading.Thread] = {}
        self.instrument = BankNiftyInstrument()


def banknifty_json(state: BankNiftyState) -> dict:
    with state.lock:
        candles_1m = _completed_rows([dict(c) for c in state.live_candles])
        historical = {
            timeframe: _completed_rows(
                [dict(c) for c in state.historical.get(timeframe, [])]
            )
            for timeframe in ("5m", "15m", "1h")
        }
        ltp = state.last_ltp
        ltt = state.last_ltt
        session_date = state.session_date
        session_status = state.session_status

    return {
        "service": "PSYGRID",
        "schema_version": "3.0",
        "symbol": BANKNIFTY_SYMBOL,
        "security_id": BANKNIFTY_SECURITY_ID,
        "exchange_segment": BANKNIFTY_EXCHANGE_SEGMENT,
        "instrument": BANKNIFTY_INSTRUMENT,
        "session": {
            "status": session_status,
            "date": session_date,
            "timezone": "Asia/Kolkata",
            "current_time_ist": datetime.now(ZoneInfo("Asia/Kolkata")).strftime(
                "%Y-%m-%d %H:%M:%S IST"
            ),
        },
        "ltp": _price(ltp),
        "ltp_timestamp": _ist_timestamp(ltt),
        "timeframes": list(LIVE_TIMEFRAMES),
        "candle_source": {
            "1m": "DHAN_WEBSOCKET_FULL",
            "5m": "DHAN_NATIVE_HISTORICAL",
            "15m": "DHAN_NATIVE_HISTORICAL",
            "1h": "DHAN_NATIVE_HISTORICAL",
        },
        "synthetic_candles": False,
        "1m": [_normalize_ohlcv(row) for row in candles_1m],
        "5m": [_normalize_ohlcv(row) for row in historical["5m"]],
        "15m": [_normalize_ohlcv(row) for row in historical["15m"]],
        "1h": [_normalize_ohlcv(row) for row in historical["1h"]],
    }
