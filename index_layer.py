from __future__ import annotations

import csv
import io
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from dhanhq import DhanContext, MarketFeed

from output import _clean_candle, _ist_timestamp, _price

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
INDEX_SEGMENT = "IDX_I"
INDEX_INSTRUMENT = "INDEX"
INDEX_FALLBACK_IDS = {
    "nifty": "13",
    "banknifty": "25",
    "sensex": "51",
    "nifty500": "17",
    "finnifty": "27",
    "indiavix": "26",
    "niftyit": "29",
}

INDEX_SPECS = {
    "nifty": ("NIFTY", ("NIFTY", "NIFTY 50")),
    "banknifty": ("BANKNIFTY", ("BANKNIFTY", "NIFTY BANK", "NIFTY BANK 50")),
    "sensex": ("SENSEX", ("SENSEX", "S&P BSE SENSEX")),
    "nifty500": ("NIFTY_500", ("NIFTY 500", "NIFTY500", "NIFTY_500")),
    "niftymidcap100": ("NIFTY_MIDCAP_100", ("NIFTY MIDCAP 100", "NIFTY_MIDCAP_100", "NIFTYMIDCAP100")),
    "niftysmallcap100": ("NIFTY_SMALLCAP_100", ("NIFTY SMALLCAP 100", "NIFTY_SMALLCAP_100", "NIFTYSMALLCAP100")),
    "finnifty": ("NIFTY_FIN_SERVICE", ("NIFTY FIN SERVICE", "NIFTY FINANCIAL SERVICES", "NIFTY_FIN_SERVICE", "NIFTYFINSERVICE")),
    "indiavix": ("INDIA VIX", ("INDIA VIX", "INDIAVIX")),
    "niftyit": ("NIFTY_IT", ("NIFTY IT", "NIFTY_IT", "NIFTYIT")),
    "niftyauto": ("NIFTY_AUTO", ("NIFTY AUTO", "NIFTY_AUTO", "NIFTYAUTO")),
    "niftypharma": ("NIFTY_PHARMA", ("NIFTY PHARMA", "NIFTY_PHARMA", "NIFTYPHARMA")),
    "niftymetal": ("NIFTY_METAL", ("NIFTY METAL", "NIFTY_METAL", "NIFTYMETAL")),
    "niftyfmcg": ("NIFTY_FMCG", ("NIFTY FMCG", "NIFTY_FMCG", "NIFTYFMCG")),
    "niftyrealty": ("NIFTY_REALTY", ("NIFTY REALTY", "NIFTY_REALTY", "NIFTYREALTY")),
    "niftyenergy": ("NIFTY_ENERGY", ("NIFTY ENERGY", "NIFTY_ENERGY", "NIFTYENERGY")),
    "niftyinfra": ("NIFTY_INFRA", ("NIFTY INFRA", "NIFTY_INFRA", "NIFTYINFRA")),
}

def _norm(value: object) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

def _resolve_one(key: str) -> tuple["IndexInstrument | None", str]:
    _symbol, aliases = INDEX_SPECS[key]
    fallback = INDEX_FALLBACK_IDS.get(key)
    if fallback:
        return IndexInstrument(fallback, "IDX_I", INDEX_INSTRUMENT), ""

    try:
        response = requests.get(MASTER_URL, timeout=30)
        response.raise_for_status()
        rows = csv.DictReader(io.StringIO(response.text))
        wanted = {_norm(v) for v in aliases}
        matches: dict[tuple[str, str, str], IndexInstrument] = {}
        for row in rows:
            if str(row.get("SEM_INSTRUMENT_NAME", "")).strip().upper() != INDEX_INSTRUMENT:
                continue
            if str(row.get("SEM_SEGMENT", "")).strip().upper() not in {"I", "IDX_I", "INDEX"}:
                continue
            security_id = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
            if not security_id:
                continue
            values = (row.get("SEM_TRADING_SYMBOL", ""), row.get("SEM_CUSTOM_SYMBOL", ""), row.get("SM_SYMBOL_NAME", ""))
            if {_norm(v) for v in values} & wanted:
                exchange = str(row.get("SEM_EXM_EXCH_ID", "")).strip().upper()
                matches[(security_id, exchange, INDEX_INSTRUMENT)] = IndexInstrument(
                    security_id, exchange or "NSE", INDEX_INSTRUMENT
                )
        if len(matches) == 1:
            return next(iter(matches.values())), ""
        if not matches:
            return None, "not found in Dhan instrument master"
        return None, f"ambiguous instrument master matches: {sorted(matches)}"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _resolve_all() -> tuple[dict[str, "IndexInstrument"], dict[str, str]]:
    resolved: dict[str, IndexInstrument] = {}
    errors: dict[str, str] = {}
    for key in INDEX_SPECS:
        instrument, error = _resolve_one(key)
        if instrument is not None:
            resolved[key] = instrument
        else:
            errors[key] = error
    return resolved, errors

@dataclass(frozen=True)
class IndexInstrument:
    security_id: str
    exchange_segment: str
    instrument: str = INDEX_INSTRUMENT

class IndexState:
    def __init__(self, settings, key: str, symbol: str, instrument: IndexInstrument):
        self.settings = settings
        self.key = key
        self.symbol = symbol
        self.instrument = instrument
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.session_date: Optional[str] = None
        self.session_status = "CLOSED"
        self.feed_status = "STOPPED"
        self.last_feed_error = ""
        self.last_ltp: Optional[float] = None
        self.last_ltt: Optional[int] = None
        self.last_tick_received_epoch: Optional[float] = None
        self.live_candles: list[dict] = []
        self.current_1m: Optional[dict] = None
        self.historical: dict[str, list[dict]] = {}
        self.prev_cumulative_volume = 0
        self.last_trade_key: Optional[tuple] = None
        self.feed_messages = 0
        self.quote_packets = 0

    def begin(self, session_date: str) -> None:
        with self.lock:
            self.session_date = session_date
            self.session_status = "LIVE"
            self.feed_status = "STARTING"
            self.last_feed_error = ""
            self.last_ltp = self.last_ltt = self.last_tick_received_epoch = None
            self.live_candles = []
            self.current_1m = None
            self.historical = {}
            self.prev_cumulative_volume = 0
            self.last_trade_key = None
            self.feed_messages = self.quote_packets = 0

    def reset(self) -> None:
        with self.lock:
            self.session_date = None
            self.session_status = "CLOSED"
            self.feed_status = "STOPPED"
            self.last_ltp = self.last_ltt = self.last_tick_received_epoch = None
            self.live_candles = []
            self.current_1m = None
            self.historical = {}
            self.prev_cumulative_volume = 0
            self.last_trade_key = None

    def set_feed_status(self, status: str, error: str = "") -> None:
        with self.lock:
            self.feed_status = status
            if error:
                self.last_feed_error = error

    def update_quote(self, ltp: float, ltt_epoch: int, cumulative_volume: int, ltq: int) -> None:
        if ltp <= 0 or ltt_epoch <= 0 or cumulative_volume < 0:
            return
        with self.lock:
            if self.session_status != "LIVE":
                return
            minute_key = ltt_epoch - (ltt_epoch % 60)
            if self.current_1m is not None:
                current_minute = int(self.current_1m["epoch"])
                if minute_key < current_minute:
                    return
                if minute_key > current_minute:
                    self.current_1m["complete"] = True
                    self.live_candles.append(dict(self.current_1m))
                    self.current_1m = None
            delta_volume = max(0, cumulative_volume - self.prev_cumulative_volume)
            self.prev_cumulative_volume = cumulative_volume
            trade_key = (ltt_epoch, cumulative_volume, ltq, ltp)
            self.last_ltp = ltp
            self.last_ltt = ltt_epoch
            self.last_tick_received_epoch = time.time()
            if trade_key == self.last_trade_key:
                return
            self.last_trade_key = trade_key
            candle = self.current_1m
            if candle is None:
                self.current_1m = {
                    "timestamp": minute_key, "epoch": minute_key,
                    "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                    "volume": delta_volume, "source": "DHAN_WEBSOCKET_FULL", "complete": False,
                }
            else:
                candle["high"] = max(float(candle["high"]), ltp)
                candle["low"] = min(float(candle["low"]), ltp)
                candle["close"] = ltp
                candle["volume"] = int(candle["volume"]) + delta_volume

    def merge_history(self, rows: list[dict], timeframe: str) -> None:
        with self.lock:
            existing = {int(c["timestamp"]): dict(c) for c in self.historical.get(timeframe, [])}
            for row in rows:
                if row.get("complete", True):
                    existing[int(row["timestamp"])] = dict(row)
            self.historical[timeframe] = [existing[k] for k in sorted(existing)]

    def merge_today_1m(self, rows: list[dict]) -> None:
        with self.lock:
            existing = {int(c["timestamp"]): dict(c) for c in self.live_candles if c.get("complete", True)}
            for row in rows:
                if row.get("complete", True):
                    existing[int(row["timestamp"])] = dict(row)
            self.live_candles = [existing[k] for k in sorted(existing)]

    def finalize_current(self) -> None:
        with self.lock:
            if self.current_1m is not None:
                candle = dict(self.current_1m)
                candle["complete"] = True
                self.live_candles.append(candle)
                self.current_1m = None

class IndexLayerFeed:
    def __init__(self, settings, states: dict[str, IndexState]):
        self.settings = settings
        self.states = states
        self._feed = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connection_stop = threading.Event()
        self._backoff = 5.0

    def _build_feed(self):
        context = DhanContext(self.settings.client_id, self.settings.access_token)
        instruments = [
            (MarketFeed.IDX, state.instrument.security_id, MarketFeed.Full)
            for state in self.states.values()
        ]
        return MarketFeed(
            context, instruments, version="v2",
            on_connect=self._on_connect, on_message=self._on_message,
            on_close=self._on_close, on_error=self._on_error,
        )

    @staticmethod
    def _parse_ltt(value) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            if isinstance(value, (int, float)) or str(value).strip().isdigit():
                return int(value)
        except (TypeError, ValueError):
            return None
        text = str(value).strip()
        for fmt in ("%H:%M:%S", "%H:%M:%S.%f"):
            try:
                parsed = datetime.strptime(text, fmt).replace(
                    year=datetime.now().year, month=datetime.now().month, day=datetime.now().day,
                    tzinfo=ZoneInfo("UTC"),
                )
                return int(parsed.timestamp())
            except ValueError:
                pass
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
            return int(parsed.timestamp())
        except ValueError:
            return None

    def _on_connect(self, _feed):
        self._backoff = 5.0
        self._connection_stop.clear()
        for state in self.states.values():
            state.set_feed_status("CONNECTED")

    def _on_close(self, _feed):
        if not self._stop.is_set():
            for state in self.states.values():
                state.set_feed_status("RECONNECTING", "Dhan index websocket closed by peer")

    def _on_error(self, _feed, error):
        if not self._stop.is_set():
            for state in self.states.values():
                state.set_feed_status("ERROR", f"index websocket:{type(error).__name__}:{error}")

    def _on_message(self, _feed, data):
        if not isinstance(data, dict):
            return
        packet_type = str(data.get("type", data.get("Type", ""))).strip().lower()
        if packet_type in {"previous close", "prev close", "previous day"}:
            return
        try:
            security_id = str(data.get("security_id", data.get("securityId", ""))).strip()
            ltt = self._parse_ltt(data.get("LTT", data.get("ltt", data.get("last_trade_time"))))
            ltp = float(data.get("LTP", data.get("ltp")))
            volume = int(data.get("volume", 0) or 0)
            ltq = int(data.get("LTQ", data.get("ltq", 0)) or 0)
        except (TypeError, ValueError):
            return
        if ltt is None:
            return
        for state in self.states.values():
            if state.instrument.security_id == security_id:
                state.feed_messages += 1
                if packet_type in {"quote data", "quote", "full data", "full"}:
                    state.quote_packets += 1
                    state.update_quote(ltp, ltt, volume, ltq)
                return

    def _run(self):
        while not self._stop.is_set():
            feed = None
            try:
                feed = self._build_feed()
                self._feed = feed
                for state in self.states.values():
                    state.set_feed_status("CONNECTING")
                feed.run()
                if not self._stop.is_set():
                    for state in self.states.values():
                        state.set_feed_status("RECONNECTING", "index feed loop ended")
            except Exception as exc:
                for state in self.states.values():
                    state.set_feed_status("RECONNECTING", f"index websocket:{type(exc).__name__}:{exc}")
            finally:
                self._connection_stop.set()
                try:
                    if feed is not None:
                        feed.close_connection()
                except Exception:
                    pass
                self._feed = None
            if self._stop.wait(self._backoff):
                break
            self._backoff = min(self._backoff * 2.0, 120.0)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-index-feed")
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._connection_stop.set()
        if self._feed is not None:
            try:
                self._feed.close_connection()
            except Exception:
                pass
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=8)
        self._thread = None
        self._feed = None
        for state in self.states.values():
            state.set_feed_status("STOPPED")

class IndexLayerManager:
    def __init__(self, settings, dhan_api):
        self.settings = settings
        self.dhan_api = dhan_api
        self.instruments, self.resolution_errors = _resolve_all()
        self.states = {
            key: IndexState(settings, INDEX_SPECS[key][0], instrument)
            for key, instrument in self.instruments.items()
        }
        self.feed = IndexLayerFeed(settings, self.states)
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def _in_market(self, now: datetime) -> bool:
        sh, sm = map(int, self.settings.market_start.split(":"))
        eh, em = map(int, self.settings.market_end.split(":"))
        return dt_time(sh, sm) <= now.time() < dt_time(eh, em)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-index-session")
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.feed.stop()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)
        self.thread = None
        for state in self.states.values():
            state.reset()

    def _loop(self):
        started_for_date = None
        tz = ZoneInfo(self.settings.timezone)
        while not self.stop_event.is_set():
            now = datetime.now(tz)
            if self._in_market(now):
                if started_for_date != now.date().isoformat():
                    self._start_session(now)
                    started_for_date = now.date().isoformat()
            elif started_for_date is not None:
                self._end_session()
                started_for_date = None
            self.stop_event.wait(2.0)

    def _start_session(self, now: datetime):
        for state in self.states.values():
            state.begin(now.date().isoformat())
        for key, state in self.states.items():
            try:
                snap = self.dhan_api.quote_snapshot([state.instrument])
                state.prev_cumulative_volume = max(0, int(snap.get(state.instrument.security_id, {}).get("volume", 0) or 0))
                state.merge_today_1m(self.dhan_api.load_today_completed_intraday(state.instrument, 1))
                for interval, tf in ((5, "5m"), (15, "15m"), (60, "1h")):
                    state.merge_history(self.dhan_api.load_today_completed_intraday(state.instrument, interval), tf)
            except Exception as exc:
                state.set_feed_status("STARTING", f"history bootstrap: {exc}")
        self.feed.start()

    def _end_session(self):
        self.feed.stop()
        for state in self.states.values():
            state.finalize_current()
            state.reset()

    def snapshot(self, key: str) -> dict:
        if key not in self.states:
            return {
                "service": "PSYGRID",
                "schema_version": "3.0",
                "route": key,
                "symbol": INDEX_SPECS[key][0],
                "status": "INDEX_DATA_UNAVAILABLE",
                "resolution_error": self.resolution_errors.get(key, "unknown"),
            }
        state = self.states[key]
        with state.lock:
            candles_1m = [dict(c) for c in state.live_candles if c.get("complete", True)]
            historical = {
                tf: [dict(c) for c in state.historical.get(tf, []) if c.get("complete", True)]
                for tf in ("5m", "15m", "1h")
            }
            return {
                "service": "PSYGRID",
                "schema_version": "3.0",
                "symbol": state.symbol,
                "security_id": state.instrument.security_id,
                "exchange_segment": state.instrument.exchange_segment,
                "instrument": state.instrument.instrument,
                "session": {
                    "status": state.session_status,
                    "date": state.session_date,
                    "timezone": self.settings.timezone,
                    "current_time_ist": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST"),
                },
                "feed": {
                    "status": state.feed_status,
                    "messages": state.feed_messages,
                    "quote_packets": state.quote_packets,
                    "last_error": state.last_feed_error,
                    "last_tick_received_epoch": state.last_tick_received_epoch,
                },
                "ltp": _price(state.last_ltp),
                "ltp_timestamp": _ist_timestamp(state.last_ltt),
                "timeframes": ["1m", "5m", "15m", "1h"],
                "candle_source": {
                    "1m": "DHAN_WEBSOCKET_FULL",
                    "5m": "DHAN_HISTORICAL_API",
                    "15m": "DHAN_HISTORICAL_API",
                    "1h": "DHAN_HISTORICAL_API",
                },
                "synthetic_candles": False,
                "1m": [_clean_candle(x) for x in candles_1m],
                "5m": [_normalize_ohlcv(x) for x in historical["5m"]],
                "15m": [_normalize_ohlcv(x) for x in historical["15m"]],
                "1h": [_normalize_ohlcv(x) for x in historical["1h"]],
            }

    def status(self, key: str) -> dict:
        state = self.states[key]
        with state.lock:
            return {
                "service": "PSYGRID",
                "symbol": state.symbol,
                "security_id": state.instrument.security_id,
                "exchange_segment": state.instrument.exchange_segment,
                "status": "LIVE" if state.feed_status == "CONNECTED" and state.last_tick_received_epoch else "STARTING",
                "feed_status": state.feed_status,
                "last_error": state.last_feed_error,
            }
