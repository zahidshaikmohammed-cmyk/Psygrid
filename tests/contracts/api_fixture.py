"""Deterministic full-app state for API contract tests.

Every manager global in ``app`` is replaced by a real state object filled with
fixed data, so each of the 91 public routes serialises through its real
builder. Nothing here starts a thread, opens a socket or calls Dhan.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import app as app_module
from config import Instrument, Settings, _load_symbol_universe
from futures_layer import FuturesContract, FuturesState
from global_context import GlobalContextState
from index_depth import DepthContract, IndexDepthManager
from index_layer import IndexInstrument, IndexLayerManager, IndexState
from index_options import INDEX_DERIVATIVES, IndexOptionsManager
from indicator_runtime import IndicatorRuntime
from midcpnifty_underlying import MidcapNiftyUnderlyingState
from output import market_live_json
from rbi_news import RbiNewsState
from state_runtime import RuntimeFreshnessState
from stock_depth import StockDepthContract, StockDepthManager
from stock_options import StockOptionsManager
from underlying_indicators import UnderlyingIndicatorRuntime

IST = ZoneInfo("Asia/Kolkata")
SESSION_DATE = "2026-09-30"
EPOCH_0915 = int(datetime(2026, 9, 30, 9, 15, tzinfo=IST).timestamp())
CANDLES_PER_INSTRUMENT = 30
SETTINGS = Settings(client_id="contract-test", access_token="contract-test")
# The symbol used for every {symbol} route; it has data in every domain.
SAMPLE_SYMBOL = "RELIANCE"


def _candle(minute: int, base: float) -> dict:
    close = base + (minute % 7) - 3
    epoch = EPOCH_0915 + 60 * minute
    return {
        "timestamp": epoch,
        "epoch": epoch,
        "open": base,
        "high": max(base, close) + 1,
        "low": min(base, close) - 1,
        "close": close,
        "volume": 1000 + minute,
        "complete": True,
    }


def _candles(base: float) -> list[dict]:
    return [_candle(m, base) for m in range(CANDLES_PER_INSTRUMENT)]


def _equity_state() -> RuntimeFreshnessState:
    symbols = _load_symbol_universe()
    instruments = [Instrument(symbol=s, security_id=str(100000 + i)) for i, s in enumerate(symbols)]
    state = RuntimeFreshnessState(SETTINGS)
    state.begin(SESSION_DATE, instruments)
    for i, item in enumerate(instruments):
        base = 100.0 + i
        state.set_market_reference(item.security_id, previous_close=base - 1, today_open=base)
        state.live_candles[item.security_id].extend(_candles(base))
        state.last_ltp_by_security[item.security_id] = base + 0.5
    return state


def _index_manager() -> IndexLayerManager:
    manager = IndexLayerManager.__new__(IndexLayerManager)
    manager.settings = SETTINGS
    manager.resolution_errors = {}
    manager.states = {}
    for n, route in enumerate(app_module.INDEX_ROUTES):
        state = IndexState(SETTINGS, route, route.upper(), IndexInstrument(str(13 + n), "IDX_I"))
        state.begin(SESSION_DATE)
        state.live_candles = _candles(20000.0 + n)
        state.historical = {tf: _candles(20000.0 + n)[:5] for tf in ("5m", "15m", "1h")}
        state.last_ltp = 20000.5 + n
        state.last_ltt = EPOCH_0915 + 60 * CANDLES_PER_INSTRUMENT
        manager.states[route] = state
    return manager


def _option_chain(base: float) -> dict:
    return {
        "last_price": base,
        "oc": {
            str(base + 50 * k): {
                "ce": {"security_id": 5000 + k, "last_price": 100.0 - k, "oi": 1000 + k, "volume": 10 + k},
                "pe": {"security_id": 6000 + k, "last_price": 90.0 + k, "oi": 900 + k, "volume": 20 + k},
            }
            for k in range(-5, 6)
        },
    }


def _derivatives(api) -> dict:
    managers = {}
    for spec in INDEX_DERIVATIVES:
        options = IndexOptionsManager(SETTINGS, api, spec)
        options.state.set_snapshot(_option_chain(25000.0), ["2026-10-07", "2026-10-14"], "2026-10-07")
        depth = IndexDepthManager(SETTINGS, api, options, spec)
        contract = DepthContract("5000", 25000.0, "CE", "2026-10-07")
        depth.state.set_contracts([contract], "2026-10-07")
        depth.state.set_underlying_ltp(25000.0)
        level = [{"level": 1, "price": 100.0, "quantity": 50, "orders": 2}]
        depth.state.update_depth("5000", "bid", level)
        depth.state.update_depth("5000", "ask", [{"level": 1, "price": 101.0, "quantity": 40, "orders": 1}])
        depth.state.update_quotes({"5000": {"last_price": 100.5, "volume": 10, "oi": 1000, "ohlc": {"open": 99.0}}})
        managers[f"{spec.key}_options_manager"] = options
        managers[f"{spec.key}_depth_manager"] = depth
    return managers


def _futures(symbol: str, security_id: str) -> SimpleNamespace:
    state = FuturesState(symbol, SETTINGS)
    state.set_contract(
        FuturesContract(
            symbol=symbol,
            security_id=security_id,
            exchange_segment="NSE_FNO",
            instrument="FUTIDX",
            trading_symbol=f"{symbol}-FUT",
            expiry_date="2026-10-27",
            lot_size=75,
            tick_size=0.05,
        )
    )
    state.set_quote(
        {
            "last_price": 25100.0,
            "volume": 1000,
            "oi": 500000,
            "ohlc": {"open": 25000.0, "high": 25200.0, "low": 24950.0, "close": 25050.0},
        }
    )
    return SimpleNamespace(state=state)


def _stock_derivatives(api) -> tuple[StockOptionsManager, StockDepthManager]:
    from stock_options import NIFTY50_SYMBOLS

    # Every tenth symbol stays unresolved so the listing carries both shapes.
    resolved = {s: str(200000 + i) for i, s in enumerate(NIFTY50_SYMBOLS) if i % 10 or s == SAMPLE_SYMBOL}
    with patch("stock_options.fetch_nse_equity_security_ids", return_value=resolved):
        options = StockOptionsManager(SETTINGS, api)
    for symbol in options.instruments:
        options.states[symbol].set_snapshot(_option_chain(1000.0), ["2026-10-28"], "2026-10-28")
    depth = StockDepthManager(SETTINGS, api, options)
    state = depth.states[SAMPLE_SYMBOL]
    state.set_contracts([StockDepthContract("7000", SAMPLE_SYMBOL, 1000.0, "CE", "2026-10-28")], "2026-10-28")
    state.update_depth("7000", "bid", [{"level": 1, "price": 10.0, "quantity": 5, "orders": 1}])
    state.update_depth("7000", "ask", [{"level": 1, "price": 10.5, "quantity": 4, "orders": 1}])
    state.set_rotation_status("ACTIVE")
    return options, depth


def _underlying(symbol: str, candles: list[dict]) -> UnderlyingIndicatorRuntime:
    runtime = UnderlyingIndicatorRuntime(symbol, lambda: candles, SETTINGS)
    runtime._sync_once()
    return runtime


def install(monkeypatch) -> None:
    """Replace every manager global in ``app`` with fixed, fully populated state."""
    api = SimpleNamespace(settings=SETTINGS)
    equity = _equity_state()
    indicator_runtime = IndicatorRuntime(equity, market_live_json)
    indicator_runtime._sync_once()
    index_manager = _index_manager()
    stock_options, stock_depth = _stock_derivatives(api)
    midcp = MidcapNiftyUnderlyingState(SETTINGS)
    midcp.merge_candles([{**c, "source": "DHAN_HISTORICAL_API"} for c in _candles(12000.0)])
    context = GlobalContextState(SETTINGS)
    context.set_series(
        {"sp500": {"series_id": "SP500", "value": 6500.0, "source_date": "2026-09-29", "source": "FRED"}}
    )
    rbi = RbiNewsState(SETTINGS)
    rbi.set_items(
        [{"id": "1", "headline": "Press release", "published": "2026-09-30", "link": "https://rbi.org.in/x"}], {}
    )

    values = {
        "settings": SETTINGS,
        "state": equity,
        "config_error": "",
        "indicator_error": "",
        "index_error": "",
        "indicator_runtime": indicator_runtime,
        "index_manager": index_manager,
        "nifty_futures_manager": _futures("NIFTY", "49081"),
        "banknifty_futures_manager": _futures("BANKNIFTY", "49082"),
        "sensex_futures_manager": _futures("SENSEX", "49083"),
        "stock_options_manager": stock_options,
        "stock_depth_manager": stock_depth,
        "midcpnifty_underlying_manager": SimpleNamespace(state=midcp),
        "global_context_manager": SimpleNamespace(state=context),
        "rbi_news_manager": SimpleNamespace(state=rbi),
        "archive_manager": None,
        **_derivatives(api),
    }
    for symbol in ("nifty", "banknifty", "sensex"):
        values[f"{symbol}_underlying_indicators"] = _underlying(symbol.upper(), index_manager.snapshot(symbol)["1m"])
    values["midcpnifty_underlying_indicators"] = _underlying("MIDCPNIFTY", midcp.snapshot()["candles_1m"])
    for name, value in values.items():
        assert hasattr(app_module, name), name
        monkeypatch.setattr(app_module, name, value)
    # /health judges feed freshness only inside the equity session window; pin the window
    # closed so the pinned shape does not depend on when the suite runs.
    monkeypatch.setattr(app_module, "_in_equity_session", lambda _now: False)
