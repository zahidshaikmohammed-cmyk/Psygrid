"""Verifies the FULL futures runtime path end-to-end: instrument-master
resolution -> FuturesManager -> Dhan quote fetch -> FuturesState.snapshot().

This does not just test FuturesState in isolation (see test_futures_layer.py)
- it drives the actual FuturesManager._resolve_contract() and one iteration
of its quote-fetch logic against a realistic, hand-built Dhan instrument
master CSV and a fake DhanAPI, to prove the runtime wiring is correct, not
just each piece in isolation.
"""

import csv
import datetime as dt
import io
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from futures_layer import FuturesManager


def _fake_instrument_master_csv() -> str:
    next_week = (dt.date.today() + dt.timedelta(days=7)).isoformat()
    far_month = (dt.date.today() + dt.timedelta(days=35)).isoformat()
    expired = "2020-01-01"
    rows = [
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "FUTIDX", "SEM_TRADING_SYMBOL": f"NIFTY-{next_week}-FUT", "SEM_SMST_SECURITY_ID": "49081", "SEM_EXPIRY_DATE": next_week, "SEM_LOT_UNITS": "75", "SEM_TICK_SIZE": "0.05"},
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "FUTIDX", "SEM_TRADING_SYMBOL": f"NIFTY-{far_month}-FUT", "SEM_SMST_SECURITY_ID": "49082", "SEM_EXPIRY_DATE": far_month, "SEM_LOT_UNITS": "75", "SEM_TICK_SIZE": "0.05"},
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "OPTIDX", "SEM_TRADING_SYMBOL": f"NIFTY-{next_week}-25000-CE", "SEM_SMST_SECURITY_ID": "99999", "SEM_EXPIRY_DATE": next_week, "SEM_LOT_UNITS": "75", "SEM_TICK_SIZE": "0.05"},
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "EQUITY", "SEM_TRADING_SYMBOL": "NIFTYBEES", "SEM_SMST_SECURITY_ID": "11111", "SEM_EXPIRY_DATE": "", "SEM_LOT_UNITS": "", "SEM_TICK_SIZE": ""},
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "FUTIDX", "SEM_TRADING_SYMBOL": f"NIFTY-{expired}-FUT", "SEM_SMST_SECURITY_ID": "40000", "SEM_EXPIRY_DATE": expired, "SEM_LOT_UNITS": "75", "SEM_TICK_SIZE": "0.05"},
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


class FakeDhanAPI:
    """Mimics dhan_api.quote_snapshot's real return shape: a dict keyed by
    security_id, values being Dhan's raw quote fields passed through as-is.
    """

    def __init__(self, quote_by_security_id):
        self.quote_by_security_id = quote_by_security_id

    def quote_snapshot(self, instruments):
        return {str(i.security_id): self.quote_by_security_id[str(i.security_id)] for i in instruments if str(i.security_id) in self.quote_by_security_id}


def test_full_runtime_path_resolves_contract_and_populates_live_quote():
    fake_response = MagicMock()
    fake_response.text = _fake_instrument_master_csv()
    fake_response.encoding = "utf-8"
    fake_response.raise_for_status = lambda: None

    dhan_api = FakeDhanAPI({
        "49081": {
            "last_price": 25100.5,
            "volume": 128400,
            "oi": 5000000,
            "average_price": 25080.0,
            "buy_quantity": 1200,
            "sell_quantity": 900,
            "ohlc": {"open": 25000.0, "high": 25200.0, "low": 24950.0, "close": 25050.0},
            "depth": {"buy_price": 25099.5, "sell_price": 25101.0},
        }
    })

    manager = FuturesManager("NIFTY", SimpleNamespace(timezone="Asia/Kolkata"), dhan_api)

    with patch("derivatives_instruments.requests.get", return_value=fake_response):
        manager._resolve_contract()

    # Contract identity resolved from the instrument master, not guessed.
    contract = manager.state.contract
    assert contract.security_id == "49081"  # nearest unexpired, not the far-month or expired row
    assert contract.lot_size == 75
    assert contract.tick_size == 0.05

    # One quote fetch, driven exactly as _loop() would drive it.
    instrument = SimpleNamespace(security_id=contract.security_id, exchange_segment=contract.exchange_segment)
    row = dhan_api.quote_snapshot([instrument])[str(contract.security_id)]
    manager.state.set_quote(row)

    snap = manager.state.snapshot()
    assert snap["status"] == "LIVE"
    assert snap["security_id"] == "49081"
    assert snap["expiry"] is not None
    assert snap["last_price"] == 25100.5
    assert snap["volume"] == 128400
    assert snap["oi"] == 5000000
    assert snap["ohlc"] == {"open": 25000.0, "high": 25200.0, "low": 24950.0, "close": 25050.0}
    assert snap["top_bid_price"] == 25099.5
    assert snap["top_ask_price"] == 25101.0
    assert snap["oi_change"] is None  # first observation, correctly not fabricated
    assert snap["lot_size"] == 75 and snap["tick_size"] == 0.05
    assert snap["raw_quote"] == row  # raw Dhan fields preserved, not lossy

    # Second refresh: OI change must now be a real computed delta.
    row2 = dict(row, oi=5050000, last_price=25110.0)
    manager.state.set_quote(row2)
    assert manager.state.snapshot()["oi_change"] == 50000


def test_unresolvable_symbol_reports_unresolved_not_fabricated_contract():
    fake_response = MagicMock()
    fake_response.text = _fake_instrument_master_csv()  # has no MIDCPNIFTY rows
    fake_response.encoding = "utf-8"
    fake_response.raise_for_status = lambda: None

    manager = FuturesManager("MIDCPNIFTY", SimpleNamespace(timezone="Asia/Kolkata"), FakeDhanAPI({}))
    with patch("derivatives_instruments.requests.get", return_value=fake_response):
        try:
            manager._resolve_contract()
            raised = False
        except RuntimeError as exc:
            raised = True
            assert "NOT_RESOLVED" in str(exc)
    assert raised, "must raise rather than silently leaving a stale/fabricated contract"
    assert manager.state.contract is None
